import asyncio
import functools
import importlib
import logging
import sys
import threading
import typing as tp
from abc import ABC, abstractmethod
from types import ModuleType

import betterproto

from protobunny.exceptions import RequeueMessage
from protobunny.models import (
    AsyncCallback,
    IncomingMessageProtocol,
    LoggerCallback,
    ProtoBunnyMessage,
    SyncCallback,
    Topic,
    configuration,
    get_message_class_from_topic,
)

log = logging.getLogger(__name__)


def get_backend(backend: str | None = None) -> ModuleType:
    """
    Retrieve and import the specified backend module.

    Load the backend module based on the provided name or falls back
    to the default backend specified in the configuration. If the backend is unavailable
    or cannot be imported, it exits the program.

    Args:
        backend (str | None): The name of the backend to import. If None, the backend from
            the configuration is used.

    Returns:
        The imported backend module.
    """
    backend = backend or configuration.backend
    try:
        module = importlib.import_module(f"protobunny.backends.{backend}")
    except ModuleNotFoundError as exc:
        suggestion = ""
        if backend not in configuration.available_backends:
            suggestion = f" Invalid backend or backend not supported.\nAvailable backends: {configuration.available_backends}"
        else:
            suggestion = (
                f" Install the backend with pip install protobunny[{backend}]."
                if backend != "python"
                else suggestion
            )
        sys.exit(f"Could not import backend: {exc}.{suggestion}")
    return module


class BaseConnection(ABC):
    @abstractmethod
    def __init__(self, *args, **kwargs):
        ...

    @abstractmethod
    def publish(
        self,
        topic: str,
        message: "IncomingMessageProtocol",
        mandatory: bool = True,
        immediate: bool = False,
    ) -> None | tp.Awaitable[None]:
        ...

    @abstractmethod
    def disconnect(self, timeout: float = 30) -> None | tp.Awaitable[None]:
        ...

    @classmethod
    @abstractmethod
    def get_connection(cls, vhost: str = "") -> tp.Any | tp.Awaitable[tp.Any]:
        ...

    @abstractmethod
    def is_connected(self) -> bool | tp.Awaitable[bool]:
        ...

    @abstractmethod
    def connect(self, timeout: float = 30) -> None | tp.Awaitable[None]:
        ...

    @abstractmethod
    def subscribe(
        self, topic: str, callback: SyncCallback, shared: bool = False
    ) -> str | tp.Awaitable[str]:
        ...

    @abstractmethod
    def unsubscribe(self, topic: str, **kwargs) -> None | tp.Awaitable[None]:
        ...

    @abstractmethod
    def purge(self, topic: str, **kwargs) -> None | tp.Awaitable[None]:
        ...

    @abstractmethod
    def get_message_count(self, topic: str) -> int | tp.Awaitable[int]:
        ...

    @abstractmethod
    def get_consumer_count(self, topic: str) -> int | tp.Awaitable[int]:
        ...

    @abstractmethod
    def setup_queue(self, topic: str, shared: bool) -> tp.Any | tp.Awaitable[tp.Any]:
        ...


class BaseAsyncConnection(BaseConnection, ABC):
    instance_by_vhost: dict[str, "BaseAsyncConnection"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.vhost = kwargs.get("vhost", "")


class BaseSyncConnection(BaseConnection, ABC):
    _lock: threading.RLock
    _stopped: asyncio.Event | None
    _instance_by_vhost: dict[str, "BaseSyncConnection"]
    async_class: tp.Type["BaseAsyncConnection | None"]

    @abstractmethod
    def get_async_connection(self, **kwargs) -> "BaseAsyncConnection":
        ...

    def __init__(self, **kwargs):
        """Initialize sync connection.

        Args:
            **kwargs: Same arguments as AsyncConnection
        """
        super().__init__()
        self._async_conn = self.get_async_connection(**kwargs)
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ready = threading.Event()
        self._stopped: asyncio.Event | None = None
        self.vhost = self._async_conn.vhost
        self._started = False

    def _run_loop(self) -> None:
        """Run the event loop in a dedicated thread."""
        loop = None
        try:
            # Create a fresh loop for this specific thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop

            # Run the 'stop' watcher
            loop.create_task(self._async_run_watcher())

            # Signal readiness NOW that self._loop is assigned and running
            self._ready.set()
            loop.run_forever()
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("Event loop thread crashed")
        finally:
            if loop:
                loop.close()
            self._loop = None
            log.info("Event loop thread stopped")

    async def _async_run_watcher(self) -> None:
        """Wait for the stop signal inside the loop."""
        self._stopped = asyncio.Event()
        await self._stopped.wait()
        asyncio.get_running_loop().stop()

    async def _async_run(self) -> None:
        """Async event loop runner."""
        self._loop = asyncio.get_running_loop()
        self._stopped = asyncio.Event()
        self._loop.call_soon_threadsafe(self._ready.set)
        await self._stopped.wait()

    def _ensure_loop(self) -> None:
        """Ensure event loop thread is running.

        Raises:
            ConnectionError: If event loop fails to start
        """
        # Check if the thread exists AND is actually running
        if self._thread and self._thread.is_alive() and self._loop and self._loop.is_running():
            return

        log.info("Starting (or restarting) wrapping event loop thread")

        # Reset state for a fresh start
        self._ready.clear()
        self._loop = None

        self._thread = threading.Thread(
            target=self._run_loop, name="protobunny_event_loop", daemon=True
        )
        self._thread.start()

        if not self._ready.wait(timeout=10.0):
            # Cleanup on failure to prevent stale state for next attempt
            self._thread = None
            raise ConnectionError("Event loop thread failed to start or signal readiness")

    def _run_coro(self, coro, timeout: float | None = None):
        """Run a coroutine in the event loop thread and return result.

        Args:
            coro: The coroutine to run
            timeout: Maximum time to wait for result (seconds)

        Returns:
            The coroutine result

        Raises:
            TimeoutError: If operation times out
            ConnectionError: If event loop is not available
        """
        self._ensure_loop()
        if self._loop is None:
            raise ConnectionError("Event loop not initialized")

        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return future.result(timeout=timeout)
        except TimeoutError:
            future.cancel()
            raise

    def is_connected(self) -> bool:
        """Check if connection is established."""
        if not self._loop or not self._loop.is_running():
            return False
        return self._run_coro(self._async_conn.is_connected())

    @classmethod
    def get_connection(cls, vhost: str = "") -> "BaseSyncConnection":
        """Get singleton instance (sync)."""
        with cls._lock:
            if not cls._instance_by_vhost.get(vhost):
                cls._instance_by_vhost[vhost] = cls(vhost=vhost)
            if not cls._instance_by_vhost[vhost].is_connected():
                cls._instance_by_vhost[vhost].connect()
            log.info("Returning singleton SyncConnection instance for vhost %s", vhost)
            return cls._instance_by_vhost[vhost]

    def publish(
        self,
        topic: str,
        message: "IncomingMessageProtocol",
        mandatory: bool = False,
        immediate: bool = False,
        timeout: float = 10.0,
    ) -> None:
        """Publish a message to a topic.

        Args:
            topic: The routing key/topic
            message: The message to publish
            mandatory: If True, raise error if message cannot be routed
            immediate: If True, publish message immediately to the queue
            timeout: Maximum time to wait for publish (seconds)

        Raises:
            ConnectionError: If not connected
            TimeoutError: If operation times out
        """
        self._run_coro(
            self._async_conn.publish(topic, message, mandatory, immediate), timeout=timeout
        )

    def setup_queue(self, topic: str, shared: bool, timeout: int = 10) -> tp.Any:
        self._run_coro(self._async_conn.setup_queue(topic, shared), timeout=timeout)

    def subscribe(
        self, topic: str, callback: tp.Callable, shared: bool = False, timeout: float = 10.0
    ) -> str:
        """Subscribe to a queue/topic.

        Args:
            topic: The routing key/topic to subscribe to
            callback: Function to handle incoming messages
            shared: if True, use shared queue (round-robin delivery)
            timeout: Maximum time to wait for subscription (seconds)

        Returns:
            Subscription tag identifier

        Raises:
            ConnectionError: If not connected
            TimeoutError: If operation times out
        """
        return self._run_coro(self._async_conn.subscribe(topic, callback, shared), timeout=timeout)

    def unsubscribe(
        self, tag: str, timeout: float = 10.0, if_unused: bool = True, if_empty: bool = True
    ) -> None:
        """Unsubscribe from a queue.

        Args:
            if_unused:
            if_empty:
            tag: Subscription identifier returned from subscribe()
            timeout: Maximum time to wait (seconds)

        Raises:
            TimeoutError: If operation times out
        """
        self._run_coro(
            self._async_conn.unsubscribe(tag, if_empty=if_empty, if_unused=if_unused),
            timeout=timeout,
        )

    def purge(self, topic: str, timeout: float = 10.0, **kwargs) -> None:
        """Empty a queue of all messages.

        Args:
            topic: The queue topic to purge
            timeout: Maximum time to wait (seconds)

        Raises:
            ConnectionError: If not connected
            TimeoutError: If operation times out
        """
        self._run_coro(self._async_conn.purge(topic, **kwargs), timeout=timeout)

    def get_message_count(self, topic: str, timeout: float = 10.0) -> int:
        """Get the number of messages in a queue.

        Args:
            topic: The queue topic
            timeout: Maximum time to wait (seconds)

        Returns:
            Number of messages in the queue

        Raises:
            ConnectionError: If not connected
            TimeoutError: If operation times out
        """
        return self._run_coro(self._async_conn.get_message_count(topic), timeout=timeout)

    def get_consumer_count(self, topic: str, timeout: float = 10.0) -> int:
        """Get the number of messages in a queue.

        Args:
            topic: The queue topic
            timeout: Maximum time to wait (seconds)

        Returns:
            Number of messages in the queue

        Raises:
            ConnectionError: If not connected
            TimeoutError: If operation times out
        """
        return self._run_coro(self._async_conn.get_consumer_count(topic), timeout=timeout)

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        return False

    def connect(self, timeout: float = 10.0) -> None:
        """Establish Sync connection.

        Args:
            timeout: Maximum time to wait for connection (seconds)

        Raises:
            ConnectionError: If connection fails
            TimeoutError: If connection times out
        """
        self._run_coro(self._async_conn.connect(timeout), timeout=timeout)
        self.__class__._instance_by_vhost[self.vhost] = self

    def disconnect(self, timeout: float = 10.0) -> None:
        """Close sync and the underlying async connections and stop event loop.

        Args:
            timeout: Maximum time to wait for cleanup (seconds)
        """
        with self._lock:
            try:
                if self._loop and self._loop.is_running():
                    self._run_coro(self._async_conn.disconnect(timeout), timeout=timeout)
                # Stop the loop (see _async_run_watcher)
                if self._stopped and self._loop:
                    self._loop.call_soon_threadsafe(self._stopped.set)
            except Exception as e:
                log.warning("Async disconnect failed during sync shutdown: %s", e)
            finally:
                if self._thread and self._thread.is_alive():
                    self._thread.join(timeout=5.0)
                    if self._thread.is_alive():
                        log.warning("Event loop thread did not stop within timeout")
                self._started = None
                self._loop = None
                self._thread = None
                self.async_class.instance_by_vhost.pop(self.vhost, None)
            type(self)._instance_by_vhost.pop(self.vhost, None)


class BaseQueue(ABC):
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return False
        return self.topic == other.topic and self.shared_queue == other.shared_queue

    def __str__(self):
        return f"{self.__class__.__name__}({self.topic})"

    def __init__(self, topic: "Topic"):
        """Initialize Queue.

        Args:
            topic: a Topic value object
        """
        self.topic: str = topic.name
        self.shared_queue: bool = topic.is_task_queue
        self.subscription: str | None = None
        self.result_subscription: str | None = None

    @property
    def result_topic(self) -> str:
        return f"{self.topic}.result"

    @abstractmethod
    def get_tag(self) -> str:
        ...

    @abstractmethod
    def publish(self, message: "ProtoBunnyMessage") -> None:
        ...

    @abstractmethod
    def subscribe(self, callback: "SyncCallback | LoggerCallback") -> None:
        ...

    @abstractmethod
    def unsubscribe(self, if_unused: bool = True, if_empty: bool = True) -> None:
        ...

    @abstractmethod
    def purge(self) -> None:
        ...

    @abstractmethod
    def get_message_count(self) -> int:
        ...

    @abstractmethod
    def get_consumer_count(self) -> int:
        ...

    @staticmethod
    @abstractmethod
    def send_message(
        topic: str, content: bytes, correlation_id: str | None, persistent: bool = False
    ) -> None | tp.Awaitable[None]:
        ...

    @abstractmethod
    def publish_result(
        self, result: "Result", topic: str | None = None, correlation_id: str | None = None
    ):
        ...

    @abstractmethod
    def subscribe_results(self, callback: tp.Callable[["Result"], tp.Any]):
        ...

    @abstractmethod
    def unsubscribe_results(self):
        ...

    def get_connection_sync(self) -> BaseConnection:
        backend = get_backend()
        return backend.connection.get_connection_sync()

    async def get_connection(self) -> BaseConnection:
        backend = get_backend()
        return await backend.connection.get_connection()


class BaseAsyncQueue(BaseQueue, ABC):
    async def publish(self, message: ProtoBunnyMessage) -> None:
        """Publish a message to the queue.

        Args:
            message: a protobuf message
        """
        await self.send_message(self.topic, bytes(message))

    async def _receive(
        self, callback: "AsyncCallback | LoggerCallback", message: IncomingMessageProtocol
    ) -> None:
        """Handle a message from the queue.

        Args:
            callback: a callable accepting a message as only argument.
            message: the IncomingMessageProtocol object received from the queue.
        """

        if not message.routing_key:
            raise ValueError("Routing key was not set. Invalid topic")
        if message.routing_key == self.result_topic or message.routing_key.endswith(".result"):
            # Skip a result message. Handling result messages happens in `_receive_results` method.
            # In case the subscription has .# as binding key,
            # this method catches also results message for all the topics in that namespace.
            return

        msg: "ProtoBunnyMessage" = deserialize_message(message.routing_key, message.body)
        try:
            await callback(msg)
        except RequeueMessage:
            raise
        except Exception as exc:  # pylint: disable=W0703
            log.exception("Could not process message: %s", str(message.body))
            result = msg.make_result(return_code=ReturnCode.FAILURE, error=str(exc))
            await self.publish_result(
                result, topic=msg.result_topic, correlation_id=message.correlation_id
            )

    async def subscribe(self, callback: "AsyncCallback | LoggerCallback") -> None:
        """Subscribe to messages from the queue.

        Args:
            callback: The user async callback to call when a message is received.
              The callback should accept a single argument of type `ProtoBunnyMessage`.

        Note: The real callback that consumes the incoming aio-pika message is the method AsyncConnection._on_message
        The AsyncQueue._receive method is called from there to deserialize the message and in turn calls the user callback.
        """
        if self.subscription is not None:
            raise ValueError("Cannot subscribe twice")
        func = functools.partial(self._receive, callback)
        conn = await self.get_connection()
        self.subscription = await conn.subscribe(self.topic, func, shared=self.shared_queue)

    async def unsubscribe(self, if_unused: bool = True, if_empty: bool = True) -> None:
        """Unsubscribe from the queue."""
        if self.subscription is not None:
            conn = await self.get_connection()
            await conn.unsubscribe(self.get_tag(), if_unused=if_unused, if_empty=if_empty)
            self.subscription = None

    async def publish_result(
        self,
        result: "Result",
        topic: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Publish a message to the results topic.

        Args:
            result: a amlogic_messages.results.Result message
            topic:
            correlation_id:
        """
        result_topic = topic or self.result_topic
        log.info("Publishing result to: %s", result_topic)
        await self.send_message(
            result_topic, bytes(result), correlation_id=correlation_id, persistent=False
        )

    async def _receive_result(
        self,
        callback: "AsyncCallback",
        message: IncomingMessageProtocol,
    ) -> None:
        """Handle a message from the queue.

        Args:
            callback : function to call with deserialized result.
                Accept parameters like (message: Message, return_code: int, return_value: dict, error:str)
            message : `IncomingMessageProtocol` serialized message from the queue.
        """
        try:
            result = deserialize_result_message(message.body)
            # `result.source_message` is a protobuf.Any instance.
            # It has `type_url` property that describes the type of message.
            # To reconstruct the source message you can  do it by using the Result.source property or
            # base methods.
            # >>> source_message = result.source
            # or more explicitly
            # >> message_type = get_message_class_from_type_url(result.source_message.type_url)
            # >> source_message = message_type().parse(result.source_message.value)
            await callback(result)
        except Exception:
            log.exception("Could not process result: %s", str(message.body))

    async def subscribe_results(self, callback: "AsyncCallback") -> None:
        """Subscribe to results from the queue.

        See the deserialize_result method for return params.

        Args:
            callback : function to call when results come in.
        """
        if self.result_subscription is not None:
            raise ValueError("Can not subscribe to results twice")
        func = functools.partial(self._receive_result, callback)
        conn = await self.get_connection()
        self.result_subscription = await conn.subscribe(self.result_topic, func, shared=False)

    async def unsubscribe_results(self) -> None:
        """Unsubscribe from results. Will always delete the underlying queues"""
        if self.result_subscription is not None:
            conn = await self.get_connection()
            await conn.unsubscribe(self.result_subscription, if_unused=False, if_empty=False)
            self.result_subscription = None

    async def purge(self) -> None:
        """Delete all messages from the queue."""
        if not self.shared_queue:
            raise RuntimeError("Can only purge shared queues")
        conn = await self.get_connection()
        await conn.purge(self.topic)

    async def get_message_count(self) -> int | None:
        """Get current message count."""
        if not self.shared_queue:
            raise RuntimeError("Can only get count of shared queues")
        conn = await self.get_connection()
        return await conn.get_message_count(self.topic)

    async def get_consumer_count(self) -> int | None:
        """Get current message count."""
        if not self.shared_queue:
            raise RuntimeError("Can only get count of shared queues")
        conn = await self.get_connection()
        return await conn.get_consumer_count(self.topic)


class BaseSyncQueue(BaseQueue, ABC):
    def publish(self, message: "ProtoBunnyMessage") -> None:
        """Publish a message to the queue.

        Args:
            message: a ProtoBunnyMessage message
        """
        self.send_message(self.topic, bytes(message))

    def publish_result(
        self,
        result: "Result",
        topic: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Publish a message to the results topic.

        Args:
            result: a amlogic_messages.results.Result message
            topic:
            correlation_id:
        """
        result_topic = topic or self.result_topic
        log.info("Publishing result to: %s", result_topic)
        self.send_message(
            result_topic, bytes(result), correlation_id=correlation_id, persistent=False
        )

    def _receive(
        self, callback: "SyncCallback | LoggerCallback", message: "IncomingMessageProtocol"
    ) -> None:
        """Handle a message from the queue.

        Args:
            callback: a callable accepting a message as only argument.
            message: the aio_pika.IncomingMessage object received from the queue.
        """
        if not message.routing_key:
            raise ValueError("Routing key was not set. Invalid topic")
        if message.routing_key == self.result_topic or message.routing_key.endswith(".result"):
            # Skip a result message. Handling result messages happens in `_receive_results` method.
            # In case the subscription has .# as binding key,
            # this method catches also results message for all the topics in that namespace.
            return
        msg: "ProtoBunnyMessage" = deserialize_message(message.routing_key, message.body)
        try:
            callback(msg)
        except RequeueMessage:
            raise
        except Exception as exc:  # pylint: disable=W0703
            log.exception("Could not process message: %s", str(message.body))
            result = msg.make_result(return_code=ReturnCode.FAILURE, error=str(exc))
            self.publish_result(
                result, topic=msg.result_topic, correlation_id=message.correlation_id
            )

    def subscribe(self, callback: "SyncCallback | LoggerCallback") -> None:
        """Subscribe to messages from the queue.

        Args:
            callback:

        """
        if self.subscription is not None:
            raise ValueError("Cannot subscribe twice")
        func = functools.partial(self._receive, callback)
        self.subscription = self.get_connection_sync().subscribe(
            self.topic, func, shared=self.shared_queue
        )

    def unsubscribe(self, if_unused: bool = True, if_empty: bool = True) -> None:
        """Unsubscribe from the queue."""
        if self.subscription is not None:
            self.get_connection_sync().unsubscribe(
                self.get_tag(), if_unused=if_unused, if_empty=if_empty
            )
            self.subscription = None

    def _receive_result(
        self,
        callback: tp.Callable[["Result"], tp.Any],
        message: IncomingMessageProtocol,
    ) -> None:
        """Handle a message from the queue.

        Args:
            callback : function to call with deserialized result.
                Accept parameters like (message: Message, return_code: int, return_value: dict, error:str)
            message : an `aio_pika.IncomingMessage` or Envelope serialized message from the queue.
        """
        try:
            result = deserialize_result_message(message.body)
            # `result.source_message` is a protobuf.Any instance.
            # It has `type_url` property that describes the type of message.
            # To reconstruct the source message you can  do it by using the Result.source property or
            # base methods.
            # >>> source_message = result.source
            # or more explicitly
            # >> message_type = get_message_class_from_type_url(result.source_message.type_url)
            # >> source_message = message_type().parse(result.source_message.value)
            callback(result)
        except Exception:  # pylint: disable=W0703
            log.exception("Could not process result: %s", str(message.body))

    def subscribe_results(self, callback: tp.Callable[["Result"], tp.Any]) -> None:
        """Subscribe to results from the queue.

        See the deserialize_result method for return params.

        Args:
            callback : function to call when results come in.
        """
        if self.result_subscription is not None:
            raise ValueError("Can not subscribe to results twice")
        func = functools.partial(self._receive_result, callback)
        self.result_subscription = self.get_connection_sync().subscribe(
            self.result_topic, func, shared=False
        )

    def unsubscribe_results(self) -> None:
        """Unsubscribe from results. Will always delete the underlying queues"""
        if self.result_subscription is not None:
            self.get_connection_sync().unsubscribe(
                self.result_subscription, if_unused=False, if_empty=False
            )
            self.result_subscription = None

    def purge(self) -> None:
        """Delete all messages from the queue."""
        if not self.shared_queue:
            raise RuntimeError("Can only purge shared queues")
        self.get_connection_sync().purge(self.topic)

    def get_message_count(self) -> int:
        """Get current message count."""
        if not self.shared_queue:
            raise RuntimeError("Can only get count of shared queues")
        log.debug("Getting queue message count")
        return self.get_connection_sync().get_message_count(self.topic)

    def get_consumer_count(self) -> int:
        """Get current message count."""
        if not self.shared_queue:
            raise RuntimeError("Can only get count of shared queues")
        log.debug("Getting queue message count")
        return self.get_connection_sync().get_consumer_count(self.topic)


def deserialize_message(topic: str | None, body: bytes) -> "ProtoBunnyMessage | None":
    """Deserialize the body of a serialized pika message.

    Args:
        topic: str. The topic. It's used to determine the type of message.
        body: bytes. The serialized message

    Returns:
        A deserialized message.
    """
    if not topic:
        raise ValueError("Routing key was not set. Invalid topic")
    message_type: type["ProtoBunnyMessage"] = get_message_class_from_topic(topic)
    return message_type().parse(body) if message_type else None


def deserialize_result_message(body: bytes) -> "Result":
    """Deserialize the result message.

    Args:
        body: bytes. The serialized protobunny.core.results.Result

    Returns:
        Instance of Result
    """
    return tp.cast(Result, Result().parse(body))


def get_body(message: "IncomingMessageProtocol") -> str:
    """Get the json string representation of the message body to use for the logger service.
    If message couldn't be parsed, it returns the raw content.
    """
    msg: ProtoBunnyMessage | None
    body: str | bytes
    if message.routing_key and message.routing_key.endswith(".result"):
        # log result message. Need to extract the source here
        result = deserialize_result_message(message.body)
        # original message for which this result was generated
        msg = result.source
        return_code = ReturnCode(result.return_code).name
        # stringify to json
        source = msg.to_json(casing=betterproto.Casing.SNAKE, include_default_values=True)
        if result.return_code != ReturnCode.SUCCESS:
            body = f"{return_code} - error: [{result.error}] - {source}"
        else:
            body = f"{return_code} - {source}"
    else:
        msg = deserialize_message(message.routing_key, message.body)

        body = (
            msg.to_json(casing=betterproto.Casing.SNAKE, include_default_values=True)
            if msg is not None
            # can't parse the message - just log the raw content
            else message.body
        )
    return str(body)


class LoggingSyncQueue(BaseSyncQueue):
    """Represents a specialized queue for logging purposes.

    >>> import protobunny as pb
    >>> pb.subscribe_logger_sync()  # it uses the default logger_callback

    You can add a custom callback that accepts message: aio_pika.IncomingMessage, msg_content: str as arguments.

    >>> def log_callback(message: aio_pika.IncomingMessage, msg_content: str):
    >>>     print(message.body)
    >>> pb.subscribe_logger_sync(log_callback)

    You can use functools.partial to add more arguments

    >>> def log_callback_with_args(message: aio_pika.IncomingMessage, msg_content: str, maxlength: int):
    >>>     print(message.body[maxlength])
    >>> import functools
    >>> functools.partial(log_callback_with_args, maxlength=100)
    >>> pb.subscribe_logger(log_callback_with_args)
    """

    async def send_message(
        self, topic: str, content: bytes, correlation_id: str | None, persistent: bool = False
    ) -> None:
        # This queue is only for receiving messages, so it doesn't need to send messages'
        raise NotImplementedError()

    def __init__(self, prefix: str) -> None:
        super().__init__(Topic(f"{prefix}.#"))

    @property
    def result_topic(self) -> str:
        return ""

    def publish(self, message: "ProtoBunnyMessage") -> None:
        raise NotImplementedError

    def publish_result(
        self,
        result: "Result",
        topic: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        raise NotImplementedError

    def _receive(
        self,
        log_callback: "LoggerCallback",
        message: IncomingMessageProtocol,
    ):
        """Call the logging callback.

        Args:
            log_callback: The callback function passed to pb.subscribe_logger().
              It receives the aio_pika IncomingMessage as first argument and the string to log as second.

            message: the IncomingMessage
        """
        if message.routing_key is None:
            raise ValueError("Routing key was not set. Invalid topic")
        try:
            body = get_body(message)
            log_callback(message, body)
        except RequeueMessage:
            raise
        except Exception as exc:  # pylint: disable=W0703
            log.exception(
                "Could not process message on Logging queue: %s - %s", str(message.body), str(exc)
            )

    def get_tag(self) -> str:
        return self.topic


class LoggingAsyncQueue(BaseAsyncQueue):
    """Represents a specialized queue for logging purposes.

    >>> import protobunny as pb
    >>> async def add_logger():
    >>>     await pb.subscribe_logger()  # it uses the default logger_callback

    You can add a custom callback that accepts message: aio_pika.IncomingMessage, msg_content: str as arguments.
    Note that the callback must be sync even for the async logger and
    it must be a function who purely calls the logging module and can perform other non IO operations

    >>> def log_callback(message: aio_pika.IncomingMessage, msg_content: str):
    >>>     print(message.body)
    >>> async def add_logger():
    >>>     await pb.subscribe_logger(log_callback)

    You can use functools.partial to add more arguments

    >>> def log_callback_with_args(message: aio_pika.IncomingMessage, msg_content: str, maxlength: int):
    >>>     print(message.body[maxlength])
    >>> import functools
    >>> functools.partial(log_callback_with_args, maxlength=100)
    >>> async def add_logger():
    >>>     await pb.subscribe_logger(log_callback_with_args)
    """

    def get_tag(self) -> str:
        return self.topic

    async def send_message(
        self, topic: str, content: bytes, correlation_id: str | None, persistent: bool = False
    ) -> None:
        raise NotImplementedError()

    def __init__(self, prefix: str) -> None:
        super().__init__(Topic(f"{prefix}.#"))

    @property
    def result_topic(self) -> str:
        return ""

    async def publish(self, message: "ProtoBunnyMessage") -> None:
        raise NotImplementedError

    async def publish_result(
        self,
        result: "Result",
        topic: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        raise NotImplementedError

    async def _receive(
        self,
        log_callback: "LoggerCallback",  # the callback function for logging is always a sync function
        message: "IncomingMessageProtocols",
    ) -> None:
        """Call the logging callback.

        Args:
            log_callback: The callback function passed to pb.subscribe_logger().
              It receives the aio_pika IncomingMessage as first argument and the string to log as second.

            message: the aio_pika IncomingMessage
        """
        if message.routing_key is None:
            raise ValueError("Routing key was not set. Invalid topic")
        try:
            body = get_body(message)

            log_callback(message, body)
        except RequeueMessage:
            raise
        except Exception as exc:  # pylint: disable=W0703
            log.exception(
                "Could not process message on Logging queue: %s - %s", str(message.body), str(exc)
            )


# keep always the imports of generated code at the end of the file
from protobunny.core.results import Result, ReturnCode
