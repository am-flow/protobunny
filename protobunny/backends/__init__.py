import asyncio
import functools
import logging
import threading
import typing as tp
from abc import ABC, abstractmethod

from ..exceptions import RequeueMessage
from ..helpers import get_backend
from ..models import (
    BaseQueue,
    IncomingMessageProtocol,
    LoggerCallback,
    ProtoBunnyMessage,
    SyncCallback,
    default_configuration,
    deserialize_message,
    deserialize_result_message,
    get_body,
)

log = logging.getLogger(__name__)


class BaseConnection(ABC):
    username: str | None
    password: str | None
    host: str | None
    port: int | None
    vhost: str
    worker_threads: int
    prefetch_count: int
    requeue_delay: float
    exchange_name: str | None
    dl_exchange: str | None
    dl_queue: str | None
    heartbeat: int | None
    timeout: int | None
    url: str | None = None
    queues: dict[str, tp.Any] = {}

    @abstractmethod
    def __init__(self, *args, **kwargs):
        ...

    @abstractmethod
    def publish(
        self,
        topic: str,
        message: "IncomingMessageProtocol",
        **kwargs,
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
    instance_by_vhost: dict[str, "BaseSyncConnection"]
    async_class: tp.Type["BaseAsyncConnection | None"]

    def __init__(self, **kwargs):
        """Initialize sync connection.

        It's a wrapper around the async connection
        The Async connection is in _async_conn attribute

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
        self.instance_by_vhost = {}

    @abstractmethod
    def get_async_connection(self, **kwargs) -> "BaseAsyncConnection":
        ...

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
        return self._async_conn.is_connected()

    @classmethod
    def get_connection(cls, vhost: str = "") -> "BaseSyncConnection":
        """Get singleton instance (sync)."""
        with cls._lock:
            if not cls.instance_by_vhost.get(vhost):
                cls.instance_by_vhost[vhost] = cls(vhost=vhost)
            if not cls.instance_by_vhost[vhost].is_connected():
                cls.instance_by_vhost[vhost].connect()
            return cls.instance_by_vhost[vhost]

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
            self._async_conn.publish(topic, message, mandatory=mandatory, immediate=immediate),
            timeout=timeout,
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
        self.__class__.instance_by_vhost[self.vhost] = self

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
            type(self).instance_by_vhost.pop(self.vhost, None)


class BaseSyncQueue(BaseQueue, ABC):
    def get_connection(self) -> BaseConnection:
        backend = get_backend()
        return backend.connection.get_connection()

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
        delimiter = default_configuration.backend_config.topic_delimiter
        if message.routing_key == self.result_topic or message.routing_key.endswith(
            f"{delimiter}result"
        ):
            # Skip a result message. Handling result messages happens in `_receive_results` method.
            # In case the subscription has .# as binding key,
            # this method catches also results message for all the topics in that namespace.
            return
        # msg: "ProtoBunnyMessage" = deserialize_message(message.routing_key, message.body)
        try:
            callback(deserialize_message(message.routing_key, message.body))
        except RequeueMessage:
            raise
        except Exception as exc:  # pylint: disable=W0703
            log.exception("Could not process message: %s", str(message.body))
            msg = deserialize_message(message.routing_key, message.body)
            if not msg:
                log.warning(
                    "Could not deserialize with routing key: %s - not publishing a result",
                    str(message.routing_key),
                )
                return
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
            log.warning("Already subscribed...")
            return
        func = functools.partial(self._receive, callback)
        self.subscription = self.get_connection().subscribe(
            self.topic, func, shared=self.shared_queue
        )

    def unsubscribe(self, if_unused: bool = True, if_empty: bool = True) -> None:
        """Unsubscribe from the queue."""
        if self.subscription is not None:
            self.get_connection().unsubscribe(
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
            """
            `result.source_message` is a protobuf.Any instance.
            It has `type_url` property that describes the type of message.
            To reconstruct the source message you can  do it by using the Result.source property or
            base methods.
            >>> source_message = result.source
            or more explicitly
            >> message_type = get_message_class_from_type_url(result.source_message.type_url)
            >> source_message = message_type().parse(result.source_message.value)
            """
            callback(result)
        except Exception:  # pylint: disable=W0703
            log.exception("Could not process result: %s", str(message.body))

    def subscribe_results(self, callback: tp.Callable[["Result"], tp.Any]) -> None:
        """Subscribe to results from the queue.

        See the deserialize_result method for return params.

        Args:
            callback : function to call when results come in.
        """
        if self.subscription is not None:
            log.warning("Already subscribed...")
            return
        func = functools.partial(self._receive_result, callback)
        self.result_subscription = self.get_connection().subscribe(
            self.result_topic, func, shared=False
        )

    def unsubscribe_results(self) -> None:
        """Unsubscribe from results. Will always delete the underlying queues"""
        if self.result_subscription is not None:
            self.get_connection().unsubscribe(
                self.result_subscription, if_unused=False, if_empty=False
            )
            self.result_subscription = None

    def purge(self, **kwargs) -> None:
        """Delete all messages from the queue."""
        if not self.shared_queue:
            raise RuntimeError("Can only purge shared queues")
        self.get_connection().purge(self.topic, **kwargs)

    def get_message_count(self) -> int:
        """Get current message count."""
        if not self.shared_queue:
            raise RuntimeError("Can only get count of shared queues")
        log.debug("Getting queue message count")
        return self.get_connection().get_message_count(self.topic)

    def get_consumer_count(self) -> int:
        """Get current message count."""
        if not self.shared_queue:
            raise RuntimeError("Can only get count of shared queues")
        log.debug("Getting queue message count")
        return self.get_connection().get_consumer_count(self.topic)


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
        self,
        topic: str,
        content: bytes,
        correlation_id: str | None = None,
        persistent: bool = False,
    ) -> None:
        # This queue is only for receiving messages, so it doesn't need to send messages'
        raise NotImplementedError()

    def __init__(self, prefix: str) -> None:
        backend = default_configuration.backend_config
        delimiter = backend.topic_delimiter
        wildcard = backend.multi_wildcard_delimiter
        prefix = prefix or default_configuration.messages_prefix
        super().__init__(f"{prefix}{delimiter}{wildcard}")

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


def is_task(topic: str) -> bool:
    delimiter = default_configuration.backend_config.topic_delimiter
    return "tasks" in topic.split(delimiter)


# keep always the imports of generated code at the end of the file
from ..core.results import Result, ReturnCode
