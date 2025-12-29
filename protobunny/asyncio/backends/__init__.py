import asyncio
import functools
import logging
import typing as tp
from abc import ABC, abstractmethod

from ...exceptions import RequeueMessage
from ...helpers import get_backend
from ...models import (
    AsyncCallback,
    BaseQueue,
    Envelope,
    IncomingMessageProtocol,
    LoggerCallback,
    ProtoBunnyMessage,
    SyncCallback,
    default_configuration,
    deserialize_message,
    deserialize_result_message,
    get_body,
)

if tp.TYPE_CHECKING:
    # This only runs during IDE linting or Mypy runs
    from typing_extensions import Self
else:
    from typing import Any as Self

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
    def is_connected(self) -> bool:
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
    _lock: asyncio.Lock | None = None
    instance_by_vhost: dict[str, Self] = {}

    @abstractmethod
    async def connect(self, timeout: float = 30) -> None:
        ...

    @abstractmethod
    async def disconnect(self, timeout: float = 30) -> None:
        ...

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._is_connected_event: asyncio.Event | None = None
        self.vhost = kwargs.get("vhost", "")
        self._loop: asyncio.AbstractEventLoop | None = None
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            # Fallback if __init__ is called outside a running loop
            # (though get_connection should be called inside one)
            self._loop = None

    @classmethod
    def _get_class_lock(cls) -> asyncio.Lock:
        """Ensure the class lock is bound to the current running loop."""
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

    @property
    @abstractmethod
    def is_connected_event(self) -> asyncio.Event:
        ...

    @classmethod
    async def get_connection(cls, vhost: str = "/") -> Self:
        """Get singleton instance (async)."""
        current_loop = asyncio.get_running_loop()
        async with cls._get_class_lock():
            instance = cls.instance_by_vhost.get(vhost)
            # Check if we have an instance AND if it belongs to the CURRENT loop
            if instance:
                # We need to check if the instance's internal loop matches our current loop
                # and if that loop is actually still running.
                if instance._loop != current_loop or not instance.is_connected_event.is_set():
                    log.warning("Found stale connection for %s (loop mismatch). Resetting.", vhost)
                    await instance.disconnect()  # Cleanup the old one
                    instance = None

            if instance is None:
                log.debug("Creating fresh connection for %s", vhost)
                new_instance = cls(vhost=vhost)
                new_instance._loop = current_loop  # Store the loop it was born in
                await new_instance.connect()
                cls.instance_by_vhost[vhost] = new_instance
                instance = new_instance
            return instance

    def is_connected(self) -> bool:
        """Check if connection is established and healthy."""
        return self.is_connected_event.is_set()


class BaseAsyncQueue(BaseQueue, ABC):
    async def get_connection(self) -> "BaseAsyncConnection":
        backend = get_backend()
        return await backend.connection.get_connection()

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
        delimiter = default_configuration.backend_config.topic_delimiter
        if not message.routing_key:
            raise ValueError("Routing key was not set. Invalid topic")
        if message.routing_key == self.result_topic or message.routing_key.endswith(
            f"{delimiter}result"
        ):
            # Skip a result message. Handling result messages happens in `_receive_results` method.
            # In case the subscription has .# as binding key,
            # this method would catch results message for all the topics in that namespace.
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
                result, topic=self.result_topic, correlation_id=message.correlation_id
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
            log.warning("Already subscribed...")
            return

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
        if self.subscription is not None:
            log.warning("Already subscribed...")
            return
        func = functools.partial(self._receive_result, callback)
        conn = await self.get_connection()
        self.result_subscription = await conn.subscribe(self.result_topic, func, shared=False)

    async def unsubscribe_results(self) -> None:
        """Unsubscribe from results. Will always delete the underlying queues"""
        if self.result_subscription is not None:
            conn = await self.get_connection()
            await conn.unsubscribe(self.result_subscription, if_unused=False, if_empty=False)
            self.result_subscription = None

    async def purge(self, reset_groups: bool = False) -> None:
        """Delete all messages from the queue."""
        if not self.shared_queue:
            raise RuntimeError("Can only purge shared queues")
        conn = await self.get_connection()
        await conn.purge(self.topic, reset_groups=reset_groups)

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

    def get_tag(self) -> str:
        return self.subscription

    @staticmethod
    async def send_message(
        topic: str, body: bytes, correlation_id: str | None = None, persistent: bool = True
    ) -> None:
        """Low-level message sending implementation.

        Args:
            topic: a topic name for direct routing or a routing key with special binding keys
            body: serialized message (e.g. a serialized protobuf message or a json string)
            correlation_id: is present for result messages
            persistent: if true will use aio_pika.DeliveryMode.PERSISTENT

        Returns:

        """
        backend = get_backend()
        message = Envelope(
            body=body,
            correlation_id=correlation_id or b"",
        )
        conn = await backend.connection.get_connection()
        await conn.publish(topic, message)


class LoggingAsyncQueue(BaseAsyncQueue):
    """Represents a specialized queue for logging purposes.

    >>> from protobunny import asyncio as pb
    >>> async def add_logger():
    >>>     await pb.subscribe_logger()  # it uses the default logger_callback

    You can add a custom callback that accepts message: aio_pika.IncomingMessage, msg_content: str as arguments.
    Note that the callback must be sync even for the async logger and
    it must be a function who purely calls the logging module and can perform other non IO operations

    >>> def log_callback(message, msg_content: str):
    >>>     print(message.body)
    >>> async def add_logger():
    >>>     await pb.subscribe_logger(log_callback)

    You can use functools.partial to add more arguments

    >>> def log_callback_with_args(message, msg_content: str, maxlength: int):
    >>>     print(message.body[maxlength])
    >>> import functools
    >>> functools.partial(log_callback_with_args, maxlength=100)
    >>> async def add_logger():
    >>>     await pb.subscribe_logger(log_callback_with_args)
    """

    def __init__(self, prefix: str) -> None:
        backend = default_configuration.backend_config
        delimiter = backend.topic_delimiter
        wildcard = backend.multi_wildcard_delimiter
        prefix = prefix or default_configuration.messages_prefix
        super().__init__(f"{prefix}{delimiter}{wildcard}")

    def get_tag(self) -> str:
        return self.topic

    async def send_message(self, **kwargs: tp.Any) -> None:
        raise NotImplementedError()

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
        message: "IncomingMessageProtocol",
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


def is_task(topic: str) -> bool:
    delimiter = default_configuration.backend_config.topic_delimiter
    return "tasks" in topic.split(delimiter)


# keep always the imports of generated code at the end of the file
from protobunny.core.results import Result, ReturnCode
