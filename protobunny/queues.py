import functools
import logging
import typing as tp
from abc import ABC, abstractmethod

import aio_pika
import betterproto
from aio_pika import DeliveryMode, IncomingMessage
from betterproto import Message

from .backends.rabbitmq import RequeueMessage, get_connection, get_connection_sync
from .config import load_config
from .models import ProtoBunnyMessage, Topic, get_message_class_from_topic

log = logging.getLogger(__name__)
configuration = load_config()


class BaseQueue(ABC):
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return False
        return self.topic == other.topic and self.shared_queue == other.shared_queue

    def __str__(self):
        return f"{self.__class__.__name__}({self.topic})"

    def __init__(self, topic: "Topic"):
        """Initialize RabbitMQ connection.

        Args:
            topic: a Topic value object
        """
        self.topic = topic.name
        self.shared_queue = topic.is_task_queue
        self.subscription = None
        self.result_subscription = None

    @property
    def result_topic(self) -> str:
        return f"{self.topic}.result"

    @abstractmethod
    def publish(self, message: "ProtoBunnyMessage") -> None:
        ...

    @abstractmethod
    def _receive(
        self,
        callback: tp.Callable[["ProtoBunnyMessage"], tp.Any],
        message: aio_pika.IncomingMessage,
    ) -> None:
        ...

    @abstractmethod
    def subscribe(self, callback: tp.Callable[["ProtoBunnyMessage"], tp.Any]) -> None:
        ...

    @abstractmethod
    def unsubscribe(self, if_unused: bool = True, if_empty: bool = True) -> None:
        ...

    @staticmethod
    @abstractmethod
    def send_message(
        topic: str, body: bytes, correlation_id: str | None = None, persistent: bool = True
    ):
        ...

    @abstractmethod
    def publish_result(
        self, result: "Result", topic: str | None = None, correlation_id: str | None = None
    ) -> None:
        ...

    @abstractmethod
    def _receive_result(
        self, callback: tp.Callable[["Result"], tp.Any], message: aio_pika.IncomingMessage
    ) -> None:
        ...

    @abstractmethod
    def subscribe_results(self, callback: tp.Callable[["pb.results.Result"], tp.Any]) -> None:
        ...

    @abstractmethod
    def unsubscribe_results(self) -> None:
        ...

    @abstractmethod
    def purge(self) -> None:
        ...

    @abstractmethod
    def get_message_count(self) -> int:
        ...


class Queue(BaseQueue):
    """Message queue backed by pika and RabbitMQ."""

    def publish(self, message: ProtoBunnyMessage) -> None:
        """Publish a message to the queue.

        Args:
            message: a protobuf message
        """
        self.send_message(self.topic, bytes(message))

    def _receive(
        self, callback: tp.Callable[[ProtoBunnyMessage], tp.Any], message: aio_pika.IncomingMessage
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

    def subscribe(self, callback: tp.Callable[["ProtoBunnyMessage"], tp.Any]) -> None:
        """Subscribe to messages from the queue.

        Args:
            callback:

        """

        if self.subscription is not None:
            raise ValueError("Cannot subscribe twice")
        func = functools.partial(self._receive, callback)
        self.subscription = get_connection_sync().subscribe(
            self.topic, func, shared=self.shared_queue
        )

    def unsubscribe(self, if_unused: bool = True, if_empty: bool = True) -> None:
        """Unsubscribe from the queue."""
        if self.subscription is not None:
            get_connection_sync().unsubscribe(
                self.subscription, if_unused=if_unused, if_empty=if_empty
            )
            self.subscription = None

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

    def _receive_result(
        self,
        callback: tp.Callable[["Result"], tp.Any],
        message: aio_pika.IncomingMessage,
    ) -> None:
        """Handle a message from the queue.

        Args:
            callback : function to call with deserialized result.
                Accept parameters like (message: Message, return_code: int, return_value: dict, error:str)
            message : `aio_pika.IncomingMessage`
                serialized message from the queue.
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

    def subscribe_results(self, callback: tp.Callable[["pb.results.Result"], tp.Any]) -> None:
        """Subscribe to results from the queue.

        See the deserialize_result method for return params.

        Args:
            callback : function to call when results come in.
        """
        if self.result_subscription is not None:
            raise ValueError("Can not subscribe to results twice")
        func = functools.partial(self._receive_result, callback)
        self.result_subscription = get_connection_sync().subscribe(
            self.result_topic, func, shared=False
        )

    def unsubscribe_results(self):
        """Unsubscribe from results. Will always delete the underlying queues"""
        if self.result_subscription is not None:
            get_connection_sync().unsubscribe(
                self.result_subscription, if_unused=False, if_empty=False
            )
            self.result_subscription = None

    def purge(self) -> None:
        """Delete all messages from the queue."""
        if not self.shared_queue:
            raise RuntimeError("Can only purge shared queues")
        get_connection_sync().purge(self.topic)

    def get_message_count(self) -> int:
        """Get current message count."""
        if not self.shared_queue:
            raise RuntimeError("Can only get count of shared queues")
        log.debug("Getting queue message count")
        return get_connection_sync().get_message_count(self.topic)

    @staticmethod
    def send_message(
        topic: str, body: bytes, correlation_id: str | None = None, persistent: bool = True
    ):
        """Low-level message sending implementation.

        Args:
            topic: a topic name for direct routing or a routing key with special binding keys
            body: serialized message (e.g. a serialized protobuf message or a json string)
            correlation_id: is present for result messages
            persistent: if true will use aio_pika.DeliveryMode.PERSISTENT

        Returns:

        """
        message = aio_pika.Message(
            body,
            correlation_id=correlation_id,
            delivery_mode=DeliveryMode.PERSISTENT if persistent else DeliveryMode.NOT_PERSISTENT,
        )
        get_connection_sync().publish(topic, message)


class AsyncQueue(BaseQueue):
    async def publish(self, message: ProtoBunnyMessage) -> None:
        """Publish a message to the queue.

        Args:
            message: a protobuf message
        """
        await self.send_message(self.topic, bytes(message))

    async def _receive(
        self, callback: tp.Callable[[ProtoBunnyMessage], tp.Any], message: aio_pika.IncomingMessage
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
            await callback(msg)
        except RequeueMessage:
            raise
        except Exception as exc:  # pylint: disable=W0703
            log.exception("Could not process message: %s", str(message.body))
            result = msg.make_result(return_code=ReturnCode.FAILURE, error=str(exc))
            await self.publish_result(
                result, topic=msg.result_topic, correlation_id=message.correlation_id
            )

    async def subscribe(self, callback: tp.Callable[["ProtoBunnyMessage"], tp.Any]) -> None:
        """Subscribe to messages from the queue.

        Args:
            callback:

        """

        if self.subscription is not None:
            raise ValueError("Cannot subscribe twice")
        func = functools.partial(self._receive, callback)
        conn = await get_connection()
        self.subscription = await conn.subscribe(self.topic, func, shared=self.shared_queue)

    async def unsubscribe(self, if_unused: bool = True, if_empty: bool = True) -> None:
        """Unsubscribe from the queue."""
        if self.subscription is not None:
            conn = await get_connection()
            await conn.unsubscribe(self.subscription, if_unused=if_unused, if_empty=if_empty)
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
        callback: tp.Callable[["Result"], tp.Any],
        message: aio_pika.IncomingMessage,
    ) -> None:
        """Handle a message from the queue.

        Args:
            callback : function to call with deserialized result.
                Accept parameters like (message: Message, return_code: int, return_value: dict, error:str)
            message : `aio_pika.IncomingMessage`
                serialized message from the queue.
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
        except Exception:  # pylint: disable=W0703
            log.exception("Could not process result: %s", str(message.body))

    async def subscribe_results(self, callback: tp.Callable[["pb.results.Result"], tp.Any]) -> None:
        """Subscribe to results from the queue.

        See the deserialize_result method for return params.

        Args:
            callback : function to call when results come in.
        """
        if self.result_subscription is not None:
            raise ValueError("Can not subscribe to results twice")
        func = functools.partial(self._receive_result, callback)
        conn = await get_connection()
        self.result_subscription = await conn.subscribe(self.result_topic, func, shared=False)

    async def unsubscribe_results(self):
        """Unsubscribe from results. Will always delete the underlying queues"""
        if self.result_subscription is not None:
            conn = await get_connection()
            await conn.unsubscribe(self.result_subscription, if_unused=False, if_empty=False)
            self.result_subscription = None

    async def purge(self) -> None:
        """Delete all messages from the queue."""
        if not self.shared_queue:
            raise RuntimeError("Can only purge shared queues")
        conn = await get_connection()
        await conn.purge(self.topic)

    async def get_message_count(self) -> int:
        """Get current message count."""
        if not self.shared_queue:
            raise RuntimeError("Can only get count of shared queues")
        conn = await get_connection()
        return await conn.get_message_count(self.topic)

    @staticmethod
    async def send_message(
        topic: str, body: bytes, correlation_id: str | None = None, persistent: bool = True
    ):
        """Low-level message sending implementation.

        Args:
            topic: a topic name for direct routing or a routing key with special binding keys
            body: serialized message (e.g. a serialized protobuf message or a json string)
            correlation_id: is present for result messages
            persistent: if true will use aio_pika.DeliveryMode.PERSISTENT

        Returns:

        """
        message = aio_pika.Message(
            body,
            correlation_id=correlation_id,
            delivery_mode=DeliveryMode.PERSISTENT if persistent else DeliveryMode.NOT_PERSISTENT,
        )
        conn = await get_connection()
        await conn.publish(topic, message)


class LoggingSyncQueue(Queue):
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

    def __init__(self, prefix: str) -> None:
        super().__init__(Topic(f"{prefix}.#"))

    @property
    def result_topic(self) -> str:
        return ""

    def publish(self, message: Message) -> None:
        raise NotImplementedError

    def publish_result(
        self,
        result: "pb.results.Result",
        topic: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        raise NotImplementedError

    def _receive(
        self,
        log_callback: tp.Callable[[aio_pika.Message, str], tp.Any],
        message: aio_pika.IncomingMessage,
    ):
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


class LoggingAsyncQueue(AsyncQueue):
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

    def __init__(self, prefix: str) -> None:
        super().__init__(Topic(f"{prefix}.#"))

    @property
    def result_topic(self) -> str:
        return ""

    async def publish(self, message: Message) -> None:
        raise NotImplementedError

    async def publish_result(
        self,
        result: "pb.results.Result",
        topic: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        raise NotImplementedError

    async def _receive(
        self,
        log_callback: tp.Callable[[aio_pika.Message, str], tp.Any],
        message: aio_pika.IncomingMessage,
    ):
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


def deserialize_message(topic: str | None, body: bytes) -> ProtoBunnyMessage | None:
    """Deserialize the body of a serialized pika message.

    Args:
        topic: str. The topic. It's used to determine the type of message.
        body: bytes. The serialized message

    Returns:
        A deserialized message.
    """
    if not topic:
        raise ValueError("Routing key was not set. Invalid topic")
    message_type: tp.Type[Message] = get_message_class_from_topic(topic)
    return message_type().parse(body) if message_type else None


def deserialize_result_message(body: bytes) -> "Result":
    """Deserialize the result message.

    Args:
        body: bytes. The serialized amlogic_messages.results.Result

    Returns:
        Instance of amlogic_messages.results.Result
    """
    return Result().parse(body)


def get_body(message: IncomingMessage) -> str:
    if message.routing_key.endswith(".result"):
        # log result message. Need to extract the source here
        result = deserialize_result_message(message.body)
        # original message
        msg = result.source
        return_code = ReturnCode(result.return_code).name
        # stringify to json
        source = msg.to_json(casing=betterproto.Casing.SNAKE, include_default_values=True)
        if result.return_code != ReturnCode.SUCCESS:
            body = f"{return_code} - {result.error} - {source}"
        else:
            body = f"{return_code} - {source}"
    else:
        msg: ProtoBunnyMessage | None = deserialize_message(message.routing_key, message.body)
        body = (
            msg.to_json(casing=betterproto.Casing.SNAKE, include_default_values=True)
            if msg is not None
            # can't parse the message but log the raw content
            else message.body
        )
    return str(body)


from .core.results import Result, ReturnCode
