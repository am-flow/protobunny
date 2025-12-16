"""Implementation of amlogic-messages base queues."""

import copy
import dataclasses
import datetime
import functools
import json
import logging
import textwrap
import typing as tp
import uuid
from collections import defaultdict
from decimal import Decimal
from io import BytesIO
from types import ModuleType

import aio_pika
import betterproto
import numpy as np
from aio_pika import DeliveryMode
from betterproto import Casing, Message
from betterproto.lib.google.protobuf import Any

import protobunny as pb

from .connection import RequeueMessage, get_connection
from .core import results
from .introspect import (
    AMMessage,
    ProtoBunnyMessage,
    build_routing_key,
    get_message_class_from_topic,
    get_message_class_from_type_url,
    user_messages_prefix,
)

log = logging.getLogger(__name__)


class ProtobunnyJsonEncoder(json.JSONEncoder):
    def default(self, obj: object) -> tp.Any:
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        elif isinstance(obj, uuid.UUID):
            return str(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


class MissingRequiredFields(Exception):
    """Exception raised by MessageMixin.validate_required_fields when required fields are missing."""

    def __init__(self, msg: ProtoBunnyMessage, missing_fields: list[str]) -> None:
        self.missing_fields = missing_fields
        missing = ", ".join(missing_fields)
        super().__init__(f"Required fields for message {msg.topic} were not set: {missing}")


class MessageMixin:
    """Utility mixin for amlogic messages."""

    def validate_required_fields(self: AMMessage) -> None:
        """Raises a ValueError if non optional fields are missing.

        This check happens during serialization (see MessageMixin.__bytes__ method).
        """
        missing = [
            field_name
            for field_name, meta in self._betterproto.meta_by_field_name.items()
            if not meta.optional and not self.is_set(field_name)
        ]
        if missing:
            raise MissingRequiredFields(self, missing)

    @functools.cached_property
    def json_content_fields(self: AMMessage) -> list[str]:
        """Returns: the list of fieldnames that are of type commons.JsonContent."""
        return [
            field_name
            for field_name, clz in self._betterproto.cls_by_field.items()
            if clz is pb.commons.JsonContent
        ]

    def __bytes__(self: ProtoBunnyMessage) -> bytes:
        # Override Message.__bytes__ method
        # to support transparent serialization of dictionaries to JsonContent fields.
        # This method validates for required fields as well,
        # Note: it's not a typical override but rather a runtime patching.
        # See introspect.decorate_protobuf_classes
        self.validate_required_fields()
        msg = self.serialize_json_content()
        with BytesIO() as stream:
            Message.dump(msg, stream)
            return stream.getvalue()

    def from_dict(self: AMMessage, value: dict[str, tp.Any]) -> AMMessage:
        json_fields = {field: value.pop(field, None) for field in self.json_content_fields}
        msg = Message.from_dict(self, value)
        for field in json_fields:
            setattr(msg, field, json_fields[field])
        return msg

    def to_dict(
        self: AMMessage, casing: Casing = Casing.CAMEL, include_default_values: bool = False
    ) -> dict[str, tp.Any]:
        """Returns a JSON serializable dict representation of this object.

        Note: betterproto `to_dict` converts INT64 to strings, to allow js compatibility.
        """
        json_fields = {}
        for field in self.json_content_fields:
            json_fields[field] = getattr(self, field)
            delattr(self, field)
        out_dict = Message.to_dict(self, casing, include_default_values)
        out_dict.update(json_fields)
        return out_dict

    def to_pydict(
        self: AMMessage, casing: Casing = Casing.CAMEL, include_default_values: bool = False
    ) -> dict[str, tp.Any]:
        """Returns a dict representation of this object.

        Conversely to the `to_dict` method, betterproto `to_pydict` doesn't convert INT64 to strings.
        """
        json_fields = {}
        for field in self.json_content_fields:
            json_fields[field] = getattr(self, field)
            delattr(self, field)
        out_dict = Message.to_pydict(self, casing, include_default_values)
        out_dict.update(json_fields)
        # update the dict to use enum names instead of int values
        out_dict = self._use_enum_names(casing, out_dict)
        return out_dict

    def _use_enum_names(self, casing, out_dict: dict[str, tp.Any]) -> dict[str, tp.Any]:
        """Used to reprocess betterproto.Message.to_pydict output to use names for Enum fields.

        Process only first level fields.

        Warning: enums that are inside a nested message are left untouched
        note: to_pydict is used in LoggerQueue (am-mqtt-logger service)
        """
        # The original Message.to_pydict writes int values instead of names for Enum.
        # Copying implementation from Message.to_dict to handle enums properly

        updated_out_enums = copy.deepcopy(out_dict)

        def _process_enum_field():
            field_types = self._type_hints()
            defaults = self._betterproto.default_gen
            field_is_repeated = defaults[field_name] is list
            res = None
            if field_is_repeated:
                enum_class = field_types[field_name].__args__[0]
                if isinstance(value, tp.Iterable) and not isinstance(value, str):
                    res = [enum_class(el).name for el in value]
                else:
                    # transparently upgrade single value to repeated
                    res = [enum_class(value).name]
            elif meta.optional:
                # get the real Enum class from Optional field
                enum_class = field_types[field_name].__args__[0]
                res = enum_class(value).name
            else:
                enum_class = field_types[field_name]  # noqa
                res = enum_class(value).name if value is not None else value
            return res

        for field_name, meta in self._betterproto.meta_by_field_name.items():
            if not meta.proto_type == betterproto.TYPE_ENUM:
                continue
            cased_name = casing(field_name).rstrip("_")  # type: ignore
            value = getattr(self, field_name)
            if value is None:
                updated_out_enums[cased_name] = None
                continue
            try:
                # process a enum field
                updated_out_enums[cased_name] = _process_enum_field()
            except (ValueError, TypeError, KeyError, Exception) as e:
                log.error(
                    "Couldn't get enum value for %s with value %s: %s", cased_name, value, str(e)
                )
                continue
        return updated_out_enums

    def to_json(
        self,
        indent: None | int | str = None,
        include_default_values: bool = False,
        casing: Casing = Casing.CAMEL,
    ) -> str:
        """Overwrite the betterproto to_json to use the custom encoder"""
        return json.dumps(
            self.to_pydict(include_default_values=include_default_values, casing=casing),
            indent=indent,
            cls=ProtobunnyJsonEncoder,
        )

    def parse(self: ProtoBunnyMessage, data: bytes) -> AMMessage:
        # Override Message.parse() method
        # to support transparent deserialization of JsonContent fields
        json_content_fields = copy.copy(self.json_content_fields)
        msg = Message.parse(self, data)

        for field in json_content_fields:
            json_content_value = getattr(msg, field)
            if not json_content_value:
                setattr(msg, field, None)
                continue
            deserialized_content = _deserialize_content(json_content_value)
            setattr(msg, field, deserialized_content)
        return msg

    @property
    def type_url(self: ProtoBunnyMessage) -> str:
        """Return the class fqn for this message."""
        return f"{self.__class__.__module__}.{self.__class__.__name__}"

    @property
    def source(self: "pb.results.Result") -> ProtoBunnyMessage:
        """Return the source message from a Result

        The source message is stored as a protobuf.Any message, with its type info  and serialized value.
        The `Result.source_message.type_url` is used to instantiate the right class to deserialize the source message.
        """
        if not isinstance(self, pb.results.Result):
            raise ValueError("Message is not a Result: no source message to build.")
        message_type = get_message_class_from_type_url(self.source_message.type_url)
        source_message = message_type().parse(self.source_message.value)
        return source_message

    @functools.cached_property
    def topic(self: ProtoBunnyMessage) -> str:
        """Build the topic name for the message."""
        return get_topic(self).name

    @functools.cached_property
    def result_topic(self: ProtoBunnyMessage) -> str:
        """
        Build the result topic name for the message.
        """
        return f"{get_topic(self).name}.result"

    def make_result(
        self: ProtoBunnyMessage,
        return_code: results.ReturnCode = results.ReturnCode.SUCCESS,
        error: str = "",
        return_value: dict[str, tp.Any] | None = None,
    ) -> results.Result:
        """Returns a pb.results.Result message for the message,
        using the betterproto.lib.std.google.protobuf.Any message type.

        The property `result.source` represents the source message.

        Args:
            return_code:
            error:
            return_value:

        Returns: a Result message.
        """
        any_message = Any(type_url=self.type_url, value=bytes(self))
        # The "return_value" argument is a dictionary.
        # It will be internally packed as commons.JsonContent field when serialized
        # and automatically deserialized to a dictionary during parsing
        return pb.results.Result(
            source_message=any_message,
            return_code=return_code,
            return_value=return_value,
            error=error,
        )

    def serialize_json_content(self: ProtoBunnyMessage) -> AMMessage:
        msg = copy.deepcopy(self)
        json_content_fields = msg.json_content_fields
        for field in json_content_fields:
            value = getattr(msg, field)
            serialized_content = to_json_content(value)
            setattr(msg, field, serialized_content)
        return msg


@dataclasses.dataclass
class Topic:
    """A dataclass to hold get_topic() return value."""

    name: str
    is_task_queue: bool = False


class Queue:
    """Message queue backed by pika and RabbitMQ."""

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return False
        return self.topic == other.topic and self.shared_queue == other.shared_queue

    def __init__(self, topic: Topic):
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

    def publish(self, message: AMMessage) -> None:
        """Publish a message to the queue.

        Args:
            message: a protobuf message
        """
        self._send_message(self.topic, bytes(message))

    def _receive(
        self, callback: tp.Callable[[AMMessage], tp.Any], message: aio_pika.IncomingMessage
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
        msg: AMMessage = deserialize_message(message.routing_key, message.body)
        try:
            _ = callback(msg)
        except RequeueMessage:
            raise
        except Exception as exc:  # pylint: disable=W0703
            log.exception("Could not process message: %s", str(message.body))
            result = msg.make_result(return_code=pb.results.ReturnCode.FAILURE, error=str(exc))
            self.publish_result(
                result, topic=msg.result_topic, correlation_id=message.correlation_id
            )

    def subscribe(self, callback: tp.Callable[[AMMessage], tp.Any]) -> None:
        """Subscribe to messages from the queue.

        Args:
            callback:

        """

        if self.subscription is not None:
            raise ValueError("Cannot subscribe twice")
        func = functools.partial(self._receive, callback)
        self.subscription = get_connection().subscribe(self.topic, func, shared=self.shared_queue)

    def unsubscribe(self) -> None:
        """Unsubscribe from the queue."""
        if self.subscription is not None:
            get_connection().unsubscribe(self.subscription)
            self.subscription = None

    def publish_result(
        self,
        result: "pb.results.Result",
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
        self._send_message(
            result_topic, bytes(result), correlation_id=correlation_id, persistent=False
        )

    def _receive_result(
        self,
        callback: tp.Callable[["pb.results.Result"], tp.Any],
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
        self.result_subscription = get_connection().subscribe(self.result_topic, func, shared=False)

    def unsubscribe_results(self):
        """Unsubscribe from results."""
        if self.result_subscription is not None:
            get_connection().unsubscribe(self.result_subscription)
            self.result_subscription = None

    def purge(self) -> None:
        """Delete all messages from the queue."""
        if not self.shared_queue:
            raise RuntimeError("Can only purge shared queues")
        get_connection().purge(self.topic)

    def get_message_count(self) -> int:
        """Get current message count."""
        if not self.shared_queue:
            raise RuntimeError("Can only get count of shared queues")
        log.debug("Getting queue message count")
        return get_connection().get_message_count(self.topic)

    @staticmethod
    def _send_message(
        topic: str, body: bytes, correlation_id: str | None = None, persistent: bool = True
    ):
        """
        Low-level message sending implementation.

        Args:
            topic: a topic name for direct routing or a routing key with special binding keys
            body: serialized message
            correlation_id:
            persistent:

        Returns:

        """
        message = aio_pika.Message(
            body,
            correlation_id=correlation_id,
            delivery_mode=DeliveryMode.PERSISTENT if persistent else DeliveryMode.NOT_PERSISTENT,
        )
        get_connection().publish(topic, message)


class LoggingQueue(Queue):
    def __init__(self) -> None:
        super().__init__(Topic(f"{user_messages_prefix}.#"))

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
            if message.routing_key.endswith(".result"):
                # log result message. Need to extract the source here
                result = deserialize_result_message(message.body)
                # original message
                msg = result.source
                return_code = pb.results.ReturnCode(result.return_code).name
                # stringify to json
                source = msg.to_json(casing=betterproto.Casing.SNAKE, include_default_values=True)
                if result.return_code != pb.results.ReturnCode.SUCCESS:
                    body = f"{return_code} - {result.error} - {source}"
                else:
                    body = f"{return_code} - {source}"
            else:
                msg: AMMessage | None = deserialize_message(message.routing_key, message.body)
                body = (
                    msg.to_json(casing=betterproto.Casing.SNAKE, include_default_values=True)
                    if msg is not None
                    # can't parse the message but log the raw content
                    else message.body
                )

            log_callback(message, body)
        except RequeueMessage:
            raise
        except Exception as exc:  # pylint: disable=W0703
            log.exception(
                "Could not process message on Logging queue: %s - %s", str(message.body), str(exc)
            )


########################
# Base Methods
########################

# subscriptions registry
subscriptions: dict[tp.Any, list[Queue]] = defaultdict(list)


def get_topic(pkg_or_msg: Message | type[Message] | ModuleType) -> Topic:
    """Return a Topic dataclass object based on a Message (instance or class) or a ModuleType.

    It uses build_routing_key to determine the topic name.
    Note: The topic name can be a routing key with a binding key

    Args:
        pkg_or_msg: a Message instance, a Message class or a amlogic_messages.codegen.* module

    Returns: Topic
        A Topic(name: str, is_task_queue: bool)
    """
    topic_name = f"{user_messages_prefix}.{build_routing_key(pkg_or_msg)}"
    is_task_queue = ".tasks." in topic_name
    return Topic(name=topic_name, is_task_queue=is_task_queue)


def get_queue(pkg: Message | type[Message] | ModuleType) -> Queue:
    """

    Args:
        pkg:

    Returns: a amlogic_messages.Queue instance

    """
    topic = get_topic(pkg)
    queue = Queue(topic)
    return queue


def deserialize_message(topic: str | None, body: bytes) -> Message | None:
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


def deserialize_result_message(body: bytes) -> "pb.results.Result":
    """Deserialize the result message.

    Args:
        body: bytes. The serialized amlogic_messages.results.Result

    Returns:
        Instance of amlogic_messages.results.Result
    """
    return pb.results.Result().parse(body)


def to_json_content(data: dict[str, tp.Any]) -> "pb.commons.JsonContent | None":
    """Serialize an object and build a JsonContent message.

    Args:
        data: A json-serializable object

    Returns: A pb.commons.JsonContent instance
    """
    # Encode a json string to bytes
    if not data:
        return None
    encoded = json.dumps(data, cls=ProtobunnyJsonEncoder).encode()
    # build the JsonContent field
    content = pb.commons.JsonContent(content=encoded)
    return content


def _deserialize_content(msg: "pb.commons.JsonContent") -> dict[str, tp.Any]:
    """Deserialize a JsonContent message back into a dictionary.

    Note: To not use directly.
    Deserialization of this type of field happens in the parse() method of the container object.

    Args:
        msg: The JsonContent object

    Returns: The decoded dictionary

    """
    # Decode bytes back to JSON string and parse
    return json.loads(msg.content.decode()) or None


def publish(message: AMMessage) -> None:
    """Publish the message on its own queue

    Args:
        message:
    """
    q = get_queue(message)
    q.publish(message)


def publish_result(
    result: "pb.results.Result", topic: str | None = None, correlation_id: str | None = None
) -> None:
    """Publish the result message to the result topic of the source message

    Args:
        result: a Result instance.
        topic: The topic to send the message to.
            Default to the source message result topic (e.g. "pb.vision.ExtractFeature.result")
        correlation_id:
    """
    q = get_queue(result.source)
    q.publish_result(result, topic, correlation_id)


def subscribe(
    message: AMMessage | type[Message] | ModuleType, callback: tp.Callable[[AMMessage], tp.Any]
) -> Queue:
    """Subscribe a callback function to the topic.

    Args:
        message:
        callback:

    Returns:
        The Queue object. You can access the subscription via its `subscription` attribute.
    """
    q = get_queue(message)
    q.subscribe(callback)
    # register subscription to unsubscribe later
    sub_key = type(message) if isinstance(message, Message) else message
    subscriptions[sub_key].append(q)
    return q


def unsubscribe(message: AMMessage | type[Message] | ModuleType) -> None:
    """Remove all in-process subscriptions for a message/package"""
    for q in subscriptions[message]:
        q.unsubscribe()


def unsubscribe_results(message: AMMessage | type[Message] | ModuleType) -> None:
    """Remove all in-process subscriptions for a message/package result topic"""
    for q in subscriptions[message]:
        q.unsubscribe_results()


def unsubscribe_all() -> None:
    """Remove all in-process subscriptions"""
    all_queues = [q for topic in subscriptions for q in subscriptions[topic]]
    for q in all_queues:
        q.unsubscribe()
        q.unsubscribe_results()


def subscribe_results(
    message: AMMessage | type[Message] | ModuleType,
    callback: tp.Callable[["pb.results.Result"], tp.Any],
) -> Queue:
    """Subscribe a callback function to the result topic.

    Args:
        message:
        callback:
    """
    q = get_queue(message)
    q.subscribe_results(callback)
    # register subscription to unsubscribe later
    sub_key = type(message) if isinstance(message, Message) else message
    subscriptions[sub_key].append(q)
    return q


def get_message_count(msg_type: AMMessage | type[Message]) -> int:
    q = get_queue(msg_type)
    count = q.get_message_count()
    return count


def default_log_callback(message: aio_pika.IncomingMessage, msg_content: str):
    """Log queue message to stdout."""
    log.info(
        "<%s>(cid:%s) %s",
        message.routing_key,
        message.correlation_id,
        textwrap.shorten(msg_content, width=80),
    )


def subscribe_logger(log_callback=None) -> LoggingQueue:
    log_callback = log_callback or default_log_callback
    q = LoggingQueue()
    q.subscribe(log_callback)
    return q
