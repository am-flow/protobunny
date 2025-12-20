import copy
import dataclasses
import functools
import importlib
import json
import logging
import typing as tp
from io import BytesIO
from types import ModuleType

import aio_pika
import betterproto
from betterproto.lib.std.google.protobuf import Any

if tp.TYPE_CHECKING:
    from .queues import LoggingAsyncQueue, LoggingSyncSyncQueue
from .config import load_config
from .utils import ProtobunnyJsonEncoder

# - types
# ProtoBunnyMessage = tp.TypeVar("ProtoBunnyMessage", bound="MessageMixin | betterproto.Message")
SyncCallback: tp.TypeAlias = tp.Callable[["ProtoBunnyMessage"], tp.Any]
AsyncCallback: tp.TypeAlias = tp.Callable[["ProtoBunnyMessage"], tp.Awaitable[tp.Any]]
ResultCallback: tp.TypeAlias = tp.Callable[["Result"], tp.Any]
LogQueue = tp.TypeVar("LogQueue", "LoggingAsyncQueue", "LoggingSyncSyncQueue")
LoggerCallback: tp.TypeAlias = tp.Callable[[aio_pika.IncomingMessage, str], tp.Any]

log = logging.getLogger(__name__)

configuration = load_config()


class MessageMixin:
    """Utility mixin for protobunny messages."""

    # if tp.TYPE_CHECKING:
    #     _betterproto: tp.Any
    #     __name__: str

    # def is_set(self, field_name: str) -> bool:
    #     ...

    def validate_required_fields(self: "ProtoBunnyMessage") -> None:
        """Raises a MissingRequiredFields if non optional fields are missing.
        Note: Ignore missing repeated fields.
        This check happens during serialization (see MessageMixin.__bytes__ method).
        """
        defaults = self._betterproto.default_gen
        missing = [
            field_name
            for field_name, meta in self._betterproto.meta_by_field_name.items()
            if not (meta.optional or self.is_set(field_name) or defaults[field_name] is list)
        ]
        if missing:
            raise MissingRequiredFields(self, missing)

    @functools.cached_property
    def json_content_fields(self: "ProtoBunnyMessage") -> list[str]:
        """Returns: the list of fieldnames that are of type commons.JsonContent."""
        return [
            field_name
            for field_name, clz in self._betterproto.cls_by_field.items()
            if "JsonContent" in clz.__name__
        ]

    def __bytes__(self: "ProtoBunnyMessage") -> bytes:
        # Override Message.__bytes__ method
        # to support transparent serialization of dictionaries to JsonContent fields.
        # This method validates for required fields as well
        if configuration.force_required_fields:
            self.validate_required_fields()
        msg = self.serialize_json_content()
        with BytesIO() as stream:
            betterproto.Message.dump(msg, stream)
            return stream.getvalue()

    def from_dict(self: "ProtoBunnyMessage", value: dict) -> "ProtoBunnyMessage":
        json_fields = {field: value.pop(field, None) for field in self.json_content_fields}
        msg = betterproto.Message.from_dict(tp.cast(betterproto.Message, self), value)
        for field in json_fields:
            setattr(msg, field, json_fields[field])
        return msg

    def to_dict(
        self: "ProtoBunnyMessage",
        casing: betterproto.Casing = betterproto.Casing.CAMEL,
        include_default_values: bool = False,
    ) -> dict[str, tp.Any]:
        """Returns a JSON serializable dict representation of this object.

        Note: betterproto `to_dict` converts INT64 to strings, to allow js compatibility.
        """
        betterproto_func = functools.partial(
            betterproto.Message.to_dict,
            casing=casing,
            include_default_values=include_default_values,
        )
        out_dict = self._to_dict_with_json_content(betterproto_func)
        return out_dict

    def _to_dict_with_json_content(self, betterproto_func: tp.Callable[..., tp.Any]) -> dict:
        json_fields = {}
        self_ = copy.deepcopy(self)
        for field in self.json_content_fields:
            json_fields[field] = getattr(self_, field)
            delattr(self_, field)
        out_dict = betterproto_func(self_)
        out_dict.update(json_fields)
        return out_dict

    def to_pydict(
        self: "ProtoBunnyMessage",
        casing: betterproto.Casing = betterproto.Casing.CAMEL,
        include_default_values: bool = False,
    ) -> dict[str, tp.Any]:
        """Returns a dict representation of this object. Uses enum names instead of int values. Useful for logging

        Conversely to the `to_dict` method, betterproto `to_pydict` doesn't convert INT64 to strings.
        """
        betterproto_func = functools.partial(
            betterproto.Message.to_pydict,
            casing=casing,
            include_default_values=include_default_values,
        )
        out_dict = self._to_dict_with_json_content(betterproto_func)
        # update the dict to use enum names instead of int values
        out_dict = self._use_enum_names(casing, out_dict)
        return out_dict

    def _use_enum_names(
        self: "ProtoBunnyMessage", casing, out_dict: dict[str, tp.Any]
    ) -> dict[str, tp.Any]:
        """Used to reprocess betterproto.Message.to_pydict output to use names for Enum fields.

        Process only first level fields.

        Warning: enums that are inside a nested message are left untouched
        note: to_pydict is used in LoggerQueue (am-mqtt-logger service)
        """
        # The original Message.to_pydict writes int values instead of names for Enum.
        # Copying implementation from Message.to_dict to handle enums with names the same way as in to_dict

        updated_out_enums = out_dict

        def _process_enum_field():
            field_types = self._type_hints()
            defaults = self._betterproto.default_gen
            field_is_repeated = defaults[field_name] is list
            if field_is_repeated:
                enum_class = field_types[field_name].__args__[0]  # noqa
                if isinstance(value, tp.Iterable) and not isinstance(value, str):
                    res = [enum_class(el).name for el in value]
                else:
                    # transparently upgrade single value to repeated
                    res = [enum_class(value).name]
            elif meta.optional:
                # get the real Enum class from Optional field
                enum_class = field_types[field_name].__args__[0]  # noqa
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
        self: "ProtoBunnyMessage",
        indent: None | int | str = None,
        include_default_values: bool = False,
        casing: betterproto.Casing = betterproto.Casing.CAMEL,
    ) -> str:
        """Overwrite the betterproto to_json to use the custom encoder"""
        return json.dumps(
            self.to_pydict(include_default_values=include_default_values, casing=casing),
            indent=indent,
            cls=ProtobunnyJsonEncoder,
        )

    def parse(self: "ProtoBunnyMessage", data: bytes) -> "ProtoBunnyMessage":
        # Override Message.parse() method
        # to support transparent deserialization of JsonContent fields
        json_content_fields = list(self.json_content_fields)
        msg = betterproto.Message.parse(self, data)

        for field in json_content_fields:
            json_content_value = getattr(msg, field)
            if not json_content_value:
                setattr(msg, field, None)
                continue
            deserialized_content = _deserialize_content(json_content_value)
            setattr(msg, field, deserialized_content)
        return msg

    @property
    def type_url(self: "ProtoBunnyMessage") -> str:
        """Return the class fqn for this message."""
        return f"{self.__class__.__module__}.{self.__class__.__name__}"

    @property
    def source(self: "ProtoBunnyMessage") -> "ProtoBunnyMessage":
        """Return the source message from a Result

        The source message is stored as a protobuf.Any message, with its type info  and serialized value.
        The `Result.source_message.type_url` is used to instantiate the right class to deserialize the source message.
        """
        if not isinstance(self, Result):
            raise ValueError("Message is not a Result: no source message to build.")
        message_type = get_message_class_from_type_url(self.source_message.type_url)
        source_message = message_type().parse(self.source_message.value)
        return source_message

    @functools.cached_property
    def topic(self: "ProtoBunnyMessage") -> str:
        """Build the topic name for the message."""
        return get_topic(self).name

    @functools.cached_property
    def result_topic(self: "ProtoBunnyMessage") -> str:
        """
        Build the result topic name for the message.
        """
        return f"{get_topic(self).name}.result"

    def make_result(
        self: "ProtoBunnyMessage",
        return_code: "ReturnCode | None" = None,
        error: str = "",
        return_value: dict[str, tp.Any] | None = None,
    ) -> "Result":
        """Returns a pb.results.Result message for the message,
        using the betterproto.lib.std.google.protobuf.Any message type.

        The property `result.source` represents the source message.

        Args:
            return_code:
            error:
            return_value:

        Returns: a Result message.
        """
        if isinstance(self, Result):
            log.warning("Message is already a Result. Returning it as is.")
            return self
        any_message = Any(type_url=str(self.type_url), value=bytes(self))
        # The "return_value" argument is a dictionary.
        # It will be internally packed as commons.JsonContent field when serialized
        # and automatically deserialized to a dictionary during parsing
        return Result(
            source_message=any_message,
            return_code=return_code or ReturnCode.SUCCESS,
            return_value=return_value,
            error=error,
        )

    def serialize_json_content(self: "ProtoBunnyMessage") -> "ProtoBunnyMessage":
        json_content_fields = self.json_content_fields
        msg = copy.deepcopy(self)
        for field in json_content_fields:
            value = getattr(msg, field)
            serialized_content = to_json_content(value)
            setattr(msg, field, serialized_content)
        return msg


class ProtoBunnyMessage(MessageMixin, betterproto.Message):
    """Base class for all protobunny messages."""

    ...


class MissingRequiredFields(Exception):
    """Exception raised by MessageMixin.validate_required_fields when required fields are missing."""

    def __init__(self, msg: "ProtoBunnyMessage", missing_fields: list[str]) -> None:
        self.missing_fields = missing_fields
        missing = ", ".join(missing_fields)
        super().__init__(f"Non optional fields for message {msg.topic} were not set: {missing}")


def to_json_content(data: dict) -> "JsonContent | None":
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
    content = JsonContent(content=encoded)
    return content


def _deserialize_content(msg: "JsonContent") -> dict | None:
    """Deserialize a JsonContent message back into a dictionary.

    Note: To not use directly.
    Deserialization of this type of field happens in the parse() method of the container object.

    Args:
        msg: The JsonContent object

    Returns: The decoded dictionary

    """
    # Decode bytes back to JSON string and parse
    return json.loads(msg.content.decode()) if msg.content else None


def _get_submodule(
    package: ModuleType, paths: list[str]
) -> "type[ProtoBunnyMessage] | ModuleType | None":
    """Import module/class from package

    Args:
        package: Root package to import the submodule from (e.g. amlogic_messages.codegen)
        paths: Path to submodule/class expressed as list (e.g. ['vision', 'control', 'Start'])

    Note: you can get the path list by splitting the topic
    >>> msg = acme.vision.control.Start()
    >>> paths = msg.topic.split('.')  # ['vision', 'control', 'Start']

    Returns: the submodule
    """
    try:
        submodule = getattr(package, paths.pop(0))
    except AttributeError:
        return None
    if paths:
        return _get_submodule(submodule, paths)
    return submodule


@functools.lru_cache
def get_message_class_from_topic(topic: str) -> "type[ProtoBunnyMessage] | None | Result":
    """Return the message class from a topic with lazy import of the user library

    Args:
        topic: the RabbitMQ topic that represents the message queue

    Returns: the message class
    """
    if topic.endswith(".result"):
        message_type = Result
    else:
        route = topic.removeprefix(f"{configuration.messages_prefix}.")
        if route == topic:
            # Allow pb.* internal messages
            route = topic.removeprefix("pb.")
        codegen_module = importlib.import_module(configuration.generated_package_name)
        # if route is not recognized, the message_type will be None
        message_type = _get_submodule(codegen_module, route.split("."))
    return message_type


@functools.lru_cache
def get_message_class_from_type_url(url: str) -> type["ProtoBunnyMessage"]:
    """Return the message class from a topic with lazy import of the user library

    Args:
        url: the fullname message class

    Returns: the message class
    """
    module_path, clz = url.rsplit(".", 1)
    if not module_path.startswith(configuration.generated_package_name):
        raise ValueError(
            f"Invalid type url {url}, must start with {configuration.generated_package_name}."
        )
    module = importlib.import_module(module_path)
    message_type = getattr(module, clz)
    return message_type


def build_routing_key(
    pkg_or_msg: "ProtoBunnyMessage | type[ProtoBunnyMessage] | ModuleType",
) -> str:
    """Return a routing key based on a message instance, a message class, or a module.
    The string will be later composed with the configured message-prefix to build the exact topic name.

    Examples:
        build_routing_key(mymessaginglib.vision.control) -> "vision.control.#" routing with binding key
        build_routing_key(mymessaginglib.vision.control.Start) -> "vision.control.Start" direct routing
        build_routing_key(mymessaginglib.vision.control.Start()) -> "vision.control.Start" direct routing

    Args:
        pkg_or_msg: a Message instance, class or module to mymessaginglib codegen packages

    Returns: a routing key based on the type of message or package

    """
    module_name = ""
    class_name = ""
    if isinstance(pkg_or_msg, betterproto.Message):
        module_name = pkg_or_msg.__module__
        class_name = pkg_or_msg.__class__.__name__
    elif isinstance(pkg_or_msg, type(betterproto.Message)):
        module_name = pkg_or_msg.__module__
        class_name = pkg_or_msg.__name__
    elif isinstance(pkg_or_msg, ModuleType):
        module_name = pkg_or_msg.__name__
        class_name = "#"
    routing_key = f"{module_name}.{class_name}"
    if not routing_key.startswith(configuration.generated_package_name):
        raise ValueError(
            f"Invalid topic {routing_key}, must start with {configuration.generated_package_name}."
        )
    # As convention, we set the topic name to the message class name,
    # left-stripped of the root generated package name
    # (e.g. mymessaginglib.codegen.vision.control.Start => vision.control.Start)
    routing_key = routing_key.split(f"{configuration.generated_package_name}.", maxsplit=1)[1]
    return routing_key


@dataclasses.dataclass
class Topic:
    """A dataclass to hold get_topic() return value."""

    name: str
    is_task_queue: bool = False


def get_topic(pkg_or_msg: "ProtoBunnyMessage | type[ProtoBunnyMessage] | ModuleType") -> Topic:
    """Return a Topic dataclass object based on a Message (instance or class) or a ModuleType.

    It uses build_routing_key to determine the topic name.
    Note: The topic name can be a routing key with a binding key

    Args:
        pkg_or_msg: a Message instance, a Message class or a module

    Returns: Topic
    """
    topic_name = f"{configuration.messages_prefix}.{build_routing_key(pkg_or_msg)}"
    is_task_queue = ".tasks." in topic_name
    return Topic(name=topic_name, is_task_queue=is_task_queue)


from .core.commons import JsonContent
from .core.results import Result as Result
from .core.results import ReturnCode as ReturnCode
