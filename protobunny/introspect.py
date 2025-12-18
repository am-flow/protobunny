import functools
import importlib
import typing as tp
from types import ModuleType

from betterproto import Message

import protobunny as pb
from protobunny.config import load_config

# Define a type variable that represents any subclass of betterproto.Message and add the mixin interface
ProtoBunnyMessage = tp.Union["Message", "MessageMixin"]

configuration = load_config()


def _get_submodule(
    package: ModuleType, paths: list[str]
) -> type[ProtoBunnyMessage] | ModuleType | None:
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
def get_message_class_from_topic(topic: str) -> type[ProtoBunnyMessage] | None:
    """Return the message class from a topic with lazy import of the user library

    Args:
        topic: the RabbitMQ topic that represents the message queue

    Returns: the message class
    """
    if topic.endswith(".result"):
        message_type = pb.results.Result
    else:
        topic = topic.removeprefix(f"{configuration.messages_prefix}.")
        codegen_module = importlib.import_module(configuration.generated_package_name)
        message_type = _get_submodule(codegen_module, topic.split("."))
    return message_type


@functools.lru_cache
def get_message_class_from_type_url(url: str) -> type[ProtoBunnyMessage]:
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


def build_routing_key(pkg_or_msg: ProtoBunnyMessage | type[ProtoBunnyMessage] | ModuleType) -> str:
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
    if isinstance(pkg_or_msg, Message):
        module_name = pkg_or_msg.__module__
        class_name = pkg_or_msg.__class__.__name__
    elif isinstance(pkg_or_msg, type(Message)):
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
