import functools
import importlib
import sys
import typing
from types import ModuleType

import betterproto

from .config import default_configuration

if typing.TYPE_CHECKING:
    from .asyncio.backends import BaseAsyncQueue
    from .backends import BaseSyncQueue
    from .models import ProtoBunnyMessage


def get_topic(pkg_or_msg: "ProtoBunnyMessage | type[ProtoBunnyMessage] | ModuleType") -> str:
    """Return a Topic dataclass object based on a Message (instance or class) or a ModuleType.

    It uses build_routing_key to determine the topic name.
    Note: The topic name can be a routing key with a binding key

    Args:
        pkg_or_msg: a Message instance, a Message class or a module

    Returns: topic string
    """
    delimiter = default_configuration.backend_config.topic_delimiter
    return f"{default_configuration.messages_prefix}{delimiter}{build_routing_key(pkg_or_msg)}"


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
    backend = backend or default_configuration.backend
    module = ".asyncio" if default_configuration.use_async else ""
    module_name = f"protobunny{module}.backends.{backend}"
    if module_name in sys.modules:
        return sys.modules[module_name]
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        suggestion = ""
        if backend not in default_configuration.available_backends:
            suggestion = f" Invalid backend or backend not supported.\nAvailable backends: {default_configuration.available_backends}"
        else:
            suggestion = (
                f" Install the backend with pip install protobunny[{backend}]."
                if backend != "python"
                else suggestion
            )
        sys.exit(f"Could not import backend: {exc}.{suggestion}")
    return module


def get_queue(
    pkg_or_msg: "ProtoBunnyMessage | type['ProtoBunnyMessage'] | ModuleType",
    backend_name: str | None = None,
) -> "BaseSyncQueue|BaseAsyncQueue":
    """Factory method to get an AsyncQueue/SyncQueue instance based on
      - the message type (e.g. mylib.subpackage.subsubpackage.MyMessage)
      - the mode (e.g. async)
      - the configured backend or the parameter passed (e.g. "rabbitmq")

    Args:
        pkg_or_msg: A message instance, a message class, or a module
            containing message definitions.
        backend_name: backend name to use

    Returns:
        Async/SyncQueue: A queue instance configured for the relevant topic.
    """
    backend_name = backend_name or default_configuration.backend
    queue_type = "AsyncQueue" if default_configuration.use_async else "SyncQueue"
    return getattr(get_backend(backend=backend_name).queues, queue_type)(get_topic(pkg_or_msg))


@functools.lru_cache(maxsize=100)
def _build_routing_key(module: str, cls_name: str) -> str:
    # Build the routing key from the module and class name
    backend = default_configuration.backend_config
    delimiter = backend.topic_delimiter
    routing_key = f"{module}.{cls_name}"
    config = default_configuration
    if not routing_key.startswith(config.generated_package_name):
        raise ValueError(
            f"Invalid topic {routing_key}, must start with {config.generated_package_name}."
        )
    # As convention, we set the topic name to the message class name,
    # left-stripped of the root generated package name
    # (e.g. my_messaging_lib.codegen.vision.control.Start => vision.control.Start)
    routing_key = routing_key.split(f"{config.generated_package_name}.", maxsplit=1)[1].replace(
        ".", delimiter
    )
    return routing_key


def build_routing_key(
    pkg_or_msg: "ProtoBunnyMessage | type[ProtoBunnyMessage] | ModuleType",
) -> str:
    """Returns a routing key based on a message instance, a message class, or a module.
    The string will be later composed with the configured message-prefix to build the exact topic name.

    This is the main logic that builds keys strings for topics/streaming, adding wildcards when needed

    Examples:
        build_routing_key(mymessaginglib.vision.control) -> "vision.control.#" routing with binding key
        build_routing_key(mymessaginglib.vision.control.Start) -> "vision.control.Start" direct routing
        build_routing_key(mymessaginglib.vision.control.Start()) -> "vision.control.Start" direct routing

    Args:
        pkg_or_msg: a Message instance, class or module to mymessaginglib codegen packages

    Returns: a routing key based on the type of message or package

    """
    backend = default_configuration.backend_config
    wildcard = backend.multi_wildcard_delimiter
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
        class_name = wildcard  # wildcard routing key for subscribing to all messages in a module

    return _build_routing_key(module_name, class_name)
