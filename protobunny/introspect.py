import importlib
import os
import typing as tp
from pathlib import Path
from types import ModuleType

from betterproto import Message

import protobunny as pb
from protobunny.config import load_config

# Define a type variable that represents any subclass of betterproto.Message
AMMessage = tp.TypeVar("AMMessage", bound=Message)
ProtoBunnyMessage = tp.Union["AMMessage", "MessageMixin"]
conf = load_config()

user_messages_directory = conf.get("messages-directory", "messages")
user_messages_prefix = conf.get("messages-prefix", "pb.")
user_generated_package_name = conf.get("generated-package-name", "codegen")
user_project_root = conf["project-root"]

Path(user_messages_directory).mkdir(parents=True, exist_ok=True)
Path(user_generated_package_name.replace(".", os.sep)).mkdir(parents=True, exist_ok=True)


def _get_submodule(package: ModuleType, paths: list[str]) -> type[AMMessage] | ModuleType | None:
    """Import module/class from package

    Args:
        package: Root package to import the submodule from (e.g. amlogic_messages.codegen)
        paths: Path to submodule/class expressed as list (e.g. ['vision', 'control', 'Start'])

    Note: you can get the path list by splitting the topic
    >>> msg = pb.vision.Start()
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


def get_message_class_from_topic(topic: str) -> type[AMMessage] | None:
    """

    Args:
        topic:

    Returns:

    """
    if topic.endswith(".result"):
        message_type = pb.results.Result
    else:
        topic = topic.removeprefix(f"{user_messages_prefix}.")
        if len(user_generated_package_name.split(".")) == 1:
            print(f"--Dinamically importing {user_generated_package_name} from {user_project_root}")
            codegen = importlib.import_module(
                f".{user_generated_package_name}", package=user_project_root
            )
        else:
            print(f"--Dinamically importing {user_generated_package_name}")
            codegen = importlib.import_module(user_generated_package_name)
        message_type = _get_submodule(codegen, topic.split("."))
    return message_type


def get_message_class_from_type_url(url: str) -> type[AMMessage]:
    """

    Args:
        url:

    Returns:

    """
    module_path, clz = url.rsplit(".", 1)
    if not module_path.startswith(user_generated_package_name):
        raise ValueError("Invalid type url")
    module = importlib.import_module(module_path)
    message_type = getattr(module, clz)
    return message_type


def build_routing_key(pkg_or_msg: AMMessage | type[AMMessage] | ModuleType) -> str:
    """Return a routing key based on a message instance, a message class, or a module.

    Examples:
        build_routing_key(amlogic_messages.vision.control) -> "vision.control.#" routing with binding key
        build_routing_key(amlogic_messages.vision.control.Start) -> "vision.control.Start" direct routing
        build_routing_key(amlogic_messages.vision.control.Start()) -> "vision.control.Start" direct routing

    Args:
        pkg_or_msg: a Message instance, class or module to amlogic_messages.codegen packages

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
    if not routing_key.startswith(user_generated_package_name):
        raise ValueError("Invalid topic")
    # As convention, we set the topic name to the message class name,
    # left-stripped of the root generated package name
    # (e.g. amlogic_messages.codegen.vision.control.Start => vision.control.Start)
    routing_key = routing_key.split(f"{user_generated_package_name}.")[1]
    return routing_key


# TODO in post_compile.py, add MessageMixin to the generated classes definitions instead of patching here
def decorate_protobuf_classes() -> None:
    """Decorate all Message classes with MessageMixin."""
    am_classes = [
        clz for clz in Message.__subclasses__() if user_generated_package_name in clz.__module__
    ]
    for clz in am_classes:
        # Add the MessageMixin as base class
        clz.__bases__ += (pb.base.MessageMixin,)
        # Patch the original betterproto.Message methods
        # to support transparent serialization/deserialization of JsonContent fields
        # mypy ignore: see https://github.com/python/mypy/issues/708
        clz.parse = pb.base.MessageMixin.parse  # type: ignore
        clz.__bytes__ = pb.base.MessageMixin.__bytes__  # type: ignore
        clz.from_dict = pb.base.MessageMixin.from_dict  # type: ignore
        clz.to_dict = pb.base.MessageMixin.to_dict  # type: ignore
        clz.to_json = pb.base.MessageMixin.to_json  # type: ignore
        clz.to_pydict = pb.base.MessageMixin.to_pydict  # type: ignore
