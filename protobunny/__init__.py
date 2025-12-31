"""
A module providing support for messaging and communication using the configured broker as the backend.

This module includes functionality for publishing, subscribing, and managing message queues,
as well as dynamically managing imports and configurations for the backend.
"""

__all__ = [
    "get_backend",
    "get_message_count",
    "publish",
    "publish_result",
    "subscribe",
    "subscribe_logger",
    "subscribe_results",
    "unsubscribe",
    "unsubscribe_all",
    "unsubscribe_results",
    # from .config
    "GENERATED_PACKAGE_NAME",
    "PACKAGE_NAME",
    "ROOT_GENERATED_PACKAGE_NAME",
    "default_configuration",
    "RequeueMessage",
    "ConnectionError",
    "reset_connection",
    "connect",
    "disconnect",
    "run_forever",
    # from .core
    "commons",
    "results",
]

import logging
import signal
import sys
import textwrap
import typing as tp
from importlib.metadata import version
from types import FrameType, ModuleType

from .backends import BaseSyncConnection, BaseSyncQueue, LoggingSyncQueue
from .config import (  # noqa
    GENERATED_PACKAGE_NAME,
    PACKAGE_NAME,
    ROOT_GENERATED_PACKAGE_NAME,
    default_configuration,
)
from .exceptions import ConnectionError, RequeueMessage
from .helpers import get_backend, get_queue
from .registry import default_registry

if tp.TYPE_CHECKING:
    from .core.results import Result
    from .models import (
        IncomingMessageProtocol,
        LoggerCallback,
        ProtoBunnyMessage,
        SyncCallback,
    )

__version__ = version(PACKAGE_NAME)

log = logging.getLogger(PACKAGE_NAME)


############################
# -- Sync top-level methods
############################


def connect() -> "BaseSyncConnection":
    """Get the singleton async connection."""
    connection_module = get_backend().connection
    conn = connection_module.Connection.get_connection(vhost=connection_module.VHOST)
    return conn


def disconnect() -> None:
    connection_module = get_backend().connection
    conn = connection_module.Connection.get_connection(vhost=connection_module.VHOST)
    conn.disconnect()


def reset_connection() -> "BaseSyncConnection":
    """Reset the singleton connection."""
    connection = connect()
    connection.disconnect()
    return connect()


def publish(message: "ProtoBunnyMessage") -> None:
    """Synchronously publish a message to its corresponding queue.

    This method automatically determines the correct topic based on the
    protobuf message type.

    Args:
        message: The Protobuf message instance to be published.
    """
    queue = get_queue(message)
    queue.publish(message)


def publish_result(
    result: "Result", topic: str | None = None, correlation_id: str | None = None
) -> None:
    """Publish the result message to the result topic of the source message

    Args:
        result: a Result instance.
        topic: The topic to send the message to.
            Default to the source message result topic (e.g. "pb.vision.ExtractFeature.result")
        correlation_id:
    """
    queue = get_queue(result.source)
    queue.publish_result(result, topic, correlation_id)


def subscribe(
    pkg_or_msg: "type[ProtoBunnyMessage] | ModuleType",
    callback: "SyncCallback",
) -> "BaseSyncQueue":
    """Subscribe a callback function to the topic.

    Args:
        pkg_or_msg: The topic to subscribe to as message class or module.
        callback: The callback function that consumes the received message.

    Returns:
        The Queue object. You can access the subscription via its `subscription` attribute.
    """
    register_key = str(pkg_or_msg)

    with default_registry.sync_lock:
        queue = get_queue(pkg_or_msg)
        if queue.shared_queue:
            # It's a task. Handle multiple subscriptions
            # queue = get_queue(pkg_or_msg)
            queue.subscribe(callback)
            default_registry.register_task(register_key, queue)
        else:
            # exclusive queue
            queue = default_registry.get_subscription(register_key) or queue
            queue.subscribe(callback)
            # register subscription to unsubscribe later
            default_registry.register_subscription(register_key, queue)
    return queue


def subscribe_results(
    pkg: "type[ProtoBunnyMessage] | ModuleType",
    callback: "SyncCallback",
) -> "BaseSyncQueue":
    """Subscribe a callback function to the result topic.

    Args:
        pkg:
        callback:
    """
    queue = get_queue(pkg)
    queue.subscribe_results(callback)
    # register subscription to unsubscribe later
    with default_registry.sync_lock:
        default_registry.register_results(pkg, queue)
    return queue


def unsubscribe(
    pkg: "type[ProtoBunnyMessage] | ModuleType",
    if_unused: bool = True,
    if_empty: bool = True,
) -> None:
    """Remove a subscription for a message/package"""
    module_name = pkg.__module__ if hasattr(pkg, "__module__") else pkg.__name__
    registry_key = default_registry.get_key(pkg)

    with default_registry.sync_lock:
        if is_module_tasks(module_name):
            queues = default_registry.get_tasks(registry_key)
            for queue in queues:
                queue.unsubscribe()
            default_registry.unregister_tasks(registry_key)
        else:
            queue = default_registry.get_subscription(registry_key)
            if queue:
                queue.unsubscribe(if_unused=if_unused, if_empty=if_empty)
                default_registry.unregister_subscription(registry_key)


def unsubscribe_results(
    pkg: "type[ProtoBunnyMessage] | ModuleType",
) -> None:
    """Remove all in-process subscriptions for a message/package result topic"""
    with default_registry.sync_lock:
        queue = default_registry.unregister_results(pkg)
        if queue:
            queue.unsubscribe_results()


def unsubscribe_all(if_unused: bool = True, if_empty: bool = True) -> None:
    """
    Remove all active in-process subscriptions.

    This clears standard subscriptions, result subscriptions, and task
    subscriptions, effectively stopping all message consumption for this process.
    """
    with default_registry.sync_lock:
        queues = default_registry.get_all_subscriptions()
        for q in queues:
            q.unsubscribe(if_unused=False, if_empty=False)
        default_registry.unregister_all_subscriptions()
        queues = default_registry.get_all_results()
        for q in queues:
            q.unsubscribe_results()
        default_registry.unregister_all_results()
        queues = default_registry.get_all_tasks(flat=True)
        for q in queues:
            q.unsubscribe(if_unused=if_unused, if_empty=if_empty)
        default_registry.unregister_all_tasks()


def get_message_count(
    msg_type: "ProtoBunnyMessage | type[ProtoBunnyMessage] | ModuleType",
) -> int | None:
    q = get_queue(msg_type)
    count = q.get_message_count()
    return count


def is_module_tasks(module_name: str) -> bool:
    return "tasks" in module_name.split(".")


def default_log_callback(message: "IncomingMessageProtocol", msg_content: str) -> None:
    """Default callback for the logging service"""
    log.info(
        "<%s>(cid:%s) %s",
        message.routing_key,
        message.correlation_id,
        textwrap.shorten(msg_content, width=120),
    )


def _prepare_logger_queue(
    queue_cls: type["LoggingSyncQueue"],
    log_callback: "LoggerCallback | None",
    prefix: str | None,
) -> tuple["LoggingSyncQueue", "LoggerCallback"]:
    """Initializes the requested queue class."""
    resolved_callback = log_callback or default_log_callback
    return queue_cls(prefix), resolved_callback


def subscribe_logger(
    log_callback: "LoggerCallback | None" = None, prefix: str | None = None
) -> "LoggingSyncQueue":
    resolved_callback = log_callback or default_log_callback
    queue, cb = LoggingSyncQueue(prefix), resolved_callback
    queue.subscribe(cb)
    return queue


def run_forever() -> None:
    def shutdown(signum: int, _: FrameType | None) -> None:
        log.info("Shutting down protobunny connections %s", signal.Signals(signum).name)
        unsubscribe_all()
        disconnect()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    log.info("Protobunny Started.")
    signal.pause()
    log.info("Protobunny Stopped.")


def config_lib() -> None:
    """Add the generated package root to the sys.path."""
    lib_root = default_configuration.generated_package_root
    if lib_root and lib_root not in sys.path:
        sys.path.append(lib_root)


#######################################################
# Dynamically added by post_compile.py
from .core import (  # noqa
    commons,
    results,
)

#######################################################
