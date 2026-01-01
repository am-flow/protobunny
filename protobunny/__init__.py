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
    "config",
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
from .conf import (  # noqa
    GENERATED_PACKAGE_NAME,
    PACKAGE_NAME,
    ROOT_GENERATED_PACKAGE_NAME,
    config,
)
from .exceptions import ConnectionError, RequeueMessage
from .helpers import get_backend, get_queue
from .registry import registry

if tp.TYPE_CHECKING:
    from .core.results import Result
    from .models import PBM, IncomingMessageProtocol, LoggerCallback, SyncCallback

__version__ = version(PACKAGE_NAME)

log = logging.getLogger(PACKAGE_NAME)


############################
# -- Sync top-level methods
############################


def connect(**kwargs: tp.Any) -> "BaseSyncConnection":
    """Establishes a connection to the configured messaging broker.

    This method initializes and returns a singleton connection. Subsequent calls
    return the existing connection instance unless it has been explicitly
    disconnected.

    Args:
        **kwargs: Backend-specific connection arguments (e.g., host, port,
            credentials, or protocol-specific tuning parameters).

    Returns:
        BaseSyncConnection: The active connection singleton.
    """
    connection_module = get_backend().connection
    conn = connection_module.Connection.get_connection(vhost=connection_module.VHOST, **kwargs)
    return conn


def disconnect() -> None:
    """Closes the active connection to the broker.

    Gracefully terminates heartbeats and background networking tasks. Safe to
    call if no connection is active.
    """
    connection_module = get_backend().connection
    conn = connection_module.Connection.get_connection(vhost=connection_module.VHOST)
    conn.disconnect()


def reset_connection(**kwargs: tp.Any) -> "BaseSyncConnection":
    """Resets the singleton connection and returns it."""
    connection_module = get_backend().connection
    conn = connection_module.Connection.get_connection(vhost=connection_module.VHOST)
    conn.disconnect()
    return conn.connect(**kwargs)


def publish(message: "PBM") -> None:
    """Publishes a Protobuf message to its corresponding topic.

    The destination topic is automatically derived from the message class and
    package structure. Messages within a '.tasks' package are automatically
    treated as persistent tasks requiring reliable delivery and queuing logic
    across all supported backends.

    Args:
        message: An instance of a class derived from ProtoBunnyMessage.
    """
    queue = get_queue(message)
    queue.publish(message)


def publish_result(
    result: "Result", topic: str | None = None, correlation_id: str | None = None
) -> None:
    """Publishes a processing result to be consumed by results subscribers (See subscribe_results).

    Args:
        result: The Result object containing the response payload and source message from which the Result was generated.
        topic: Optional override for the result topic. Defaults to the
            automatically generated '.result' topic associated with the source message.
        correlation_id: Optional ID used to link this result to a specific request.
    """
    queue = get_queue(result.source)
    queue.publish_result(result, topic, correlation_id)


def subscribe(
    pkg_or_msg: "type[PBM] | ModuleType",
    callback: "SyncCallback",
) -> "BaseSyncQueue":
    """Registers a callback to consume messages from a specific topic or package.

    If a message class is provided, subscribes to that specific topic. If a
    module is provided, subscribes to all message types defined within that module.
    For shared tasks (identified by the '.tasks' convention), Protobunny
    automatically manages shared consumer groups and load balancing.

    Args:
        pkg_or_msg: The message class (type[PBM]) or module to subscribe to.
        callback: The function to execute when a message is received.

    Returns:
        BaseSyncQueue: The queue object managing the active subscription.
    """
    register_key = str(pkg_or_msg)

    with registry.sync_lock:
        queue = get_queue(pkg_or_msg)
        if queue.shared_queue:
            # It's a task. Handle multiple subscriptions
            queue.subscribe(callback)
            registry.register_task(register_key, queue)
        else:
            # exclusive queue
            queue = registry.get_subscription(register_key) or queue
            queue.subscribe(callback)
            registry.register_subscription(register_key, queue)
    return queue


def subscribe_results(
    pkg: "type[PBM] | ModuleType",
    callback: "SyncCallback",
) -> "BaseSyncQueue":
    """Subscribes to result topics for a specific message type or package.

    Used by services that need to listen for completion signals or data
    returned by workers processing specific message types.

    Args:
        pkg: The message class or module whose results should be monitored.
        callback: The function to execute when a result message is received.

    Returns:
        BaseSyncQueue: The queue object managing the result subscription.
    """
    queue = get_queue(pkg)
    queue.subscribe_results(callback)
    # register subscription to unsubscribe later
    with registry.sync_lock:
        registry.register_results(pkg, queue)
    return queue


def unsubscribe(
    pkg: "type[PBM] | ModuleType",
    if_unused: bool = True,
    if_empty: bool = True,
) -> None:
    """Cancels an active subscription for a specific message type or package.

    Args:
        pkg: The message class or module to unsubscribe from.
        if_unused: If True, only unsubscribes if no other callbacks are attached.
        if_empty: If True, only unsubscribes if the buffer is empty.
    """
    module_name = pkg.__module__ if hasattr(pkg, "__module__") else pkg.__name__
    registry_key = registry.get_key(pkg)

    with registry.sync_lock:
        if is_module_tasks(module_name):
            queues = registry.get_tasks(registry_key)
            for queue in queues:
                queue.unsubscribe()
            registry.unregister_tasks(registry_key)
        else:
            queue = registry.get_subscription(registry_key)
            if queue:
                queue.unsubscribe(if_unused=if_unused, if_empty=if_empty)
                registry.unregister_subscription(registry_key)


def unsubscribe_results(
    pkg: "type[PBM] | ModuleType",
) -> None:
    """Remove all in-process subscriptions for a message results topic

    Args:
        pkg: The message class or module to unsubscribe from its results topic.
    """
    with registry.sync_lock:
        queue = registry.unregister_results(pkg)
        if queue:
            queue.unsubscribe_results()


def unsubscribe_all(if_unused: bool = True, if_empty: bool = True) -> None:
    """Stops all message consumption by canceling every in-process subscription.

    Clears standard subscriptions, result listeners, and task workers. Typically
    invoked during graceful application shutdown.

    Args:
        if_unused: Policy for evaluating unused standard queues.
        if_empty: Policy for evaluating empty standard queues.
    """
    with registry.sync_lock:
        queues = registry.get_all_subscriptions()
        for q in queues:
            q.unsubscribe(if_unused=False, if_empty=False)
        registry.unregister_all_subscriptions()
        queues = registry.get_all_results()
        for q in queues:
            q.unsubscribe_results()
        registry.unregister_all_results()
        queues = registry.get_all_tasks(flat=True)
        for q in queues:
            q.unsubscribe(if_unused=if_unused, if_empty=if_empty)
        registry.unregister_all_tasks()


def get_message_count(
    msg_type: "PBM | type[PBM]",
) -> int | None:
    """Retrieves the current number of pending messages in a queue.

    Args:
        msg_type: The message instance or class

    Returns:
        int | None: The count of messages waiting to be processed, or None
            if the backend does not support count retrieval for this queue type.
    """
    q = get_queue(msg_type)
    count = q.get_message_count()
    return count


def get_consumer_count(
    msg_type: "PBM | type[PBM]",
) -> int | None:
    """Retrieves the number of active consumers currently attached to a shared (aka tasks) queue.

    Args:
        msg_type: The message instance or class representing the queue.

    Returns:
        int | None: The number of active subscribers/workers, or None if
            unsupported by the current backend.
    """
    q = get_queue(msg_type)
    count = q.get_consumer_count()
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
    """Subscribes a specialized logging callback to monitor message traffic.

    This creates a logging-specific queue that captures and logs metadata
    for messages matching the optional prefix.

    Args:
        log_callback: A custom function to handle log messages. Defaults
            to `default_log_callback` (logs routing key, cid, and content).
        prefix: An optional subject/topic prefix to filter which messages
            are logged.

    Returns:
        LoggingSyncQueue: The specialized queue object for logging.
    """
    resolved_callback = log_callback or default_log_callback
    queue, cb = LoggingSyncQueue(prefix), resolved_callback
    queue.subscribe(cb)
    return queue


def run_forever() -> None:
    """Blocks the main thread to maintain active message consumption.

    Installs signal handlers for SIGINT and SIGTERM to trigger an orderly
    shutdown, ensuring all subscriptions are canceled and the connection
    is closed before the process exits.
    """

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
    lib_root = config.generated_package_root
    if lib_root and lib_root not in sys.path:
        sys.path.append(lib_root)


#######################################################
# Dynamically added by post_compile.py
from .core import (  # noqa
    commons,
    results,
)

#######################################################
