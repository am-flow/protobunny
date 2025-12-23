"""Implementation of protobunny core methods
They will be imported in __init__.py

>>> import protobunny as pb
>>> from tests import tests
>>> msg = tests.TestMessage(content="test", number=123, color=tests.Color.GREEN)
>>> pb.get_connection_sync()  # connect to backend
>>> pb.publish_sync(msg)  # publish message to its queue
"""
import inspect
import itertools
import logging
import textwrap
import typing as tp
from types import ModuleType

import betterproto

from protobunny.models import IncomingMessageProtocol, ProtoBunnyMessage

from .backends import (
    BaseAsyncQueue,
    BaseSyncQueue,
    LoggingAsyncQueue,
    LoggingSyncQueue,
    get_backend,
)
from .config import configuration

if tp.TYPE_CHECKING:
    from .core.results import Result

from .models import (
    AsyncCallback,
    LoggerCallback,
    LogQueue,
    ProtoBunnyMessage,
    SyncCallback,
    get_topic,
)
from .registry import default_registry

log = logging.getLogger(__name__)


########################
# Base Methods
########################


def get_queue(
    pkg_or_msg: "ProtoBunnyMessage | type['ProtoBunnyMessage'] | ModuleType",
    backend: str | None = None,
) -> "BaseSyncQueue | BaseAsyncQueue":
    """Factory method to get an AsyncQueue/SyncQueue instance based on
      - the message type (e.g. mylib.subpackage.subsubpackage.MyMessage)
      - the mode (e.g. async)
      - the configured backend or the parameter passed (e.g. "rabbitmq")

    Args:
        pkg_or_msg: A message instance, a message class, or a module
            containing message definitions.
        backend: backend name to use

    Returns:
        Async/SyncQueue: A queue instance configured for the relevant topic.
    """
    return getattr(get_backend(backend=backend).queues, f"{configuration.mode.capitalize()}Queue")(
        get_topic(pkg_or_msg)
    )


# -- Async top-level methods


async def publish(message: "ProtoBunnyMessage") -> None:
    """Asynchronously publish a message to its corresponding queue.

    Args:
        message: The Protobuf message instance to be published.
    """
    queue = get_queue(message)
    await queue.publish(message)


async def publish_result(
    result: "Result", topic: str | None = None, correlation_id: str | None = None
) -> None:
    """
    Asynchronously publish a result message to a specific result topic.

    Args:
        result: The Result object to publish.
        topic: Optional override for the destination topic. Defaults to the
            source message's result topic (e.g., "namespace.Message.result").
        correlation_id: Optional ID to link the result to the original request.
    """
    queue = get_queue(result.source)
    await queue.publish_result(result, topic, correlation_id)


async def subscribe(
    pkg: "type[ProtoBunnyMessage] | ModuleType",
    callback: "AsyncCallback",
) -> "BaseAsyncQueue":
    """
    Subscribe an asynchronous callback to a specific topic or namespace.

    If the module name contains '.tasks', it is treated as a shared task queue
    allowing multiple subscribers. Otherwise, it is treated as a standard
    subscription (exclusive queue).

    Args:
        pkg: The message class, instance, or module to subscribe to.
        callback: An async callable that accepts the received message.

    Returns:
        AsyncQueue: The queue object managing the subscription.
    """
    # obj = type(pkg) if isinstance(pkg, betterproto.Message) else pkg
    module_name = pkg.__name__ if inspect.ismodule(pkg) else pkg.__module__
    registry_key = str(pkg)
    async with default_registry.lock:
        is_task = "tasks" in module_name.split(".")
        if is_task:
            # It's a task. Handle multiple in-process subscriptions
            queue = get_queue(pkg)
            await queue.subscribe(callback)
            default_registry.register_task(registry_key, queue)
        else:
            # exclusive queue
            queue = default_registry.get_subscription(registry_key) or get_queue(pkg)
            # queue already exists, but not subscribed yet (otherwise raise ValueError)
            await queue.subscribe(callback)
            default_registry.register_subscription(registry_key, queue)
        return queue


async def unsubscribe(
    pkg: "type[ProtoBunnyMessage] | ModuleType",
    if_unused: bool = True,
    if_empty: bool = True,
) -> None:
    """Remove a subscription for a message/package"""

    # obj = type(pkg) if isinstance(pkg, betterproto.Message) else pkg
    module_name = pkg.__name__ if inspect.ismodule(pkg) else pkg.__module__
    registry_key = default_registry.get_key(pkg)
    async with default_registry.lock:
        if "tasks" in module_name.split("."):
            queues = default_registry.get_tasks(registry_key)
            for q in queues:
                await q.unsubscribe(if_unused=if_unused)
            default_registry.unregister_tasks(registry_key)
        else:
            queue = default_registry.get_subscription(registry_key)
            if queue:
                await queue.unsubscribe(if_unused=if_unused, if_empty=if_empty)
            default_registry.unregister_subscription(registry_key)


async def unsubscribe_results(
    pkg: "type[ProtoBunnyMessage] | ModuleType",
) -> None:
    """Remove all in-process subscriptions for a message/package result topic"""
    async with default_registry.lock:
        queue = default_registry.get_results(pkg)
        if queue:
            await queue.unsubscribe_results()
        default_registry.unregister_results(pkg)


async def unsubscribe_all(if_unused: bool = True, if_empty: bool = True) -> None:
    """
    Asynchronously remove all active in-process subscriptions.

    This clears standard subscriptions, result subscriptions, and task
    subscriptions, effectively stopping all message consumption for this process.
    """
    async with default_registry.lock:
        queues = itertools.chain(
            default_registry.get_all_subscriptions(), default_registry.get_all_tasks(flat=True)
        )
        for queue in queues:
            await queue.unsubscribe(if_unused=False, if_empty=False)
        default_registry.unregister_all_subscriptions()
        default_registry.unregister_all_tasks()
        queues = default_registry.get_all_results()
        for queue in queues:
            await queue.unsubscribe_results()
        default_registry.unregister_all_results()


async def subscribe_results(
    pkg: "type[ProtoBunnyMessage] | ModuleType",
    callback: "AsyncCallback",
) -> "BaseAsyncQueue":
    """Subscribe a callback function to the result topic.

    Args:
        pkg:
        callback:
    """
    queue = get_queue(pkg)
    await queue.subscribe_results(callback)
    # register subscription to unsubscribe later
    async with default_registry.lock:
        default_registry.register_results(pkg, queue)
    return queue


# -- Sync top-level methods


def publish_sync(message: "ProtoBunnyMessage") -> None:
    """Synchronously publish a message to its corresponding queue.

    This method automatically determines the correct topic based on the
    protobuf message type.

    Args:
        message: The Protobuf message instance to be published.
    """
    queue = get_queue(message)
    queue.publish(message)


def publish_result_sync(
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


def subscribe_sync(
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
    obj = type(pkg_or_msg) if isinstance(pkg_or_msg, betterproto.Message) else pkg_or_msg
    module_name = obj.__name__ if inspect.ismodule(obj) else obj.__module__
    register_key = str(pkg_or_msg)

    with default_registry.sync_lock:
        is_task = "tasks" in module_name.split(".")
        if is_task:
            # It's a task. Handle multiple subscriptions
            queue = get_queue(pkg_or_msg)
            queue.subscribe(callback)
            default_registry.register_task(register_key, queue)
        else:
            # exclusive queue
            queue = default_registry.get_subscription(register_key) or get_queue(pkg_or_msg)
            queue.subscribe(callback)
            # register subscription to unsubscribe later
            default_registry.register_subscription(register_key, queue)
    return queue


def subscribe_results_sync(
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
        # results_subscriptions_sync[pkg] = queue
    return queue


def unsubscribe_sync(
    pkg: "type[ProtoBunnyMessage] | ModuleType",
    if_unused: bool = True,
    if_empty: bool = True,
) -> None:
    """Remove a subscription for a message/package"""
    module_name = pkg.__module__ if hasattr(pkg, "__module__") else pkg.__name__
    registry_key = default_registry.get_key(pkg)

    with default_registry.sync_lock:
        if "tasks" in module_name.split("."):
            queues = default_registry.get_tasks(registry_key)
            for queue in queues:
                queue.unsubscribe()
            default_registry.unregister_tasks(registry_key)
        else:
            queue = default_registry.get_subscription(registry_key)
            # if not queue:
            #     raise ValueError(f"No subscription found for {registry_key}")
            if queue:
                queue.unsubscribe(if_unused=if_unused, if_empty=if_empty)
            default_registry.unregister_subscription(registry_key)


def unsubscribe_results_sync(
    pkg: "type[ProtoBunnyMessage] | ModuleType",
) -> None:
    """Remove all in-process subscriptions for a message/package result topic"""
    with default_registry.sync_lock:
        queue = default_registry.unregister_results(pkg)
        if queue:
            queue.unsubscribe_results()


def unsubscribe_all_sync(if_unused: bool = True, if_empty: bool = True) -> None:
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


def get_message_count_sync(
    msg_type: "ProtoBunnyMessage | type[ProtoBunnyMessage] | ModuleType",
) -> int | None:
    q = get_queue(msg_type)
    count = q.get_message_count()
    return count


async def get_message_count(
    msg_type: "ProtoBunnyMessage | type[ProtoBunnyMessage] | ModuleType",
) -> int | None:
    q = get_queue(msg_type)
    count = await q.get_message_count()
    return count


def default_log_callback(message: "IncomingMessageProtocol", msg_content: str) -> None:
    """Default callback for the logging service"""
    log.info(
        "<%s>(cid:%s) %s",
        message.routing_key,
        message.correlation_id,
        textwrap.shorten(msg_content, width=120),
    )


def _prepare_logger_queue(
    queue_cls: type[LogQueue], log_callback: "LoggerCallback | None", prefix: str | None
) -> tuple[LogQueue, "LoggerCallback"]:
    """Initializes the requested queue class."""
    resolved_callback = log_callback or default_log_callback
    resolved_prefix = prefix or configuration.messages_prefix
    return queue_cls(resolved_prefix), resolved_callback


async def subscribe_logger(
    log_callback: "LoggerCallback | None" = None, prefix: str | None = None
) -> "LoggingAsyncQueue":
    queue, cb = _prepare_logger_queue(LoggingAsyncQueue, log_callback, prefix)
    await queue.subscribe(cb)
    return queue


def subscribe_logger_sync(
    log_callback: "LoggerCallback | None" = None, prefix: str | None = None
) -> "LoggingSyncQueue":
    queue, cb = _prepare_logger_queue(LoggingSyncQueue, log_callback, prefix)
    queue.subscribe(cb)
    return queue
