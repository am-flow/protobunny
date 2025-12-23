"""Implementation of amlogic-messages base queues."""
import asyncio
import inspect
import logging
import textwrap
import threading
import typing as tp
from collections import defaultdict
from types import ModuleType

import betterproto

from protobunny.models import IncomingMessageProtocol

from .backends import (
    BaseAsyncQueue,
    BaseQueue,
    BaseSyncQueue,
    LoggingAsyncQueue,
    LoggingSyncQueue,
    get_backend,
)
from .config import load_config

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

log = logging.getLogger(__name__)

configuration = load_config()

########################
# Base Methods
########################

# subscriptions registries
_registry_lock = threading.Lock()
_async_registry_lock = asyncio.Lock()
subscriptions: dict[type["ProtoBunnyMessage"], "BaseAsyncQueue"] = dict()
results_subscriptions: dict[type["ProtoBunnyMessage"], "BaseAsyncQueue"] = dict()
tasks_subscriptions: dict[type["ProtoBunnyMessage"], list["BaseAsyncQueue"]] = defaultdict(list)
subscriptions_sync: dict[type["ProtoBunnyMessage"], "BaseSyncQueue"] = dict()
results_subscriptions_sync: dict[type["ProtoBunnyMessage"], "BaseQueue"] = dict()
tasks_subscriptions_sync: dict[type["ProtoBunnyMessage"], list["BaseQueue"]] = defaultdict(list)


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


def publish_sync(message: "ProtoBunnyMessage") -> None:
    """Synchronously publish a message to its corresponding queue.

    This method automatically determines the correct topic based on the
    protobuf message type.

    Args:
        message: The Protobuf message instance to be published.
    """
    queue = get_queue(message)
    queue.publish(message)


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
    pkg_or_msg: "ProtoBunnyMessage | type[ProtoBunnyMessage] | ModuleType",
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
    with _registry_lock:
        if "tasks" in module_name.split("."):
            # It's a task. Handle multiple subscriptions
            queue = get_queue(pkg_or_msg)
            queue.subscribe(callback)
            tasks_subscriptions_sync[obj].append(queue)
        else:
            queue = (
                get_queue(pkg_or_msg) if obj not in subscriptions_sync else subscriptions_sync[obj]
            )
            queue.subscribe(callback)
            # register subscription to unsubscribe later
            subscriptions_sync[obj] = queue
    return queue


async def subscribe(
    pkg_or_msg: "ProtoBunnyMessage | type[ProtoBunnyMessage] | ModuleType",
    callback: "AsyncCallback",
) -> "BaseAsyncQueue":
    """
    Subscribe an asynchronous callback to a specific topic or namespace.

    If the module name contains '.tasks', it is treated as a shared task queue
    allowing multiple subscribers. Otherwise, it is treated as a standard
    subscription (exclusive queue).

    Args:
        pkg_or_msg: The message class, instance, or module to subscribe to.
        callback: An async callable that accepts the received message.

    Returns:
        AsyncQueue: The queue object managing the subscription.
    """
    sub_key = type(pkg_or_msg) if isinstance(pkg_or_msg, betterproto.Message) else pkg_or_msg
    module_name = sub_key.__module__ if hasattr(sub_key, "__module__") else sub_key.__name__
    async with _async_registry_lock:
        if "tasks" in module_name.split("."):
            # It's a task. Handle multiple subscriptions
            queue = get_queue(pkg_or_msg)
            await queue.subscribe(callback)
            tasks_subscriptions[sub_key].append(queue)
        else:
            queue = (
                get_queue(pkg_or_msg) if sub_key not in subscriptions else subscriptions[sub_key]
            )
            await queue.subscribe(callback)
            # register subscription to unsubscribe later
            subscriptions[sub_key] = queue
    return queue


async def subscribe_results(
    pkg_or_msg: "ProtoBunnyMessage | type[ProtoBunnyMessage] | ModuleType",
    callback: "AsyncCallback",
) -> "BaseAsyncQueue":
    """Subscribe a callback function to the result topic.

    Args:
        pkg_or_msg:
        callback:
    """
    q = get_queue(pkg_or_msg)
    await q.subscribe_results(callback)
    # register subscription to unsubscribe later
    sub_key = type(pkg_or_msg) if isinstance(pkg_or_msg, betterproto.Message) else pkg_or_msg
    async with _async_registry_lock:
        results_subscriptions[sub_key] = q
    return q


def subscribe_results_sync(
    pkg_or_msg: "ProtoBunnyMessage | type[ProtoBunnyMessage] | ModuleType",
    callback: "SyncCallback",
) -> "BaseSyncQueue":
    """Subscribe a callback function to the result topic.

    Args:
        pkg_or_msg:
        callback:
    """
    queue = get_queue(pkg_or_msg)
    queue.subscribe_results(callback)
    # register subscription to unsubscribe later
    sub_key = type(pkg_or_msg) if isinstance(pkg_or_msg, betterproto.Message) else pkg_or_msg
    with _registry_lock:
        results_subscriptions_sync[sub_key] = queue
    return queue


async def unsubscribe(
    pkg_or_msg: "ProtoBunnyMessage | type[ProtoBunnyMessage] | ModuleType",
    if_unused: bool = True,
    if_empty: bool = True,
) -> None:
    """Remove a subscription for a message/package"""
    sub_key = type(pkg_or_msg) if isinstance(pkg_or_msg, betterproto.Message) else pkg_or_msg
    module_name = sub_key.__module__ if hasattr(sub_key, "__module__") else sub_key.__name__
    async with _async_registry_lock:
        if sub_key in subscriptions:
            q = subscriptions.pop(sub_key)
            await q.unsubscribe(if_unused=if_unused, if_empty=if_empty)
        elif "tasks" in module_name.split("."):
            queues = tasks_subscriptions.pop(sub_key)
            for q in queues:
                await q.unsubscribe(if_unused=if_unused)


def unsubscribe_sync(
    pkg_or_msg: "ProtoBunnyMessage | type[ProtoBunnyMessage] | ModuleType",
    if_unused: bool = True,
    if_empty: bool = True,
) -> None:
    """Remove a subscription for a message/package"""
    sub_key = type(pkg_or_msg) if isinstance(pkg_or_msg, betterproto.Message) else pkg_or_msg
    module_name = sub_key.__module__ if hasattr(sub_key, "__module__") else sub_key.__name__
    with _registry_lock:
        if sub_key in subscriptions_sync:
            queue = subscriptions_sync.pop(sub_key)
            queue.unsubscribe(if_unused=if_unused, if_empty=if_empty)
        elif "tasks" in module_name.split("."):
            queues = tasks_subscriptions_sync.pop(sub_key, [])
            for queue in queues:
                queue.unsubscribe()


def unsubscribe_results_sync(
    pkg_or_msg: "ProtoBunnyMessage | type[ProtoBunnyMessage] | ModuleType",
) -> None:
    """Remove all in-process subscriptions for a message/package result topic"""
    sub_key = type(pkg_or_msg) if isinstance(pkg_or_msg, betterproto.Message) else pkg_or_msg
    with _registry_lock:
        if sub_key in results_subscriptions_sync:
            q = results_subscriptions_sync.pop(sub_key)
            q.unsubscribe_results()


async def unsubscribe_results(
    pkg_or_msg: "ProtoBunnyMessage | type[ProtoBunnyMessage] | ModuleType",
) -> None:
    """Remove all in-process subscriptions for a message/package result topic"""
    sub_key = type(pkg_or_msg) if isinstance(pkg_or_msg, betterproto.Message) else pkg_or_msg
    async with _async_registry_lock:
        if sub_key in results_subscriptions:
            q = subscriptions.pop(sub_key)
            await q.unsubscribe_results()


def unsubscribe_all_sync(if_unused: bool = True, if_empty: bool = True) -> None:
    """
    Remove all active in-process subscriptions.

    This clears standard subscriptions, result subscriptions, and task
    subscriptions, effectively stopping all message consumption for this process.
    """
    with _registry_lock:
        for q in subscriptions_sync.values():
            q.unsubscribe(if_unused=False, if_empty=False)
        subscriptions_sync.clear()
        for q in results_subscriptions_sync.values():
            q.unsubscribe_results()
        results_subscriptions_sync.clear()
        for queues in tasks_subscriptions_sync.values():
            for q in queues:
                q.unsubscribe(if_unused=if_unused, if_empty=if_empty)
        tasks_subscriptions_sync.clear()


async def unsubscribe_all(if_unused: bool = True, if_empty: bool = True) -> None:
    """
    Asynchronously remove all active in-process subscriptions.

    This clears standard subscriptions, result subscriptions, and task
    subscriptions, effectively stopping all message consumption for this process.
    """
    async with _async_registry_lock:
        for q in subscriptions.values():
            await q.unsubscribe(if_unused=False, if_empty=False)
        subscriptions.clear()
        for q in results_subscriptions.values():
            await q.unsubscribe_results()
        results_subscriptions.clear()
        for queues in tasks_subscriptions.values():
            for q in queues:
                await q.unsubscribe(if_unused=if_unused, if_empty=if_empty)
        tasks_subscriptions.clear()


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
