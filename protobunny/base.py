"""Implementation of amlogic-messages base queues."""
import asyncio
import logging
import textwrap
import threading
import typing as tp
from collections import defaultdict
from types import ModuleType

import aio_pika
from betterproto import Message

from .config import load_config

if tp.TYPE_CHECKING:
    from .core.results import Result
    from .models import (
        ProtoBunnyMessage,
    )
from .models import get_topic
from .queues import AsyncQueue, LoggingAsyncQueue, LoggingSyncQueue, Queue

log = logging.getLogger(__name__)

configuration = load_config()

########################
# Base Methods
########################

# subscriptions registries
_registry_lock = threading.Lock()
_async_registry_lock = asyncio.Lock()
subscriptions: dict[tp.Any, "Queue | AsyncQueue"] = dict()
results_subscriptions: dict[tp.Any, "Queue | AsyncQueue"] = dict()
tasks_subscriptions: dict[tp.Any, list["Queue | AsyncQueue"]] = defaultdict(list)


def get_queue(
    pkg: "ProtoBunnyMessage | type['ProtoBunnyMessage'] | ModuleType",
) -> "Queue | AsyncQueue":
    """Factory method to get a Queue instance based on the message type

    Args:
        pkg:

    Returns: a Queue instance

    """
    topic = get_topic(pkg)
    return AsyncQueue(topic) if configuration.use_async else Queue(topic)


def publish_sync(message: "ProtoBunnyMessage") -> None:
    """Publish the message on its own queue

    Args:
        message:
    """
    queue = get_queue(message)
    queue.publish(message)


async def publish(message: "ProtoBunnyMessage") -> None:
    """Publish the message on its own queue

    Args:
        message:
    """
    queue = get_queue(message)
    await queue.publish(message)


async def publish_result(
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
    callback: tp.Callable[["ProtoBunnyMessage"], tp.Any],
) -> "Queue":
    """Subscribe a callback function to the topic.

    Args:
        pkg_or_msg: The topic to subscribe to as message class or module.
        callback: The callback function that consumes the received message.

    Returns:
        The Queue object. You can access the subscription via its `subscription` attribute.
    """

    sub_key = type(pkg_or_msg) if isinstance(pkg_or_msg, Message) else pkg_or_msg
    module_name = sub_key.__module__ if hasattr(sub_key, "__module__") else sub_key.__name__
    with _registry_lock:
        if "tasks" in module_name.split("."):
            # It's a task. Handle multiple subscriptions
            queue = get_queue(pkg_or_msg)
            queue.subscribe(callback)
            tasks_subscriptions[sub_key].append(queue)
        else:
            queue = (
                get_queue(pkg_or_msg) if sub_key not in subscriptions else subscriptions[sub_key]
            )
            queue.subscribe(callback)
            # register subscription to unsubscribe later
            subscriptions[sub_key] = queue
    return queue


async def subscribe(
    pkg_or_msg: "ProtoBunnyMessage | type[ProtoBunnyMessage] | ModuleType",
    callback: tp.Callable[["ProtoBunnyMessage"], tp.Any],
) -> "AsyncQueue":
    """Subscribe a callback function to the topic.

    Args:
        pkg_or_msg: The topic to subscribe to as message class or module.
        callback: The callback function that consumes the received message.

    Returns:
        The Queue object. You can access the subscription via its `subscription` attribute.
    """
    sub_key = type(pkg_or_msg) if isinstance(pkg_or_msg, Message) else pkg_or_msg
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
    pkg_or_msg: "ProtoBunnyMessage | type[Message] | ModuleType",
    callback: tp.Callable[["Result"], tp.Any],
) -> "AsyncQueue":
    """Subscribe a callback function to the result topic.

    Args:
        pkg_or_msg:
        callback:
    """
    q = get_queue(pkg_or_msg)
    await q.subscribe_results(callback)
    # register subscription to unsubscribe later
    sub_key = type(pkg_or_msg) if isinstance(pkg_or_msg, Message) else pkg_or_msg
    async with _async_registry_lock:
        results_subscriptions[sub_key] = q
    return q


def subscribe_results_sync(
    pkg_or_msg: "ProtoBunnyMessage | type[Message] | ModuleType",
    callback: tp.Callable[["Result"], tp.Any],
) -> "Queue":
    """Subscribe a callback function to the result topic.

    Args:
        pkg_or_msg:
        callback:
    """
    q = get_queue(pkg_or_msg)
    q.subscribe_results(callback)
    # register subscription to unsubscribe later
    sub_key = type(pkg_or_msg) if isinstance(pkg_or_msg, Message) else pkg_or_msg
    with _registry_lock:
        results_subscriptions[sub_key] = q
    return q


async def unsubscribe(
    pkg_or_msg: "ProtoBunnyMessage | type[Message] | ModuleType",
    if_unused: bool = True,
    if_empty: bool = True,
) -> None:
    """Remove all in-process subscriptions for a message/package"""
    sub_key = type(pkg_or_msg) if isinstance(pkg_or_msg, Message) else pkg_or_msg
    async with _async_registry_lock:
        if sub_key in subscriptions:
            q = subscriptions.pop(sub_key)
            await q.unsubscribe(if_unused=if_unused, if_empty=if_empty)
        module_name = sub_key.__module__ if hasattr(sub_key, "__module__") else sub_key.__name__
        if "tasks" in module_name.split("."):
            queues = tasks_subscriptions.pop(sub_key)
            for q in queues:
                await q.unsubscribe()


def unsubscribe_sync(
    pkg_or_msg: "ProtoBunnyMessage | type[Message] | ModuleType",
    if_unused: bool = True,
    if_empty: bool = True,
) -> None:
    """Remove all in-process subscriptions for a message/package"""
    sub_key = type(pkg_or_msg) if isinstance(pkg_or_msg, Message) else pkg_or_msg
    with _registry_lock:
        if sub_key in subscriptions:
            q = subscriptions.pop(sub_key)
            q.unsubscribe(if_unused=if_unused, if_empty=if_empty)
        module_name = sub_key.__module__ if hasattr(sub_key, "__module__") else sub_key.__name__
        if "tasks" in module_name.split("."):
            queues = tasks_subscriptions.pop(sub_key)
            for q in queues:
                q.unsubscribe()


def unsubscribe_results_sync(pkg_or_msg: "ProtoBunnyMessage | type[Message] | ModuleType") -> None:
    """Remove all in-process subscriptions for a message/package result topic"""
    sub_key = type(pkg_or_msg) if isinstance(pkg_or_msg, Message) else pkg_or_msg
    with _registry_lock:
        if sub_key in results_subscriptions:
            q = subscriptions.pop(sub_key)
            q.unsubscribe_results()


async def unsubscribe_results(pkg_or_msg: "ProtoBunnyMessage | type[Message] | ModuleType") -> None:
    """Remove all in-process subscriptions for a message/package result topic"""
    sub_key = type(pkg_or_msg) if isinstance(pkg_or_msg, Message) else pkg_or_msg
    async with _async_registry_lock:
        if sub_key in results_subscriptions:
            q = subscriptions.pop(sub_key)
            await q.unsubscribe_results()


def unsubscribe_all_sync() -> None:
    """Remove all in-process subscriptions"""
    with _registry_lock:
        for q in subscriptions.values():
            q.unsubscribe(if_unused=False, if_empty=False)
        subscriptions.clear()
        for q in results_subscriptions.values():
            q.unsubscribe_results()
        results_subscriptions.clear()
        for queues in tasks_subscriptions.values():
            for q in queues:
                q.unsubscribe()
        tasks_subscriptions.clear()


async def unsubscribe_all() -> None:
    """Remove all in-process subscriptions"""
    async with _async_registry_lock:
        for q in subscriptions.values():
            await q.unsubscribe(if_unused=False, if_empty=False)
        subscriptions.clear()
        for q in results_subscriptions.values():
            await q.unsubscribe_results()
        results_subscriptions.clear()
        for queues in tasks_subscriptions.values():
            for q in queues:
                await q.unsubscribe()
        tasks_subscriptions.clear()


def get_message_count_sync(msg_type: "ProtoBunnyMessage | type[Message]") -> int:
    q = get_queue(msg_type)
    count = q.get_message_count()
    return count


async def get_message_count(msg_type: "ProtoBunnyMessage | type[Message]") -> int:
    q = get_queue(msg_type)
    count = await q.get_message_count()
    return count


def default_log_callback_sync(message: aio_pika.IncomingMessage, msg_content: str):
    """Log queue message to stdout."""
    log.info(
        "<%s>(cid:%s) %s",
        message.routing_key,
        message.correlation_id,
        textwrap.shorten(msg_content, width=80),
    )


async def default_log_callback(message: aio_pika.IncomingMessage, msg_content: str) -> None:
    default_log_callback_sync(message, msg_content)


async def subscribe_logger(
    log_callback: tp.Callable | None = None, prefix: str | None = None
) -> "LoggingAsyncQueue":
    log_callback = log_callback or default_log_callback
    prefix = prefix or configuration.messages_prefix
    q = LoggingAsyncQueue(prefix)
    await q.subscribe(log_callback)
    return q


def subscribe_logger_sync(
    log_callback: tp.Callable = None, prefix: str | None = None
) -> "LoggingSyncQueue":
    log_callback = log_callback or default_log_callback_sync
    prefix = prefix or configuration.messages_prefix
    q = LoggingSyncQueue(prefix)
    q.subscribe(log_callback)
    return q
