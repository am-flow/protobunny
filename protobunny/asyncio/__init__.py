"""
A module providing support for messaging and communication using RabbitMQ as the backend.

This module includes functionality for publishing, subscribing, and managing message queues,
as well as dynamically managing imports and configurations for RabbitMQ-based communication
logics. It enables both synchronous and asynchronous operations, while also supporting
connection resetting and management.

Modules and functionality are primarily imported from the core RabbitMQ backend, dynamically
generated package-specific configurations, and other base utilities. Exports are adjusted
as per the backend configuration.

"""

__all__ = [
    "get_message_count",
    "get_queue",
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
    "config_lib",
    # from .core
    "commons",
    "results",
]

import asyncio
import inspect
import itertools
import logging
import signal
import textwrap
import typing as tp
from importlib.metadata import version

#######################################################
from ..conf import (  # noqa
    GENERATED_PACKAGE_NAME,
    PACKAGE_NAME,
    ROOT_GENERATED_PACKAGE_NAME,
    config,
)
from ..exceptions import ConnectionError, RequeueMessage
from ..registry import registry

if tp.TYPE_CHECKING:
    from types import ModuleType

    from ..core.results import Result
    from ..models import (
        AsyncCallback,
        IncomingMessageProtocol,
        LoggerCallback,
        ProtoBunnyMessage,
    )


from .. import config_lib as config_lib
from ..helpers import get_backend, get_queue
from .backends import BaseAsyncConnection, BaseAsyncQueue, LoggingAsyncQueue

__version__ = version(PACKAGE_NAME)


log = logging.getLogger(PACKAGE_NAME)


############################
# -- Async top-level methods
############################


async def connect() -> "BaseAsyncConnection":
    """Get the singleton async connection."""
    connection_module = get_backend().connection
    conn = await connection_module.Connection.get_connection(vhost=connection_module.VHOST)
    return conn


async def disconnect() -> None:
    connection_module = get_backend().connection
    conn = await connection_module.Connection.get_connection(vhost=connection_module.VHOST)
    await conn.disconnect()


async def reset_connection() -> "BaseAsyncConnection":
    """Reset the singleton connection."""
    connection = await connect()
    await connection.disconnect()
    return await connect()


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
    async with registry.lock:
        if is_module_tasks(module_name):
            # It's a task. Handle multiple in-process subscriptions
            queue = get_queue(pkg)
            await queue.subscribe(callback)
            registry.register_task(registry_key, queue)
        else:
            # exclusive queue
            queue = registry.get_subscription(registry_key) or get_queue(pkg)
            # queue already exists, but not subscribed yet (otherwise raise ValueError)
            await queue.subscribe(callback)
            registry.register_subscription(registry_key, queue)
        return queue


async def unsubscribe(
    pkg: "type[ProtoBunnyMessage] | ModuleType",
    if_unused: bool = True,
    if_empty: bool = True,
) -> None:
    """Remove a subscription for a message/package"""

    # obj = type(pkg) if isinstance(pkg, betterproto.Message) else pkg
    module_name = pkg.__name__ if inspect.ismodule(pkg) else pkg.__module__
    registry_key = registry.get_key(pkg)
    async with registry.lock:
        if is_module_tasks(module_name):
            queues = registry.get_tasks(registry_key)
            for q in queues:
                await q.unsubscribe(if_unused=if_unused)
            registry.unregister_tasks(registry_key)
        else:
            queue = registry.get_subscription(registry_key)
            if queue:
                await queue.unsubscribe(if_unused=if_unused, if_empty=if_empty)
            registry.unregister_subscription(registry_key)


async def unsubscribe_results(
    pkg: "type[ProtoBunnyMessage] | ModuleType",
) -> None:
    """Remove all in-process subscriptions for a message/package result topic"""
    async with registry.lock:
        queue = registry.get_results(pkg)
        if queue:
            await queue.unsubscribe_results()
        registry.unregister_results(pkg)


async def unsubscribe_all(if_unused: bool = True, if_empty: bool = True) -> None:
    """
    Asynchronously remove all active in-process subscriptions.

    This clears standard subscriptions, result subscriptions, and task
    subscriptions, effectively stopping all message consumption for this process.
    """
    async with registry.lock:
        queues = itertools.chain(
            registry.get_all_subscriptions(),
            registry.get_all_tasks(flat=True),
        )
        for queue in queues:
            await queue.unsubscribe(if_unused=False, if_empty=False)
        registry.unregister_all_subscriptions()
        registry.unregister_all_tasks()
        queues = registry.get_all_results()
        for queue in queues:
            await queue.unsubscribe_results()
        registry.unregister_all_results()


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
    async with registry.lock:
        registry.register_results(pkg, queue)
    return queue


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


async def subscribe_logger(
    log_callback: "LoggerCallback | None" = None, prefix: str | None = None
) -> "LoggingAsyncQueue":
    resolved_callback = log_callback or default_log_callback
    queue, cb = LoggingAsyncQueue(prefix), resolved_callback
    await queue.subscribe(cb)
    return queue


def is_module_tasks(module_name: str) -> bool:
    return "tasks" in module_name.split(".")


def run_forever(main: tp.Callable[..., tp.Awaitable[None]]) -> None:
    asyncio.run(_run_forever(main))


async def _run_forever(main: tp.Callable[..., tp.Awaitable[None]]) -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    async def shutdown(signum: int) -> None:
        log.info("Shutting down protobunny connections %s", signal.Signals(signum).name)
        await unsubscribe_all()
        await disconnect()
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        # Note: add_signal_handler requires a callback, so we use a lambda
        def _handler(s: int) -> asyncio.Task[None]:
            return asyncio.create_task(shutdown(s))

        loop.add_signal_handler(sig, _handler, sig)

    log.info("Started. Press Ctrl+C to exit.")
    # Wait here forever (non-blocking) until shutdown() is called
    await main()
    await stop_event.wait()


#######################################################
# Dynamically added by post_compile.py
from ..core import (  # noqa
    commons,
    results,
)

#######################################################
