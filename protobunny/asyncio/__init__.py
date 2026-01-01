"""
A module providing async support for messaging and communication using the configured broker as the backend.

This module includes functionality for publishing, subscribing, and managing message queues,
as well as dynamically managing imports and configurations for the backend.
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
    from ..models import PBM, AsyncCallback, IncomingMessageProtocol, LoggerCallback


from .. import config_lib as config_lib
from ..helpers import get_backend, get_queue
from .backends import BaseAsyncConnection, BaseAsyncQueue, LoggingAsyncQueue

__version__ = version(PACKAGE_NAME)


log = logging.getLogger(PACKAGE_NAME)


############################
# -- Async top-level methods
############################


async def connect(**kwargs) -> "BaseAsyncConnection":
    """Establishes an asynchronous connection to the configured messaging broker.

    This method initializes and returns a singleton async connection. Subsequent
    calls return the existing connection instance unless it has been disconnected.

    Args:
        **kwargs: Backend-specific connection arguments (e.g., host, port,
            credentials, or protocol-specific tuning parameters).

    Returns:
        BaseAsyncConnection: The active asynchronous connection singleton.
    """
    connection_module = get_backend().connection
    conn = await connection_module.Connection.get_connection(
        vhost=connection_module.VHOST, **kwargs
    )
    return conn


async def disconnect() -> None:
    """Closes the active asynchronous connection to the broker.

    Gracefully terminates heartbeats and background networking tasks. Safe to
    call even if no connection is active.
    """
    connection_module = get_backend().connection
    conn = await connection_module.Connection.get_connection(vhost=connection_module.VHOST)
    await conn.disconnect()


async def reset_connection() -> "BaseAsyncConnection":
    """Resets the singleton connection and returns it."""
    connection = await connect()
    await connection.disconnect()
    return await connect()


async def publish(message: "PBM") -> None:
    """Asynchronously publishes a Protobuf message to its corresponding topic.

    The destination topic is automatically derived from the message class and
    package structure. Messages within a '.tasks' package are automatically
    treated as persistent tasks requiring reliable delivery and queuing logic.

    Args:
        message: An instance of a class derived from ProtoBunnyMessage.
    """
    queue = get_queue(message)
    await queue.publish(message)


async def publish_result(
    result: "Result", topic: str | None = None, correlation_id: str | None = None
) -> None:
    """Asynchronously publishes a processing result to the results topic of the source message.

    Args:
        result: The Result object containing the response payload and source message.
        topic: Optional override for the result topic. Defaults to the
            automatically generated '.result' topic associated with the source message.
        correlation_id: Optional ID used to link this result to a specific request.
    """
    queue = get_queue(result.source)
    await queue.publish_result(result, topic, correlation_id)


async def subscribe(
    pkg: "type[PBM] | ModuleType",
    callback: "AsyncCallback",
) -> "BaseAsyncQueue":
    """Registers an async callback to consume messages from a specific topic or package.

    If a message class is provided, subscribes to that specific topic. If a
    module is provided, subscribes to all message types defined within that module.
    For shared tasks (identified by the '.tasks' convention), Protobunny
    automatically manages shared consumer groups and load balancing.

    Args:
        pkg: The message class (type[PBM]) or module to subscribe to.
        callback: An async callable that accepts the received message.

    Returns:
        BaseAsyncQueue: The queue object managing the active subscription.
    """
    module_name = pkg.__name__ if inspect.ismodule(pkg) else pkg.__module__
    registry_key = str(pkg)
    async with registry.lock:
        if is_module_tasks(module_name):
            # It's a task. Handle multiple in-process subscriptions
            queue = get_queue(pkg)
            await queue.subscribe(callback)
            registry.register_task(registry_key, queue)
        else:
            # exclusive queue, cannot register more than one callback
            queue = registry.get_subscription(registry_key) or get_queue(pkg)
            if not queue.subscription:
                await queue.subscribe(callback)
                registry.register_subscription(registry_key, queue)
        return queue


async def unsubscribe(
    pkg: "type[PBM] | ModuleType",
    if_unused: bool = True,
    if_empty: bool = True,
) -> None:
    """Asynchronously removes a subscription for a specific message or package.

    Args:
        pkg: The message class or module to unsubscribe from.
        if_unused: If True, only unsubscribes if no other callbacks are attached.
        if_empty: If True, only unsubscribes if the local message buffer is empty.
    """

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
    pkg: "type[PBM] | ModuleType",
) -> None:
    """Remove all in-process subscriptions for a message/package result topic"""
    async with registry.lock:
        queue = registry.get_results(pkg)
        if queue:
            await queue.unsubscribe_results()
        registry.unregister_results(pkg)


async def unsubscribe_all(if_unused: bool = True, if_empty: bool = True) -> None:
    """Asynchronously stops all message consumption by canceling every subscription.

    Clears standard subscriptions, result listeners, and task workers. Typically
    invoked during graceful application shutdown.

    Args:
        if_unused: Policy for evaluating unused standard queues.
        if_empty: Policy for evaluating empty standard queues.
    """
    async with registry.lock:
        queues = itertools.chain(
            registry.get_all_subscriptions(), registry.get_all_tasks(flat=True)
        )
        for queue in queues:
            await queue.unsubscribe(if_unused=if_unused, if_empty=if_empty)
        registry.unregister_all_subscriptions()
        registry.unregister_all_tasks()
        queues = registry.get_all_results()
        for queue in queues:
            await queue.unsubscribe_results()
        registry.unregister_all_results()


async def subscribe_results(
    pkg: "type[PBM] | ModuleType",
    callback: "AsyncCallback",
) -> "BaseAsyncQueue":
    """Asynchronously subscribes to result topics for a message type or package.

    Used by services that need to listen for completion signals or data
    returned by workers processing specific message types.

    Args:
        pkg: The message class or module whose results should be monitored.
        callback: The async function to execute when a result is received.

    Returns:
        BaseAsyncQueue: The queue object managing the result subscription.
    """
    queue = get_queue(pkg)
    await queue.subscribe_results(callback)
    # register subscription to unsubscribe later
    async with registry.lock:
        registry.register_results(pkg, queue)
    return queue


async def get_message_count(
    msg_type: "PBM | type[PBM] | ModuleType",
) -> int | None:
    """Asynchronously retrieves the current number of pending messages in a queue.

    Args:
        msg_type: The message instance, class, or module representing the queue.

    Returns:
        int | None: The count of messages waiting to be processed, or None
            if the backend does not support count retrieval for this type.
    """
    q = get_queue(msg_type)
    count = await q.get_message_count()
    return count


async def get_consumer_count(
    msg_type: "PBM | type[PBM] | ModuleType",
) -> int | None:
    """Retrieves the number of active consumers currently attached to a shared (aka tasks) queue.

    Args:
        msg_type: The message instance, class representing the queue.

    Returns:
        int | None: The number of active subscribers/workers, or None if
            unsupported by the current backend.
    """
    q = get_queue(msg_type)
    count = await q.get_consumer_count()
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
    """Asynchronously subscribes a logging callback to monitor message traffic.

    Args:
        log_callback: A custom function to handle log messages. Defaults
            to `default_log_callback`.
        prefix: An optional subject/topic prefix to filter logged messages.

    Returns:
        LoggingAsyncQueue: The specialized async queue object for logging.
    """
    resolved_callback = log_callback or default_log_callback
    queue, cb = LoggingAsyncQueue(prefix), resolved_callback
    await queue.subscribe(cb)
    return queue


def is_module_tasks(module_name: str) -> bool:
    return "tasks" in module_name.split(".")


def run_forever(main: tp.Callable[..., tp.Awaitable[None]]) -> None:
    """Starts the event loop and keeps the process alive to consume messages.

    Installs signal handlers for SIGINT and SIGTERM to trigger an orderly
    async shutdown.

    Args:
        main: The entry point async function to run before entering the
            permanent wait state.
    """
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

        def _handler(s: int) -> asyncio.Task[None]:
            return asyncio.create_task(shutdown(s))

        loop.add_signal_handler(sig, _handler, sig)

    log.info("Protobunny started")
    await main()
    # Wait here forever (non-blocking) until shutdown() is called
    await stop_event.wait()


#######################################################
# Dynamically added by post_compile.py
from ..core import (  # noqa
    commons,
    results,
)

#######################################################
