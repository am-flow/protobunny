"""Implements a RabbitMQ Connection with both sync and async support using aio_pika."""
import asyncio
import functools
import inspect
import logging
import os
import threading
import typing as tp
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractQueue,
    AbstractRobustConnection,
)

from ...exceptions import ConnectionError, RequeueMessage
from .. import BaseConnection

log = logging.getLogger(__name__)

VHOST = os.environ.get("RABBITMQ_VHOST", "/")


async def get_connection() -> "AsyncConnection":
    """Get the singleton async connection."""
    conn = await AsyncConnection.get_connection(vhost=VHOST)
    return conn


async def reset_connection() -> "AsyncConnection":
    """Reset the singleton connection."""
    connection = await get_connection()
    await connection.disconnect()
    return await get_connection()


async def disconnect() -> None:
    connection = await get_connection()
    await connection.disconnect()


def disconnect_sync() -> None:
    connection = get_connection_sync()
    connection.disconnect()


def reset_connection_sync() -> "SyncConnection":
    connection = get_connection_sync()
    connection.disconnect()
    return get_connection_sync()


def get_connection_sync() -> "SyncConnection":
    connection = SyncConnection.get_connection(vhost=VHOST)
    return connection


class AsyncConnection:
    """Async RabbitMQ Connection wrapper."""

    _lock: asyncio.Lock | None = None
    _instance_by_vhost: dict[str, "AsyncConnection | None"] = {}

    def __init__(
        self,
        username: str | None = None,
        password: str | None = None,
        host: str | None = None,
        port: int | None = None,
        vhost: str = "",
        worker_threads: int = 2,
        prefetch_count: int = 1,
        requeue_delay: int = 3,
        exchange_name: str = "amq.topic",
        dl_exchange: str = "protobunny-dlx",
        dl_queue: str = "protobunny-dlq",
        heartbeat: int = 1200,
        timeout: int = 1500,
    ):
        """Initialize RabbitMQ connection.

        Args:
            username: RabbitMQ username
            password: RabbitMQ password
            host: RabbitMQ host
            port: RabbitMQ port
            vhost: RabbitMQ virtual host
            worker_threads: number of concurrent callback workers to use
            prefetch_count: how many messages to prefetch from the queue
            requeue_delay: how long to wait before re-queueing a message (seconds)
            exchange_name: name of the main exchange
            dl_exchange: name of the dead letter exchange
            dl_queue: name of the dead letter queue
            heartbeat: heartbeat interval in seconds
            timeout: connection timeout in seconds
        """
        uname = username or os.environ.get(
            "RABBITMQ_USERNAME", os.environ.get("RABBITMQ_DEFAULT_USER", "guest")
        )
        passwd = password or os.environ.get(
            "RABBITMQ_PASSWORD", os.environ.get("RABBITMQ_DEFAULT_PASS", "guest")
        )
        host = host or os.environ.get("RABBITMQ_HOST", "127.0.0.1")
        port = port or int(os.environ.get("RABBITMQ_PORT", "5672"))
        # URL encode credentials and vhost to prevent injection
        username = urllib.parse.quote(uname, safe="")
        password = urllib.parse.quote(passwd, safe="")
        self.vhost = vhost
        clean_vhost = urllib.parse.quote(vhost, safe="")
        clean_vhost = clean_vhost.lstrip("/")
        self._url = f"amqp://{username}:{password}@{host}:{port}/{clean_vhost}?heartbeat={heartbeat}&timeout={timeout}&fail_fast=no"
        self._loop: asyncio.AbstractEventLoop | None = None
        self._exchange_name = exchange_name
        self._dl_exchange = dl_exchange
        self._dl_queue = dl_queue
        self._exchange: AbstractExchange | None = None
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractChannel | None = None
        self.prefetch_count = prefetch_count
        self.requeue_delay = requeue_delay
        self.queues: dict[str, AbstractQueue] = {}
        self.consumers: dict[str, str] = {}
        self.executor = ThreadPoolExecutor(max_workers=worker_threads)
        self._instance_lock: asyncio.Lock | None = None
        self._is_connected_event: asyncio.Event | None = None
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            # Fallback if __init__ is called outside a running loop
            # (though get_connection should be called inside one)
            self._loop = None

    @property
    def is_connected_event(self) -> asyncio.Event:
        """Lazily create the event in the current running loop."""
        if self._is_connected_event is None:
            self._is_connected_event = asyncio.Event()
        return self._is_connected_event

    @classmethod
    def _get_class_lock(cls) -> asyncio.Lock:
        """Ensure the class lock is bound to the current running loop."""
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

    @property
    def lock(self) -> asyncio.Lock:
        """Lazy instance lock."""
        if self._instance_lock is None:
            self._instance_lock = asyncio.Lock()
        return self._instance_lock

    @classmethod
    async def get_connection(cls, vhost: str = "/") -> "AsyncConnection":
        """Get singleton instance (async)."""
        current_loop = asyncio.get_running_loop()
        async with cls._get_class_lock():
            instance = cls._instance_by_vhost.get(vhost)
            # Check if we have an instance AND if it belongs to the CURRENT loop
            if instance:
                # We need to check if the instance's internal loop matches our current loop
                # and if that loop is actually still running.
                if instance._loop != current_loop or not instance.is_connected_event.is_set():
                    log.warning("Found stale connection for %s (loop mismatch). Resetting.", vhost)
                    await instance.disconnect()  # Cleanup the old one
                    instance = None

            if instance is None:
                log.debug("Creating fresh connection for %s", vhost)
                new_instance = cls(vhost=vhost)
                new_instance._loop = current_loop  # Store the loop it was born in
                await new_instance.connect()
                cls._instance_by_vhost[vhost] = new_instance
                instance = new_instance
            log.info("Returning singleton AsyncConnection instance for vhost %s", vhost)
            return instance

    async def is_connected(self) -> bool:
        """Check if connection is established and healthy."""
        return self.is_connected_event.is_set()

    @property
    def connection(self) -> AbstractRobustConnection:
        """Get the connection object.

        Raises:
            ConnectionError: If not connected
        """
        if not self._connection:
            raise ConnectionError("Connection not initialized. Call connect() first.")
        return self._connection

    @property
    def channel(self) -> AbstractChannel:
        if not self.is_connected_event.is_set() or self._channel is None:
            # In a sync context, this usually means connect() wasn't called
            # or it failed silently.
            raise ConnectionError(
                "RabbitMQ Channel is not available. Ensure connect() finished successfully."
            )
        return self._channel

    @property
    def exchange(self) -> AbstractExchange:
        """Get the exchange object.

        Raises:
            ConnectionError: If not connected
        """
        if not self._exchange:
            raise ConnectionError("Exchange not initialized. Call connect() first.")
        return self._exchange

    async def connect(self, timeout: float = 30.0) -> None:
        """Establish RabbitMQ connection.

        Args:
            timeout: Maximum time to wait for connection establishment (seconds)

        Raises:
            ConnectionError: If connection fails
            asyncio.TimeoutError: If connection times out
        """
        async with self.lock:
            if (
                self._instance_by_vhost.get(self.vhost)
                and await self.is_connected()
                and self._channel is not None
            ):
                return
            try:
                log.info(
                    "Establishing RabbitMQ connection to %s", self._url.split("@")[1].split("?")[0]
                )

                connection = await asyncio.wait_for(
                    aio_pika.connect_robust(self._url), timeout=timeout
                )
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=self.prefetch_count)

                # Declare main exchange
                exchange = await channel.declare_exchange(
                    self._exchange_name, "topic", durable=True, auto_delete=False
                )

                # Declare dead letter exchange and queue
                await channel.declare_exchange(
                    self._dl_exchange, "fanout", durable=True, auto_delete=False
                )

                dlq = await channel.declare_queue(
                    self._dl_queue, exclusive=False, durable=True, auto_delete=False
                )
                await dlq.bind(self._dl_exchange)

                self._connection = connection
                self._channel = channel
                self._exchange = exchange
                self.is_connected_event.set()
                log.info("Successfully connected to RabbitMQ")
                self._instance_by_vhost[self.vhost] = self
            except asyncio.TimeoutError:
                log.error("RabbitMQ connection timeout after %.1f seconds", timeout)
                self.is_connected_event.clear()
                raise
            except Exception as e:
                if "connection" in locals() and not connection.is_closed:
                    await connection.close()
                    await channel.close()
                self.is_connected_event.clear()
                self._channel = None
                self._connection = None
                log.exception("Failed to establish RabbitMQ connection")
                raise ConnectionError(f"Failed to connect to RabbitMQ: {e}") from e

    async def reset(self):
        await self.disconnect()
        await self.connect()

    async def disconnect(self, timeout: float = 10.0) -> None:
        """Close RabbitMQ connection and cleanup resources.

        Args:
            timeout: Maximum time to wait for cleanup (seconds)
        """
        async with self.lock:
            if not self.is_connected_event.is_set():
                log.debug("Already disconnected from RabbitMQ")
                return
            try:
                log.info("Closing RabbitMQ connection")
                # Cancel all consumers and delete exclusive queues
                consumers_copy = list(self.consumers.items())
                for tag, queue_name in consumers_copy:
                    try:
                        if queue_name in self.queues:
                            queue = self.queues[queue_name]
                            await asyncio.wait_for(queue.cancel(tag), timeout=5.0)
                            if queue.exclusive:
                                log.debug("Force delete exclusive queue %s", queue_name)
                                await asyncio.wait_for(
                                    queue.delete(if_empty=False, if_unused=False), timeout=5.0
                                )
                            self.queues.pop(queue_name, None)
                    except asyncio.TimeoutError:
                        log.warning("Timeout cleaning up consumer %s", tag)
                    except aio_pika.exceptions.ChannelInvalidStateError as e:
                        log.warning("Invalid state for queue %s: %s", queue_name, str(e))
                    except Exception as e:
                        log.warning("Error cleaning up consumer %s: %s", tag, e)
                    finally:
                        self.consumers.pop(tag, None)

                # Shutdown executor
                self.executor.shutdown(wait=False, cancel_futures=True)

                # Close the underlying aio-pika connection
                if self._connection and not self._connection.is_closed:
                    await asyncio.wait_for(self._connection.close(), timeout=timeout)

            except asyncio.TimeoutError:
                log.warning("RabbitMQ connection close timeout after %.1f seconds", timeout)
            except Exception:
                log.exception("Error during RabbitMQ disconnect")
            finally:
                self._connection = None
                self._channel = None
                self._exchange = None
                self.queues.clear()
                self.consumers.clear()
                self.is_connected_event.clear()
                # 6. Remove from CLASS registry
                # Explicitly use the class name to ensure we hit the registry
                AsyncConnection._instance_by_vhost.pop(self.vhost, None)
                log.info("RabbitMQ connection closed")

    async def setup_queue(self, topic: str, shared: bool = False) -> AbstractQueue:
        """Set up a RabbitMQ queue.

        Args:
            topic: the queue/routing key topic
            shared: if True, all clients share the same queue and receive messages
                round-robin (task queue). If False, each client has its own anonymous
                queue and all receive copies of each message (pub/sub).

        Returns:
            The configured queue

        Raises:
            ConnectionError: If not connected
        """
        queue_name = topic if shared else ""
        log.debug("Setting up queue for topic '%s' (shared=%s)", topic, shared)

        # Reuse existing shared queues
        if shared and queue_name in self.queues:
            return self.queues[queue_name]

        args = {"x-dead-letter-exchange": self._dl_exchange}
        queue = await self.channel.declare_queue(
            queue_name,
            exclusive=not shared,
            durable=shared,
            auto_delete=not shared,
            arguments=args,
        )
        await queue.bind(self.exchange, topic)
        self.queues[queue.name] = queue
        return queue

    async def publish(
        self,
        topic: str,
        message: aio_pika.Message,
        mandatory: bool = True,
        immediate: bool = False,
    ) -> None:
        """Publish a message to a topic.

        Args:
            topic: The routing key/topic
            message: The message to publish
            message: The message to publish
            mandatory: If True, raise an error if message cannot be routed
            immediate: IF True, send message immediately to the queue

        Raises:
            ConnectionError: If not connected
        """
        if not await self.is_connected():
            raise ConnectionError("Not connected to RabbitMQ")

        log.debug("Publishing message to topic '%s'", topic)
        await self.exchange.publish(
            message, routing_key=topic, mandatory=mandatory, immediate=immediate
        )

    async def _on_message(
        self, topic: str, callback: tp.Callable, message: aio_pika.IncomingMessage
    ) -> None:
        """Handle incoming queue messages.

        Args:
            topic: The topic this message was received on
            callback: The callback function to process the message
            message: The incoming message
        """
        try:
            # 1. Check if the callback is a coroutine function
            # Note: inspect.iscoroutinefunction works on partials too
            if inspect.iscoroutinefunction(callback):
                # Run directly in the event loop
                await callback(message)
            else:
                # Run the callback in a thread pool to avoid blocking the event loop
                res = await asyncio.get_event_loop().run_in_executor(
                    self.executor, callback, message
                )
                # If the result of the executor is a coroutine, await it here!
                if asyncio.iscoroutine(res):
                    await res
            await message.ack()
            log.debug("Message processed successfully on topic '%s'", topic)

        except RequeueMessage:
            log.warning("Requeuing message on topic '%s' after RequeueMessage exception", topic)
            await message.reject(requeue=True)
            await asyncio.sleep(self.requeue_delay)

        except Exception:
            log.exception(
                "Unhandled exception processing message on topic '%s'. "
                "Rejecting without requeue to prevent poison message.",
                topic,
            )
            # Reject without requeue on unexpected errors to avoid poison messages
            # The message will go to the dead letter queue if configured
            await message.reject(requeue=False)

    async def subscribe(self, topic: str, callback: tp.Callable, shared: bool = False) -> str:
        """Subscribe to a queue/topic.

        Args:
            topic: The routing key/topic to subscribe to
            callback: Function to handle incoming messages. Should accept an
                aio_pika.IncomingMessage parameter.
            shared: if True, use shared queue for round-robin delivery (task queue).
                If False, use anonymous queue where all subscribers receive all messages
                (pub/sub).

        Returns:
            Subscription tag identifier needed to unsubscribe later

        Raises:
            ConnectionError: If not connected

        Example:
            .. code-block:: python

                def handle_message(message: aio_pika.IncomingMessage):
                    print(f"Received: {message.body.decode()}")

                tag = await conn.subscribe("my.events.*", handle_message)

        """
        async with self.lock:
            if not await self.is_connected():
                raise ConnectionError("Not connected to RabbitMQ")

            queue = await self.setup_queue(topic, shared)
            log.info("Subscribing to topic '%s' (queue=%s, shared=%s)", topic, queue.name, shared)

            func = functools.partial(self._on_message, topic, callback)
            tag = await queue.consume(func)
            self.consumers[tag] = queue.name
            return tag

    async def unsubscribe(self, tag: str, if_unused: bool = True, if_empty: bool = True) -> None:
        """Unsubscribe from a queue.

        Args:
            if_empty: will delete non empty queues if False
            if_unused: will delete used queues if False
            tag: The subscription identifier returned from subscribe()

        Raises:
            ValueError: If tag is not found
        """
        async with self.lock:
            if not await self.is_connected():
                raise ConnectionError("Not connected to RabbitMQ")
            if tag not in self.consumers:
                log.debug("Consumer tag '%s' not found, nothing to unsubscribe", tag)
                return

            queue_name = self.consumers[tag]

            if queue_name not in self.queues:
                log.debug("Queue '%s' not found, skipping cleanup", queue_name)
                return

            queue = self.queues[queue_name]
            log.info("Unsubscribing from queue '%s'", queue.name)

        try:
            await queue.cancel(tag)
            # Delete exclusive (anonymous) queues when last consumer is removed
            if queue.exclusive:
                self.queues.pop(queue.name)
                await queue.delete(if_empty=if_empty, if_unused=if_unused)
            self.consumers.pop(tag, None)
        except Exception:
            log.exception("Error unsubscribing from queue '%s'", queue_name)
            raise

    async def purge(self, topic: str) -> None:
        """Empty a queue of all messages.

        Args:
            topic: The queue topic to purge

        Raises:
            ConnectionError: If not connected
        """
        async with self.lock:
            if not await self.is_connected():
                raise ConnectionError("Not connected to RabbitMQ")

            log.info("Purging topic '%s'", topic)
            await self.setup_queue(topic, shared=True)
            await self.queues[topic].purge()

    async def get_message_count(self, topic: str) -> int | None:
        """Get the number of messages in a queue.

        Args:
            topic: The queue topic

        Returns:
            Number of messages currently in the queue

        Raises:
            ConnectionError: If not connected
        """
        if not await self.is_connected():
            raise ConnectionError("Not connected to RabbitMQ")

        log.debug("Getting message count for topic '%s'", topic)
        queue = await self.channel.declare_queue(
            topic, exclusive=False, durable=True, auto_delete=False, passive=True
        )
        return queue.declaration_result.message_count

    async def get_consumer_count(self, topic: str) -> int:
        """Get the number of messages in a queue.

            Args:
                topic: The queue topic

            Raises:
                ConnectionError: If not connected
        """
        async with self.lock:
            if not await self.is_connected():
                raise ConnectionError("Not connected to RabbitMQ")
            queue = await self.channel.declare_queue(
                topic, exclusive=False, durable=True, auto_delete=False, passive=True
            )
            res = queue.declaration_result.consumer_count
            log.info("Consumer count for topic '%s': %s", topic, res)
            return res

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
        return False


class SyncConnection(BaseConnection):
    """Synchronous wrapper around AsyncConnection.

    Manages a dedicated event loop in a background thread to run async operations.

    Example:
        .. code-block:: python

            with SyncConnection() as conn:
                conn.publish("my.topic", message)
                tag = conn.subscribe("my.topic", callback)

    """

    _lock = threading.RLock()
    _stopped: asyncio.Event | None = None
    _instance_by_vhost: dict[str, "SyncConnection"] = {}

    def __init__(self, **kwargs):
        """Initialize sync connection.

        Args:
            **kwargs: Same arguments as AsyncConnection
        """
        self._async_conn = AsyncConnection(**kwargs)
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ready = threading.Event()
        self._stopped: asyncio.Event | None = None
        self.vhost = self._async_conn.vhost

    @classmethod
    def get_connection(cls, vhost: str = "/") -> "SyncConnection":
        """Get singleton instance (sync)."""
        with cls._lock:
            if not cls._instance_by_vhost.get(vhost):
                cls._instance_by_vhost[vhost] = cls(vhost=vhost)
            if not cls._instance_by_vhost[vhost].is_connected():
                cls._instance_by_vhost[vhost].connect()
            log.info("Returning singleton SyncConnection instance for vhost %s", vhost)
            return cls._instance_by_vhost[vhost]

    def _run_loop(self) -> None:
        """Run the event loop in a dedicated thread."""
        loop = None
        try:
            # Create a fresh loop for this specific thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop

            # Run the 'stop' watcher
            loop.create_task(self._async_run_watcher())

            # Signal readiness NOW that self._loop is assigned and running
            self._ready.set()
            loop.run_forever()
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("Event loop thread crashed")
        finally:
            if loop:
                loop.close()
            self._loop = None
            log.info("Event loop thread stopped")

    async def _async_run_watcher(self) -> None:
        """Wait for the stop signal inside the loop."""
        self._stopped = asyncio.Event()
        await self._stopped.wait()
        asyncio.get_running_loop().stop()

    async def _async_run(self) -> None:
        """Async event loop runner."""
        self._loop = asyncio.get_running_loop()
        self._stopped = asyncio.Event()
        self._loop.call_soon_threadsafe(self._ready.set)
        await self._stopped.wait()

    def _ensure_loop(self) -> None:
        """Ensure event loop thread is running.

        Raises:
            ConnectionError: If event loop fails to start
        """
        # Check if the thread exists AND is actually running
        if self._thread and self._thread.is_alive() and self._loop and self._loop.is_running():
            return

        log.info("Starting (or restarting) RabbitMQ event loop thread")

        # Reset state for a fresh start
        self._ready.clear()
        self._loop = None

        self._thread = threading.Thread(
            target=self._run_loop, name="protobunny_event_loop", daemon=True
        )
        self._thread.start()

        if not self._ready.wait(timeout=10.0):
            # Cleanup on failure to prevent stale state for next attempt
            self._thread = None
            raise ConnectionError("Event loop thread failed to start or signal readiness")

    def _run_coro(self, coro, timeout: float | None = None):
        """Run a coroutine in the event loop thread and return result.

        Args:
            coro: The coroutine to run
            timeout: Maximum time to wait for result (seconds)

        Returns:
            The coroutine result

        Raises:
            TimeoutError: If operation times out
            ConnectionError: If event loop is not available
        """
        self._ensure_loop()
        if self._loop is None:
            raise ConnectionError("Event loop not initialized")

        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return future.result(timeout=timeout)
        except TimeoutError:
            future.cancel()
            raise

    def is_connected(self) -> bool:
        """Check if connection is established."""
        if not self._loop or not self._loop.is_running():
            return False
        return self._run_coro(self._async_conn.is_connected())

    def connect(self, timeout: float = 10.0) -> None:
        """Establish RabbitMQ connection.

        Args:
            timeout: Maximum time to wait for connection (seconds)

        Raises:
            ConnectionError: If connection fails
            TimeoutError: If connection times out
        """
        self._run_coro(self._async_conn.connect(timeout), timeout=timeout)
        SyncConnection._instance_by_vhost[self.vhost] = self

    def disconnect(self, timeout: float = 10.0) -> None:
        """Close RabbitMQ connection and stop event loop.

        Args:
            timeout: Maximum time to wait for cleanup (seconds)
        """
        with self._lock:
            try:
                if self._loop and self._loop.is_running():
                    self._run_coro(self._async_conn.disconnect(timeout), timeout=timeout)
                # 2. Stop the loop (see _async_run_watcher)
                if self._stopped and self._loop:
                    self._loop.call_soon_threadsafe(self._stopped.set)
            except Exception as e:
                log.warning("Async disconnect failed during sync shutdown: %s", e)
            finally:
                if self._thread and self._thread.is_alive():
                    self._thread.join(timeout=5.0)
                    if self._thread.is_alive():
                        log.warning("Event loop thread did not stop within timeout")
                self._started = None
                self._loop = None
                self._thread = None
                SyncConnection._instance_by_vhost.pop(self.vhost, None)

    def publish(
        self,
        topic: str,
        message: aio_pika.Message,
        mandatory: bool = False,
        immediate: bool = False,
        timeout: float = 10.0,
    ) -> None:
        """Publish a message to a topic.

        Args:
            topic: The routing key/topic
            message: The message to publish
            mandatory: If True, raise error if message cannot be routed
            immediate: If True, publish message immediately to the queue
            timeout: Maximum time to wait for publish (seconds)

        Raises:
            ConnectionError: If not connected
            TimeoutError: If operation times out
        """
        self._run_coro(
            self._async_conn.publish(topic, message, mandatory, immediate), timeout=timeout
        )

    def subscribe(
        self, topic: str, callback: tp.Callable, shared: bool = False, timeout: float = 10.0
    ) -> str:
        """Subscribe to a queue/topic.

        Args:
            topic: The routing key/topic to subscribe to
            callback: Function to handle incoming messages
            shared: if True, use shared queue (round-robin delivery)
            timeout: Maximum time to wait for subscription (seconds)

        Returns:
            Subscription tag identifier

        Raises:
            ConnectionError: If not connected
            TimeoutError: If operation times out
        """
        return self._run_coro(self._async_conn.subscribe(topic, callback, shared), timeout=timeout)

    def unsubscribe(
        self, tag: str, timeout: float = 10.0, if_unused: bool = True, if_empty: bool = True
    ) -> None:
        """Unsubscribe from a queue.

        Args:
            if_unused:
            if_empty:
            tag: Subscription identifier returned from subscribe()
            timeout: Maximum time to wait (seconds)

        Raises:
            TimeoutError: If operation times out
        """
        self._run_coro(
            self._async_conn.unsubscribe(tag, if_empty=if_empty, if_unused=if_unused),
            timeout=timeout,
        )

    def purge(self, topic: str, timeout: float = 10.0) -> None:
        """Empty a queue of all messages.

        Args:
            topic: The queue topic to purge
            timeout: Maximum time to wait (seconds)

        Raises:
            ConnectionError: If not connected
            TimeoutError: If operation times out
        """
        self._run_coro(self._async_conn.purge(topic), timeout=timeout)

    def get_message_count(self, topic: str, timeout: float = 10.0) -> int:
        """Get the number of messages in a queue.

        Args:
            topic: The queue topic
            timeout: Maximum time to wait (seconds)

        Returns:
            Number of messages in the queue

        Raises:
            ConnectionError: If not connected
            TimeoutError: If operation times out
        """
        return self._run_coro(self._async_conn.get_message_count(topic), timeout=timeout)

    def get_consumer_count(self, topic: str, timeout: float = 10.0) -> int:
        """Get the number of messages in a queue.

        Args:
            topic: The queue topic
            timeout: Maximum time to wait (seconds)

        Returns:
            Number of messages in the queue

        Raises:
            ConnectionError: If not connected
            TimeoutError: If operation times out
        """
        return self._run_coro(self._async_conn.get_consumer_count(topic), timeout=timeout)

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        return False
