"""Implements a RabbitMQ Connection with both sync and async support using aio_pika."""
import asyncio
import functools
import inspect
import logging
import os
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

from ....exceptions import ConnectionError, RequeueMessage
from .. import BaseAsyncConnection

log = logging.getLogger(__name__)

VHOST = os.environ.get("RABBITMQ_VHOST", "/")


async def get_connection() -> "Connection":
    """Get the singleton async connection."""
    conn = await Connection.get_connection(vhost=VHOST)
    return conn


async def reset_connection() -> "Connection":
    """Reset the singleton connection."""
    connection = await get_connection()
    await connection.disconnect()
    return await get_connection()


async def disconnect() -> None:
    connection = await get_connection()
    await connection.disconnect()


class Connection(BaseAsyncConnection):
    """Async RabbitMQ Connection wrapper."""

    _lock: asyncio.Lock | None = None
    instance_by_vhost: dict[str, "Connection | None"] = {}

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
        url: str | None = None,
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
        super().__init__()
        uname = username or os.environ.get(
            "RABBITMQ_USERNAME", os.environ.get("RABBITMQ_DEFAULT_USER", "guest")
        )
        passwd = password or os.environ.get(
            "RABBITMQ_PASSWORD", os.environ.get("RABBITMQ_DEFAULT_PASS", "guest")
        )
        host = host or os.environ.get("RABBITMQ_HOST", "127.0.0.1")
        port = port or int(os.environ.get("RABBITMQ_PORT", "5672"))
        url = url or os.environ.get("RABBITMQ_URL")
        username = urllib.parse.quote(uname, safe="")
        password = urllib.parse.quote(passwd, safe="")
        self.vhost = vhost
        clean_vhost = urllib.parse.quote(vhost, safe="")
        clean_vhost = clean_vhost.lstrip("/")
        if not url:
            self._url = f"amqp://{username}:{password}@{host}:{port}/{clean_vhost}?heartbeat={heartbeat}&timeout={timeout}&fail_fast=no"
        else:
            self._url = url
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
    async def get_connection(cls, vhost: str = "/") -> "Connection":
        """Get singleton instance (async)."""
        current_loop = asyncio.get_running_loop()
        async with cls._get_class_lock():
            instance = cls.instance_by_vhost.get(vhost)
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
                cls.instance_by_vhost[vhost] = new_instance
                instance = new_instance
            log.info("Returning singleton AsyncRmqConnection instance for vhost %s", vhost)
            return instance

    def is_connected(self) -> bool:
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
        if not self.is_connected() or self._channel is None:
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

    async def connect(self, timeout: float = 30.0) -> "Connection":
        """Establish RabbitMQ connection.

        Args:
            timeout: Maximum time to wait for connection establishment (seconds)

        Raises:
            ConnectionError: If connection fails
            asyncio.TimeoutError: If connection times out
        """
        async with self.lock:
            if self.instance_by_vhost.get(self.vhost) and self.is_connected():
                return self.instance_by_vhost[self.vhost]
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
                self.instance_by_vhost[self.vhost] = self
                return self
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
            if not self.is_connected():
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
                Connection.instance_by_vhost.pop(self.vhost, None)
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

        if not self.is_connected():
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
            if not self.is_connected():
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
            if not self.is_connected():
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

    async def purge(self, topic: str, **kwargs) -> None:
        """Empty a queue of all messages.

        Args:
            topic: The queue topic to purge

        Raises:
            ConnectionError: If not connected
        """

        async with self.lock:
            if not self.is_connected():
                raise ConnectionError("Not connected to RabbitMQ")

            log.info("Purging topic '%s'", topic)
            queue = await self.setup_queue(topic, shared=True)
            await queue.purge()

    async def get_message_count(self, topic: str) -> int | None:
        """Get the number of messages in a queue.

        Args:
            topic: The queue topic

        Returns:
            Number of messages currently in the queue

        Raises:
            ConnectionError: If not connected
        """
        if not self.is_connected():
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
            if not self.is_connected():
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
