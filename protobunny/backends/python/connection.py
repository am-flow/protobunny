import asyncio
import logging
import os
import threading
import time
import typing as tp
from abc import ABC
from collections import defaultdict
from queue import Empty, Queue

from ... import RequeueMessage
from ...base import configuration
from ...models import AsyncCallback, Envelope, SyncCallback
from .. import BaseConnection

log = logging.getLogger(__name__)

VHOST = os.environ.get("PYTHON_VHOST", "/")


# Shared state for both sync and async connections
class MessageBroker:
    """Centralized message broker"""

    def __init__(self):
        self._shared_queues: dict[str, Queue] = {}
        self._exclusive_queues: dict[str, list[Queue]] = defaultdict(list)
        self._lock = threading.RLock()
        self.logger_queue: Queue | None = None

    def publish(self, topic: str, message: Envelope) -> bool:
        """Publish a message to all relevant queues."""
        published = False

        with self._lock:
            # Logger queue
            if self.logger_queue:
                self.logger_queue.put(message)

            # Shared queue
            if topic in self._shared_queues:
                self._shared_queues[topic].put(message)
                published = True

            # Exclusive queues (fanout)
            for sub_topic, queues in self._exclusive_queues.items():
                if topic == sub_topic or topic.startswith(f"{sub_topic.removesuffix('#')}"):
                    for queue in queues:
                        queue.put(message)
                        published = True

        return published

    def create_shared_queue(self, topic: str) -> Queue:
        """Get or create a shared queue."""
        with self._lock:
            if topic not in self._shared_queues:
                self._shared_queues[topic] = Queue()
            return self._shared_queues[topic]

    def create_exclusive_queue(self, topic: str) -> Queue:
        """Create an exclusive queue for a topic."""
        with self._lock:
            queue = Queue()
            self._exclusive_queues[topic].append(queue)
            return queue

    def remove_exclusive_queue(self, topic: str, queue: Queue) -> None:
        """Remove an exclusive queue."""
        with self._lock:
            if topic in self._exclusive_queues:
                try:
                    self._exclusive_queues[topic].remove(queue)
                except ValueError:
                    pass

    def remove_shared_queue(self, topic: str) -> None:
        """Remove a shared queue."""
        with self._lock:
            self._shared_queues.pop(topic, None)

    def purge_queue(self, topic: str) -> None:
        """Empty a shared queue."""
        with self._lock:
            if topic in self._shared_queues:
                queue = self._shared_queues[topic]
                while not queue.empty():
                    try:
                        queue.get_nowait()
                    except Empty:
                        break

    def get_message_count(self, topic: str) -> int:
        """Get queue size."""
        with self._lock:
            if topic in self._shared_queues:
                return self._shared_queues[topic].qsize()
            return 0


# Module-level broker instance
_broker = MessageBroker()


class BaseLocalConnection(BaseConnection, ABC):
    """Base class with shared logic."""

    _instances_by_vhost: dict[str, tp.Any] = {}
    _lock = threading.RLock()

    def __init__(self, vhost: str = "/", requeue_delay: int = 3):
        self.vhost = vhost
        self.requeue_delay = requeue_delay
        self._is_connected = False
        self._subscriptions: dict[str, dict] = {}
        self.logger_prefix = configuration.logger_prefix

    def is_connected(self) -> bool:
        return self._is_connected

    def _get_tag(self, topic: str, shared: bool, context_id: tp.Any) -> str:
        """Generate subscription tag."""
        suffix = "shared" if shared else context_id
        return f"local-sub-{topic}-{suffix}"

    def _create_queue(self, topic: str, shared: bool) -> Queue:
        """Create appropriate queue type."""
        if topic == self.logger_prefix:
            _broker.logger_queue = Queue()
            return _broker.logger_queue
        elif shared:
            return _broker.create_shared_queue(topic)
        else:
            return _broker.create_exclusive_queue(topic)

    def _cleanup_queue(self, topic: str, queue: Queue, shared: bool, if_unused: bool) -> None:
        """Clean up queue resources."""
        if topic == self.logger_prefix:
            _broker.logger_queue = None
        elif not shared:
            _broker.remove_exclusive_queue(topic, queue)
        elif not if_unused:
            _broker.remove_shared_queue(topic)

    def get_message_count(self, topic: str) -> int:
        return _broker.get_message_count(topic)

    def purge(self, topic: str) -> None:
        _broker.purge_queue(topic)


class SyncConnection(BaseLocalConnection):
    """Synchronous local connection using threads."""

    @classmethod
    def get_connection(cls, vhost: str = "/") -> "SyncConnection":
        if vhost not in cls._instances_by_vhost:
            with cls._lock:
                if vhost not in cls._instances_by_vhost:
                    instance = cls(vhost=vhost)
                    instance.connect()
                    cls._instances_by_vhost[vhost] = instance
        return cls._instances_by_vhost[vhost]

    def connect(self, timeout: float = 10.0) -> None:
        with self._lock:
            if self._is_connected:
                return
            log.info("Connecting SyncLocalConnection for vhost: %s", self.vhost)
            self._is_connected = True

    def disconnect(self) -> None:
        with self._lock:
            log.info("Disconnecting SyncLocalConnection for vhost: %s", self.vhost)
            for tag in list(self._subscriptions.keys()):
                self._unsubscribe_by_tag(tag, if_unused=False)
            self._is_connected = False
            self._instances_by_vhost.pop(self.vhost, None)

    def publish(self, topic: str, message: Envelope) -> None:
        if not _broker.publish(topic, message):
            log.warning("No subscribers for topic '%s'", topic)

    def _message_worker(
        self, topic: str, callback: SyncCallback, stop_event: threading.Event, queue: Queue
    ) -> None:
        """Worker thread for processing messages."""
        while not stop_event.is_set():
            try:
                message = queue.get(timeout=0.1)
                if stop_event.is_set():
                    break
                callback(message)
                log.debug("Message processed on topic '%s'", topic)
            except RequeueMessage:
                log.warning("Requeuing message on topic '%s'", topic)
                time.sleep(self.requeue_delay)
                queue.put(message)
            except Empty:
                continue
            except Exception:
                log.exception("Error processing message on topic '%s'", topic)

    def subscribe(self, topic: str, callback: SyncCallback, shared: bool = False) -> str:
        with self._lock:
            context_id = threading.get_ident()
            tag = self._get_tag(topic, shared, context_id)

            if tag in self._subscriptions:
                log.warning("Already subscribed with tag '%s'", tag)
                return tag

            queue = self._create_queue(topic, shared)
            stop_event = threading.Event()

            thread = threading.Thread(
                target=self._message_worker, args=(topic, callback, stop_event, queue), daemon=True
            )

            self._subscriptions[tag] = {
                "topic": topic,
                "shared": shared,
                "queue": queue,
                "stop_event": stop_event,
                "thread": thread,
            }

            thread.start()
            log.info("Subscribed to topic '%s' (tag: %s)", topic, tag)
            return tag

    def unsubscribe(self, topic: str, if_unused: bool = True, **kwargs) -> None:
        with self._lock:
            tags = [tag for tag, info in self._subscriptions.items() if info["topic"] == topic]
            for tag in tags:
                self._unsubscribe_by_tag(tag, if_unused)

    def _unsubscribe_by_tag(self, tag: str, if_unused: bool = True) -> None:
        """Internal unsubscribe by tag."""
        info = self._subscriptions.pop(tag, None)
        if not info:
            return

        log.info("Unsubscribing from topic '%s' (tag: %s)", info["topic"], tag)

        # Stop worker thread
        info["stop_event"].set()
        info["thread"].join(timeout=3)

        # Cleanup queue
        self._cleanup_queue(info["topic"], info["queue"], info["shared"], if_unused)

    def get_consumer_count(self, topic: str) -> int:
        with self._lock:
            return sum(1 for info in self._subscriptions.values() if info["topic"] == topic)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False


class Connection(BaseLocalConnection):
    """Asynchronous local connection using asyncio."""

    _lock = asyncio.Lock()

    @classmethod
    async def get_connection(cls, vhost: str = "/") -> "Connection":
        if vhost not in cls._instances_by_vhost:
            async with cls._lock:
                if vhost not in cls._instances_by_vhost:
                    instance = cls(vhost=vhost)
                    await instance.connect()
                    cls._instances_by_vhost[vhost] = instance
        return cls._instances_by_vhost[vhost]

    async def connect(self, timeout: float = 10.0) -> None:
        if self._is_connected:
            return
        log.info("Connecting AsyncLocalConnection for vhost: %s", self.vhost)
        self._is_connected = True

    async def disconnect(self) -> None:
        log.info("Disconnecting AsyncLocalConnection for vhost: %s", self.vhost)
        tags = list(self._subscriptions.keys())
        # Cancel all tasks first
        for tag in tags:
            info = self._subscriptions.get(tag)
            if info and "task" in info:
                info["stop_event"].set()
                info["task"].cancel()

        # Wait for cancellation to complete
        tasks = [info["task"] for info in self._subscriptions.values() if "task" in info]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        for tag in tags:
            await self._unsubscribe_by_tag(tag, if_unused=False)

        async with self._lock:
            self._is_connected = False
            self._instances_by_vhost.pop(self.vhost, None)

    async def publish(self, topic: str, message: Envelope, **kwargs) -> None:
        # Broker's publish is thread-safe
        published = await asyncio.to_thread(_broker.publish, topic, message)
        if not published:
            log.warning("No subscribers for topic '%s'", topic)

    async def _message_worker(
        self, topic: str, callback: AsyncCallback, stop_event: asyncio.Event, queue: Queue
    ) -> None:
        """Async worker for processing messages."""
        while not stop_event.is_set():
            try:
                # Use thread pool for blocking queue.get
                message = await asyncio.to_thread(queue.get, timeout=0.1)
                if stop_event.is_set():
                    break
                await callback(message)
                log.debug("Message processed on topic '%s'", topic)
            except RequeueMessage:
                log.warning("Requeuing message on topic '%s'", topic)
                await asyncio.sleep(self.requeue_delay)
                await asyncio.to_thread(queue.put, message)
            except Empty:
                continue
            except asyncio.CancelledError:
                log.debug("Worker cancelled for topic '%s'", topic)
                raise
            except Exception:
                log.exception("Error processing message on topic '%s'", topic)

    async def subscribe(self, topic: str, callback: AsyncCallback, shared: bool = False) -> str:
        async with self._lock:
            context_id = id(asyncio.current_task())
            tag = self._get_tag(topic, shared, context_id)

            if tag in self._subscriptions:
                log.warning("Already subscribed with tag '%s'", tag)
                return tag

            queue = self._create_queue(topic, shared)
            stop_event = asyncio.Event()

            task = asyncio.create_task(self._message_worker(topic, callback, stop_event, queue))

            self._subscriptions[tag] = {
                "topic": topic,
                "shared": shared,
                "queue": queue,
                "stop_event": stop_event,
                "task": task,
            }

            log.info("Subscribed to topic '%s' (tag: %s)", topic, tag)
            return tag

    async def unsubscribe(self, topic: str, if_unused: bool = True, if_empty: bool = True) -> None:
        async with self._lock:
            tags = [tag for tag, info in self._subscriptions.items() if info["topic"] == topic]
            for tag in tags:
                await self._unsubscribe_by_tag(tag, if_unused)

    async def _unsubscribe_by_tag(self, tag: str, if_unused: bool = True) -> None:
        """Internal unsubscribe by tag."""
        info = self._subscriptions.pop(tag, None)
        if not info:
            return

        log.info("Unsubscribing from topic '%s' (tag: %s)", info["topic"], tag)

        # Stop worker task
        info["stop_event"].set()
        try:
            await asyncio.wait_for(info["task"], timeout=3.0)
        except asyncio.TimeoutError:
            info["task"].cancel()
        except asyncio.CancelledError:
            pass
        # Cleanup queue
        self._cleanup_queue(info["topic"], info["queue"], info["shared"], if_unused)

    async def get_consumer_count(self, topic: str) -> int:
        async with self._lock:
            return sum(1 for info in self._subscriptions.values() if info["topic"] == topic)

    async def purge(self, topic: str) -> None:
        await asyncio.to_thread(_broker.purge_queue, topic)

    async def get_message_count(self, topic: str) -> int:
        return await asyncio.to_thread(_broker.get_message_count, topic)

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()


# Convenience functions
async def get_connection() -> Connection:
    return await Connection.get_connection(vhost=VHOST)


async def reset_connection() -> Connection:
    connection = await get_connection()
    await connection.disconnect()
    return await get_connection()


async def disconnect() -> None:
    connection = await get_connection()
    await connection.disconnect()


def get_connection_sync() -> SyncConnection:
    return SyncConnection.get_connection(vhost=VHOST)


def reset_connection_sync() -> SyncConnection:
    connection = get_connection_sync()
    connection.disconnect()
    return get_connection_sync()


def disconnect_sync() -> None:
    connection = get_connection_sync()
    connection.disconnect()
