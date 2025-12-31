import asyncio
import logging
import os
import typing as tp
import uuid
from abc import ABC
from collections import defaultdict
from queue import Empty, Queue

from ....config import default_configuration
from ....models import AsyncCallback, Envelope
from ... import RequeueMessage
from .. import BaseConnection, is_task

log = logging.getLogger(__name__)

VHOST = os.environ.get("PYTHON_VHOST", "/")


class MessageBroker:
    """Centralized message broker"""

    def __init__(self):
        self._shared_queues: dict[str, list[asyncio.Queue]] = defaultdict(list)
        self._exclusive_queues: dict[str, list[asyncio.Queue]] = defaultdict(list)
        self.lock = asyncio.Lock()
        self.logger_queue: asyncio.Queue | None = None

    async def publish(self, topic: str, message: Envelope) -> bool:
        """Publish a message to all relevant queues."""
        published = False
        shared = is_task(topic)
        async with self.lock:
            # Logger queue
            if self.logger_queue:
                await self.logger_queue.put(message)
            # Shared queue
            if shared:
                queues = self._shared_queues.get(topic)
                if not queues:
                    # A task message will create a shared queue if not existing yet
                    queue = asyncio.Queue()
                    self._shared_queues[topic].append(queue)
                    log.warning("No subscribers for tasks %s. Creating new queue", topic)
                    await queue.put(message)
                    return True
                # Select the queue with the least messages (Load Balancing)
                best_queue = min(queues, key=lambda q: q.qsize())
                await best_queue.put(message)
                return True

            # For exact matches:
            if topic in self._exclusive_queues:
                for queue in self._exclusive_queues[topic]:
                    await queue.put(message)
                    published = True

            # Fan out
            for sub_topic, queues in self._exclusive_queues.items():
                if sub_topic.endswith("#") and topic.startswith(sub_topic[:-1]):
                    for queue in queues:
                        await queue.put(message)
                        published = True

        return published

    async def create_shared_queue(self, topic: str) -> asyncio.Queue:
        """Get or create a shared queue."""
        async with self.lock:
            queue = asyncio.Queue()
            self._shared_queues[topic].append(queue)
            return queue

    async def create_exclusive_queue(self, topic: str) -> asyncio.Queue:
        """Create an exclusive queue for a topic."""
        async with self.lock:
            queue = asyncio.Queue()
            self._exclusive_queues[topic].append(queue)
            return queue

    async def remove_exclusive_queue(self, topic: str, queue: Queue) -> None:
        """Remove an exclusive queue."""
        async with self.lock:
            if topic in self._exclusive_queues:
                try:
                    self._exclusive_queues[topic].remove(queue)
                except ValueError:
                    pass

    async def remove_shared_queues(self, topic: str) -> None:
        """Remove all in process subscriptions for a shared queue."""
        async with self.lock:
            self._shared_queues.pop(topic, None)

    async def purge_queue(self, topic: str) -> None:
        """Empty a shared queue."""
        async with self.lock:
            queues = self._shared_queues.get(topic, [])
            for queue in queues:
                while not queue.empty():
                    try:
                        queue.get_nowait()
                    except Empty:
                        break

    async def get_message_count(self, topic: str) -> int:
        """Get queue size of the shared queues."""
        async with self.lock:
            return (
                sum(queue.qsize() for queue in self._shared_queues.get(topic, []))
                if topic in self._shared_queues
                else 0
            )


class BaseLocalConnection(BaseConnection, ABC):
    """Base class with shared logic for python "connections"."""

    instance_by_vhost: dict[str, tp.Any]
    lock: tp.Any
    _connection: MessageBroker

    def __init__(self, vhost: str = "/", requeue_delay: int = 3):
        self.vhost = vhost
        self.requeue_delay = requeue_delay
        self._is_connected = False
        self._subscriptions: dict[str, dict] = {}
        self.logger_prefix = default_configuration.logger_prefix


class Connection(BaseLocalConnection):
    """Asynchronous local connection using asyncio."""

    _connection_cls = MessageBroker
    instance_by_vhost: dict[str, "Connection"] = {}
    lock = asyncio.Lock()
    _is_connected_event: asyncio.Event | None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._connection = self._connection_cls()
        self._is_connected_event = None

    @staticmethod
    def create_tag(topic: str, shared: bool) -> str:
        """Generate subscription tag."""
        context_id = id(asyncio.current_task()) if not shared else uuid.uuid4()
        suffix = f"shared-{context_id}" if shared else context_id
        return f"local-sub-{topic}-{suffix}"

    async def setup_queue(self, topic: str, shared: bool = False) -> asyncio.Queue:
        """Create appropriate queue type."""
        if topic == self.logger_prefix:
            self._connection.logger_queue = asyncio.Queue()
            return self._connection.logger_queue
        elif shared:
            return await self._connection.create_shared_queue(topic)
        else:
            return await self._connection.create_exclusive_queue(topic)

    @property
    def is_connected_event(self) -> asyncio.Event:
        """Lazily create the event in the current running loop."""
        if self._is_connected_event is None:
            self._is_connected_event = asyncio.Event()
        return self._is_connected_event

    @classmethod
    async def get_connection(cls, vhost: str = "/") -> "Connection":
        if vhost not in cls.instance_by_vhost:
            async with cls.lock:
                if vhost not in cls.instance_by_vhost:
                    instance = cls(vhost=vhost)
                    await instance.connect()
                    cls.instance_by_vhost[vhost] = instance
        return cls.instance_by_vhost[vhost]

    def is_connected(self) -> bool:
        """Check if connection is established"""
        return self.is_connected_event.is_set()

    async def connect(self, timeout: float = 10.0) -> "Connection":
        log.info("Connecting async connection for vhost: %s", self.vhost)
        self.is_connected_event.set()
        return self

    async def disconnect(self, **kwargs) -> None:
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

        async with self.lock:
            self.is_connected_event.clear()
            self.instance_by_vhost.pop(self.vhost, None)

    async def publish(self, topic: str, message: Envelope, **kwargs) -> None:
        # Broker's publish is thread-safe
        published = await self._connection.publish(topic, message)
        if not published:
            log.warning("No subscribers for topic '%s'", topic)

    async def _message_worker(
        self, topic: str, callback: AsyncCallback, stop_event: asyncio.Event, queue: asyncio.Queue
    ) -> None:
        """Async worker for processing messages."""
        message = None
        while not stop_event.is_set():
            try:
                # Use thread pool for blocking queue.get
                message = await queue.get()
                if stop_event.is_set():
                    break
                await callback(message)
                queue.task_done()
                log.debug("Message processed on topic '%s'", topic)
            except RequeueMessage:
                log.warning("Requeuing message on topic '%s'", topic)
                await asyncio.sleep(self.requeue_delay)
                await queue.put(message)
                queue.task_done()
            except Empty:
                continue
            except asyncio.CancelledError:
                log.debug("Worker cancelled for topic '%s'", topic)
                raise
            except RuntimeError as e:
                if "Event loop is closed" in str(e):
                    log.debug("Worker stopped for topic '%s'", topic)
                    break
            except Exception:
                log.exception("Error processing message on topic '%s'", topic)
                continue

    async def subscribe(self, topic: str, callback: AsyncCallback, shared: bool = False) -> str:
        async with self.lock:
            tag = self.create_tag(topic, shared)

            if tag in self._subscriptions:
                log.warning("Already subscribed with tag '%s'", tag)
                return tag

            queue = await self.setup_queue(topic, shared)
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
        async with self.lock:
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
        task = info.get("task")
        if task and not task.done():
            task.cancel()
            try:
                # Await the task to ensure it's cleaned up from the event loop
                await asyncio.gather(task, return_exceptions=True)
            except asyncio.CancelledError:
                pass
        try:
            await asyncio.wait_for(task, timeout=1.0)
        except asyncio.TimeoutError:
            pass
        except asyncio.CancelledError:
            pass
        await self._cleanup_queue(info["topic"], info["queue"], info["shared"], if_unused)

    async def _cleanup_queue(
        self, topic: str, queue: asyncio.Queue, shared: bool, if_unused: bool
    ) -> None:
        """Clean up queue resources."""
        if topic == self.logger_prefix:
            self._connection.logger_queue = None
        elif not shared:
            await self._connection.remove_exclusive_queue(topic, queue)
        elif not if_unused:
            await self._connection.remove_shared_queues(topic)

    async def get_consumer_count(self, topic: str) -> int:
        async with self.lock:
            return sum(1 for info in self._subscriptions.values() if info["topic"] == topic)

    async def purge(self, topic: str, **kwargs) -> None:
        await self._connection.purge_queue(topic)

    async def get_message_count(self, topic: str) -> int:
        return await self._connection.get_message_count(topic)

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()
