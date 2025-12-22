import asyncio
import functools
import logging
import multiprocessing
import os
import threading
import time
import typing as tp
from collections import defaultdict
from multiprocessing import Queue
from queue import Empty

from ... import RequeueMessage
from ...base import configuration
from ...models import AsyncCallback, Envelope, SyncCallback
from .. import BaseConnection

log = logging.getLogger(__name__)

VHOST = os.environ.get("PYTHON_VHOST", "/")

try:
    # 'spawn' is safer for multi-threaded applications
    multiprocessing.set_start_method("spawn", force=True)
except RuntimeError:
    # Method might already be set
    pass


async def get_connection() -> "AsyncLocalConnection":
    """Get the singleton async connection."""
    conn = await AsyncLocalConnection.get_connection(vhost=VHOST)
    return conn


async def reset_connection() -> "AsyncLocalConnection":
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


def reset_connection_sync() -> "SyncLocalConnection":
    connection = get_connection_sync()
    connection.disconnect()
    return get_connection_sync()


def get_connection_sync() -> "SyncLocalConnection":
    connection = SyncLocalConnection.get_connection(vhost=VHOST)
    return connection


class SyncLocalConnection(BaseConnection):
    # Registry of SHARED queues for tasks
    _shared_queues = {}
    # Registry of EXCLUSIVE subscribers
    _exclusive_subscribers = defaultdict(list)

    _lock = threading.RLock()
    _manager = None
    _instance_by_vhost: dict[str, "SyncLocalConnection"] = {}

    def __init__(self, vhost: str = "/", requeue_delay: int = 3):
        # Maps tag -> { "thread": ThreadObj, "topic": str, "is_shared": bool, "queue": QueueObj }
        self._subscription_registry: dict[str, tp.Any] = {}
        self.vhost = vhost
        self.consumers: dict[str, threading.Thread] = {}
        self._is_connected = False
        self.requeue_delay = requeue_delay
        self.logger_queue = None
        self.logger_prefix = configuration.logger_prefix

    def _sync_publish(self, topic: str, message: Envelope) -> bool:
        # 0. Deliver to the Logger Queue if subscribed
        published = False
        if self.logger_queue:
            self.logger_queue.put(message)
        # 1. Deliver to the Shared Queue (if any)
        if topic in self._shared_queues:
            published = True
            self._shared_queues[topic].put(message)
        # 2. Deliver to Exclusive Subscribers (Hierarchical/Fanout)
        # We check if the published 'topic' starts with any registered 'sub_topic'
        for sub_topic, queues in self._exclusive_subscribers.items():
            # If sub_topic is "acme.tests.#", it matches "acme.tests.TestMessage"
            if topic == sub_topic or topic.startswith(f"{sub_topic.removesuffix('#')}"):
                for queue in queues:
                    published = True
                    queue.put(message)
        return published

    def publish(self, topic: str, message: Envelope) -> None:
        """Logic to handle both shared and exclusive distribution."""

        with self._lock:
            published = self._sync_publish(topic, message)
            if not published:
                log.warning("No subscribers for topic '%s'", topic)

    @classmethod
    def get_connection(cls, vhost: str = "/") -> "SyncLocalConnection":
        if vhost not in cls._instance_by_vhost:
            with cls._lock:
                if vhost not in cls._instance_by_vhost:
                    instance = cls(vhost=vhost)
                    instance.connect()
                    cls._instance_by_vhost[vhost] = instance
        return cls._instance_by_vhost[vhost]

    def is_connected(self) -> bool:
        """
        Checks if the local 'connection' is active.
        Mimics the healthy state check in AsyncConnection.is_connected().
        """
        with self._lock:
            return self._is_connected

    def connect(self, timeout: float = 10.0) -> None:
        """
        Initializes the local state.
        In the RabbitMQ version, this sets up exchanges; here, it marks the broker ready.
        """
        with self._lock:
            if self._is_connected:
                return

            log.info("Initializing SyncLocalConnection for vhost: %s", self.vhost)
            # In a local scenario, 'connecting' simply means the registry
            # and internal state are ready for use.
            self._is_connected = True

    def disconnect(self) -> None:
        """
        Cleanup logic to match SyncConnection.disconnect().
        """
        with self._lock:
            log.info("Disconnecting local broker for vhost: %s", self.vhost)
            # Stop all background subscriber threads
            tags = list(self.consumers.keys())
            for tag in tags:
                self.unsubscribe(tag)

            self._is_connected = False
            # Remove from singleton registry
            self._instance_by_vhost.pop(self.vhost, None)

    def _on_message(
        self,
        topic: str,
        callback: tp.Callable,
        stop_event: threading.Event,
        queue: Queue,
    ) -> None:
        """
        Handle incoming queue messages.

        Args:
            topic: The topic this message was received on
            callback: The callback function to process the message
        """
        while not stop_event.is_set():
            try:
                message = queue.get(timeout=0.1)
                if stop_event.is_set():  # Double check before triggering callback
                    break
                callback(message)
                log.debug("Message processed successfully on topic '%s'", topic)
            except RequeueMessage:
                log.warning("Requeuing message on topic '%s' after RequeueMessage exception", topic)
                time.sleep(self.requeue_delay)
                queue.put(message)
            except (Empty, TimeoutError):
                # Normal: No message within the timeout, loop again to check stop_event
                continue
            except (OSError, EOFError):
                # 2. This is the fix for your specific error.
                # It means the queue handle was closed (e.g., via disconnect() or unsubscribe())
                log.debug("Queue handle for '%s' closed. Stopping worker.", topic)
                stop_event.set()
            except Exception:
                log.exception(
                    "Unhandled exception processing message on topic '%s'. "
                    "Rejecting without requeue to prevent poison message.",
                    topic,
                )

    def subscribe(self, topic: str, callback: SyncCallback, shared: bool = False) -> str:
        """
        Subscribe to a topic. Non-blocking; starts a background thread.
        """

        with self._lock:
            log.info("Local Subscribing to topic '%s'", topic)
            if topic == self.logger_prefix:
                self.logger_queue = queue = multiprocessing.Queue()
                tag = f"local-sub-{topic}"
            else:
                # We use the hash of the task to generate a unique tag for async
                tag_suffix = "shared" if shared else threading.get_ident()
                tag = f"local-sub-{topic}-{tag_suffix}"

            if shared:
                if topic not in self._shared_queues:
                    self._shared_queues[topic] = multiprocessing.Queue()
                queue = self._shared_queues[topic]
            elif topic != self.logger_prefix:
                private_q = multiprocessing.Queue()
                self._exclusive_subscribers[topic].append(private_q)
                queue = private_q

            stop_event = threading.Event()
            func = functools.partial(
                self._on_message, topic, callback, stop_event=stop_event, queue=queue
            )
            thread = threading.Thread(target=func, daemon=True)
            # Store everything needed for cleanup
            self._subscription_registry[tag] = {
                "thread": thread,
                "topic": topic,
                "is_shared": shared,
                "queue": queue,
                "stop_event": stop_event,
            }
            thread.start()
            self.consumers[tag] = thread
            return tag

    def unsubscribe(self, topic: str, if_unused: bool = True, **kwargs) -> None:
        """
        Stops the background worker and cleans up resources.

        Args:
            tag: The tag returned by subscribe()
            if_unused:
        """
        with self._lock:
            tags_to_remove = []
            for tag, info in self._subscription_registry.items():
                if info["topic"] == topic:
                    tags_to_remove.append(tag)
            sub_infos = [
                sub_info
                for tag, sub_info in self._subscription_registry.items()
                if tag in tags_to_remove
            ]

            if not sub_infos:
                log.debug("Tag %s not found in local registry", tag)
                return
            for sub_info in sub_infos:
                # Retrieve the metadata we stored during subscribe()
                # sub_info = self._subscription_registry.pop(tag)
                topic = sub_info["topic"]
                is_shared = sub_info["is_shared"]
                queue_obj = sub_info["queue"]
                stop_event = sub_info["stop_event"]
                thread = sub_info["thread"]
                stop_event.set()
                log.info("Unsubscribing from topic '%s' (tag: %s)", topic, tag)
                # 1. Signal the background thread to stop
                stop_event.set()
                # close the consumer thread
                thread.join(timeout=3)
                # 2. Cleanup for Exclusive (Non-shared) queues
                if not is_shared:
                    # Remove this specific queue from the topic's fanout list
                    if (
                        topic in self._exclusive_subscribers
                        and queue_obj in self._exclusive_subscribers[topic]
                    ):
                        self._exclusive_subscribers[topic].remove(queue_obj)
                elif not if_unused:
                    self._shared_queues.pop(topic)
                    # Close the queue for tasks if forced
                    queue_obj.close()
                    queue_obj.join_thread()
            for tag in tags_to_remove:
                self.consumers.pop(tag, None)

    def purge(self, topic: str) -> None:
        """Empty the local queue of all messages."""
        if topic in self._shared_queues:
            log.info("Purging shared topic '%s'", topic)
            queue = self._shared_queues[topic]
            # Standard way to clear a multiprocessing queue
            try:
                while not queue.empty():
                    queue.get_nowait()
            except Exception:
                # Handle race condition if queue becomes empty during loop
                pass

    def get_message_count(self, topic: str) -> int:
        """Get the number of messages currently in the local queue."""
        if topic not in self._shared_queues:
            return 0
        # qsize() is available on multiprocessing.Queue but may raise
        # NotImplementedError on some platforms (like macOS).
        try:
            return self._shared_queues[topic].qsize()
        except NotImplementedError:
            log.warning("qsize() not supported on this platform; returning 0")
            return 0

    def get_consumer_count(self, topic: str) -> int:
        """Get the number of active background threads/processes for this topic."""
        # We track active subscriptions in the self.consumers dictionary
        # by matching the tag prefix we generated in subscribe()
        with self._lock:
            prefix = f"local-sub-{topic}-"
            consumers_for_topic = [tag for tag in self.consumers if tag.startswith(prefix)]
            return len(consumers_for_topic)

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        return False


class AsyncLocalConnection(BaseConnection):
    # Registry of SHARED queues for tasks
    _shared_queues: tp.Dict[str, multiprocessing.Queue] = {}
    # Registry of EXCLUSIVE subscribers
    _exclusive_subscribers = defaultdict(list)

    _lock = asyncio.Lock()
    _instance_by_vhost: tp.Dict[str, "AsyncLocalConnection"] = {}

    def __init__(self, vhost: str = "/", requeue_delay: int = 3):
        self.vhost = vhost
        self.requeue_delay = requeue_delay
        self._is_connected = False

        # Maps tag -> { "task": Task, "topic": str, "is_shared": bool, "queue": Queue, "stop_event": Event }
        self._subscription_registry = {}
        self.consumers: tp.Dict[str, asyncio.Task] = {}
        self.logger_queue = None
        self.logger_prefix = configuration.logger_prefix

    @classmethod
    async def get_connection(cls, vhost: str = "/") -> "AsyncLocalConnection":
        if vhost not in cls._instance_by_vhost:
            async with cls._lock:
                if vhost not in cls._instance_by_vhost:
                    instance = cls(vhost=vhost)
                    await instance.connect()
                    cls._instance_by_vhost[vhost] = instance
        return cls._instance_by_vhost[vhost]

    async def connect(self, timeout: float = 10.0) -> None:
        # async with self._lock:
        if self._is_connected:
            return
        log.info("Initializing AsyncLocalConnection for vhost: %s", self.vhost)
        self._is_connected = True

    async def disconnect(self) -> None:
        log.info("Disconnecting local broker for vhost: %s", self.vhost)
        tags = list(self.consumers.keys())
        await self.unsubscribe_by_tags(tags)
        async with self._lock:
            self._is_connected = False
            AsyncLocalConnection._instance_by_vhost.pop(self.vhost, None)

    def is_connected(self) -> bool:
        return self._is_connected

    async def publish(self, topic: str, message: Envelope) -> None:
        # Note: multiprocessing.Queue.put is thread-safe
        # but we use a thread to be safe in an async context.
        await asyncio.to_thread(SyncLocalConnection._sync_publish, self, topic, message)

    async def _on_message(
        self,
        topic: str,
        callback: AsyncCallback,
        stop_event: asyncio.Event,
        queue: multiprocessing.Queue,
    ) -> None:
        log.debug("Async worker started for topic '%s'", topic)
        while not stop_event.is_set():
            try:
                # Wrap the blocking queue.get in a thread
                message = await asyncio.to_thread(queue.get, timeout=0.1)
                if stop_event.is_set():  # Double check before triggering callback
                    break
                # Execute the async callback
                await callback(message)
                log.debug("Message processed successfully on topic '%s'", topic)

            except (Empty, TimeoutError):
                continue
            except RequeueMessage:
                log.warning("Requeuing message on topic '%s'", topic)
                await asyncio.sleep(self.requeue_delay)
                await asyncio.to_thread(queue.put, message)
            except (OSError, EOFError):
                log.debug("Queue handle for '%s' closed. Stopping worker.", topic)
                stop_event.set()
            except Exception:
                log.exception("Unhandled exception on topic '%s'. Rejecting.", topic)

    async def subscribe(self, topic: str, callback: "AsyncCallback", shared: bool = False) -> str:
        async with self._lock:
            log.info("Local Async Subscribing to topic '%s'", topic)
            if topic == self.logger_prefix:
                self.logger_queue = queue = multiprocessing.Queue()
                tag = f"local-sub-{topic}"
            else:
                # We use the hash of the task to generate a unique tag for async
                tag_suffix = "shared" if shared else id(asyncio.current_task())
                tag = f"local-sub-{topic}-{tag_suffix}"

            if shared:
                if topic not in self._shared_queues:
                    self._shared_queues[topic] = multiprocessing.Queue()
                queue = self._shared_queues[topic]
            elif topic != self.logger_prefix:
                private_q = multiprocessing.Queue()
                self._exclusive_subscribers[topic].append(private_q)
                queue = private_q

            stop_event = asyncio.Event()

            # Start the background task
            loop = asyncio.get_running_loop()
            task = loop.create_task(self._on_message(topic, callback, stop_event, queue))

            self._subscription_registry[tag] = {
                "task": task,
                "topic": topic,
                "is_shared": shared,
                "queue": queue,
                "stop_event": stop_event,
                "asyncio_loop": loop,
            }
            self.consumers[tag] = task
            return tag

    async def unsubscribe(self, topic: str, if_unused: bool = True, if_empty: bool = True) -> None:
        # Search for the tag associated with this topic in this context
        # In async, we check the registry for matching topics
        tags_to_remove = []
        async with self._lock:
            for tag, info in self._subscription_registry.items():
                if info["topic"] == topic:
                    tags_to_remove.append(tag)
            await self.unsubscribe_by_tags(tags_to_remove, if_unused, if_empty)

    async def unsubscribe_by_tags(
        self, tags: list[str], if_unused: bool = True, if_empty: bool = True
    ) -> None:
        sub_infos = [
            sub_info for tag, sub_info in self._subscription_registry.items() if tag in tags
        ]
        for sub_info in sub_infos:
            stop_event: asyncio.Event = sub_info["stop_event"]
            task: asyncio.Task = sub_info["task"]
            queue_obj = sub_info["queue"]
            topic = sub_info["topic"]
            is_shared = sub_info["is_shared"]

            log.info("Unsubscribing from topic '%s' (tag: %s)", topic, tags)
            stop_event.set()

            # Wait for the task to finish
            try:
                await asyncio.wait_for(task, timeout=3.0)
            except asyncio.TimeoutError:
                task.cancel()

            if not is_shared:
                if topic in self._exclusive_subscribers:
                    self._exclusive_subscribers[topic].remove(queue_obj)
            elif not if_unused:
                self._shared_queues.pop(topic, None)
                await asyncio.to_thread(queue_obj.close)
        for tag in tags:
            await self.consumers.pop(tag, None)

    async def purge(self, topic: str) -> None:
        if topic in self._shared_queues:
            queue = self._shared_queues[topic]

            def _sync_purge():
                try:
                    while not queue.empty():
                        queue.get_nowait()
                except Exception:
                    pass

            await asyncio.to_thread(_sync_purge)

    async def get_message_count(self, topic: str) -> int:
        if topic not in self._shared_queues:
            return 0
        try:
            return self._shared_queues[topic].qsize()
        except NotImplementedError:
            return 0

    async def get_consumer_count(self, topic: str) -> int:
        async with self._lock:
            prefix = f"local-sub-{topic}-"
            return len([t for t in self.consumers if t.startswith(prefix)])

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()
