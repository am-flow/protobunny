import logging
import os
import threading
import typing as tp
import uuid
from abc import ABC
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from queue import Empty, Queue

from ... import RequeueMessage
from ...config import default_configuration
from ...models import Envelope, SyncCallback, is_task
from .. import BaseConnection

log = logging.getLogger(__name__)

VHOST = os.environ.get("PYTHON_VHOST", "/")


class MessageBroker:
    def __init__(self):
        self._shared_queues: dict[str, list[Queue]] = defaultdict(list)
        self._exclusive_queues: dict[str, list[Queue]] = defaultdict(list)
        self.lock = threading.RLock()
        self.logger_queue: Queue | None = None

    def publish(self, topic: str, message: Envelope) -> bool:
        """Publish a message to all relevant queues."""
        published = False
        shared = is_task(topic)
        with self.lock:
            if self.logger_queue:
                self.logger_queue.put(message)
            if shared:
                queues = self._shared_queues.get(topic)
                if not queues:
                    log.warning("No subscribers for tasks %s", topic)
                    return False
                # Select the queue with the least messages (Load Balancing)
                best_queue = min(queues, key=lambda q: q.qsize())
                best_queue.put(message)
                return True
            # For exact matches:
            if topic in self._exclusive_queues:
                for queue in self._exclusive_queues[topic]:
                    queue.put(message)
                    published = True
            # Fan out
            for sub_topic, queues in self._exclusive_queues.items():
                if sub_topic.endswith("#") and topic.startswith(sub_topic[:-1]):
                    for queue in queues:
                        queue.put(message)
                        published = True

        return published

    def create_shared_queue(self, topic: str) -> Queue:
        """Get or create a shared queue."""
        with self.lock:
            queue = Queue()
            self._shared_queues[topic].append(queue)
            return queue

    def create_exclusive_queue(self, topic: str) -> Queue:
        """Create an exclusive queue for a topic."""
        with self.lock:
            queue = Queue()
            self._exclusive_queues[topic].append(queue)
            return queue

    def remove_exclusive_queue(self, topic: str, queue: Queue) -> None:
        """Remove an exclusive queue."""
        with self.lock:
            if topic in self._exclusive_queues:
                try:
                    self._exclusive_queues[topic].remove(queue)
                except ValueError:
                    pass

    def remove_shared_queues(self, topic: str) -> None:
        """Remove all in process subscriptions for a shared queue."""
        with self.lock:
            self._shared_queues.pop(topic, None)

    def purge_queue(self, topic: str) -> None:
        """Empty a shared queue."""
        with self.lock:
            queues = self._shared_queues.get(topic, [])
            for queue in queues:
                while not queue.empty():
                    try:
                        queue.get_nowait()
                    except Empty:
                        break

    def get_message_count(self, topic: str) -> int:
        """Get queue size of the shared queues."""
        with self.lock:
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
    """Synchronous local connection using threads."""

    instance_by_vhost: dict[str, "Connection"] = {}
    _is_connected_event: threading.Event | None
    lock = threading.RLock()
    _connection_cls = MessageBroker

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._connection = self._connection_cls()
        self._is_connected_event = None
        self.executor = ThreadPoolExecutor(max_workers=10, thread_name_prefix="PB Py-Worker")

    @staticmethod
    def create_tag(topic: str, shared: bool) -> str:
        """Generate subscription tag."""
        context_id = threading.get_ident() if not shared else uuid.uuid4()
        suffix = f"shared-{context_id}" if shared else context_id
        return f"local-sub-{topic}-{suffix}"

    def purge(self, topic: str, **kwargs) -> None:
        self._connection.purge_queue(topic)

    def get_message_count(self, topic: str) -> int:
        return self._connection.get_message_count(topic)

    def setup_queue(self, topic: str, shared: bool) -> Queue:
        """Create appropriate queue type."""
        if topic == self.logger_prefix:
            self._connection.logger_queue = Queue()
            return self._connection.logger_queue
        elif shared:
            return self._connection.create_shared_queue(topic)
        else:
            return self._connection.create_exclusive_queue(topic)

    def is_connected(self) -> bool:
        return self.is_connected_event.is_set()

    @property
    def is_connected_event(self) -> threading.Event:
        """Lazily create the event in the current running loop."""
        if self._is_connected_event is None:
            self._is_connected_event = threading.Event()
        return self._is_connected_event

    @classmethod
    def get_connection(cls, vhost: str = "/") -> "Connection":
        if vhost not in cls.instance_by_vhost:
            with cls.lock:
                if vhost not in cls.instance_by_vhost:
                    instance = cls(vhost=vhost)
                    instance.connect()
                    cls.instance_by_vhost[vhost] = instance
        return cls.instance_by_vhost[vhost]

    def connect(self, timeout: float = 10.0) -> "Connection":
        with self.lock:
            self.is_connected_event.set()
            return self

    def disconnect(self, **kwargs) -> None:
        with self.lock:
            log.info("Disconnecting SyncLocalConnection for vhost: %s", self.vhost)
            for tag in list(self._subscriptions.keys()):
                self._unsubscribe_by_tag(tag, if_unused=False)
            # Shutdown the pool
            self.executor.shutdown(wait=True)
            self.is_connected_event.clear()
            self.instance_by_vhost.pop(self.vhost, None)

    def publish(self, topic: str, message: Envelope, **kwargs) -> None:
        if not self._connection.publish(topic, message):
            log.warning("No subscribers for topic '%s'", topic)

    def _message_worker(
        self, topic: str, callback: SyncCallback, stop_event: threading.Event, queue: Queue
    ) -> None:
        """Worker thread for processing messages."""
        while not stop_event.is_set():
            message = None
            try:
                message = queue.get(timeout=0.5)
                if stop_event.is_set():
                    break
                callback(message)
                queue.task_done()
                log.debug("Message processed on topic '%s'", topic)
            except RequeueMessage:
                log.warning("Requeuing message on topic '%s'", topic)
                # Start a timer to requeue so this thread isn't blocked
                threading.Timer(self.requeue_delay, lambda m=message: queue.put(m)).start()
                queue.task_done()
            except Empty:
                continue
            except Exception:
                log.exception("Error processing message on topic '%s'", topic)

    def subscribe(self, topic: str, callback: SyncCallback, shared: bool = False) -> str:
        with self.lock:
            tag = self.create_tag(topic, shared)
            if tag in self._subscriptions:
                log.warning("Already subscribed with tag '%s'", tag)
                return tag

            queue = self.setup_queue(topic, shared)
            stop_event = threading.Event()
            future = self.executor.submit(self._message_worker, topic, callback, stop_event, queue)

            self._subscriptions[tag] = {
                "topic": topic,
                "shared": shared,
                "queue": queue,
                "stop_event": stop_event,
                "future": future,
            }

            log.info("Subscribed to topic '%s' (tag: %s)", topic, tag)
            return tag

    def unsubscribe(self, topic: str, if_unused: bool = True, **kwargs) -> None:
        with self.lock:
            tags = [tag for tag, info in self._subscriptions.items() if info["topic"] == topic]
            for tag in tags:
                self._unsubscribe_by_tag(tag, if_unused)

    def _cleanup_queue(self, topic: str, queue: Queue, shared: bool, if_unused: bool) -> None:
        """Clean up queue resources."""
        if topic == self.logger_prefix:
            self._connection.logger_queue = None
        elif not shared:
            self._connection.remove_exclusive_queue(topic, queue)
        elif not if_unused:
            self._connection.remove_shared_queues(topic)

    def _unsubscribe_by_tag(self, tag: str, if_unused: bool = True) -> None:
        """Internal unsubscribe by tag."""
        log.debug("Unsubscribing by tag: %s", tag)
        info = self._subscriptions.pop(tag, None)
        if not info:
            return

        log.info("Unsubscribing from topic '%s' (tag: %s)", info["topic"], tag)

        # Stop worker thread
        info["stop_event"].set()
        # Cleanup queue
        self._cleanup_queue(info["topic"], info["queue"], info["shared"], if_unused)

    def get_consumer_count(self, topic: str) -> int:
        with self.lock:
            return sum(1 for info in self._subscriptions.values() if info["topic"] == topic)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False


# Convenience functions
def get_connection() -> Connection:
    return Connection.get_connection(vhost=VHOST)


def reset_connection() -> Connection:
    connection = get_connection()
    connection.disconnect()
    return get_connection()


def disconnect() -> None:
    connection = get_connection()
    connection.disconnect()
