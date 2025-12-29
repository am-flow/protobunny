"""Implements a Mosquitto (MQTT) Connection with both sync and async support"""
import asyncio
import logging
import os
import typing as tp
import uuid
from concurrent.futures import ThreadPoolExecutor

import aiomqtt
import can_ada

from ....config import default_configuration
from ....exceptions import ConnectionError, PublishError, RequeueMessage
from ....models import Envelope, IncomingMessageProtocol
from .. import BaseAsyncConnection

log = logging.getLogger(__name__)


VHOST = os.environ.get("MOSQUITTO_VHOST", "/")


async def connect() -> "Connection":
    """Get the singleton async connection."""
    conn = await Connection.get_connection(vhost=VHOST)
    return conn


async def reset_connection() -> "Connection":
    """Reset the singleton connection."""
    connection = await connect()
    await connection.disconnect()
    return await connect()


async def disconnect() -> None:
    connection = await connect()
    await connection.disconnect()


class Connection(BaseAsyncConnection):
    """Async Mosquitto Connection wrapper using aiomqtt."""

    instance_by_vhost: dict[str, "Connection | None"] = {}

    def __init__(
        self,
        username: str | None = None,
        password: str | None = None,
        host: str | None = None,
        port: int | None = None,
        url: str | None = None,
        worker_threads: int = 2,
        requeue_delay: int = 3,
        **kwargs,
    ):
        super().__init__()
        if url:
            # Parse the URL instead
            parsed = can_ada.parse(url)
            host = parsed.hostname or host
            port = parsed.port or port or 1883
            username = parsed.username
            password = parsed.password
        else:
            host or os.environ.get("MQTT_HOST") or os.environ.get("MOSQUITTO_HOST")
            port or os.environ.get("MQTT_PORT") or os.environ.get("MOSQUITTO_PORT")
            username or os.environ.get("MQTT_USERNAME") or os.environ.get("MOSQUITTO_USERNAME")
            password or os.environ.get("MQTT_PASSWORD") or os.environ.get("MOSQUITTO_PASSWORD")

        self.host = host or "127.0.0.1"
        self.port = int(port) if port else 1883
        self.username = username
        self.password = password

        self.requeue_delay = requeue_delay

        self.executor = ThreadPoolExecutor(
            max_workers=worker_threads
        )  # executor for sync callbacks
        self.consumers: dict[str, asyncio.Task] = {}
        self.queues: dict[str, dict] = {}
        self.stop_events: dict[str, asyncio.Event] = {}

        self._connection: aiomqtt.Client | None = None
        self._instance_lock: asyncio.Lock | None = None
        self._delimiter = default_configuration.backend_config.topic_delimiter  # e.g. "/"
        self._exchange = default_configuration.backend_config.namespace  # used as root prefix

    @property
    def is_connected_event(self) -> asyncio.Event:
        """Lazily create the event in the current running loop."""
        if self._is_connected_event is None:
            self._is_connected_event = asyncio.Event()
        return self._is_connected_event

    @property
    def lock(self) -> asyncio.Lock:
        if self._instance_lock is None:
            self._instance_lock = asyncio.Lock()
        return self._instance_lock

    def build_topic_key(self, topic: str) -> str:
        """Joins project prefix with topic using the configured delimiter."""
        return f"{self._exchange}{self._delimiter}{topic}"

    async def connect(self, timeout: float = 30.0) -> "Connection":
        async with self.lock:
            if self.is_connected():
                return self

            try:
                log.info("Connecting to Mosquitto at %s:%s", self.host, self.port)
                self._connection = aiomqtt.Client(
                    hostname=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                    timeout=timeout,
                )
                # We enter the context manager to establish the connection
                await self._connection.__aenter__()
                self.is_connected_event.set()
                log.info("Successfully connected to Mosquitto")
                return self
            except Exception as e:
                self.is_connected_event.clear()
                self._connection = None
                log.exception("Failed to connect to Mosquitto")
                raise ConnectionError(f"MQTT Connect failed: {e}")

    async def disconnect(self, **kwargs) -> None:
        async with self.lock:
            for tag, task in self.consumers.items():
                task.cancel()
            # Wait for them to finish so they exit their 'async with client' blocks
            if self.consumers:
                await asyncio.gather(*self.consumers.values(), return_exceptions=True)
            try:
                if self._connection:
                    await self._connection.__aexit__(None, None, None)
                    self._connection = None
            except Exception as e:
                log.warning("Failed to disconnect from Mosquitto: %s", e)

            self.is_connected_event.clear()
            log.info("Mosquitto connection closed")

    async def publish(self, topic: str, message: IncomingMessageProtocol, **kwargs) -> None:
        if not self.is_connected():
            raise ConnectionError("Not connected")

        topic_key = self.build_topic_key(topic)
        log.debug("MQTT Publish to %s", topic_key)

        # MQTT handles the distribution. If it's a task, the subscribers
        # will handle the 'shared' logic via their subscription string.
        await self._connection.publish(
            topic_key,
            payload=message.body,
            qos=kwargs.get("qos", 1),
            retain=kwargs.get("retain", False),
        )

    async def subscribe(self, topic: str, callback: tp.Callable, shared: bool = False) -> str:
        """Subscribes to a topic and starts a consumer loop in the background.
        Args:
            topic
            callback
            shared

        Returns:
            The consumer tag
        """
        timeout = 3.0
        log.debug("Subscribing topic: %s", topic)
        async with self.lock:
            queue_meta = await self.setup_queue(topic, shared)
            tag = queue_meta["tag"]
            log.debug("Subscribing consumer: %s", tag)
            ready_event = asyncio.Event()
            stop_event = asyncio.Event()
            task = asyncio.create_task(
                self._consumer_loop(queue_meta["sub_key"], callback, ready_event, stop_event)
            )

            try:
                await asyncio.wait_for(ready_event.wait(), timeout=timeout)
                self.consumers[tag] = task
                self.stop_events[tag] = stop_event
                log.debug("Subscribed consumer: %s", tag)
                return tag
            except asyncio.TimeoutError:
                stop_event.set()
                task.cancel()
                raise TimeoutError(f"Failed to subscribe to {topic} within {timeout}s.")

    async def _consumer_loop(
        self,
        sub_key: str,
        callback: tp.Callable,
        ready_event: asyncio.Event,
        stop_event: asyncio.Event,
    ):
        # Instantiate a NEW client locally instead of reusing self._connection
        client = aiomqtt.Client(
            hostname=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
        )
        try:
            async with client:
                await client.subscribe(sub_key)
                ready_event.set()
                # In 3.10, we manually iterate the generator to apply timeouts
                messages_iter = client.messages.__aiter__()
                # The loop needs to handle the stop_event signal even if no messages arrive
                while not stop_event.is_set():
                    try:
                        # Python 3.10 compatible timeout
                        message = await asyncio.wait_for(messages_iter.__anext__(), timeout=1.0)
                        # wrap the iterator in a time out context manager to avoid blocking if no messages arrive
                        if stop_event.is_set():
                            break
                        original_topic = message.topic.value.removeprefix(
                            f"{self._exchange}{self._delimiter}"
                        )
                        envelope = Envelope(body=message.payload, routing_key=original_topic)
                        await self._handle_callback(callback, envelope, original_topic)
                    except asyncio.TimeoutError:
                        continue  # Just a heartbeat to check stop_event

        except asyncio.CancelledError:
            pass
        finally:
            log.debug("Consumer loop stopped for %s", sub_key)

    async def _handle_callback(self, callback: tp.Callable, envelope: Envelope, topic: str):
        """Executes callback with Requeue logic similar to Redis/Rabbit."""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(envelope)
            else:
                await asyncio.get_event_loop().run_in_executor(self.executor, callback, envelope)
        except RequeueMessage:
            log.warning("Requeuing MQTT message for %s", topic)
            await asyncio.sleep(self.requeue_delay)
            await self.publish(topic, envelope)
        except Exception as exc:
            log.exception("Callback failed for topic %s", topic)
            raise PublishError(f"Failed to publish message to topic {topic}") from exc

    async def unsubscribe(self, tag: str, **kwargs):
        """Gracefully stops a consumer loop and unsubscribes from the broker."""
        async with self.lock:
            queue = self.queues.get(tag, None)
            log.debug("Unsubscribing tag: %s - %s", tag, str(queue) or "No queue found")
            stop_event = self.stop_events.get(tag)
            task = self.consumers.get(tag)

            if stop_event:
                stop_event.set()  # Signals the _consumer_loop to break

            if task:
                # We wait for the consumer loop to exit naturally and perform its cleanup
                try:
                    await asyncio.wait_for(task, timeout=5.0)
                except asyncio.TimeoutError:
                    task.cancel()  # Force kill if it doesn't stop
            # Cleanup state
            _ = self.consumers.pop(tag, None)
            self.stop_events.pop(tag, None)
            queue = self.queues.pop(tag, None)
            log.info("Successfully unsubscribed tag: %s", tag)

    async def purge(self, topic: str, **kwargs) -> None:
        """
        Clears the retained message for the topic.
        Note: This does not affect messages currently in flight to
        offline clients with persistent sessions.
        """

        async with self.lock:
            if not self.is_connected():
                raise ConnectionError("Not connected to Mosquitto")

            topic_key = self.build_topic_key(topic)
            sub_key = f"$share/shared_group/{topic_key}"
            log.debug("Purging shared topic: %s", sub_key)
            # Publishing a zero-length payload with retain=True deletes the retained message
            await self._connection.publish(sub_key, payload=None, qos=1, retain=True)

    def get_message_count(self, topic: str) -> int:
        # Mosquitto doesn't have a count messages API
        raise NotImplementedError

    def get_consumer_count(self, topic: str) -> int:
        # TODO
        raise NotImplementedError

    async def setup_queue(self, topic: str, shared: bool = False) -> dict:
        tag = f"consumer_{uuid.uuid4().hex[:8]}"
        topic_key = self.build_topic_key(topic)  # as used in Mosquitto
        group_name = "shared_group" if shared else ""

        # MQTT 5.0 Shared Subscription: $share/<group_name>/<topic>
        if shared:
            sub_key = f"$share/shared_group/{topic_key}"
        else:
            sub_key = topic_key

        queue = {
            "is_shared": shared,
            "group_name": group_name,
            "sub_key": sub_key,
            "tag": tag,
            "topic": topic,
            "topic_key": topic_key,
        }
        self.queues[tag] = queue
        return queue
