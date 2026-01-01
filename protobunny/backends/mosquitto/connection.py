"""Implements a Mosquitto Connection with sync support."""

import logging
import os
import threading
import time
import typing as tp
import uuid
from concurrent.futures import ThreadPoolExecutor

import can_ada
import paho.mqtt.client as mqtt

from ...conf import config
from ...exceptions import ConnectionError, PublishError, RequeueMessage
from ...models import Envelope, IncomingMessageProtocol
from .. import BaseConnection

log = logging.getLogger(__name__)

VHOST = os.environ.get("MOSQUITTO_VHOST", "/")


class Connection(BaseConnection):
    """Synchronous Mosquitto Connection wrapper using paho-mqtt."""

    _lock = threading.RLock()  # class level lock
    instance_by_vhost: dict[str, "Connection"] = {}

    @classmethod
    def get_connection(cls, vhost: str = "") -> "Connection":
        """Get singleton instance (sync)."""
        with cls._lock:
            if not cls.instance_by_vhost.get(vhost):
                cls.instance_by_vhost[vhost] = cls(vhost=vhost)
            if not cls.instance_by_vhost[vhost].is_connected():
                cls.instance_by_vhost[vhost].connect()
            return cls.instance_by_vhost[vhost]

    def is_connected(self) -> bool | tp.Awaitable[bool]:
        return self._main_client is not None and self._main_client.is_connected()

    def purge(self, topic: str, **kwargs) -> None:
        """
        Clears the retained message for the topic by publishing an empty payload.
        In Paho, we must wait for the publication to confirm the purge is complete.
        """
        with self._lock:
            if not self.is_connected():
                raise ConnectionError("Not connected to Mosquitto")

            topic_key = self.build_topic_key(topic)
            log.debug("Purging topic (clearing retained message): %s", topic_key)

            # Publish None (null payload) with retain=True
            result = self._main_client.publish(topic_key, payload=None, qos=1, retain=True)

            try:
                result.wait_for_publish(timeout=kwargs.get("timeout", 5.0))
                log.info("Successfully purged topic: %s", topic_key)
            except RuntimeError as e:
                # wait_for_publish raises RuntimeError if not connected
                raise ConnectionError(f"Could not purge topic {topic_key}: {e}")
            except Exception as e:
                log.error("Purge failed: %s", e)
                raise PublishError(f"Failed to clear retained message on {topic_key}")

    def get_message_count(self, topic: str) -> int:
        raise NotImplementedError("Mosquitto does not support message count")

    def get_consumer_count(self, topic: str) -> int:
        raise NotImplementedError("Mosquitto does not support consumer count per topic")

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
            parsed = can_ada.parse(url)
            self.host = parsed.hostname or host or "localhost"
            self.port = parsed.port or port or 1883
            self.username = parsed.username or username
            self.password = parsed.password or password
        else:
            self.host = host or os.environ.get("MQTT_HOST") or "localhost"
            self.port = int(port or os.environ.get("MQTT_PORT") or 1883)
            self.username = username or os.environ.get("MQTT_USERNAME")
            self.password = password or os.environ.get("MQTT_PASSWORD")

        self.requeue_delay = requeue_delay
        self.executor = ThreadPoolExecutor(max_workers=worker_threads)

        self.consumers: dict[str, threading.Thread] = {}
        self.stop_events: dict[str, threading.Event] = {}
        self.queues: dict[str, dict] = {}

        self._main_client: mqtt.Client | None = None

        self._delimiter = config.backend_config.topic_delimiter
        self._exchange = config.backend_config.namespace

    def build_topic_key(self, topic: str) -> str:
        return f"{self._exchange}{self._delimiter}{topic}"

    def connect(self, **kwargs) -> "Connection":
        with self._lock:
            if self.is_connected():
                return self
            try:
                # Use MQTT v5 for shared subscriptions support
                kwargs.pop("callback_api_version", None)
                self._main_client = mqtt.Client(
                    callback_api_version=mqtt.CallbackAPIVersion.VERSION2, **kwargs
                )
                if self.username:
                    self._main_client.username_pw_set(self.username, self.password)

                self._main_client.connect(self.host, self.port, keepalive=60)
                # Start the background loop for the main client (handles pings/reconnects)
                self._main_client.loop_start()
                return self
            except Exception as e:
                log.exception("Failed to connect to Mosquitto")
                raise ConnectionError(f"MQTT Connect failed: {e}")

    def disconnect(self, **kwargs) -> None:
        with self._lock:
            # Stop all consumers
            for tag in list(self.stop_events.keys()):
                self.unsubscribe(tag)
            # Disconnect paho client
            if self._main_client:
                self._main_client.loop_stop()
                self._main_client.disconnect()
                self._main_client = None

    def publish(self, topic: str, message: IncomingMessageProtocol, **kwargs) -> None:
        topic_key = self.build_topic_key(topic)
        result = self._main_client.publish(
            topic_key,
            payload=message.body,
            qos=kwargs.get("qos", 1),
            retain=kwargs.get("retain", False),
        )
        result.wait_for_publish(timeout=5.0)

    def subscribe(self, topic: str, callback: tp.Callable, shared: bool = False) -> str:
        with self._lock:
            queue_meta = self.setup_queue(topic, shared)
            tag = queue_meta["tag"]

            stop_event = threading.Event()
            ready_event = threading.Event()

            thread = threading.Thread(
                target=self._consumer_worker,
                args=(queue_meta["sub_key"], callback, ready_event, stop_event),
                daemon=True,
            )
            thread.start()

            if ready_event.wait(timeout=5.0):
                self.consumers[tag] = thread
                self.stop_events[tag] = stop_event
                return tag
            else:
                stop_event.set()
                raise ConnectionError(f"Consumer for {topic} failed to start")

    def _consumer_worker(
        self,
        sub_key: str,
        callback: tp.Callable,
        ready_event: threading.Event,
        stop_event: threading.Event,
    ):
        """Worker thread running a dedicated Paho client."""
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"sub_{uuid.uuid4().hex[:8]}",
        )

        def on_message(client, userdata, msg):
            original_topic = msg.topic.removeprefix(f"{self._exchange}{self._delimiter}")
            envelope = Envelope(body=msg.payload, routing_key=original_topic)
            try:
                # Bridge to the shared handling logic
                self._handle_callback(callback, envelope, original_topic)
            except Exception:
                log.exception("Callback error in sync worker")

        client.on_message = on_message
        if self.username:
            client.username_pw_set(self.username, self.password)

        try:
            client.connect(self.host, self.port)
            client.subscribe(sub_key)
            client.loop_start()
            ready_event.set()

            while not stop_event.is_set():
                time.sleep(0.1)

            client.unsubscribe(sub_key)
            client.loop_stop()
            client.disconnect()
        except Exception as e:
            log.error(f"Worker error for {sub_key}: {e}")
        finally:
            if not ready_event.is_set():
                ready_event.set()

    def _handle_callback(self, callback: tp.Callable, envelope: Envelope, topic: str):
        try:
            # Note: We use the executor even here to keep behavior consistent with Async version
            # If the callback is sync, it runs in the thread pool.
            self.executor.submit(callback, envelope).result()
        except RequeueMessage:
            time.sleep(self.requeue_delay)
            self.publish(topic, envelope)
        except Exception as exc:
            log.exception(f"Callback failed for topic {topic}")
            raise PublishError(f"Callback failure: {exc}")

    def unsubscribe(self, tag: str, **kwargs):
        with self._lock:
            stop_event = self.stop_events.pop(tag, None)
            thread = self.consumers.pop(tag, None)
            self.queues.pop(tag, None)

            if stop_event:
                stop_event.set()
            if thread:
                thread.join(timeout=2.0)

    def setup_queue(self, topic: str, shared: bool = False) -> dict:
        tag = f"consumer_{uuid.uuid4().hex[:8]}"
        topic_key = self.build_topic_key(topic)
        sub_key = f"$share/shared_group/{topic_key}" if shared else topic_key

        queue = {"tag": tag, "sub_key": sub_key, "topic": topic}
        self.queues[tag] = queue
        return queue
