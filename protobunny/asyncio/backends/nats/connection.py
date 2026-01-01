"""Implements a NATS Connection"""
import asyncio
import functools
import logging
import os
import typing as tp
import urllib.parse
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import can_ada
import nats
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.errors import ConnectionClosedError, TimeoutError
from nats.js.errors import BadRequestError, NoStreamResponseError

from ....conf import config
from ....exceptions import ConnectionError, PublishError, RequeueMessage
from ....models import Envelope, IncomingMessageProtocol
from .. import BaseAsyncConnection, is_task

log = logging.getLogger(__name__)

VHOST = os.environ.get("NATS_VHOST", "/")


class Connection(BaseAsyncConnection):
    """Async NATS Connection wrapper."""

    _lock: asyncio.Lock | None = None
    instance_by_vhost: dict[str, "Connection | None"] = {}

    def __init__(
        self,
        username: str | None = None,
        password: str | None = None,
        host: str | None = None,
        port: int | None = None,
        vhost: str = "",
        url: str | None = None,
        worker_threads: int = 2,
        prefetch_count: int = 1,
        requeue_delay: int = 3,
        heartbeat: int = 1200,
    ):
        """Initialize NATS connection.

        Args:
            username: NATS username
            password: NATS password
            host: NATS host
            port: NATS port
            url: NATS URL. It will override username, password, host and port
            vhost: NATS virtual host (it's used as db number string)
            worker_threads: number of concurrent callback workers to use
            prefetch_count: how many messages to prefetch from the queue
            requeue_delay: how long to wait before re-queueing a message (seconds)
        """
        super().__init__()
        uname = username or os.environ.get("NATS_USERNAME", "")
        passwd = password or os.environ.get("NATS_PASSWORD", "")
        host = host or os.environ.get("NATS_HOST", "localhost")
        port = port or int(os.environ.get("NATS_PORT", "4222"))
        # URL encode credentials and vhost to prevent injection
        vhost = vhost or VHOST
        self.vhost = vhost
        username = urllib.parse.quote(uname, safe="")
        password = urllib.parse.quote(passwd, safe="")
        host = urllib.parse.quote(host, safe="")
        # URL for connection
        url = url or os.environ.get("NATS_URL", "")
        if url:
            # reconstruct url for safety
            parsed = can_ada.parse(url)
            url = f"{parsed.protocol}//{parsed.username}:{parsed.password}@{parsed.host}{parsed.pathname}{parsed.search}"
        else:
            # Build the URL based on what is available
            if username and password:
                url = f"nats://{username}:{password}@{host}:{port}{vhost}"
            elif password:
                url = f"nats://:{password}@{host}:{port}{vhost}"
            elif username:
                url = f"nats://{username}@{host}:{port}{vhost}"
            else:
                url = f"nats://{host}:{port}{vhost}"

        self._url = url
        self._connection: nats.NATS | None = None
        self.prefetch_count = prefetch_count
        self.requeue_delay = requeue_delay
        self.heartbeat = heartbeat
        self.queues: dict[str, list[dict]] = defaultdict(list)
        self.consumers: dict[str, dict] = {}
        # to run sync callbacks
        self.executor = ThreadPoolExecutor(max_workers=worker_threads)
        self._instance_lock: asyncio.Lock | None = None
        self._delimiter = config.backend_config.topic_delimiter
        self._namespace = config.backend_config.namespace
        self._tasks_subject_prefix = "TASKS"
        self._stream_name = f"{self._namespace.upper()}_{self._tasks_subject_prefix}"

    async def __aenter__(self) -> "Connection":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        await self.disconnect()
        return False

    @property
    def lock(self) -> asyncio.Lock:
        """Lazy instance lock."""
        if self._instance_lock is None:
            self._instance_lock = asyncio.Lock()
        return self._instance_lock

    @classmethod
    def _get_class_lock(cls) -> asyncio.Lock:
        """Ensure the class lock is bound to the current running loop."""
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

    def build_topic_key(self, topic: str) -> str:
        return f"{self._namespace}.{topic}"

    @property
    def is_connected_event(self) -> asyncio.Event:
        """Lazily create the event in the current running loop."""
        if self._is_connected_event is None:
            self._is_connected_event = asyncio.Event()
        return self._is_connected_event

    @property
    def connection(self) -> "nats.NATS":
        """Get the connection object.

        Raises:
            ConnectionError: If not connected
        """
        if not self._connection:
            raise ConnectionError("Connection not initialized. Call connect() first.")
        return self._connection

    async def connect(self, **kwargs) -> "Connection":
        """Establish NATS connection.

        Args:

        Raises:
            ConnectionError: If connection fails
            asyncio.TimeoutError: If connection times out
        """
        async with self.lock:
            if self.instance_by_vhost.get(self.vhost) and self.is_connected():
                return self.instance_by_vhost[self.vhost]
            try:
                log.info("Establishing NATS connection to %s", self._url.split("@")[-1])
                self._connection = await nats.connect(self._url, **kwargs)
                self.is_connected_event.set()
                log.info("Successfully connected to NATS")
                self.instance_by_vhost[self.vhost] = self
                if config.use_tasks_in_nats:
                    # Create the jetstream if not existing
                    js = self._connection.jetstream()
                    # For NATS, tasks package can only be at first level after main package library
                    # Warning: don't bury tasks messages after three levels of hierarchy
                    task_patterns = [
                        f"{self._tasks_subject_prefix}{self._delimiter}>",
                    ]
                    try:
                        await js.add_stream(
                            name=self._stream_name,
                            subjects=task_patterns,
                        )
                    except BadRequestError:
                        # This usually means the stream already exists with a different config
                        log.warning("Stream %s exists with different settings.", self._stream_name)
                return self

            except asyncio.TimeoutError as e:
                log.error("NATS connection timeout")
                self.is_connected_event.clear()
                self._connection = None
                raise ConnectionError(f"Failed to connect to NATS: {e}") from e
            except Exception as e:
                self.is_connected_event.clear()
                self._connection = None
                log.exception("Failed to establish NATS connection")
                raise ConnectionError(f"Failed to connect to NATS: {e}") from e

    async def disconnect(self, timeout: float = 10.0) -> None:
        """Close NATS connection and cleanup resources.

        Args:
            timeout: Maximum time to wait for cleanup (seconds)
        """
        async with self.lock:
            if not self.is_connected():
                log.debug("Already disconnected from NATS")
                return

            try:
                log.info("Closing NATS connection")
                # Cancel all subscriptions
                for tag, consumer in self.consumers.items():
                    subscription = consumer["subscription"]
                    try:
                        await subscription.unsubscribe()
                        # We give the task a moment to wrap up if needed
                        # await asyncio.sleep(0)  # force context switching
                        # await asyncio.wait([task], timeout=2.0)
                    except Exception as e:
                        log.warning("Error stopping NATS subscription %s: %s", tag, e)

                # Shutdown Thread Executor (if used for sync callbacks)
                self.executor.shutdown(wait=False, cancel_futures=True)

                # Close the NATS Connection Pool
                if self._connection:
                    await asyncio.wait_for(self._connection.close(), timeout=timeout)

            except asyncio.TimeoutError:
                log.warning("NATS connection close timeout after %.1f seconds", timeout)
            except Exception:
                log.exception("Error during NATS disconnect")
            finally:
                # Reset state
                self._connection = None
                self.queues.clear()  # (Local queue metadata)
                self.consumers.clear()
                self.is_connected_event.clear()
                # Remove from registry
                Connection.instance_by_vhost.pop(self.vhost, None)
                log.info("NATS connection closed")

    # Subscriptions methods
    async def setup_queue(
        self, topic: str, shared: bool, callback: tp.Callable | None = None
    ) -> Subscription:
        topic_key = self.build_topic_key(topic)
        cb = functools.partial(self._nats_handler, callback)
        if shared:
            js = self._connection.jetstream()
            # We use a durable name so multiple instances share the same task state
            group_name = topic_key.replace(".", "_")
            log.debug(
                "Subscribing shared worker to JetStream group %s subject %s", group_name, topic_key
            )
            subscription = await js.subscribe(
                # the topic with prefixes
                subject=f"{self._tasks_subject_prefix}{self._delimiter}{topic_key}",
                # add queue parameter to flag it as a distributed queue
                queue=group_name,
                durable=group_name,
                cb=cb,
                manual_ack=True,
                stream=self._stream_name,
            )
        else:
            log.debug("Subscribing broadcast listener to NATS Core: %s", topic_key)
            subscription = await self._connection.subscribe(subject=topic_key, cb=cb)
        return subscription

    async def subscribe(self, topic: str, callback: tp.Callable, shared: bool = False) -> str:
        async with self.lock:
            if not self.is_connected():
                raise ConnectionError("Not connected to NATS")

            topic_key = self.build_topic_key(topic)
            sub_tag = f"{topic_key}_{uuid.uuid4().hex[:8]}"
            subscription = await self.setup_queue(topic, shared, callback)
            self.consumers[sub_tag] = {
                "subscription": subscription,
                "topic": topic_key,
                "is_shared": shared,
            }
            return sub_tag

    async def _nats_handler(self, callback, msg: Msg):
        """Callback that handles the Msg object pushed from NATS"""
        topic = msg.subject
        is_shared_queue = is_task(topic)
        reply = msg.reply
        body = msg.data
        # Remove the 'TASKS.' prefix that was added to match the filtering stream group
        if is_shared_queue:
            topic = topic.removeprefix(f"{self._tasks_subject_prefix}{self._delimiter}")

        # The routing key is the string used to match the protobuf python class fqn
        routing_key = topic.removeprefix(f"{self._namespace}{self._delimiter}")
        envelope = Envelope(body=body, correlation_id=reply, routing_key=routing_key)
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(envelope)
            else:
                # Run the callback in a thread pool to avoid blocking the event loop
                await asyncio.get_event_loop().run_in_executor(self.executor, callback, envelope)
            if is_shared_queue:
                await msg.ack()
        except RequeueMessage:
            log.warning("Requeuing message on topic '%s' after RequeueMessage exception", topic)
            await asyncio.sleep(self.requeue_delay)
            if not is_shared_queue:
                await self._connection.publish(topic, body, reply=reply)
            else:
                await msg.nak(self.requeue_delay)
        except Exception:
            log.exception("Callback failed for topic %s", topic)
            await msg.term()  # avoid retry logic for potentially poisoning messages

    async def unsubscribe(self, tag: str, **kwargs) -> None:
        if tag not in self.consumers:
            return
        sub_info = self.consumers[tag]
        await sub_info["subscription"].unsubscribe()
        del sub_info["subscription"]
        log.info("Unsubscribed from %s", sub_info["topic"])
        self.consumers.pop(tag)

    async def publish(
        self,
        topic: str,
        message: "IncomingMessageProtocol",
        **kwargs,
    ) -> None:
        if not self.is_connected():
            raise ConnectionError("Not connected to NATS")
        topic_key = self.build_topic_key(topic)
        is_shared = is_task(topic)

        # Standardize headers
        headers = {"correlation_id": message.correlation_id} if message.correlation_id else None

        try:
            if is_shared:
                # Persistent "Task" publishing via JetStream
                stream_key = f"{self._tasks_subject_prefix}{self._delimiter}{topic_key}"
                log.debug("Publishing persistent task to NATS JetStream: %s", topic_key)
                js = self._connection.jetstream()
                await js.publish(subject=stream_key, payload=message.body, headers=headers)
                if config.log_task_in_nats:
                    # The logger service doesn't use jetstream so we re-publish on a normal pubsub
                    # (it won't be re-catched by the tasks consumer)
                    log.debug("Publishing logging message for task to NATS Core: %s", topic_key)
                    await self._connection.publish(
                        subject=topic_key, payload=message.body, headers=headers
                    )
            else:
                # Volatile "PubSub" publishing via NATS Core
                log.debug("Publishing broadcast to NATS Core: %s", topic_key)
                await self._connection.publish(
                    subject=topic_key, payload=message.body, headers=headers
                )
        except (ConnectionClosedError, TimeoutError, NoStreamResponseError, Exception) as e:
            log.error("NATS publish failed: %s", e)
            raise PublishError(str(e)) from e

    async def purge(self, topic: str, reset_groups: bool = False) -> None:
        if not is_task(topic):
            raise ValueError("Purge only supported for tasks")
        async with self.lock:
            if not self.is_connected():
                raise ConnectionError("Not connected to NATS")
            topic_key = self.build_topic_key(topic)
            subject = f"{self._tasks_subject_prefix}{self._delimiter}{topic_key}"
            # NATS purges messages matching a subject within the stream
            try:
                jsm = self._connection.jsm()  # Get JetStream Management context

                log.info("Purging NATS subject '%s' from stream %s", topic, self._stream_name)
                await jsm.purge_stream(self._stream_name, subject=subject)

                if reset_groups:
                    # In NATS, we must find consumers specifically tied to this topic
                    # Protobunny convention: durable name includes the topic
                    group_name = f"{topic_key.replace('.', '_')}"
                    try:
                        await jsm.delete_consumer(self._stream_name, group_name)
                        log.debug("Deleted NATS durable consumer: %s", group_name)
                    except nats.js.errors.NotFoundError:
                        pass  # Consumer already gone

            except Exception as e:
                log.error("Failed to purge NATS subject %s: %s", topic, e)
                raise ConnectionError(f"Purge failed: {e}")

    async def get_message_count(self, topic: str) -> int:
        if not is_task(topic):
            raise ValueError("Purge only supported for tasks")
        async with self.lock:
            if not self.is_connected():
                raise ConnectionError("Not connected to NATS")
            topic_key = self.build_topic_key(topic)
            try:
                jsm = self._connection.jsm()
                stream_info = await jsm.stream_info(self._stream_name, subjects_filter=topic)
                return stream_info.state.messages
            except nats.js.errors.NotFoundError:
                return 0
            except Exception as e:
                log.error("Failed to get NATS message count for %s: %s", topic_key, e)
                return 0

    async def get_consumer_count(self, topic: str) -> int:
        topic_key = self.build_topic_key(topic)
        if not is_task(topic):
            raise ValueError("Purge only supported for tasks")
        async with self.lock:
            if not self.is_connected():
                raise ConnectionError("Not connected to NATS")
            try:
                jsm = self._connection.jsm()
                stream_info = await jsm.stream_info(self._stream_name, subjects_filter=topic)
                return stream_info.state.consumer_count
            except nats.js.errors.NotFoundError:
                return 0
            except Exception as e:
                log.error("Failed to get NATS consumer count for %s: %s", topic_key, e)
                return 0
