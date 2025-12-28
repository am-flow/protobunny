"""Implements a Redis Connection with both sync and async support"""
import asyncio
import functools
import logging
import os
import typing as tp
import urllib.parse
import uuid
from concurrent.futures import ThreadPoolExecutor

import redis.asyncio as redis
from redis import RedisError, ResponseError

from ....config import default_configuration
from ....exceptions import ConnectionError, PublishError, RequeueMessage
from ....models import Envelope, IncomingMessageProtocol
from .. import BaseAsyncConnection, is_task

log = logging.getLogger(__name__)

VHOST = os.environ.get("REDIS_VHOST") or os.environ.get("REDIS_DB", "0")


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
    """Async Redis Connection wrapper."""

    _lock: asyncio.Lock | None = None
    instance_by_vhost: dict[str, "Connection | None"] = {}

    def __init__(
        self,
        username: str | None = None,
        password: str | None = None,
        host: str | None = None,
        port: int | None = None,
        db: int | None = None,
        vhost: str = "",
        url: str | None = None,
        worker_threads: int = 2,
        prefetch_count: int = 1,
        requeue_delay: int = 3,
        heartbeat: int = 1200,
    ):
        """Initialize Redis connection.

        Args:
            username: Redis username
            password: Redis password
            host: Redis host
            port: Redis port
            url: Redis URL. It will override username, password, host and port
            vhost: Redis virtual host (it's used as db number string)
            db: Redis database number
            worker_threads: number of concurrent callback workers to use
            prefetch_count: how many messages to prefetch from the queue
            requeue_delay: how long to wait before re-queueing a message (seconds)
        """
        super().__init__()
        uname = username or os.environ.get(
            "REDIS_USERNAME", os.environ.get("REDIS_DEFAULT_USER", "")
        )
        passwd = password or os.environ.get(
            "REDIS_PASSWORD", os.environ.get("REDIS_DEFAULT_PASS", "")
        )
        host = host or os.environ.get("REDIS_HOST", "127.0.0.1")
        port = port or int(os.environ.get("REDIS_PORT", "6379"))
        db = db or int(os.environ.get("REDIS_DB", "0"))
        # URL encode credentials and vhost to prevent injection
        vhost = vhost or VHOST or str(db)
        self.vhost = vhost
        username = urllib.parse.quote(uname, safe="")
        password = urllib.parse.quote(passwd, safe="")
        host = urllib.parse.quote(host, safe="")
        # URL for connection
        url = url or os.environ.get("REDIS_URL")
        if not url:
            # Build the URL based on what is available
            if username and password:
                url = f"redis://{username}:{password}@{host}:{port}/{vhost}?protocol=3"
            elif password:
                url = f"redis://:{password}@{host}:{port}/{vhost}?protocol=3"
            else:
                url = f"redis://{host}:{port}/{vhost}?protocol=3"
        self._url = url
        # self._loop: asyncio.AbstractEventLoop | None = None
        self._connection: redis.Redis | None = None
        self.prefetch_count = prefetch_count
        self.requeue_delay = requeue_delay
        self.heartbeat = heartbeat
        self.queues: dict[str, dict] = {}
        self.consumers: dict[str, dict[str, tp.Any]] = {}
        self.executor = ThreadPoolExecutor(max_workers=worker_threads)
        self._instance_lock: asyncio.Lock | None = None

        self._delimiter = default_configuration.backend_config.topic_delimiter
        # self._exchange = f"{default_configuration.project_name}"
        self._exchange = default_configuration.backend_config.namespace

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
        return f"{self._exchange}:{topic}"

    async def disconnect(self, timeout: float = 10.0) -> None:
        """Close Redis connection and cleanup resources.

        Args:
            timeout: Maximum time to wait for cleanup (seconds)
        """
        async with self.lock:
            if not self.is_connected():
                log.debug("Already disconnected from Redis")
                return

            try:
                log.info("Closing Redis connection")

                # In Redis, consumers are local asyncio Tasks.
                # We cancel them here. Note: Redis doesn't have "exclusive queues"
                # that auto-delete, so we just clear our local registry.
                for tag, consumer in self.consumers.items():
                    task = consumer["task"]
                    try:
                        task.cancel()
                        # We give the task a moment to wrap up if needed
                        await asyncio.sleep(0)  # force context switching
                        await asyncio.wait([task], timeout=2.0)
                    except Exception as e:
                        log.warning("Error stopping Redis consumer %s: %s", tag, e)

                # 2. Shutdown Thread Executor (if used for sync callbacks)
                self.executor.shutdown(wait=False, cancel_futures=True)

                # 3. Close the Redis Connection Pool
                if self._connection:
                    # aclose() closes the connection pool and all underlying connections
                    await asyncio.wait_for(self._connection.aclose(), timeout=timeout)

            except asyncio.TimeoutError:
                log.warning("Redis connection close timeout after %.1f seconds", timeout)
            except Exception:
                log.exception("Error during Redis disconnect")
            finally:
                # 4. Reset state
                self._connection = None
                self._exchange = None  # (Stream name/prefix)
                self.queues.clear()  # (Local queue metadata)
                self.consumers.clear()
                self.is_connected_event.clear()

                # 5. Remove from registry
                Connection.instance_by_vhost.pop(self.vhost, None)
                log.info("Redis connection closed")

    @property
    def is_connected_event(self) -> asyncio.Event:
        """Lazily create the event in the current running loop."""
        if self._is_connected_event is None:
            self._is_connected_event = asyncio.Event()
        return self._is_connected_event

    @property
    def connection(self) -> "redis.Redis":
        """Get the connection object.

        Raises:
            ConnectionError: If not connected
        """
        if not self._connection:
            raise ConnectionError("Connection not initialized. Call connect() first.")
        return self._connection

    async def connect(self, timeout: float = 30.0) -> "Connection":
        """Establish Redis connection.

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
                # Parsing URL for logging (removing credentials)
                log.info("Establishing Redis connection to %s", self._url.split("@")[-1])

                # Using from_url handles connection pooling automatically
                self._connection = redis.from_url(
                    self._url,
                    decode_responses=False,
                    socket_connect_timeout=timeout,
                    health_check_interval=self.heartbeat,
                )

                await asyncio.wait_for(self._connection.ping(), timeout=timeout)
                self.is_connected_event.set()
                log.info("Successfully connected to Redis")
                self.instance_by_vhost[self.vhost] = self
                return self

            except asyncio.TimeoutError:
                log.error("Redis connection timeout after %.1f seconds", timeout)
                self.is_connected_event.clear()
                self._connection = None
                raise
            except Exception as e:
                if self._connection:
                    await self._connection.aclose()

                self.is_connected_event.clear()
                self._connection = None
                log.exception("Failed to establish Redis connection")
                raise ConnectionError(f"Failed to connect to Redis: {e}") from e

    async def subscribe(self, topic: str, callback: tp.Callable, shared: bool = False) -> str:
        """Subscribe to Redis.

        Args:
            topic: The stream key/topic to subscribe to
            callback: Function to handle incoming messages.
            shared: If True, uses a shared consumer group (round-robin).
                    If False, uses a unique group for this instance and let redis pubsub manage the routing.

        Returns:
            A unique subscription tag used to stop the consumer later.
        """
        async with self.lock:
            if not self.is_connected():
                raise ConnectionError("Not connected to Redis")
            queue = await self.setup_queue(topic, shared)
            ready_event = asyncio.Event()
            stop_event = asyncio.Event()
            if queue["is_shared"]:
                assert (
                    queue["mechanism"] == "stream"
                ), f"Invalid queue mechanism: {queue['mechanism']}"

                # create the asyncio task for the consumer loop
                log.debug("Subscribing callback to Redis PubSub topic %s", queue["key"])
                task = asyncio.create_task(
                    self._tasks_consumer_loop(
                        queue["key"],
                        queue["group_name"],
                        queue["tag"],
                        callback,
                        ready_event,
                        stop_event,
                    )
                )

                # Wait for the loop to signal it has performed the first check
                await asyncio.wait_for(ready_event.wait(), timeout=1.0)
                # register the topic in the registry
                # We store the task so we can cancel it in 'disconnect'
                self.consumers[queue["tag"]] = {
                    "task": task,
                    "key": queue["key"],
                    "topic": topic,
                    "stop_event": stop_event,
                }
                log.info("Redis consumer %s subscribed to %s", queue["tag"], queue["key"])
                return queue["tag"]
            else:
                # Using redis.PubSub
                assert (
                    queue["mechanism"] == "pubsub"
                ), f"Invalid queue mechanism: {queue['mechanism']}"
                # create the asyncio task for the consumer loop
                task = asyncio.create_task(
                    self._pubsub_consumer_loop(ready_event, callback, queue, stop_event)
                )
                # Wait for the loop to signal it has performed the first check
                await asyncio.wait_for(ready_event.wait(), timeout=1.0)
                # register the topic in the registry
                # We store the task so we can cancel it in 'disconnect'
                self.consumers[queue["tag"]] = {
                    "task": task,
                    "key": queue["key"],
                    "topic": topic,
                    "stop_event": stop_event,
                }
                log.info("Redis consumer %s subscribed to %s", queue["tag"], topic)
                return queue["tag"]

    async def _pubsub_consumer_loop(
        self,
        ready_event: asyncio.Event,
        callback: tp.Callable,
        queue: dict,
        stop_event: asyncio.Event,
    ):
        pubsub = self._connection.pubsub()
        callback = functools.partial(self._on_message_pubsub, queue["key"], callback)
        log.debug("Subscribing callback to Redis PubSub topic %s", queue["key"])
        if any(char in queue["key"] for char in ("*", "[", "?")):
            await pubsub.psubscribe(queue["key"])
        else:
            await pubsub.subscribe(queue["key"])
        ready_event.set()

        async for message in pubsub.listen():
            if message.get("type") not in ("message", "pmessage"):
                continue
            routing_key = (
                message["channel"].decode().removeprefix(f"{self._exchange}{self._delimiter}")
            )
            envelope = Envelope(
                body=message["data"],
                routing_key=routing_key,
            )
            if asyncio.iscoroutinefunction(callback):
                await callback(envelope)
            else:
                asyncio.run_coroutine_threadsafe(callback(envelope), self._loop)
            if stop_event.is_set():
                await pubsub.unsubscribe()
                await pubsub.close()
                break

    async def setup_queue(self, topic: str, shared: bool = False) -> dict:
        """Set up a Redis Stream and Consumer Group for tasks if shared, otherwise use Pub/Sub.

        Args:
            topic: The stream key / routing key (queue name)
            shared: If True, uses a fixed group name for round-robin.
                    If False, just creates metadata to use to subscribe the callback

        Returns:
            The name of the consumer group to use
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to Redis")

        key = self.build_topic_key(topic)
        tag = f"consumer_{uuid.uuid4().hex[:8]}"

        # # Check local caches
        # if not shared and key in self.queues:
        #     return self.queues[key]

        if shared:
            group_name = "shared_group"
            log.debug(
                "Setting up Redis Stream %s with group %s (shared=%s)", key, group_name, shared
            )
            try:
                # Create the Consumer Group
                # MKSTREAM: Creates the stream key if it doesn't exist
                # id='$': Only read new messages arriving from now on
                await self._connection.xgroup_create(
                    name=key, groupname=group_name, id="$", mkstream=True
                )
            except Exception as e:
                if "BUSYGROUP" not in str(e):
                    log.error("Failed to setup Redis Stream group: %s", e)
                    raise

            queue_meta = {
                "mechanism": "stream",
                "key": key,
                "group_name": group_name,
                "tag": tag,
                "topic": topic,
                "is_shared": True,
            }
        else:
            # NORMAL QUEUE LOGIC (Pub/Sub)
            # No server-side 'declaration' needed for Pub/Sub.
            # We just prepare the metadata to use PSUBSCRIBE.
            queue_meta = {
                "mechanism": "pubsub",
                "tag": tag,
                "topic": topic,
                "is_shared": False,
                "group_name": f"fanout_{uuid.uuid4().hex[:8]}",
                "key": key,
            }
        self.queues[key] = queue_meta
        return queue_meta

    async def _tasks_consumer_loop(
        self,
        key: str,
        group_name: str,
        consumer_id: str,
        callback: tp.Callable,
        ready_event: asyncio.Event,
        stop_event: asyncio.Event,
    ):
        """Internal loop to read messages from Redis Stream."""
        queue_meta = self.queues.get(key)
        if not queue_meta:
            log.warning("No queue found for key %s", key)
            return

        # Signal that the loop has started before entering the blocking section
        ready_event.set()
        await asyncio.sleep(0)
        is_mock = hasattr(self._connection, "server")  # Simple way to detect fakeredis

        while not stop_event.is_set():
            try:
                # XREADGROUP: block=0 means wait indefinitely for new messages
                # ">" means "read only new messages never delivered to others"

                log.debug("Consumer loop for %s started", consumer_id)
                response = await self._connection.xreadgroup(
                    groupname=group_name,
                    consumername=consumer_id,
                    streams={key: ">"},
                    count=self.prefetch_count or 10,
                    block=None
                    if is_mock
                    else 1000,  # Block for 1 second then loop (allows for clean shutdown)
                )
                if not response:
                    continue
                messages = response.get(key.encode())[0]
                for msg_id, payload in messages:
                    await self._on_message_task(key, group_name, msg_id, payload, callback)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("Error in Redis consumer loop: %s", e)
                await asyncio.sleep(1)  # Backoff on error

    async def _on_message_pubsub(
        self, topic: str, callback, envelope: IncomingMessageProtocol
    ) -> None:
        if isinstance(envelope.body, str):
            envelope.body.encode()  # Ensure it's bytes if it was auto-decoded
        # 2. Create the Envelope
        key = self.build_topic_key(topic)
        try:
            log.debug("Running callback for topic %s", key)
            if asyncio.iscoroutinefunction(callback):
                await callback(envelope)
            else:
                # Run sync callback in the executor
                await asyncio.get_event_loop().run_in_executor(self.executor, callback, envelope)
        except RequeueMessage:
            log.warning("Requeuing message on topic '%s' after RequeueMessage exception", key)
            await asyncio.sleep(self.requeue_delay)
            await self._connection.publish(key, envelope.body)
        except Exception as e:
            log.exception("Callback failed for topic %s", key)
            raise PublishError(f"Failed to publish message to topic {key}: {e}") from e

    async def publish(
        self,
        topic: str,
        message: "IncomingMessageProtocol",
        **kwargs,
    ) -> None:
        """
        Simulates Topic Exchange routing.
        """
        if not self.is_connected():
            raise ConnectionError("Not connected")

        topic_key = self.build_topic_key(topic)
        is_shared = is_task(topic)

        if is_shared:
            log.debug("Publishing message to Redis Stream: %s", topic_key)
            payload = {
                "body": message.body,
                "correlation_id": message.correlation_id,
                "topic": topic,  # add the topic here to implement topic exchange patterns
            }
            await self._connection.xadd(name=topic_key, fields=payload, maxlen=1000)
            if default_configuration.log_task_in_redis:
                # Tasks messages go to streams but the logger do a simple pubsub psubscription to <prefix>.*
                # Send the message to the same topic with redis.publish so it appears there
                # Note: this should be used carefully (e.g. only for debugging)
                # as it doubles the network calls for publishing tasks
                log.debug("Publishing message to topic: %s for logger", topic_key)
                await self._connection.publish(topic_key, message.body)
        else:
            # For fan-out, we use native Redis Pub/Sub.
            # Redis will automatically match this to any PSUBSCRIBE patterns.
            log.debug("Publishing message to topic: %s", topic_key)
            await self._connection.publish(topic_key, message.body)

    async def _on_message_task(
        self, stream_key: str, group_name: str, msg_id: str, payload: dict, callback: tp.Callable
    ):
        """Wraps the user callback to simulate RabbitMQ behavior."""
        # Response is not decoded because we use bytes. But the keys will be bytes as well
        normalized_payload = {
            k.decode() if isinstance(k, bytes) else k: v for k, v in payload.items()
        }
        body = normalized_payload.get("body", b"")
        topic = normalized_payload.get("topic", b"").decode()
        correlation_id = normalized_payload.get("correlation_id", b"").decode()
        if isinstance(body, str):
            body = body.encode()  # Ensure it's bytes if it was auto-decoded
        # 2. Create the Envelope
        envelope = Envelope(
            body=body,
            correlation_id=correlation_id,
            routing_key=topic.decode() if isinstance(topic, bytes) else topic,
        )
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(envelope)
            else:
                # Run sync callback in the executor
                await asyncio.get_event_loop().run_in_executor(self.executor, callback, envelope)

            # Manual ACK: Message is processed, tell Redis to remove from Pending List
            await self._connection.xack(stream_key, group_name, msg_id)
        except RequeueMessage:
            log.warning("Requeuing message on topic '%s' after RequeueMessage exception", topic)
            # In Redis, to "requeue" so it's processed again:
            # 1. XADD again and 2. XACK the current ID
            await asyncio.sleep(self.requeue_delay)
            await self._connection.xadd(name=stream_key, fields=payload)
            await self._connection.xack(stream_key, group_name, msg_id)
        except Exception as e:
            log.exception("Callback failed for message %s", msg_id)
            raise PublishError(f"Failed to publish message to topic {topic}: {e}") from e
            # Avoid poisoning messages
            # Note: In Redis, if you don't XACK, the message stays in the
            # Pending Entry List (PEL) for retry logic.

    async def unsubscribe(self, tag: str, if_unused: bool = True, if_empty: bool = True) -> None:
        task_to_cancel = None
        async with self.lock:
            if tag not in self.consumers:
                return
            log.debug("Unsubscribing consumer %s", tag)
            consumer_info = self.consumers.pop(tag)
            if consumer_info:
                task_to_cancel = consumer_info["task"]
                stop_event = consumer_info["stop_event"]
                key = consumer_info["key"]
                # Stop the local asyncio loop
                log.info("Stopping consumer %s", tag)
                stop_event.set()

        if task_to_cancel:
            try:
                # wait for the task to stop (outside the lock otherwise will deadlock for python 3.10/3.11
                log.debug("Waiting for consumer %s", tag)
                task_to_cancel.cancel()
                await asyncio.sleep(0)
                # await asyncio.gather(task_to_cancel, return_exceptions=True)
                # await task_to_cancel
                await asyncio.wait_for(task_to_cancel, timeout=1.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass

        async with self.lock:
            queue_meta = self.queues.get(key)
            if not queue_meta:
                return

            group_name = queue_meta["group_name"]
            is_shared = queue_meta["is_shared"]

            # Tell Redis to remove THIS specific consumer from the group
            if is_shared:
                try:
                    await self._connection.xgroup_delconsumer(key, group_name, tag)
                    # Optionally delete the actual stream data
                    if not if_empty or (await self._connection.xlen(key) == 0):
                        await self._connection.delete(key)
                        self.queues.pop(key, None)
                    log.debug("Deleted Redis consumer %s from group %s", tag, group_name)
                except Exception as e:
                    log.warning("Could not delete Redis consumer %s: %s", tag, e)

            # Handle non shared queues
            else:
                try:
                    log.info("Unsubscribe from %s (group=%s, consumer=%s)", key, group_name, tag)
                    assert (
                        queue_meta["mechanism"] == "pubsub"
                    ), f"Invalid queue mechanism: {queue_meta['mechanism']}"
                    pubsub = self._connection.pubsub()
                    # Use psubscribe to support wildcards like 'user.*'
                    if any(char in key for char in ("*", "[", "?")):
                        await pubsub.punsubscribe(queue_meta["key"])
                    else:
                        await pubsub.unsubscribe(queue_meta["key"])
                    self.queues.pop(key, None)

                    # Delete stream if_empty=False
                    if not if_empty:
                        log.info("Deleting subscription %s even if not empty", key)
                        await self._connection.delete(key)
                except ResponseError as e:
                    log.error("Error checking during unsubscribe: %s", e)
                    raise
                except RedisError as e:
                    log.error("Redis error during unsubscribe: %s", e)

    async def purge(self, topic: str, reset_groups: bool = False) -> None:
        """Empty a Redis Stream and optionally clear all consumer groups.

        Args:
            topic: The stream/topic name to purge
            reset_groups: If True, deletes all consumer groups (resets consumer count to 0)
        """
        async with self.lock:
            if not self.is_connected():
                raise ConnectionError("Not connected to Redis")

            stream_key = self.build_topic_key(topic)
            log.info("Purging Redis stream '%s' (reset_groups=%s)", stream_key, reset_groups)

            try:
                # Clear all messages
                await self._connection.xtrim(stream_key, maxlen=0, approximate=False)
                if reset_groups:
                    try:
                        await self.reset_stream_groups(stream_key)
                        await self._connection.delete(stream_key)
                    except ResponseError as e:
                        # Ignore error if the stream key doesn't exist yet
                        if "no such key" not in str(e).lower():
                            raise e

                # 3. Clear local metadata cache
                if stream_key in self.queues:
                    self.queues.pop(stream_key)

            except Exception as e:
                log.error("Failed to purge Redis stream %s: %s", stream_key, e)
                raise ConnectionError(f"Failed to purge topic: {e}") from e

    async def get_message_count(self, topic: str) -> int:
        """Get the number of messages in the Redis Stream.

        Args:
            topic: The stream topic name

        Returns:
            Number of entries currently in the stream.
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to Redis")

        stream_key = self.build_topic_key(topic)
        log.debug("Getting message count for shared topic (stream) '%s'", stream_key)

        try:
            # XLEN returns the number of entries in a stream
            return await self._connection.xlen(stream_key)
        except Exception as e:
            log.error("Failed to get message count for %s: %s", stream_key, e)
            return 0

    async def get_consumer_count(self, topic: str) -> int:
        """Get the total number of consumers across all groups for a topic.

        Args:
            topic: The stream topic

        Returns:
            Total number of consumers
        """
        async with self.lock:
            if not self.is_connected():
                raise ConnectionError("Not connected to Redis")

            stream_key = self.build_topic_key(topic)
            total_consumers = 0

            try:
                # XINFO GROUPS returns a list of all consumer groups for this stream
                groups = await self._connection.xinfo_groups(stream_key)

                for group in groups:
                    # Each group dictionary contains a 'consumers' count
                    total_consumers += group.get("consumers", 0)

                return total_consumers

            except Exception as e:
                # If the stream doesn't exist, xinfo will raise an error
                if "no such key" in str(e).lower():
                    return 0
                log.error("Failed to get consumer count for %s: %s", stream_key, e)
                return 0

    async def reset_stream_groups(self, stream_key: str) -> None:
        """Hard reset: Deletes all consumer groups for a topic. To be used with caution."""
        if not self.is_connected():
            return
        try:
            # Get all groups for this stream
            groups = await self._connection.xinfo_groups(stream_key)
            for group in groups:
                group_name = group["name"]
                # Destroy the group (this removes all consumers inside it)
                await self._connection.xgroup_destroy(stream_key, group_name)
                log.info("Destroyed group %s on %s", group_name, stream_key)
        except Exception as e:
            if "no such key" in str(e).lower():
                return
            log.error("Failed to reset groups for %s: %s", stream_key, e)
