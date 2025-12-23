"""Implements a Redis Connection with both sync and async support"""
import asyncio
import logging
import os
import re
import threading
import typing as tp
import urllib.parse
import uuid
from concurrent.futures import ThreadPoolExecutor

import redis.asyncio as redis
from redis import RedisError, ResponseError

from ...config import load_config
from ...exceptions import ConnectionError, RequeueMessage
from ...models import Envelope, IncomingMessageProtocol
from .. import BaseAsyncConnection, BaseSyncConnection

log = logging.getLogger(__name__)

VHOST = os.environ.get("REDIS_VHOST") or os.environ.get("REDIS_DB", "0")


async def get_connection() -> "AsyncRedisConnection":
    """Get the singleton async connection."""
    conn = await AsyncRedisConnection.get_connection(vhost=VHOST)
    return conn


async def reset_connection() -> "AsyncRedisConnection":
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


def reset_connection_sync() -> "SyncRedisConnection":
    connection = get_connection_sync()
    connection.disconnect()
    return get_connection_sync()


def get_connection_sync() -> "SyncRedisConnection":
    connection = SyncRedisConnection.get_connection(vhost=VHOST)
    return connection


class AsyncRedisConnection(BaseAsyncConnection):
    """Async Redis Connection wrapper."""

    _lock: asyncio.Lock | None = None
    instance_by_vhost: dict[str, "AsyncRedisConnection | None"] = {}

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
        configuration = load_config()
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
                url = f"redis://{username}:{password}@{host}:{port}/{vhost}"
            elif password:
                url = f"redis://:{password}@{host}:{port}/{vhost}"
            else:
                url = f"redis://{host}:{port}/{vhost}"
        self._url = url
        self._loop: asyncio.AbstractEventLoop | None = None
        self._connection: redis.Redis | None = None
        self._exchange = f"{configuration.project_name}"
        self.prefetch_count = prefetch_count
        self.requeue_delay = requeue_delay
        self.heartbeat = heartbeat
        self.queues: dict[str, dict] = {}
        self.consumers: dict[str, dict[str, tp.Any]] = {}
        self.executor = ThreadPoolExecutor(max_workers=worker_threads)
        self._instance_lock: asyncio.Lock | None = None
        self._is_connected_event: asyncio.Event | None = None
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            # Fallback if __init__ is called outside a running loop
            # (though get_connection should be called inside one)
            self._loop = None

    async def __aenter__(self) -> "AsyncRedisConnection":
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

    def build_stream_key(self, topic: str) -> str:
        return f"{self._exchange}:{topic}"

    async def disconnect(self, timeout: float = 10.0) -> None:
        """Close Redis connection and cleanup resources.

        Args:
            timeout: Maximum time to wait for cleanup (seconds)
        """
        async with self.lock:
            if not self.is_connected_event.is_set():
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
                AsyncRedisConnection.instance_by_vhost.pop(self.vhost, None)
                log.info("Redis connection closed")

    @classmethod
    async def get_connection(cls, vhost: str = "/") -> "AsyncRedisConnection":
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
            log.info("Returning singleton AsyncConnection instance for vhost %s", vhost)
            return instance

    @property
    def is_connected_event(self) -> asyncio.Event:
        """Lazily create the event in the current running loop."""
        if self._is_connected_event is None:
            self._is_connected_event = asyncio.Event()
        return self._is_connected_event

    async def is_connected(self) -> bool:
        """Check if connection is established and healthy."""
        return self.is_connected_event.is_set()

    @property
    def connection(self) -> "redis.Redis":
        """Get the connection object.

        Raises:
            ConnectionError: If not connected
        """
        if not self._connection:
            raise ConnectionError("Connection not initialized. Call connect() first.")
        return self._connection

    async def connect(self, timeout: float = 30.0) -> None:
        """Establish Redis connection.

        Args:
            timeout: Maximum time to wait for connection establishment (seconds)

        Raises:
            ConnectionError: If connection fails
            asyncio.TimeoutError: If connection times out
        """
        async with self.lock:
            if self.instance_by_vhost.get(self.vhost) and await self.is_connected():
                return
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
        """Subscribe to a Redis Stream.

        Args:
            topic: The stream key/topic to subscribe to
            callback: Function to handle incoming messages.
            shared: If True, uses a shared consumer group (round-robin).
                    If False, uses a unique group for this instance (fan-out).

        Returns:
            A unique subscription tag used to stop the consumer later.
        """
        async with self.lock:
            if not await self.is_connected():
                raise ConnectionError("Not connected to Redis")

            # Setup the stream and group (returns the metadata dict)
            queue = await self.setup_queue(topic, shared)
            ready_event = asyncio.Event()
            task = asyncio.create_task(
                self._consumer_loop(
                    queue["stream_key"], queue["group_name"], queue["tag"], callback, ready_event
                )
            )
            # Wait for the loop to signal it has performed the first check
            await asyncio.wait_for(ready_event.wait(), timeout=5.0)
            # register the topic in the registry
            await self._connection.sadd("protobunny:registry:topics", topic)
            # We store the task so we can cancel it in 'disconnect'
            self.consumers[queue["tag"]] = {
                "task": task,
                "stream_key": queue["stream_key"],
                "topic": topic,
            }
            return queue["tag"]

    async def setup_queue(self, topic: str, shared: bool = False) -> dict:
        """Set up a Redis Stream and Consumer Group.

        Args:
            topic: The stream key (analogous to routing key/queue name)
            shared: If True, uses a fixed group name for round-robin.
                    If False, creates a unique group for fan-out behavior.

        Returns:
            The name of the consumer group to use
        """
        if not self._connection:
            raise ConnectionError("Not connected to Redis")

        stream_key = self.build_stream_key(topic)
        tag = f"consumer_{uuid.uuid4().hex[:8]}"

        # Define the group name
        # Shared = 'workers' for tasks messages, Not Shared = unique ID per instance
        group_name = "shared_group" if shared else f"fanout_{uuid.uuid4().hex[:8]}"

        log.debug(
            "Setting up Redis Stream %s with group %s (shared=%s)", stream_key, group_name, shared
        )

        # Check local cache
        if shared and stream_key in self.queues:
            return self.queues[stream_key]

        try:
            # Create the Consumer Group
            # MKSTREAM: Creates the stream key if it doesn't exist
            # id='$': Only read new messages arriving from now on
            read_from = "$" if not shared else "0"
            await self._connection.xgroup_create(
                name=stream_key, groupname=group_name, id=read_from, mkstream=True
            )
        except Exception as e:
            # If the group already exists, Redis throws an error; we can ignore it
            if "BUSYGROUP" not in str(e):
                log.error("Failed to setup Redis group: %s", e)
                raise

        self.queues[stream_key] = {
            "stream_key": stream_key,
            "group_name": group_name,
            "is_shared": shared,
            "tag": tag,  # Reference back to the consumer
            "topic": topic,
        }
        log.info("Subscribed to %s (group=%s, consumer=%s)", stream_key, group_name, tag)
        return self.queues[stream_key]

    async def _consumer_loop(
        self,
        stream_key: str,
        group_name: str,
        consumer_id: str,
        callback: tp.Callable,
        ready_event: asyncio.Event,
    ):
        """Internal loop to read messages from Redis Stream."""
        try:
            ready_event.set()
            while True:
                try:
                    # XREADGROUP: block=0 means wait indefinitely for new messages
                    # ">" means "read only new messages never delivered to others"
                    response = await self._connection.xreadgroup(
                        groupname=group_name,
                        consumername=consumer_id,
                        streams={stream_key: ">"},
                        count=self.prefetch_count or 10,
                        block=5000,  # Block for 5 seconds then loop (allows for clean shutdown)
                    )
                    if not response:
                        log.debug("Consumer %s: No messages found", consumer_id)
                        continue

                    for _, messages in response:
                        for msg_id, payload in messages:
                            await self._on_message(
                                stream_key, group_name, msg_id, payload, callback
                            )

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    log.error("Error in Redis consumer loop: %s", e)
                    await asyncio.sleep(1)  # Backoff on error

        finally:
            log.debug("Consumer loop for %s stopped", consumer_id)

    @staticmethod
    def _match_topics(routing_key: str, redis_topics: set[bytes]) -> list[str]:
        """Implements RabbitMQ topic matching algorithm.

        Args:
            routing_key:
            redis_topics:

        """
        matched = []
        for pattern in redis_topics:
            # Convert RabbitMQ pattern to Regex
            # . -> \.
            # * -> [^.]+ (one word)
            # # -> .* (zero or more words)
            pattern = pattern.decode()
            regex_pattern = pattern.replace(".", r"\.")
            regex_pattern = regex_pattern.replace("*", r"[^.]+")
            regex_pattern = regex_pattern.replace("#", r".*")
            regex_pattern = f"^{regex_pattern}$"

            if re.match(regex_pattern, routing_key):
                matched.append(pattern)
        return matched

    async def publish(
        self,
        topic: str,
        message: "IncomingMessageProtocol",
        mandatory: bool = True,
        immediate: bool = False,
    ) -> list[str]:
        """
        Simulates Topic Exchange routing.
        """
        if not await self.is_connected():
            raise ConnectionError("Not connected")

        # 1. Get all active 'subscription patterns' from Redis
        # In a real app, you'd store these in a Redis SET named 'active_topics'
        all_registered_topics = await self._connection.smembers("protobunny:registry:topics")
        # 2. Find which topics match our current publish topic
        # e.g. if topic is 'acme.tests.login', it matches 'acme.tests.#'
        matching_topics = self._match_topics(topic, all_registered_topics)
        message = {
            "body": message.body,
            "correlation_id": message.correlation_id,
            "topic": topic,  # add the topic here to implement topic exchange patterns
        }
        log.debug("Publishing message to Redis topics: %s", matching_topics)
        msg_ids = []
        for match in matching_topics:
            # there are subscriptions for this message
            # We send the message to all matching subscriptions
            stream_key = self.build_stream_key(match)
            log.debug("Publishing message to : %s", topic)
            msg_id = await self._connection.xadd(name=stream_key, fields=message, maxlen=1000)
            msg_ids.append(msg_id)

        return msg_ids

    async def _on_message(
        self, stream_key: str, group_name: str, msg_id: str, payload: dict, callback: tp.Callable
    ):
        """Wraps the user callback to simulate RabbitMQ behavior."""
        # Response is not decoded because we use bytes. But the keys will be bytes as well
        normalized_payload = {
            k.decode() if isinstance(k, bytes) else k: v for k, v in payload.items()
        }
        body = normalized_payload.get("body", b"")
        topic = normalized_payload.get("topic", b"")
        if isinstance(body, str):
            body = body.encode()  # Ensure it's bytes if it was auto-decoded
        # 2. Create the Envelope
        envelope = Envelope(
            body=body,
            correlation_id=normalized_payload.get("correlation_id", ""),
            routing_key=topic.decode(),
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
        except Exception:
            log.exception("Callback failed for message %s", msg_id)
            # Avoid poisoning messages
            # Note: In Redis, if you don't XACK, the message stays in the
            # Pending Entry List (PEL) for retry logic.

    async def unsubscribe(self, tag: str, if_unused: bool = True, if_empty: bool = True) -> None:
        async with self.lock:
            if tag not in self.consumers:
                return

            consumer_info = self.consumers.pop(tag)
            if consumer_info:
                task = consumer_info["task"]
                stream_key = consumer_info["stream_key"]
                topic = consumer_info["topic"]

                # 1. Stop the local asyncio loop
                log.info("Stopping consumer %s", tag)
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=3.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass

            queue_meta = self.queues.get(stream_key)
            if not queue_meta:
                return

            group_name = queue_meta["group_name"]
            is_shared = queue_meta["is_shared"]

            # Tell Redis to remove THIS specific consumer from the group
            try:
                await self._connection.xgroup_delconsumer(stream_key, group_name, tag)
                log.debug("Deleted Redis consumer %s from group %s", tag, group_name)
            except Exception as e:
                log.warning("Could not delete Redis consumer %s: %s", tag, e)

            # Handle Group/Stream destruction for non-shared (fanout) queues
            if not is_shared:
                try:
                    # For fanout, we destroy the whole unique group
                    log.info(
                        "Unsubscribe from %s (group=%s, consumer=%s)", stream_key, group_name, tag
                    )
                    await self._connection.xgroup_destroy(stream_key, group_name)

                    # Delete stream if empty
                    if not if_empty or (await self._connection.xlen(stream_key) == 0):
                        log.info("Deleting stream %s (if_empty was %s)", stream_key, if_empty)
                        await self._connection.delete(stream_key)
                        self.queues.pop(stream_key, None)
                    remaining_groups = await self._connection.xinfo_groups(stream_key)
                    if not remaining_groups:
                        # No one is listening to this stream anymore!
                        log.info("No active groups left for %s. Removing from registry.", topic)
                        # Remove from the publisher's routing registry
                        await self._connection.srem("protobunny:registry:topics", topic)

                        # Optionally delete the actual stream data
                        if not if_empty or (await self._connection.xlen(stream_key) == 0):
                            await self._connection.delete(stream_key)
                            self.queues.pop(stream_key, None)
                except ResponseError as e:
                    if "no such key" in str(e).lower():
                        # Stream is already gone, just clean up registry
                        await self._connection.srem("protobunny:registry:topics", topic)
                    else:
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
            if not await self.is_connected():
                raise ConnectionError("Not connected to Redis")

            stream_key = self.build_stream_key(topic)
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
        if not await self.is_connected():
            raise ConnectionError("Not connected to Redis")

        stream_key = self.build_stream_key(topic)
        log.debug("Getting message count for stream '%s'", stream_key)

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
            if not await self.is_connected():
                raise ConnectionError("Not connected to Redis")

            stream_key = self.build_stream_key(topic)
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

    async def reset_stream_groups(self, topic: str) -> None:
        """Hard reset: Deletes all consumer groups for a topic. To be used with caution."""
        if not await self.is_connected():
            return
        stream_key = self.build_stream_key(topic)
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
            log.error("Failed to reset groups for %s: %s", topic, e)


class SyncRedisConnection(BaseSyncConnection):
    """Synchronous wrapper around the async connection

    Example:
        .. code-block:: python

            with SyncRedisConnection() as conn:
                conn.publish("my.topic", message)
                tag = conn.subscribe("my.topic", callback)

    """

    _lock = threading.RLock()
    _stopped: asyncio.Event | None = None
    _instance_by_vhost: dict[str, "SyncRedisConnection"] = {}
    async_class = AsyncRedisConnection

    def get_async_connection(self, **kwargs) -> "AsyncRedisConnection":
        return AsyncRedisConnection(**kwargs)

    def reset_stream_groups(self, topic: str) -> None:
        self._run_coro(self._async_conn.reset_stream_groups(topic))
