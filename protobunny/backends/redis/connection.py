"""Implements a Redis Connection with both sync and async support"""
import asyncio
import logging
import os
import threading

from ...asyncio.backends.redis.connection import Connection as RedisConnection
from .. import BaseSyncConnection

log = logging.getLogger(__name__)


VHOST = os.environ.get("REDIS_VHOST") or os.environ.get("REDIS_DB", "0")


def connect() -> "Connection":
    """Get the singleton async connection."""
    conn = Connection.get_connection(vhost=VHOST)
    return conn


def reset_connection() -> "Connection":
    """Reset the singleton connection."""
    connection = connect()
    connection.disconnect()
    return connect()


def disconnect() -> None:
    connection = connect()
    connection.disconnect()


class Connection(BaseSyncConnection):
    """Synchronous wrapper around the async connection

    Example:
        .. code-block:: python

            with SyncRedisConnection() as conn:
                conn.publish("my.topic", message)
                tag = conn.subscribe("my.topic", callback)

    """

    _lock = threading.RLock()
    _stopped: asyncio.Event | None = None
    instance_by_vhost: dict[str, "Connection"] = {}
    async_class = RedisConnection

    def reset_stream_groups(self, stream_key: str) -> None:
        self._run_coro(self._async_conn.reset_stream_groups(stream_key))
