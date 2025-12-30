"""Implements a RabbitMQ Connection with both sync and async support using aio_pika."""
import asyncio
import logging
import os
import threading

from ...asyncio.backends.rabbitmq.connection import Connection as RabbitMQConnection
from .. import BaseSyncConnection

log = logging.getLogger(__name__)

VHOST = os.environ.get("RABBITMQ_VHOST", "/")


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
    """Synchronous wrapper around Async Rmq Connection.

    Manages a dedicated event loop in a background thread to run async operations.

    Example:
        .. code-block:: python

            with Connection() as conn:
                conn.publish("my.topic", message)
                tag = conn.subscribe("my.topic", callback)

    """

    _lock = threading.RLock()
    _stopped: asyncio.Event | None = None
    instance_by_vhost: dict[str, "Connection"] = {}
    async_class = RabbitMQConnection
