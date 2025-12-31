"""Implements a NATS Connection with sync methods"""
import asyncio
import logging
import os
import threading

from ...asyncio.backends.nats.connection import Connection as NATSConnection
from .. import BaseSyncConnection

log = logging.getLogger(__name__)

VHOST = os.environ.get("NATS_VHOST", "/")


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
    async_class = NATSConnection
