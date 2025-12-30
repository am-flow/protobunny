import logging

import aio_pika
from aio_pika import DeliveryMode

from protobunny.backends import (
    BaseSyncQueue,
)

log = logging.getLogger(__name__)


class SyncQueue(BaseSyncQueue):
    """Message queue backed by pika and RabbitMQ."""

    def get_tag(self) -> str:
        return self.subscription

    def send_message(
        self, topic: str, body: bytes, correlation_id: str | None = None, persistent: bool = True
    ):
        """Low-level message sending implementation.

        Args:
            topic: a topic name for direct routing or a routing key with special binding keys
            body: serialized message (e.g. a serialized protobuf message or a json string)
            correlation_id: is present for result messages
            persistent: if true will use aio_pika.DeliveryMode.PERSISTENT

        Returns:

        """
        message = aio_pika.Message(
            body,
            correlation_id=correlation_id,
            delivery_mode=DeliveryMode.PERSISTENT if persistent else DeliveryMode.NOT_PERSISTENT,
        )
        self.get_connection().publish(topic, message)
