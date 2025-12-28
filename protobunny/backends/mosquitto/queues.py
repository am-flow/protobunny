import logging

from protobunny.backends import (
    BaseSyncQueue,
)
from protobunny.models import Envelope

log = logging.getLogger(__name__)


class SyncQueue(BaseSyncQueue):
    """Message queue backed by pika and RabbitMQ."""

    def get_tag(self) -> str:
        return self.subscription

    def send_message(self, topic: str, body: bytes, correlation_id: str | None = None, **kwargs):
        """Low-level message sending implementation.

        Args:
            topic: a topic name for direct routing or a routing key with special binding keys
            body: serialized message (e.g. a serialized protobuf message or a json string)
            correlation_id: is present for result messages

        Returns:

        """
        message = Envelope(
            body=body,
            correlation_id=correlation_id or b"",
        )
        self.get_connection().publish(topic, message)
