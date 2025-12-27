import logging

from protobunny.asyncio.backends import (
    BaseAsyncQueue,
)
from protobunny.models import (
    Envelope,
)

log = logging.getLogger(__name__)


class AsyncQueue(BaseAsyncQueue):
    """Message queue backed by pika and RabbitMQ."""

    def get_tag(self) -> str:
        return self.topic

    async def send_message(
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
        message = Envelope(
            body,
            correlation_id=correlation_id,
            routing_key=topic,
        )
        connection = await self.get_connection()
        await connection.publish(topic, message)
