import logging

from protobunny.asyncio.backends import (
    BaseAsyncQueue,
)

from ....models import Envelope
from .connection import get_connection

log = logging.getLogger(__name__)


class AsyncQueue(BaseAsyncQueue):
    def get_tag(self) -> str:
        return self.subscription

    @staticmethod
    async def send_message(
        topic: str, body: bytes, correlation_id: str | None = None, persistent: bool = True
    ) -> None:
        """Low-level message sending implementation.

        Args:
            topic: a topic name for direct routing or a routing key with special binding keys
            body: serialized message (e.g. a serialized protobuf message or a json string)
            correlation_id: is present for result messages
            persistent: if true will use aio_pika.DeliveryMode.PERSISTENT

        Returns:

        """
        message = Envelope(
            body=body,
            correlation_id=correlation_id or b"",
        )
        conn = await get_connection()
        await conn.publish(topic, message)
