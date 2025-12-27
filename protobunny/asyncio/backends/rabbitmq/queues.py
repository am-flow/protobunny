import logging

import aio_pika
from aio_pika import DeliveryMode

from protobunny.asyncio.backends import (
    BaseAsyncQueue,
)
from protobunny.asyncio.backends.rabbitmq.connection import get_connection

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
        message = aio_pika.Message(
            body,
            correlation_id=correlation_id,
            delivery_mode=DeliveryMode.PERSISTENT if persistent else DeliveryMode.NOT_PERSISTENT,
        )
        conn = await get_connection()
        await conn.publish(topic, message)
