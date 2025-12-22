import pytest

from protobunny.backends.python.connection import (
    AsyncLocalConnection,
    SyncLocalConnection,
    get_connection,
    get_connection_sync,
)
from protobunny.models import Envelope

from .utils import async_wait

# --- AsyncConnection Tests ---


@pytest.mark.asyncio
async def test_async_connect_success():
    conn = AsyncLocalConnection(vhost="localhost")
    await conn.connect()
    assert conn.is_connected()


@pytest.mark.asyncio
async def test_async_publish(mock_python_connection):
    async with AsyncLocalConnection(vhost="/test") as conn:
        msg = None
        await conn.subscribe("test.routing.key", callback=lambda x: msg)
        await conn.publish("test.routing.key", Envelope(body=b"hello"))

        async def predicate():
            return msg == Envelope(body=b"hello")

        await async_wait(predicate, timeout_seconds=1, sleep_seconds=0.1)


async def test_connection_flow():
    """Test the synchronous wrapper's ability to run logic in its thread."""
    async with AsyncLocalConnection(vhost="/test") as conn:
        assert conn.is_connected()
        topic = "test.topic"

        msg = Envelope(body=b"body")
        await conn.subscribe(topic, callback=lambda _: None)
        assert await conn.get_consumer_count(topic) == 1
        await conn.unsubscribe(topic)
        await conn.publish(topic, msg)
        assert conn._exclusive_subscribers["test.topic"] is not None
        assert await conn.get_message_count("test.topic") == 0


async def test_message_count() -> None:
    async with AsyncLocalConnection() as conn:
        topic = "test.topic.tasks"
        msg = Envelope(body=b"body")
        assert await conn.get_message_count(topic) == 0
        await conn.subscribe(topic, shared=True, callback=lambda _: None)
        assert await conn.get_consumer_count(topic) == 1
        await conn.unsubscribe(topic)
        assert await conn.get_consumer_count(topic) == 0
        assert conn._shared_queues[topic] is not None
        assert conn._shared_queues[topic].qsize() == 0
        await conn.publish(topic, msg)
        await conn.publish(topic, msg)
        await conn.publish(topic, msg)
        assert conn._shared_queues[topic].qsize() == 3
        assert await conn.get_message_count(topic) == 3


# --- SyncConnection Tests ---


def test_sync_connection_flow():
    """Test the synchronous wrapper's ability to run logic in its thread."""
    with SyncLocalConnection(vhost="/test") as conn:
        assert conn.is_connected
        topic = "test.topic"

        msg = Envelope(body=b"body")
        conn.subscribe(topic, callback=lambda _: None)
        assert conn.get_consumer_count(topic) == 1
        conn.unsubscribe(topic)
        conn.publish(topic, msg)
        assert conn._exclusive_subscribers["test.topic"] is not None
        assert conn.get_message_count("test.topic") == 0
        conn.unsubscribe("test.topic")


def test_sync_message_count() -> None:
    with SyncLocalConnection() as conn:
        topic = "test.topic.tasks"
        msg = Envelope(body=b"body")
        assert conn.get_message_count(topic) == 0
        conn.subscribe(topic, shared=True, callback=lambda _: None)
        assert conn.get_consumer_count(topic) == 1
        conn.unsubscribe(topic)
        assert conn.get_consumer_count(topic) == 0
        assert conn._shared_queues[topic] is not None
        assert conn._shared_queues[topic].qsize() == 0
        conn.publish(topic, msg)
        conn.publish(topic, msg)
        conn.publish(topic, msg)
        assert conn._shared_queues[topic].qsize() == 3
        assert conn.get_message_count(topic) == 3


# --- Singleton Tests ---


@pytest.mark.asyncio
async def test_async_singleton_logic():
    conn1 = await get_connection()
    conn2 = await get_connection()
    assert conn1 is conn2


def test_sync_singleton_logic():
    conn1 = get_connection_sync()
    conn2 = get_connection_sync()
    assert conn1 is conn2
    conn1.disconnect()  # Cleanup thread
