from unittest.mock import AsyncMock, MagicMock

import pytest

from protobunny.backends.redis.connection import (
    AsyncRedisConnection,
    RequeueMessage,
    SyncRedisConnection,
    get_connection,
    get_connection_sync,
)
from protobunny.models import Envelope

# --- AsyncConnection Tests ---


@pytest.mark.asyncio
async def test_async_connect_success(mock_redis):
    conn = AsyncRedisConnection(host="localhost")
    await conn.connect()

    # Verify redis calls
    mock_redis.ping.assert_awaited_once()
    assert await conn.is_connected() is True
    assert conn.instance_by_vhost["11"] is conn


@pytest.mark.asyncio
async def test_async_publish(mock_redis):
    async with AsyncRedisConnection(vhost="/test") as conn:
        msg = Envelope(body=b"hello")
        await conn.publish("test.routing.key", msg)
        mock_redis.xadd.assert_awaited_with(
            name="protobunny:test.routing.key",
            fields=dict(body=b"hello", correlation_id="", topic="test.routing.key"),
            maxlen=1000,
        )


@pytest.mark.asyncio
async def test_on_message_success(mock_redis):
    conn = AsyncRedisConnection()
    await conn.connect()
    # Mock an incoming message
    callback = AsyncMock()

    # We call the internal _on_message
    await conn._on_message(
        "test.topic", callback=callback, payload={"body": b"test"}, group_name="test", msg_id="111"
    )
    assert callback.called
    mock_redis.xack.assert_awaited_with("test.topic", "test", "111")


@pytest.mark.asyncio
async def test_on_message_requeue(mock_redis):
    async with AsyncRedisConnection() as conn:
        conn.requeue_delay = 0.1
        payload = {"body": b"test"}

        # Callback that triggers requeue
        def side_effect(*args):
            raise RequeueMessage()

        callback = MagicMock(side_effect=side_effect)

        await conn._on_message(
            "test.topic", payload=payload, callback=callback, group_name="test", msg_id="111"
        )
        mock_redis.xack.assert_awaited_with("test.topic", "test", "111")
        mock_redis.xadd.assert_awaited_with(name="test.topic", fields=payload)


@pytest.mark.asyncio
async def test_on_message_poison_pill(mock_redis):
    async with AsyncRedisConnection() as conn:
        payload = {"body": b"test"}

        # Random crash
        def crash(*args):
            raise RuntimeError("Boom")

        callback = MagicMock(side_effect=crash)

        await conn._on_message(
            "test.topic", payload=payload, callback=callback, group_name="test", msg_id="111"
        )

    # Should reject without requeue to avoid poisoning the queue
    mock_redis.xack.assert_not_awaited()


@pytest.mark.asyncio
async def test_setup_queue_shared(mock_redis):
    async with AsyncRedisConnection() as conn:
        await conn.setup_queue("shared_topic", shared=True)

        mock_redis.xgroup_create.assert_awaited_with(
            name="protobunny:shared_topic", groupname="shared_group", id="0", mkstream=True
        )


# --- SyncConnection Tests ---


def test_sync_connection_flow(mock_redis):
    """Test the synchronous wrapper's ability to run logic in its thread."""
    with SyncRedisConnection() as conn:
        assert conn.is_connected
        msg = Envelope(body=b"sync-body")

        conn.publish("sync.topic", msg)
        mock_redis.xadd.assert_awaited_with(
            name="protobunny:sync.topic",
            fields=dict(body=b"sync-body", correlation_id="", topic="sync.topic"),
            maxlen=1000,
        )


def test_sync_get_message_count(mock_redis):
    # Configure mock result
    mock_redis.xlen = AsyncMock(return_value=42)

    with SyncRedisConnection() as conn:
        count = conn.get_message_count("test.topic")
        assert count == 42


# # --- Singleton Tests ---


@pytest.mark.asyncio
async def test_async_singleton_logic(mock_redis):
    conn1 = await get_connection()
    conn2 = await get_connection()
    assert conn1 is conn2


def test_sync_singleton_logic(mock_redis):
    conn1 = get_connection_sync()
    conn2 = get_connection_sync()
    assert conn1 is conn2
    conn1.disconnect()  # Cleanup thread
