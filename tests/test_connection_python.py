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


#
#
# @pytest.mark.asyncio
# async def test_on_message_success(mock_aio_pika):
#     conn = AsyncConnection()
#     # Mock an incoming message
#     mock_msg = AsyncMock(spec=IncomingMessage)
#     callback = MagicMock()
#
#     # We call the internal _on_message
#     await conn._on_message("test.topic", callback, mock_msg)
#
#     # Since _on_message uses run_in_executor, the callback is run in a thread
#     # We wait a tiny bit or verify the ack
#     mock_msg.ack.assert_awaited_once()
#
#
# @pytest.mark.asyncio
# async def test_on_message_requeue(mock_aio_pika):
#     conn = AsyncConnection(requeue_delay=0)  # No delay for testing
#     mock_msg = AsyncMock(spec=IncomingMessage)
#
#     # Callback that triggers requeue
#     def side_effect(*args):
#         raise RequeueMessage()
#
#     callback = MagicMock(side_effect=side_effect)
#
#     await conn._on_message("test.topic", callback, mock_msg)
#
#     mock_msg.reject.assert_awaited_once_with(requeue=True)
#
#
# @pytest.mark.asyncio
# async def test_on_message_poison_pill(mock_aio_pika):
#     conn = AsyncConnection()
#     mock_msg = AsyncMock(spec=IncomingMessage)
#
#     # Random crash
#     def crash(*args):
#         raise RuntimeError("Boom")
#
#     callback = MagicMock(side_effect=crash)
#
#     await conn._on_message("test.topic", callback, mock_msg)
#
#     # Should reject without requeue to avoid infinite loop
#     mock_msg.reject.assert_awaited_once_with(requeue=False)
#
#
# @pytest.mark.asyncio
# async def test_setup_queue_shared(mock_aio_pika):
#     async with AsyncConnection() as conn:
#         await conn.setup_queue("shared_topic", shared=True)
#
#         mock_aio_pika["channel"].declare_queue.assert_called_with(
#             "shared_topic", exclusive=False, durable=True, auto_delete=False, arguments=ANY
#         )


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
