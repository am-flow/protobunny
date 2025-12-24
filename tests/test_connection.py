import typing as tp
from unittest.mock import AsyncMock, MagicMock

import pytest
from aio_pika import IncomingMessage

import protobunny as pb
from protobunny import RequeueMessage
from protobunny.backends import python as python_backend
from protobunny.backends import rabbitmq as rabbitmq_backend
from protobunny.backends import redis as redis_backend
from protobunny.base import (
    get_queue,
)
from protobunny.models import IncomingMessageProtocol

from . import tests
from .utils import (
    assert_backend_connection,
    assert_backend_publish,
    assert_backend_setup_queue,
    async_wait,
    incoming_message_factory,
)


@pytest.mark.parametrize("backend", [rabbitmq_backend, redis_backend, python_backend])
@pytest.mark.asyncio
class TestConnection:
    @pytest.fixture(autouse=True, scope="function")
    async def mock_connection(
        self, backend, mocker, mock_redis, mock_aio_pika
    ) -> tp.AsyncGenerator[dict[str, AsyncMock | None], None]:
        from protobunny.base import configuration

        backend_name = backend.__name__.split(".")[-1]
        mocker.patch.object(pb.base.configuration, "backend", backend_name)
        mocker.patch.object(pb.base.configuration, "mode", "async")
        assert pb.base.get_backend() == backend
        assert pb.get_backend() == backend
        assert configuration.messages_prefix == "acme"
        assert isinstance(get_queue(tests.tasks.TaskMessage), backend.queues.AsyncQueue)
        mock = mocker.AsyncMock(spec=backend.connection.Connection)
        mocker.patch.object(pb, "get_connection", return_value=mock)
        mocker.patch.object(pb, "disconnect", side_effect=backend.connection.disconnect)
        mocker.patch("protobunny.backends.BaseAsyncQueue.get_connection", return_value=mock)
        mocker.patch(
            f"protobunny.backends.{backend_name}.connection.get_connection", return_value=mock
        )
        await pb.disconnect()
        yield {
            "rabbitmq": mock_aio_pika,
            "redis": mock_redis,
            "connection": mock,
            "python": python_backend.connection._broker,
        }

    async def test_connection_success(self, mock_connection: MagicMock, backend) -> None:
        conn = backend.connection.Connection(vhost="")
        await conn.connect()
        assert await conn.is_connected()
        await conn.disconnect()
        assert not await conn.is_connected()
        assert_backend_connection(backend, mock_connection)

    async def test_setup_queue_shared(self, mock_connection, backend):
        async with backend.connection.Connection() as conn:
            await conn.setup_queue("shared_topic", shared=True)

            assert_backend_setup_queue(backend, mock_connection, "shared_topic", shared=True)

    async def test_async_publish(self, mock_connection, backend):
        async with backend.connection.Connection(vhost="") as conn:
            msg = None

            async def callback(envelope: IncomingMessageProtocol):
                nonlocal msg
                msg = envelope

            incoming = incoming_message_factory(backend)
            await conn.subscribe("test.routing.key", callback=callback)
            await conn.publish("test.routing.key", incoming)
            # assert backend call
            assert_backend_publish(
                backend, mock_connection, incoming, topic="test.routing.key", count_in_queue=0
            )
            if backend == python_backend:
                # python
                async def predicate():
                    return msg == incoming

                assert await async_wait(predicate, timeout_seconds=1, sleep_seconds=0.1)

    @pytest.mark.asyncio
    async def test_async_singleton_logic(self, mock_redis, backend):
        conn1 = await backend.connection.get_connection()
        conn2 = await backend.connection.get_connection()
        assert conn1 is conn2
        await conn1.disconnect()


# --- Specific Tests for rabbitmq ---


@pytest.mark.asyncio
async def test_on_message_requeue(mock_aio_pika):
    conn = rabbitmq_backend.connection.Connection(requeue_delay=0)  # No delay for testing
    mock_msg = AsyncMock(spec=IncomingMessage)

    # Callback that triggers requeue
    def side_effect(*args):
        raise RequeueMessage()

    callback = MagicMock(side_effect=side_effect)

    await conn._on_message("test.topic", callback, mock_msg)

    mock_msg.reject.assert_awaited_once_with(requeue=True)


@pytest.mark.asyncio
async def test_on_message_poison_pill(mock_aio_pika):
    conn = rabbitmq_backend.connection.Connection()
    mock_msg = AsyncMock(spec=IncomingMessage)

    # Random crash
    def crash(*args):
        raise RuntimeError("Boom")

    callback = MagicMock(side_effect=crash)

    await conn._on_message("test.topic", callback, mock_msg)

    # Should reject without requeue to avoid infinite loop
    mock_msg.reject.assert_awaited_once_with(requeue=False)


@pytest.mark.asyncio
async def test_on_message_success(mock_redis):
    conn = redis_backend.connection.Connection()
    await conn.connect()
    # Mock an incoming message
    callback = AsyncMock()

    # We call the internal _on_message
    await conn._on_message(
        "test.topic", callback=callback, payload={"body": b"test"}, group_name="test", msg_id="111"
    )
    assert callback.called
    mock_redis.xack.assert_awaited_with("test.topic", "test", "111")


# --- Specific Tests for redis ---


@pytest.mark.asyncio
async def test_on_message_requeue(mock_redis):
    async with redis_backend.connection.Connection() as conn:
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
    async with redis_backend.connection.Connection() as conn:
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


async def test_sync_get_message_count(mock_redis):
    # Configure mock result
    mock_redis.xlen = AsyncMock(return_value=42)

    async with redis_backend.connection.Connection() as conn:
        count = await conn.get_message_count("test.topic")
        assert count == 42
