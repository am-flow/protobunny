import functools
import logging
import typing as tp
from unittest.mock import AsyncMock, MagicMock

import pytest
import redis.asyncio as redis
from aio_pika import IncomingMessage

import protobunny as pb_base
from protobunny import RequeueMessage
from protobunny import asyncio as pb
from protobunny.asyncio.backends import mosquitto as mosquitto_backend_aio
from protobunny.asyncio.backends import python as python_backend_aio
from protobunny.asyncio.backends import rabbitmq as rabbitmq_backend_aio
from protobunny.asyncio.backends import redis as redis_backend_aio
from protobunny.backends import python as python_backend
from protobunny.config import backend_configs
from protobunny.helpers import (
    get_queue,
)
from protobunny.models import Envelope, IncomingMessageProtocol

from . import tests
from .utils import (
    assert_backend_connection,
    assert_backend_publish,
    assert_backend_setup_queue,
    async_wait,
    get_mocked_connection,
    incoming_message_factory,
)


@pytest.mark.parametrize(
    "backend", [rabbitmq_backend_aio, redis_backend_aio, python_backend_aio, mosquitto_backend_aio]
)
@pytest.mark.asyncio
class TestConnection:
    @pytest.fixture(autouse=True)
    async def mock_connections(
        self, backend, mocker, mock_redis_client, mock_aio_pika, mock_mosquitto, test_config
    ) -> tp.AsyncGenerator[dict[str, AsyncMock | None], None]:
        backend_name = backend.__name__.split(".")[-1]

        test_config.mode = "async"
        test_config.backend = backend_name
        test_config.log_task_in_redis = True
        test_config.backend_config = backend_configs[backend_name]
        mocker.patch.object(pb_base.config, "default_configuration", test_config)
        mocker.patch.object(pb_base.models, "default_configuration", test_config)
        mocker.patch.object(pb_base.backends, "default_configuration", test_config)
        mocker.patch.object(pb_base.helpers, "default_configuration", test_config)
        mocker.patch.object(pb.backends, "default_configuration", test_config)
        connection_module = getattr(pb.backends, backend_name).connection
        if hasattr(connection_module, "default_configuration"):
            mocker.patch.object(connection_module, "default_configuration", test_config)

        assert pb_base.helpers.get_backend() == backend
        assert pb.get_backend() == backend
        assert isinstance(get_queue(tests.tasks.TaskMessage), backend.queues.AsyncQueue)
        conn_with_fake_internal_conn = get_mocked_connection(
            backend, mock_redis_client, mock_aio_pika, mocker, mock_mosquitto
        )
        mocker.patch.object(pb, "get_connection", return_value=conn_with_fake_internal_conn)
        mocker.patch.object(pb, "disconnect", side_effect=backend.connection.disconnect)
        mocker.patch(
            "protobunny.asyncio.backends.BaseAsyncQueue.get_connection",
            return_value=conn_with_fake_internal_conn,
        )
        mocker.patch(
            f"protobunny.asyncio.backends.{backend_name}.connection.get_connection",
            return_value=conn_with_fake_internal_conn,
        )
        yield {
            "rabbitmq": mock_aio_pika,
            "redis": mock_redis_client,
            "connection": conn_with_fake_internal_conn,
            "python": conn_with_fake_internal_conn._connection,
            "mosquitto": mock_mosquitto,
        }
        connection_module.Connection.instance_by_vhost.clear()

    @pytest.fixture
    async def mock_connection(self, mock_connections, backend):
        yield mock_connections["connection"]

    @pytest.fixture
    async def mock_internal_connection(self, mock_connections, backend):
        backend_name = backend.__name__.split(".")[-1]
        yield mock_connections[backend_name]

    async def test_connection_success(
        self, mock_connection: MagicMock, mock_internal_connection, backend
    ) -> None:
        await mock_connection.connect()
        assert mock_connection.is_connected()
        await mock_connection.disconnect()
        assert not mock_connection.is_connected()
        await assert_backend_connection(backend, mock_internal_connection)

    async def test_setup_queue_shared(
        self, mock_connection: MagicMock, mock_internal_connection, backend
    ):
        await mock_connection.connect()
        await mock_connection.setup_queue("mylib.tasks.TaskMessage", shared=True)
        await assert_backend_setup_queue(
            backend,
            mock_internal_connection,
            "mylib.tasks.TaskMessage",
            shared=True,
            mock_connection=mock_connection,
        )

    async def test_publish_tasks(
        self, mock_connection: MagicMock, mock_internal_connection, backend
    ):
        conn = await mock_connection.connect()

        incoming = incoming_message_factory(backend)
        backend_name = backend.__name__.split(".")[-1]
        topic = "test.tasks.key"
        delimiter = backend_configs[backend_name].topic_delimiter
        topic = topic.replace(".", delimiter)

        await conn.publish(topic, incoming)
        # assert backend call
        await assert_backend_publish(
            backend,
            mock_internal_connection,
            mock_connection,
            incoming,
            topic=topic,
            count_in_queue=1,
            shared_queue=True,
        )

    async def test_publish(self, mock_connection: MagicMock, mock_internal_connection, backend):
        topic = "test.routing.key"
        conn = await mock_connection.connect()
        msg = None
        incoming = incoming_message_factory(backend)

        async def callback(envelope: IncomingMessageProtocol):
            nonlocal msg
            msg = envelope

        async def predicate():
            return msg == incoming

        await conn.subscribe(topic, callback=callback)
        await conn.publish(topic, incoming)
        if backend == python_backend:
            assert await async_wait(predicate)

        await assert_backend_publish(
            backend,
            mock_internal_connection,
            mock_connection,
            incoming,
            topic=topic,
            count_in_queue=0,
        )

    @pytest.mark.asyncio
    async def test_singleton_logic(self, backend):
        conn1 = await backend.connection.get_connection()
        conn2 = await backend.connection.get_connection()
        assert conn1 is conn2
        await conn1.disconnect()


# --- Specific Tests for rabbitmq ---


@pytest.mark.asyncio
async def test_on_message_requeue_rmq(mock_aio_pika):
    conn = rabbitmq_backend_aio.connection.Connection(requeue_delay=0)  # No delay for testing
    mock_msg = AsyncMock(spec=IncomingMessage)

    # Callback that triggers requeue
    def side_effect(*args):
        raise RequeueMessage()

    callback = MagicMock(side_effect=side_effect)

    await conn._on_message("test.topic", callback, mock_msg)

    mock_msg.reject.assert_awaited_once_with(requeue=True)


@pytest.mark.asyncio
async def test_on_message_poison_pill(mock_aio_pika):
    conn = rabbitmq_backend_aio.connection.Connection()
    mock_msg = AsyncMock(spec=IncomingMessage)

    # Random crash
    def crash(*args):
        raise RuntimeError("Boom")

    callback = MagicMock(side_effect=crash)

    await conn._on_message("test.topic", callback, mock_msg)

    # Should reject without requeue to avoid infinite loop
    mock_msg.reject.assert_awaited_once_with(requeue=False)


@pytest.mark.asyncio
async def test_on_message_success(mock_redis_client):
    conn = redis_backend_aio.connection.Connection()
    await conn.connect()
    # Mock an incoming message
    callback = AsyncMock()

    # We call the internal _on_message
    await conn._on_message_task(
        "test.tasks.topic",
        callback=callback,
        payload={"body": b"test"},
        group_name="test",
        msg_id="111",
    )
    assert callback.called


# --- Specific Tests for redis ---


@pytest.mark.asyncio
async def test_on_message_requeue_redis(mock_redis_client: redis.Redis):
    async with redis_backend_aio.connection.Connection() as conn:
        conn.requeue_delay = 0.1
        payload = Envelope(body=b"test")
        topic = "test.topic"

        # Callback that triggers requeue
        def side_effect(*args):
            raise RequeueMessage()

        msg = None
        callback = MagicMock(side_effect=side_effect)
        # compose the callback for the connection and call it
        real_cb = functools.partial(conn._on_message_pubsub, topic=topic, callback=callback)
        pubsub = mock_redis_client.pubsub()
        await pubsub.subscribe(f"protobunny:{topic}")
        await real_cb(envelope=payload)
        log = logging.getLogger(__name__)

        async def predicate() -> bool:
            nonlocal msg

            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
            return msg is not None

        # the message should be requeued and appeared in the queue
        assert await async_wait(predicate)
        assert msg["type"] in ["message", "pmessage"]
        assert msg["data"] == payload.body


@pytest.mark.asyncio
async def test_on_message_poison_pill(mock_redis_client, mocker):
    spy = mocker.spy(mock_redis_client, "publish")
    async with redis_backend_aio.connection.Connection() as conn:
        # Random crash
        def crash(*args):
            raise Exception("Boom")

        callback = MagicMock(side_effect=crash)

        with pytest.raises(Exception):
            await conn._on_message_pubsub("test.topic", callback=callback, body=b"test")

    # Should reject without requeue to avoid poisoning the queue
    spy.assert_not_called()


async def test_get_message_count(mock_redis_client):
    async with redis_backend_aio.connection.Connection() as conn:
        await conn.publish("test:tasks:topic", Envelope(body=b"test message 1"))
        await conn.publish("test:tasks:topic", Envelope(body=b"test message 2"))
        await conn.publish("test:tasks:topic", Envelope(body=b"test message 3"))
        count = await conn.get_message_count("test:tasks:topic")
        assert count == 3
