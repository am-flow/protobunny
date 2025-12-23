from unittest.mock import ANY, AsyncMock, MagicMock

import pytest
from pytest_mock import MockerFixture

import protobunny as pb
from protobunny.backends import LoggingSyncQueue
from protobunny.backends.redis.connection import AsyncRedisConnection
from protobunny.base import (
    get_queue,
)
from protobunny.models import Envelope

from . import tests


@pytest.mark.asyncio
class TestAsyncQueue:
    @pytest.fixture(autouse=True)
    async def setup_connection(self, mock_redis, mocker: MockerFixture) -> None:
        from protobunny.backends import redis as redis_backend
        from protobunny.backends.redis.queues import AsyncQueue, configuration

        mocker.patch.object(pb, "get_connection", redis_backend.connection.get_connection)
        mocker.patch.object(pb, "disconnect", redis_backend.connection.disconnect)
        mocker.patch.object(pb.base.configuration, "backend", "python")
        configuration.backend = "redis"
        configuration.mode = "async"
        assert pb.base.get_backend() == redis_backend
        assert configuration.messages_prefix == "acme"
        assert isinstance(get_queue(tests.tasks.TaskMessage), AsyncQueue)
        assert pb.get_connection.__module__ == "protobunny.backends.redis.connection"
        assert pb.get_connection == redis_backend.connection.get_connection
        assert isinstance(await pb.get_connection(), AsyncRedisConnection)

    async def test_get_message_count(self, mock_redis_connection: AsyncMock) -> None:
        q = get_queue(tests.tasks.TaskMessage)
        await q.get_message_count()
        mock_redis_connection.get_message_count.assert_called_once_with(
            "acme.tests.tasks.TaskMessage"
        )
        q = get_queue(tests.TestMessage)
        with pytest.raises(RuntimeError) as exc:
            await q.get_message_count()
        assert str(exc.value) == "Can only get count of shared queues"

    async def test_purge(self, mock_redis_connection: AsyncMock) -> None:
        q = get_queue(tests.tasks.TaskMessage)
        await q.purge()
        mock_redis_connection.purge.assert_called_once_with("acme.tests.tasks.TaskMessage")
        q = get_queue(tests.TestMessage)
        with pytest.raises(RuntimeError) as exc:
            await q.purge()
        assert str(exc.value) == "Can only purge shared queues"

    async def test_receive(
        self,
        mocker: MockerFixture,
    ) -> None:
        cb = mocker.AsyncMock()
        msg = tests.tasks.TaskMessage(content="test", bbox=[], weights=[])
        q = get_queue(msg)
        incoming = Envelope(body=bytes(msg), routing_key=q.topic)
        await q._receive(cb, incoming)
        cb.assert_called_once_with(msg)

        with pytest.raises(ValueError) as e:
            incoming = Envelope(body=bytes(msg), routing_key="")
            await q._receive(cb, incoming)
        assert str(e.value) == "Routing key was not set. Invalid topic"

        cb.reset_mock()
        incoming_result = Envelope(body=bytes(msg.make_result()), routing_key=q.result_topic)
        # callback not called on result messages
        await q._receive(cb, incoming_result)
        cb.assert_not_called()

    async def test_subscribe(self, mocker: MockerFixture, mock_redis_connection: AsyncMock) -> None:
        cb = mocker.MagicMock()
        q = get_queue(tests.tasks.TaskMessage)
        await q.subscribe(cb)
        mock_redis_connection.subscribe.assert_called_once_with(
            "acme.tests.tasks.TaskMessage", ANY, shared=True
        )
        await q.unsubscribe()
        mock_redis_connection.unsubscribe.assert_called_once_with(
            ANY, if_unused=True, if_empty=True
        )

    async def test_receive_result(
        self,
        mocker: MockerFixture,
    ) -> None:
        cb = mocker.AsyncMock()
        q = get_queue(tests.tasks.TaskMessage)
        source_message = tests.tasks.TaskMessage(content="test", bbox=[], weights=[])
        result_message = source_message.make_result()
        assert result_message.return_value is None
        pika_message = Envelope(body=bytes(result_message), routing_key=q.result_topic)
        await q._receive_result(cb, pika_message)
        assert result_message.return_value is None
        cb.assert_called_once_with(result_message)

    async def test_subscribe_results(
        self, mocker: MockerFixture, mock_redis_connection: AsyncMock
    ) -> None:
        cb = mocker.MagicMock()
        q = get_queue(tests.tasks.TaskMessage)
        await q.subscribe_results(cb)
        mock_redis_connection.subscribe.assert_called_once_with(
            "acme.tests.tasks.TaskMessage.result", ANY, shared=False
        )
        await q.unsubscribe_results()
        mock_redis_connection.unsubscribe.assert_called_once_with(
            ANY, if_unused=False, if_empty=False
        )

    async def test_logger(self, mock_redis_connection: AsyncMock) -> None:
        await pb.subscribe_logger()
        mock_redis_connection.subscribe.assert_called_once_with("acme.#", ANY, shared=False)


class TestQueue:
    @pytest.fixture(autouse=True, scope="class")
    async def setup_connection(self) -> None:
        from protobunny.backends.rabbitmq.queues import configuration

        assert configuration.messages_prefix == "acme"
        configuration.mode = "sync"
        assert not configuration.use_async

    def test_get_message_count(self, mock_sync_redis_connection: MagicMock) -> None:
        q = get_queue(tests.tasks.TaskMessage)
        q.get_message_count()
        mock_sync_redis_connection.get_message_count.assert_called_once_with(
            "acme.tests.tasks.TaskMessage"
        )
        q = get_queue(tests.TestMessage)
        with pytest.raises(RuntimeError) as exc:
            q.get_message_count()
        assert str(exc.value) == "Can only get count of shared queues"

    def test_purge(self, mock_sync_redis_connection: MagicMock) -> None:
        q = get_queue(tests.tasks.TaskMessage)
        q.purge()
        mock_sync_redis_connection.purge.assert_called_once_with("acme.tests.tasks.TaskMessage")
        q = get_queue(tests.TestMessage)
        with pytest.raises(RuntimeError) as exc:
            q.purge()
        assert str(exc.value) == "Can only purge shared queues"

    def test_receive(
        self,
        mocker: MockerFixture,
    ) -> None:
        cb = mocker.MagicMock()
        msg = tests.tasks.TaskMessage(content="test", bbox=[], weights=[])
        q = get_queue(msg)
        incoming = Envelope(body=bytes(msg), routing_key=q.topic)
        q._receive(cb, incoming)
        cb.assert_called_once_with(msg)

        with pytest.raises(ValueError) as e:
            incoming = Envelope(body=bytes(msg), routing_key="")
            q._receive(cb, incoming)
        assert str(e.value) == "Routing key was not set. Invalid topic"

        cb.reset_mock()
        incoming_result = Envelope(body=bytes(msg.make_result()), routing_key=q.result_topic)
        # callback not called on result messages
        q._receive(cb, incoming_result)
        cb.assert_not_called()

    def test_subscribe(self, mocker: MockerFixture, mock_sync_redis_connection: MagicMock) -> None:
        cb = mocker.MagicMock()
        q = get_queue(tests.tasks.TaskMessage)
        q.subscribe(cb)
        mock_sync_redis_connection.subscribe.assert_called_once_with(
            "acme.tests.tasks.TaskMessage", ANY, shared=True
        )
        q.unsubscribe()
        mock_sync_redis_connection.unsubscribe.assert_called_once_with(
            ANY, if_unused=True, if_empty=True
        )

    def test_receive_result(
        self,
        mocker: MockerFixture,
    ) -> None:
        cb = mocker.MagicMock()
        q = get_queue(tests.tasks.TaskMessage)
        source_message = tests.tasks.TaskMessage(content="test", bbox=[], weights=[])
        result_message = source_message.make_result()
        assert result_message.return_value is None
        incoming = Envelope(body=bytes(result_message), routing_key=q.result_topic)
        q._receive_result(cb, incoming)
        assert result_message.return_value is None
        cb.assert_called_once_with(result_message)

    def test_subscribe_results(
        self, mocker: MockerFixture, mock_sync_redis_connection: MagicMock
    ) -> None:
        cb = mocker.MagicMock()
        q = get_queue(tests.tasks.TaskMessage)
        q.subscribe_results(cb)
        mock_sync_redis_connection.subscribe.assert_called_once_with(
            "acme.tests.tasks.TaskMessage.result", ANY, shared=False
        )
        q.unsubscribe_results()
        mock_sync_redis_connection.unsubscribe.assert_called_once_with(
            ANY, if_unused=False, if_empty=False
        )

    def test_logger(self, mock_sync_redis_connection: MagicMock) -> None:
        q = pb.subscribe_logger_sync()
        assert isinstance(q, LoggingSyncQueue)
        assert q.topic == "acme.#"
        mock_sync_redis_connection.subscribe.assert_called_once_with("acme.#", ANY, shared=False)
