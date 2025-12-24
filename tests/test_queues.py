import typing as tp
from unittest.mock import ANY, MagicMock

import pytest
from pytest_mock import MockerFixture

import protobunny as pb
from protobunny.backends import LoggingAsyncQueue, LoggingSyncQueue
from protobunny.backends import python as python_backend
from protobunny.backends import rabbitmq as rabbitmq_backend
from protobunny.backends import redis as redis_backend
from protobunny.base import (
    get_queue,
)
from protobunny.models import Envelope

from . import tests


@pytest.mark.parametrize("backend", [rabbitmq_backend, redis_backend, python_backend])
class TestQueue:
    @pytest.fixture(autouse=True, scope="function")
    async def mock_connection(
        self, backend, mocker, mock_redis, mock_aio_pika
    ) -> tp.AsyncGenerator[None, None]:
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
        pb.disconnect_sync()
        yield mock

    async def test_get_queue(self, backend) -> None:
        q = get_queue(tests.tasks.TaskMessage())
        assert q.shared_queue
        assert isinstance(q, backend.queues.AsyncQueue)

    async def test_get_message_count(self, mock_connection, backend) -> None:
        q = get_queue(tests.tasks.TaskMessage)
        await q.get_message_count()
        mock_connection.get_message_count.assert_called_once_with("acme.tests.tasks.TaskMessage")
        q = get_queue(tests.TestMessage)
        with pytest.raises(RuntimeError) as exc:
            await q.get_message_count()
        assert str(exc.value) == "Can only get count of shared queues"

    async def test_purge(self, mock_connection: MagicMock, backend) -> None:
        q = get_queue(tests.tasks.TaskMessage)
        await q.purge()
        mock_connection.purge.assert_called_once_with("acme.tests.tasks.TaskMessage")
        q = get_queue(tests.TestMessage)
        with pytest.raises(RuntimeError) as exc:
            await q.purge()
        assert str(exc.value) == "Can only purge shared queues"

    async def test_receive(
        self,
        mocker: MockerFixture,
        backend,
    ) -> None:
        cb = mocker.MagicMock()
        msg = tests.tasks.TaskMessage(content="test", bbox=[], weights=[])
        q = get_queue(msg)
        incoming = Envelope(body=bytes(msg), routing_key=q.topic)
        await q._receive(cb, incoming)
        cb.assert_called_once_with(msg)

        with pytest.raises(ValueError) as e:
            await q._receive(cb, Envelope(body=bytes(msg), routing_key=""))
        assert str(e.value) == "Routing key was not set. Invalid topic"

        cb.reset_mock()
        incoming = Envelope(bytes(msg.make_result()), routing_key=q.result_topic)
        # callback not called on result messages
        await q._receive(cb, incoming)
        cb.assert_not_called()

    async def test_subscribe(
        self, mocker: MockerFixture, mock_connection: MagicMock, backend
    ) -> None:
        cb = mocker.MagicMock()
        q = get_queue(tests.tasks.TaskMessage)
        await q.subscribe(cb)
        mock_connection.subscribe.assert_called_once_with(
            "acme.tests.tasks.TaskMessage", ANY, shared=True
        )
        await q.unsubscribe()
        mock_connection.unsubscribe.assert_called_once_with(ANY, if_unused=True, if_empty=True)

    async def test_receive_result(
        self,
        mocker: MockerFixture,
        backend,
    ) -> None:
        q = get_queue(tests.tasks.TaskMessage)
        cb = mocker.MagicMock()
        source_message = tests.tasks.TaskMessage(content="test", bbox=[], weights=[])
        result_message = source_message.make_result()
        incoming = Envelope(body=bytes(result_message), routing_key=q.result_topic)
        assert result_message.return_value is None
        await q._receive_result(cb, incoming)
        cb.assert_called_once_with(result_message)

    async def test_subscribe_results(
        self, mocker: MockerFixture, backend, mock_connection: MagicMock
    ) -> None:
        cb = mocker.MagicMock()
        q = get_queue(tests.tasks.TaskMessage)
        await q.subscribe_results(cb)
        mock_connection.subscribe.assert_called_once_with(
            "acme.tests.tasks.TaskMessage.result", ANY, shared=False
        )
        await q.unsubscribe_results()
        mock_connection.unsubscribe.assert_called_once_with(ANY, if_unused=False, if_empty=False)

    async def test_logger(self, mock_connection: MagicMock, backend) -> None:
        q = await pb.subscribe_logger()
        assert isinstance(q, LoggingAsyncQueue)
        assert q.topic == "acme.#"
        mock_connection.subscribe.assert_called_once_with("acme.#", ANY, shared=False)


@pytest.mark.parametrize("backend", [rabbitmq_backend, redis_backend, python_backend])
class TestSyncQueue:
    @pytest.fixture(autouse=True, scope="function")
    def mock_connection(
        self, backend, mocker, mock_redis, mock_aio_pika
    ) -> tp.Generator[None, None, None]:
        from protobunny.base import configuration

        backend_name = backend.__name__.split(".")[-1]
        mocker.patch.object(pb.base.configuration, "backend", backend_name)
        mocker.patch.object(pb.base.configuration, "mode", "sync")
        assert pb.base.get_backend() == backend
        assert pb.get_backend() == backend
        assert configuration.messages_prefix == "acme"
        assert isinstance(get_queue(tests.tasks.TaskMessage), backend.queues.SyncQueue)
        mock = mocker.MagicMock(spec=backend.connection.SyncConnection)
        mocker.patch.object(pb, "get_connection_sync", return_value=mock)
        mocker.patch.object(pb, "disconnect_sync", side_effect=backend.connection.disconnect_sync)
        mocker.patch("protobunny.backends.BaseSyncQueue.get_connection_sync", return_value=mock)
        mocker.patch(
            f"protobunny.backends.{backend_name}.connection.get_connection_sync", return_value=mock
        )
        pb.disconnect_sync()
        yield mock

    def test_get_queue(self, backend) -> None:
        q = get_queue(tests.tasks.TaskMessage())
        assert q.shared_queue
        assert isinstance(q, backend.queues.SyncQueue)

    def test_get_message_count(self, mock_connection, backend) -> None:
        q = get_queue(tests.tasks.TaskMessage)
        q.get_message_count()
        mock_connection.get_message_count.assert_called_once_with("acme.tests.tasks.TaskMessage")
        q = get_queue(tests.TestMessage)
        with pytest.raises(RuntimeError) as exc:
            q.get_message_count()
        assert str(exc.value) == "Can only get count of shared queues"

    def test_purge(self, mock_connection: MagicMock, backend) -> None:
        q = get_queue(tests.tasks.TaskMessage)
        q.purge()
        mock_connection.purge.assert_called_once_with("acme.tests.tasks.TaskMessage")
        q = get_queue(tests.TestMessage)
        with pytest.raises(RuntimeError) as exc:
            q.purge()
        assert str(exc.value) == "Can only purge shared queues"

    def test_receive(
        self,
        mocker: MockerFixture,
        backend,
    ) -> None:
        cb = mocker.MagicMock()
        msg = tests.tasks.TaskMessage(content="test", bbox=[], weights=[])
        q = get_queue(msg)
        incoming = Envelope(body=bytes(msg), routing_key=q.topic)
        q._receive(cb, incoming)
        cb.assert_called_once_with(msg)

        with pytest.raises(ValueError) as e:
            q._receive(cb, Envelope(body=bytes(msg), routing_key=""))
        assert str(e.value) == "Routing key was not set. Invalid topic"

        cb.reset_mock()
        incoming = Envelope(bytes(msg.make_result()), routing_key=q.result_topic)
        # callback not called on result messages
        q._receive(cb, incoming)
        cb.assert_not_called()

    def test_subscribe(self, mocker: MockerFixture, mock_connection: MagicMock, backend) -> None:
        cb = mocker.MagicMock()
        q = get_queue(tests.tasks.TaskMessage)
        q.subscribe(cb)
        mock_connection.subscribe.assert_called_once_with(
            "acme.tests.tasks.TaskMessage", ANY, shared=True
        )
        q.unsubscribe()
        mock_connection.unsubscribe.assert_called_once_with(ANY, if_unused=True, if_empty=True)

    def test_receive_result(
        self,
        mocker: MockerFixture,
        backend,
    ) -> None:
        q = get_queue(tests.tasks.TaskMessage)
        cb = mocker.MagicMock()
        source_message = tests.tasks.TaskMessage(content="test", bbox=[], weights=[])
        result_message = source_message.make_result()
        incoming = Envelope(body=bytes(result_message), routing_key=q.result_topic)
        assert result_message.return_value is None
        q._receive_result(cb, incoming)
        cb.assert_called_once_with(result_message)

    def test_subscribe_results(
        self, mocker: MockerFixture, backend, mock_connection: MagicMock
    ) -> None:
        cb = mocker.MagicMock()
        q = get_queue(tests.tasks.TaskMessage)
        assert isinstance(q, backend.queues.SyncQueue)
        q.subscribe_results(cb)
        mock_connection.subscribe.assert_called_once_with(
            "acme.tests.tasks.TaskMessage.result", ANY, shared=False
        )
        q.unsubscribe_results()
        mock_connection.unsubscribe.assert_called_once_with(ANY, if_unused=False, if_empty=False)

    def test_logger(self, mock_connection: MagicMock, backend) -> None:
        q = pb.subscribe_logger_sync()
        assert isinstance(q, LoggingSyncQueue)
        assert q.topic == "acme.#"
        mock_connection.subscribe.assert_called_once_with("acme.#", ANY, shared=False)
