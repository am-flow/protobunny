from unittest.mock import ANY, MagicMock

import pytest
from pytest_mock import MockerFixture

import protobunny as pb
from protobunny.backends import LoggingAsyncQueue, LoggingSyncQueue
from protobunny.backends.python.queues import AsyncQueue, SyncQueue
from protobunny.base import (
    get_queue,
)
from protobunny.models import Envelope

from . import tests


class TestAsyncQueue:
    @pytest.fixture(autouse=True, scope="function")
    async def setup_connection(self, mocker: MockerFixture) -> None:
        from protobunny.backends import configuration
        from protobunny.backends import python as python_backend

        assert configuration.messages_prefix == "acme"
        configuration.mode = "async"
        configuration.backend = "python"
        mocker.patch("protobunny.backends.get_backend", return_value=python_backend)

    async def test_get_queue(self, mock_python_connection) -> None:
        q = get_queue(tests.tasks.TaskMessage())
        assert q.shared_queue
        assert isinstance(q, AsyncQueue)

    async def test_get_message_count(self, mock_python_connection) -> None:
        q = get_queue(tests.tasks.TaskMessage)
        await q.get_message_count()
        mock_python_connection.get_message_count.assert_called_once_with(
            "acme.tests.tasks.TaskMessage"
        )
        q = get_queue(tests.TestMessage)
        with pytest.raises(RuntimeError) as exc:
            await q.get_message_count()
        assert str(exc.value) == "Can only get count of shared queues"

    async def test_purge(self, mock_python_connection: MagicMock) -> None:
        q = get_queue(tests.tasks.TaskMessage)
        await q.purge()
        mock_python_connection.purge.assert_called_once_with("acme.tests.tasks.TaskMessage")
        q = get_queue(tests.TestMessage)
        with pytest.raises(RuntimeError) as exc:
            await q.purge()
        assert str(exc.value) == "Can only purge shared queues"

    async def test_receive(
        self,
        mocker: MockerFixture,
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
        self, mocker: MockerFixture, mock_python_connection: MagicMock
    ) -> None:
        cb = mocker.MagicMock()
        q = get_queue(tests.tasks.TaskMessage)
        await q.subscribe(cb)
        mock_python_connection.subscribe.assert_called_once_with(
            "acme.tests.tasks.TaskMessage", ANY, shared=True
        )
        await q.unsubscribe()
        mock_python_connection.unsubscribe.assert_called_once_with(
            ANY, if_unused=True, if_empty=True
        )

    async def test_receive_result(
        self,
        mocker: MockerFixture,
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
        self, mocker: MockerFixture, mock_python_connection: MagicMock
    ) -> None:
        cb = mocker.MagicMock()
        q = get_queue(tests.tasks.TaskMessage)
        await q.subscribe_results(cb)
        mock_python_connection.subscribe.assert_called_once_with(
            "acme.tests.tasks.TaskMessage.result", ANY, shared=False
        )
        await q.unsubscribe_results()
        mock_python_connection.unsubscribe.assert_called_once_with(
            ANY, if_unused=False, if_empty=False
        )

    async def test_logger(self, mock_python_connection: MagicMock) -> None:
        q = await pb.subscribe_logger()
        assert isinstance(q, LoggingAsyncQueue)
        assert q.topic == "acme.#"
        mock_python_connection.subscribe.assert_called_once_with("acme.#", ANY, shared=False)


class TestSyncQueue:
    @pytest.fixture(autouse=True, scope="function")
    def setup_connection(self, mocker: MockerFixture) -> None:
        from protobunny.backends import configuration
        from protobunny.backends import python as python_backend

        assert configuration.messages_prefix == "acme"
        configuration.mode = "sync"
        configuration.backend = "python"
        assert not configuration.use_async
        mocker.patch("protobunny.backends.get_backend", return_value=python_backend)

    def test_get_queue(self, mock_sync_python_connection) -> None:
        q = get_queue(tests.tasks.TaskMessage())
        assert q.shared_queue
        assert isinstance(q, SyncQueue)

    def test_get_message_count(self, mock_sync_python_connection) -> None:
        q = get_queue(tests.tasks.TaskMessage)
        q.get_message_count()
        mock_sync_python_connection.get_message_count.assert_called_once_with(
            "acme.tests.tasks.TaskMessage"
        )
        q = get_queue(tests.TestMessage)
        with pytest.raises(RuntimeError) as exc:
            q.get_message_count()
        assert str(exc.value) == "Can only get count of shared queues"

    def test_purge(self, mock_sync_python_connection: MagicMock) -> None:
        q = get_queue(tests.tasks.TaskMessage)
        q.purge()
        mock_sync_python_connection.purge.assert_called_once_with("acme.tests.tasks.TaskMessage")
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
            q._receive(cb, Envelope(body=bytes(msg), routing_key=""))
        assert str(e.value) == "Routing key was not set. Invalid topic"

        cb.reset_mock()
        incoming = Envelope(bytes(msg.make_result()), routing_key=q.result_topic)
        # callback not called on result messages
        q._receive(cb, incoming)
        cb.assert_not_called()

    def test_subscribe(self, mocker: MockerFixture, mock_sync_python_connection: MagicMock) -> None:
        cb = mocker.MagicMock()
        q = get_queue(tests.tasks.TaskMessage)
        q.subscribe(cb)
        mock_sync_python_connection.subscribe.assert_called_once_with(
            "acme.tests.tasks.TaskMessage", ANY, shared=True
        )
        q.unsubscribe()
        mock_sync_python_connection.unsubscribe.assert_called_once_with(
            ANY, if_unused=True, if_empty=True
        )

    def test_receive_result(
        self,
        mocker: MockerFixture,
    ) -> None:
        q = get_queue(tests.tasks.TaskMessage)
        cb = mocker.MagicMock()
        source_message = tests.tasks.TaskMessage(content="test", bbox=[], weights=[])
        result_message = source_message.make_result()
        incoming = Envelope(body=bytes(result_message), routing_key=q.result_topic)
        assert result_message.return_value is None
        q._receive_result(cb, incoming)
        assert result_message.return_value is None
        cb.assert_called_once_with(result_message)

    def test_subscribe_results(
        self, mocker: MockerFixture, mock_sync_python_connection: MagicMock
    ) -> None:
        cb = mocker.MagicMock()
        q = get_queue(tests.tasks.TaskMessage)
        q.subscribe_results(cb)
        mock_sync_python_connection.subscribe.assert_called_once_with(
            "acme.tests.tasks.TaskMessage.result", ANY, shared=False
        )
        q.unsubscribe_results()
        mock_sync_python_connection.unsubscribe.assert_called_once_with(
            ANY, if_unused=False, if_empty=False
        )

    def test_logger(self, mock_sync_python_connection: MagicMock) -> None:
        q = pb.subscribe_logger_sync()
        assert isinstance(q, LoggingSyncQueue)
        assert q.topic == "acme.#"
        mock_sync_python_connection.subscribe.assert_called_once_with("acme.#", ANY, shared=False)
