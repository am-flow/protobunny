import typing as tp
from unittest.mock import ANY, AsyncMock, MagicMock

import aio_pika
import pytest
from pytest_mock import MockerFixture

import protobunny as pb
from protobunny.backends import LoggingSyncQueue
from protobunny.base import (
    get_queue,
    get_queue_sync,
)

from . import tests


@pytest.mark.asyncio
class TestAsyncQueue:
    @pytest.fixture(autouse=True, scope="class")
    async def setup_connection(self) -> None:
        from protobunny.backends.rabbitmq.queues import configuration

        configuration.mode = "async"
        assert configuration.use_async
        assert configuration.messages_prefix == "acme"

    async def test_get_message_count(self, mock_connection: AsyncMock) -> None:
        q = get_queue(tests.tasks.TaskMessage)
        await q.get_message_count()
        mock_connection.get_message_count.assert_called_once_with("acme.tests.tasks.TaskMessage")
        q = get_queue(tests.TestMessage)
        with pytest.raises(RuntimeError) as exc:
            await q.get_message_count()
        assert str(exc.value) == "Can only get count of shared queues"

    async def test_purge(self, mock_connection: AsyncMock) -> None:
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
        pika_incoming_message: tp.Callable[[bytes, str], aio_pika.IncomingMessage],
    ) -> None:
        cb = mocker.AsyncMock()
        msg = tests.tasks.TaskMessage(content="test", bbox=[], weights=[])
        q = get_queue(msg)
        pika_message = pika_incoming_message(bytes(msg), q.topic)
        await q._receive(cb, pika_message)
        cb.assert_called_once_with(msg)

        with pytest.raises(ValueError) as e:
            pika_message_no_routing = pika_incoming_message(bytes(msg), "")
            await q._receive(cb, pika_message_no_routing)
        assert str(e.value) == "Routing key was not set. Invalid topic"

        cb.reset_mock()
        pika_message_result = pika_incoming_message(bytes(msg.make_result()), q.result_topic)
        # callback not called on result messages
        await q._receive(cb, pika_message_result)
        cb.assert_not_called()

    async def test_subscribe(self, mocker: MockerFixture, mock_connection: AsyncMock) -> None:
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
        pika_incoming_message: tp.Callable[[bytes, str], aio_pika.IncomingMessage],
    ) -> None:
        cb = mocker.AsyncMock()
        q = get_queue(tests.tasks.TaskMessage)
        source_message = tests.tasks.TaskMessage(content="test", bbox=[], weights=[])
        result_message = source_message.make_result()
        assert result_message.return_value is None
        pika_message = pika_incoming_message(bytes(result_message), q.result_topic)
        await q._receive_result(cb, pika_message)
        assert result_message.return_value is None
        cb.assert_called_once_with(result_message)

    async def test_subscribe_results(
        self, mocker: MockerFixture, mock_connection: AsyncMock
    ) -> None:
        cb = mocker.MagicMock()
        q = get_queue(tests.tasks.TaskMessage)
        await q.subscribe_results(cb)
        mock_connection.subscribe.assert_called_once_with(
            "acme.tests.tasks.TaskMessage.result", ANY, shared=False
        )
        await q.unsubscribe_results()
        mock_connection.unsubscribe.assert_called_once_with(ANY, if_unused=False, if_empty=False)

    async def test_logger(self, mock_connection: AsyncMock) -> None:
        await pb.subscribe_logger()
        mock_connection.subscribe.assert_called_once_with("acme.#", ANY, shared=False)


class TestQueue:
    @pytest.fixture(autouse=True, scope="class")
    async def setup_connection(self) -> None:
        from protobunny.backends.rabbitmq.queues import configuration

        assert configuration.messages_prefix == "acme"
        configuration.mode = "sync"
        assert not configuration.use_async

    def test_get_message_count(self, mock_sync_connection: MagicMock) -> None:
        q = get_queue_sync(tests.tasks.TaskMessage)
        q.get_message_count()
        mock_sync_connection.get_message_count.assert_called_once_with(
            "acme.tests.tasks.TaskMessage"
        )
        q = get_queue_sync(tests.TestMessage)
        with pytest.raises(RuntimeError) as exc:
            q.get_message_count()
        assert str(exc.value) == "Can only get count of shared queues"

    def test_purge(self, mock_sync_connection: MagicMock) -> None:
        q = get_queue_sync(tests.tasks.TaskMessage)
        q.purge()
        mock_sync_connection.purge.assert_called_once_with("acme.tests.tasks.TaskMessage")
        q = get_queue_sync(tests.TestMessage)
        with pytest.raises(RuntimeError) as exc:
            q.purge()
        assert str(exc.value) == "Can only purge shared queues"

    def test_receive(
        self,
        mocker: MockerFixture,
        pika_incoming_message: tp.Callable[[bytes, str], aio_pika.IncomingMessage],
    ) -> None:
        cb = mocker.MagicMock()
        msg = tests.tasks.TaskMessage(content="test", bbox=[], weights=[])
        q = get_queue_sync(msg)
        pika_message = pika_incoming_message(bytes(msg), q.topic)
        q._receive(cb, pika_message)
        cb.assert_called_once_with(msg)

        with pytest.raises(ValueError) as e:
            pika_message_no_routing = pika_incoming_message(bytes(msg), "")
            q._receive(cb, pika_message_no_routing)
        assert str(e.value) == "Routing key was not set. Invalid topic"

        cb.reset_mock()
        pika_message_result = pika_incoming_message(bytes(msg.make_result()), q.result_topic)
        # callback not called on result messages
        q._receive(cb, pika_message_result)
        cb.assert_not_called()

    def test_subscribe(self, mocker: MockerFixture, mock_sync_connection: MagicMock) -> None:
        cb = mocker.MagicMock()
        q = get_queue_sync(tests.tasks.TaskMessage)
        q.subscribe(cb)
        mock_sync_connection.subscribe.assert_called_once_with(
            "acme.tests.tasks.TaskMessage", ANY, shared=True
        )
        q.unsubscribe()
        mock_sync_connection.unsubscribe.assert_called_once_with(ANY, if_unused=True, if_empty=True)

    def test_receive_result(
        self,
        mocker: MockerFixture,
        pika_incoming_message: tp.Callable[[bytes, str], aio_pika.IncomingMessage],
    ) -> None:
        cb = mocker.MagicMock()
        q = get_queue_sync(tests.tasks.TaskMessage)
        source_message = tests.tasks.TaskMessage(content="test", bbox=[], weights=[])
        result_message = source_message.make_result()
        assert result_message.return_value is None
        pika_message = pika_incoming_message(bytes(result_message), q.result_topic)
        q._receive_result(cb, pika_message)
        assert result_message.return_value is None
        cb.assert_called_once_with(result_message)

    def test_subscribe_results(
        self, mocker: MockerFixture, mock_sync_connection: MagicMock
    ) -> None:
        cb = mocker.MagicMock()
        q = get_queue_sync(tests.tasks.TaskMessage)
        q.subscribe_results(cb)
        mock_sync_connection.subscribe.assert_called_once_with(
            "acme.tests.tasks.TaskMessage.result", ANY, shared=False
        )
        q.unsubscribe_results()
        mock_sync_connection.unsubscribe.assert_called_once_with(
            ANY, if_unused=False, if_empty=False
        )

    def test_logger(self, mock_sync_connection: MagicMock) -> None:
        q = pb.subscribe_logger_sync()
        assert isinstance(q, LoggingSyncQueue)
        assert q.topic == "acme.#"
        mock_sync_connection.subscribe.assert_called_once_with("acme.#", ANY, shared=False)
