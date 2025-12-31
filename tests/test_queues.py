import asyncio
import sys
import typing as tp
from unittest.mock import ANY, MagicMock

import pytest
from pytest_mock import MockerFixture

import protobunny as pb_base
from protobunny import asyncio as pb
from protobunny.asyncio.backends import LoggingAsyncQueue
from protobunny.asyncio.backends import mosquitto as mosquitto_backend_aio
from protobunny.asyncio.backends import nats as nats_backend_aio
from protobunny.asyncio.backends import python as python_backend_aio
from protobunny.asyncio.backends import rabbitmq as rabbitmq_backend_aio
from protobunny.asyncio.backends import redis as redis_backend_aio
from protobunny.backends import LoggingSyncQueue
from protobunny.backends import mosquitto as mosquitto_backend
from protobunny.backends import nats as nats_backend
from protobunny.backends import python as python_backend
from protobunny.backends import rabbitmq as rabbitmq_backend
from protobunny.backends import redis as redis_backend
from protobunny.config import backend_configs
from protobunny.helpers import (
    get_queue,
)
from protobunny.models import Envelope

from . import tests

is_old_python = sys.version_info < (3, 12)


@pytest.mark.parametrize(
    "backend",
    [
        rabbitmq_backend_aio,
        redis_backend_aio,
        python_backend_aio,
        mosquitto_backend_aio,
        nats_backend_aio,
    ],
)
class TestQueue:
    @pytest.fixture(autouse=True)
    async def mock_connections(
        self,
        backend,
        mocker,
        mock_redis_client,
        mock_aio_pika,
        test_config,
        mock_mosquitto,
        mock_nats,
    ) -> tp.AsyncGenerator[dict, None]:
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

        if hasattr(backend.connection, "default_configuration"):
            mocker.patch.object(backend.connection, "default_configuration", test_config)
        if hasattr(backend.queues, "default_configuration"):
            mocker.patch.object(backend.queues, "default_configuration", test_config)

        pb.backend = backend
        mocker.patch("protobunny.helpers.get_backend", return_value=backend)
        mocker.patch.object(pb_base, "get_backend", return_value=backend)

        assert pb_base.helpers.get_backend() == backend
        assert pb_base.get_backend() == backend
        assert pb_base.helpers.get_backend() == backend
        assert pb.get_backend() == backend
        assert isinstance(get_queue(tests.TestMessage), backend.queues.AsyncQueue)

        mock = mocker.AsyncMock(spec=backend.connection.Connection)
        match backend_name:
            case "redis":
                mock._connection = mock_redis_client
            case "rabbitmq":
                mock._connection = mock_aio_pika
            case "mosquitto":
                mock._connection = mock_mosquitto
            case "nats":
                mock._connection = mock_nats
            case "python":
                mock = backend.connection.Connection()
                mock.is_connected_event.set()

        mocker.patch.object(pb, "connect", return_value=mock)
        mocker.patch("protobunny.asyncio.backends.BaseAsyncQueue.get_connection", return_value=mock)

        assert asyncio.iscoroutinefunction(pb.disconnect)
        await pb.disconnect()

        yield {
            "rabbitmq": mock_aio_pika,
            "redis": mock_redis_client,
            "connection": mock,
            "python": mock._connection,
            "mosquitto": mock_mosquitto,
            "nats": mock_nats,
        }
        connection_module = getattr(pb_base.backends, backend_name).connection
        connection_module.Connection.instance_by_vhost.clear()

    @pytest.fixture
    async def mock_connection(self, mock_connections, backend, mocker):
        conn = mock_connections["connection"]
        mocker.spy(conn, "publish")
        mocker.spy(conn, "subscribe")
        mocker.spy(conn, "unsubscribe")
        mocker.spy(conn, "get_message_count")
        mocker.spy(conn, "get_consumer_count")
        mocker.spy(conn, "purge")
        yield mock_connections["connection"]

    @pytest.fixture
    async def mock_internal_connection(self, mock_connections, backend):
        backend_name = backend.__name__.split(".")[-1]
        yield mock_connections[backend_name]

    async def test_get_queue(self, backend) -> None:
        q = get_queue(tests.tasks.TaskMessage())
        assert q.shared_queue
        assert isinstance(q, backend.queues.AsyncQueue)

    async def test_get_message_count(self, mock_connection, backend) -> None:
        backend_name = backend.__name__.split(".")[-1]
        if backend_name == "mosquitto":
            pytest.skip("Skipping Mosquitto backend: it doesn't implement get_message_count")
        delimiter = backend_configs[backend_name].topic_delimiter
        q = get_queue(tests.tasks.TaskMessage)
        await q.get_message_count()
        mock_connection.get_message_count.assert_called_once_with(
            "acme.tests.tasks.TaskMessage".replace(".", delimiter)
        )
        q = get_queue(tests.TestMessage)
        with pytest.raises(RuntimeError) as exc:
            await q.get_message_count()
        assert str(exc.value) == "Can only get count of shared queues"

    async def test_purge(self, mock_connection: MagicMock, backend) -> None:
        backend_name = backend.__name__.split(".")[-1]
        delimiter = backend_configs[backend_name].topic_delimiter
        q = get_queue(tests.tasks.TaskMessage)

        await q.purge()
        mock_connection.purge.assert_called_once_with(
            "acme.tests.tasks.TaskMessage".replace(".", delimiter), reset_groups=False
        )
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
        backend_name = backend.__name__.split(".")[-1]
        if is_old_python and backend_name == "redis":
            pytest.skip("Skipping Redis backend on Python < 3.12 due to known fakeredis hang")
        cb = mocker.MagicMock()
        q = get_queue(tests.tasks.TaskMessage)
        assert isinstance(q, backend.queues.AsyncQueue)

        delimiter = backend_configs[backend_name].topic_delimiter
        assert q.topic == "acme.tests.tasks.TaskMessage".replace(".", delimiter)
        assert q.shared_queue
        assert await q.get_connection() == mock_connection
        await q.subscribe(cb)

        topic = "acme.tests.tasks.TaskMessage".replace(".", delimiter)
        mock_connection.subscribe.assert_called_once_with(topic, ANY, shared=True)
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
        backend_name = backend.__name__.split(".")[-1]
        delimiter = backend_configs[backend_name].topic_delimiter
        topic = "acme.tests.tasks.TaskMessage.result".replace(".", delimiter)
        mock_connection.subscribe.assert_called_once_with(topic, ANY, shared=False)
        await q.unsubscribe_results()
        mock_connection.unsubscribe.assert_called_once_with(ANY, if_unused=False, if_empty=False)

    async def test_logger(self, mock_connection: MagicMock, backend) -> None:
        q = await pb.subscribe_logger()
        assert isinstance(q, LoggingAsyncQueue)
        backend_name = backend.__name__.split(".")[-1]
        delimiter = backend_configs[backend_name].topic_delimiter
        wildcard = backend_configs[backend_name].multi_wildcard_delimiter
        topic = f"acme{delimiter}{wildcard}"  # acme.#
        assert q.topic == topic
        mock_connection.subscribe.assert_called_once_with(topic, ANY, shared=False)


@pytest.mark.parametrize(
    "backend", [rabbitmq_backend, redis_backend, python_backend, mosquitto_backend, nats_backend]
)
class TestSyncQueue:
    @pytest.fixture(autouse=True, scope="function")
    def mock_connection(
        self,
        backend,
        mocker,
        mock_redis_client,
        mock_aio_pika,
        test_config,
        mock_nats,
        mock_sync_mosquitto,
    ) -> tp.Generator[None, None, None]:
        backend_name = backend.__name__.split(".")[-1]
        test_config.mode = "sync"
        test_config.backend = backend_name
        test_config.log_task_in_redis = True
        test_config.backend_config = backend_configs[backend_name]

        mocker.patch.object(pb_base.config, "default_configuration", test_config)
        mocker.patch.object(pb_base.models, "default_configuration", test_config)
        mocker.patch.object(pb_base.backends, "default_configuration", test_config)
        mocker.patch.object(pb_base.helpers, "default_configuration", test_config)
        mocker.patch.object(pb.backends.redis.connection, "default_configuration", test_config)

        if hasattr(backend.connection, "default_configuration"):
            mocker.patch.object(backend.connection, "default_configuration", test_config)
        if hasattr(backend.queues, "default_configuration"):
            mocker.patch.object(backend.queues, "default_configuration", test_config)

        pb_base.backend = backend
        mocker.patch("protobunny.helpers.get_backend", return_value=backend)
        mocker.patch.object(pb_base, "get_backend", return_value=backend)

        assert pb_base.helpers.get_backend() == backend
        assert pb_base.get_backend() == backend
        assert pb_base.helpers.get_backend() == backend
        assert pb.get_backend() == backend
        assert isinstance(get_queue(tests.TestMessage), backend.queues.SyncQueue)

        mock = mocker.MagicMock(spec=backend.connection.Connection)  # should be a Sync connection
        mocker.patch.object(pb_base, "connect", return_value=mock)
        mocker.patch("protobunny.backends.BaseSyncQueue.get_connection", return_value=mock)
        assert not asyncio.iscoroutinefunction(pb_base.disconnect)
        pb_base.disconnect()
        yield mock
        connection_module = getattr(pb_base.backends, backend_name).connection
        connection_module.Connection.instance_by_vhost.clear()

    def test_get_queue(self, backend) -> None:
        q = get_queue(tests.tasks.TaskMessage())
        assert q.shared_queue
        assert isinstance(q, backend.queues.SyncQueue)

    def test_get_message_count(self, mock_connection, backend) -> None:
        q = get_queue(tests.tasks.TaskMessage)
        q.get_message_count()
        delimiter = backend_configs[backend.__name__.split(".")[-1]].topic_delimiter
        mock_connection.get_message_count.assert_called_once_with(
            "acme.tests.tasks.TaskMessage".replace(".", delimiter)
        )
        q = get_queue(tests.TestMessage)
        with pytest.raises(RuntimeError) as exc:
            q.get_message_count()
        assert str(exc.value) == "Can only get count of shared queues"

    def test_purge(self, mock_connection: MagicMock, backend) -> None:
        q = get_queue(tests.tasks.TaskMessage)
        q.purge()
        delimiter = backend_configs[backend.__name__.split(".")[-1]].topic_delimiter
        mock_connection.purge.assert_called_once_with(
            "acme.tests.tasks.TaskMessage".replace(".", delimiter)
        )
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
        delimiter = backend_configs[backend.__name__.split(".")[-1]].topic_delimiter
        mock_connection.subscribe.assert_called_once_with(
            "acme.tests.tasks.TaskMessage".replace(".", delimiter), ANY, shared=True
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
        delimiter = backend_configs[backend.__name__.split(".")[-1]].topic_delimiter
        mock_connection.subscribe.assert_called_once_with(
            "acme.tests.tasks.TaskMessage.result".replace(".", delimiter), ANY, shared=False
        )
        q.unsubscribe_results()
        mock_connection.unsubscribe.assert_called_once_with(ANY, if_unused=False, if_empty=False)

    def test_logger(self, mock_connection: MagicMock, backend) -> None:
        q = pb_base.subscribe_logger()
        assert isinstance(q, LoggingSyncQueue)
        delimiter = backend_configs[backend.__name__.split(".")[-1]].topic_delimiter
        wildcard = backend_configs[backend.__name__.split(".")[-1]].multi_wildcard_delimiter
        assert q.topic == f"acme{delimiter}{wildcard}"
        mock_connection.subscribe.assert_called_once_with(
            f"acme{delimiter}{wildcard}", ANY, shared=False
        )
