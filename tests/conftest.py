import typing as tp
from unittest.mock import AsyncMock, MagicMock, patch

import aio_pika
import aiormq
import pamqp
import pytest
import redis
from pytest_mock import MockerFixture

import protobunny
from protobunny.backends.python import connection as python_connection
from protobunny.backends.rabbitmq import connection as rabbitmq_connection
from protobunny.backends.redis import connection as redis_connection

test_config = protobunny.config.Config(
    messages_directory="tests/proto",
    messages_prefix="acme",
    generated_package_name="tests",
    project_name="test",
    project_root="./",
    force_required_fields=True,
    mode="sync",
    backend="rabbitmq",
)

# Overwrite the module-level configuration
protobunny.base.configuration = test_config
import protobunny.backends.python.queues
import protobunny.backends.rabbitmq.queues
import protobunny.models

protobunny.models.configuration = test_config
protobunny.backends.rabbitmq.queues.configuration = test_config
protobunny.backends.redis.queues.configuration = test_config
protobunny.backends.configuration = test_config
protobunny.backends.python.connection.configuration = test_config


@pytest.fixture()
async def mock_redis(mocker: MockerFixture) -> tp.AsyncGenerator[AsyncMock, None]:
    mock = mocker.AsyncMock(spec=redis.asyncio.Redis)
    # 3. Ensure internal methods are also AsyncMocks
    mock.ping = mocker.AsyncMock(return_value=True)
    mock.xadd = mocker.AsyncMock(return_value="12345-0")
    mock.xack = mocker.AsyncMock()
    mock.aclose = mocker.AsyncMock()
    mock.xgroup_create = mocker.AsyncMock()
    mock.smembers = mocker.AsyncMock(return_value=[b"test.routing.key", b"sync.topic"])
    mocker.patch("protobunny.backends.redis.connection.redis.from_url", return_value=mock)
    yield mock


@pytest.fixture
def mock_aio_pika():
    """Mocks the entire aio_pika connection chain."""
    with patch("aio_pika.connect_robust", new_callable=AsyncMock) as mock_connect:
        # Mock Connection
        mock_conn = AsyncMock()
        mock_connect.return_value = mock_conn

        # Mock Channel
        mock_channel = AsyncMock()
        mock_conn.channel.return_value = mock_channel

        # Mock Exchange
        mock_exchange = AsyncMock()
        mock_channel.declare_exchange.return_value = mock_exchange

        # Mock Queue
        mock_queue = AsyncMock()
        mock_queue.name = "test-queue"
        mock_queue.exclusive = False
        mock_channel.declare_queue.return_value = mock_queue

        yield {
            "connect": mock_connect,
            "connection": mock_conn,
            "channel": mock_channel,
            "exchange": mock_exchange,
            "queue": mock_queue,
        }


@pytest.fixture
def mock_sync_rmq_connection(mocker: MockerFixture) -> tp.Generator[MagicMock, None, None]:
    mock = mocker.MagicMock(spec=rabbitmq_connection.SyncRmqConnection)
    mocker.patch("protobunny.backends.BaseSyncQueue.get_connection_sync", return_value=mock)
    mocker.patch("protobunny.backends.rabbitmq.connection.get_connection_sync", return_value=mock)
    yield mock


@pytest.fixture
async def mock_rmq_connection(mocker: MockerFixture) -> tp.AsyncGenerator[AsyncMock, None]:
    mock = mocker.AsyncMock(spec=rabbitmq_connection.AsyncRmqConnection)
    mocker.patch("protobunny.backends.BaseAsyncQueue.get_connection", return_value=mock)
    yield mock


@pytest.fixture
def mock_sync_redis_connection(mocker: MockerFixture) -> tp.Generator[MagicMock, None, None]:
    mock = mocker.MagicMock(spec=redis_connection.SyncRedisConnection)
    mocker.patch("protobunny.backends.BaseSyncQueue.get_connection_sync", return_value=mock)
    mocker.patch("protobunny.backends.rabbitmq.connection.get_connection_sync", return_value=mock)
    yield mock


@pytest.fixture
async def mock_redis_connection(mocker: MockerFixture) -> tp.AsyncGenerator[AsyncMock, None]:
    mock = mocker.AsyncMock(spec=redis_connection.AsyncRedisConnection)
    mocker.patch("protobunny.backends.BaseAsyncQueue.get_connection", return_value=mock)
    yield mock


@pytest.fixture
def mock_sync_python_connection(mocker: MockerFixture) -> tp.Generator[MagicMock, None, None]:
    mock = mocker.MagicMock(spec=python_connection.SyncLocalConnection)
    mocker.patch("protobunny.base.get_backend", return_value=protobunny.backends.python)
    mocker.patch("protobunny.backends.BaseSyncQueue.get_connection_sync", return_value=mock)
    yield mock


@pytest.fixture
async def mock_python_connection(mocker: MockerFixture) -> tp.AsyncGenerator[AsyncMock, None]:
    mock = mocker.AsyncMock(spec=python_connection.AsyncLocalConnection)
    mocker.patch("protobunny.backends.BaseAsyncQueue.get_connection", return_value=mock)
    mocker.patch("protobunny.base.get_backend", return_value=protobunny.backends.python)
    yield mock


@pytest.fixture
def pika_incoming_message() -> tp.Callable[[bytes, str], aio_pika.IncomingMessage]:
    def _incoming_message_factory(body: bytes, routing_key: str) -> aio_pika.IncomingMessage:
        return aio_pika.IncomingMessage(
            aiormq.abc.DeliveredMessage(
                header=pamqp.header.ContentHeader(),
                body=body,
                delivery=pamqp.commands.Basic.Deliver(routing_key=routing_key),
                channel=None,
            )
        )

    return _incoming_message_factory


@pytest.fixture
def pika_message() -> (
    tp.Callable[
        [
            bytes,
        ],
        aio_pika.Message,
    ]
):
    def _message_factory(body: bytes) -> aio_pika.Message:
        return aio_pika.Message(body=body)

    return _message_factory


@pytest.fixture(scope="session", autouse=True)
def pika_messages_eq() -> tp.Generator[None, None, None]:
    # Add support for equality in pika Messages
    # as the mock library uses args comparison for expected calls
    # and aio_pika.Message doesn't have __eq__ defined
    def compare_aio_pika_messages(a, b) -> bool:
        if not (isinstance(a, aio_pika.Message) and isinstance(b, aio_pika.Message)):
            return False
        return str(a) == str(b) and a.body == b.body

    aio_pika.Message.__eq__ = compare_aio_pika_messages  # type: ignore
    yield
    aio_pika.Message.__eq__ = object.__eq__  # type: ignore
