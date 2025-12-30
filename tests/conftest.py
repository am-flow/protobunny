import typing as tp
from unittest.mock import AsyncMock, MagicMock, patch

import aio_pika
import aiormq
import fakeredis
import pamqp
import pytest
import pytest_asyncio
from pytest_mock import MockerFixture

import protobunny
from protobunny.asyncio.backends.mosquitto import connection as mosquitto_connection_aio
from protobunny.asyncio.backends.python import connection as python_connection_aio
from protobunny.asyncio.backends.rabbitmq import connection as rabbitmq_connection_aio
from protobunny.asyncio.backends.redis import connection as redis_connection_aio
from protobunny.backends.mosquitto import connection as mosquitto_connection
from protobunny.backends.python import connection as python_connection
from protobunny.backends.rabbitmq import connection as rabbitmq_connection
from protobunny.backends.redis import connection as redis_connection


@pytest_asyncio.fixture
def test_config() -> protobunny.config.Config:
    conf = protobunny.config.Config(
        messages_directory="tests/proto",
        messages_prefix="acme",
        generated_package_name="tests",
        project_name="test",
        project_root="./",
        force_required_fields=True,
        mode="async",
        backend="rabbitmq",
        log_task_in_redis=False,
    )
    return conf


class MockMQTTConnection:
    mock_message = MagicMock()
    mock_message.payload = b"hello world"
    mock_message.topic.value = "test/topic"

    def __init__(self):
        self.subscribe = AsyncMock()
        self.unsubscribe = AsyncMock()
        self.publish = AsyncMock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def messages(self):
        for item in [self.mock_message]:
            yield item


@pytest.fixture
async def mock_mosquitto(mocker) -> tp.AsyncGenerator[MockMQTTConnection, None]:
    mock_client = MockMQTTConnection()
    mocker.spy(mock_client, "publish")
    mocker.spy(mock_client, "subscribe")
    mocker.spy(mock_client, "unsubscribe")
    mocker.spy(mock_client, "__aenter__")
    mocker.spy(mock_client, "__aexit__")

    with patch("aiomqtt.Client") as mock_client_class:
        mock_client_class.return_value = mock_client
        yield mock_client


@pytest.fixture
async def mock_redis_client(mocker) -> tp.AsyncGenerator[fakeredis.FakeAsyncRedis, None]:
    server = fakeredis.FakeServer()
    client = fakeredis.FakeAsyncRedis(decode_responses=False)

    mocker.patch("protobunny.asyncio.backends.redis.connection.redis.from_url", return_value=client)
    # Clear database before each test
    await client.flushdb()
    yield client
    # Explicitly clear internal fakeredis blocking listeners
    server.connected = False
    await client.aclose()


@pytest.fixture
async def mock_aio_pika():
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
    mock = mocker.MagicMock(spec=rabbitmq_connection.Connection)
    mocker.patch("protobunny.backends.BaseSyncQueue.get_connection", return_value=mock)
    mocker.patch("protobunny.backends.rabbitmq.connection.connect", return_value=mock)
    yield mock


@pytest.fixture
async def mock_rmq_connection(mocker: MockerFixture) -> tp.AsyncGenerator[AsyncMock, None]:
    mock = mocker.AsyncMock(spec=rabbitmq_connection_aio.Connection)
    mocker.patch("protobunny.backends.BaseAsyncQueue.get_connection", return_value=mock)
    yield mock


@pytest.fixture
def mock_sync_redis_connection(mocker: MockerFixture) -> tp.Generator[MagicMock, None, None]:
    mock = mocker.MagicMock(spec=redis_connection.Connection)
    mocker.patch("protobunny.backends.BaseSyncQueue.get_connection", return_value=mock)
    mocker.patch("protobunny.backends.rabbitmq.connection.connect", return_value=mock)
    yield mock


@pytest.fixture
async def mock_redis_connection(mocker: MockerFixture) -> tp.AsyncGenerator[AsyncMock, None]:
    mock = mocker.AsyncMock(spec=redis_connection_aio.Connection)
    mocker.patch("protobunny.backends.BaseAsyncQueue.get_connection", return_value=mock)
    yield mock


@pytest.fixture
def mock_sync_mqtt_connection(mocker: MockerFixture) -> tp.Generator[MagicMock, None, None]:
    mock = mocker.MagicMock(spec=mosquitto_connection.Connection)
    mocker.patch("protobunny.backends.BaseSyncQueue.get_connection", return_value=mock)
    mocker.patch("protobunny.backends.mosquitto.connection.connect", return_value=mock)
    yield mock


@pytest.fixture
async def mock_mqtt_connection(mocker: MockerFixture) -> tp.AsyncGenerator[AsyncMock, None]:
    mock = mocker.AsyncMock(spec=mosquitto_connection_aio.Connection)
    mocker.patch("protobunny.backends.BaseAsyncQueue.get_connection", return_value=mock)
    yield mock


@pytest.fixture
def mock_sync_python_connection(mocker: MockerFixture) -> tp.Generator[MagicMock, None, None]:
    mock = mocker.MagicMock(spec=python_connection.Connection)
    mocker.patch("protobunny.helpers.get_backend", return_value=protobunny.backends.python)
    mocker.patch("protobunny.backends.BaseSyncQueue.get_connection", return_value=mock)
    yield mock


@pytest.fixture
async def mock_python_connection(mocker: MockerFixture) -> tp.AsyncGenerator[AsyncMock, None]:
    mock = mocker.AsyncMock(spec=python_connection_aio.Connection)
    mocker.patch("protobunny.asyncio.backends.BaseAsyncQueue.get_connection", return_value=mock)
    mocker.patch("protobunny.helpers.get_backend", return_value=protobunny.backends.python)
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


@pytest.fixture(autouse=True)
def clear_cached_func() -> tp.Generator[None, None, None]:
    from protobunny.helpers import _build_routing_key

    _build_routing_key.cache_clear()
    yield
    _build_routing_key.cache_clear()
