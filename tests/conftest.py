import typing as tp
from unittest.mock import AsyncMock, MagicMock, patch

import aio_pika
import aiormq
import pamqp
import pytest
from pytest_mock import MockerFixture

import protobunny

# Create test config
from protobunny.backends.rabbitmq import AsyncConnection, SyncConnection

test_config = protobunny.config.Config(
    messages_directory="tests/proto",
    messages_prefix="acme",
    generated_package_name="tests",
    project_name="test",
    project_root="./",
    force_required_fields=True,
    mode="sync",
)

# Overwrite the module-level configuration
import protobunny.models
import protobunny.queues

protobunny.base.configuration = test_config
protobunny.models.configuration = test_config
protobunny.queues.configuration = test_config


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
def mock_sync_connection(mocker: MockerFixture) -> tp.Generator[MagicMock, None, None]:
    mock = mocker.MagicMock(spec=SyncConnection)
    mocker.patch("protobunny.queues.get_connection_sync", return_value=mock)
    yield mock


@pytest.fixture
async def mock_connection(mocker: MockerFixture) -> tp.AsyncGenerator[AsyncMock, None]:
    mock = mocker.AsyncMock(spec=AsyncConnection)
    mocker.patch("protobunny.queues.get_connection", return_value=mock)
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
