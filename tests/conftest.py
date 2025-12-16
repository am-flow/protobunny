import typing as tp
from unittest.mock import AsyncMock, MagicMock

import aio_pika
import aiormq
import pamqp
import pytest
from pytest_mock import MockerFixture

from protobunny.connection import Connection, set_stopped, stop_connection


@pytest.fixture
def mock_connection_obj(mocker: MockerFixture) -> tp.Generator[MagicMock, None, None]:
    mock = mocker.MagicMock(spec=Connection)
    mocker.patch("protobunny.connection.Connection", return_value=mock)
    yield mock
    # Reset connection
    # otherwise the next test will try to assert a different mock instance,
    # while the __CONNECTION variable remains set
    # with the first MagicMock returned by this fixture
    stop_connection()
    set_stopped(False)


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
async def mock_aiopika_connection(
    mocker: MockerFixture,
) -> tp.AsyncGenerator[AsyncMock, None]:
    mocked_connection = mocker.AsyncMock(spec=aio_pika.RobustConnection)
    mocked_channel = mocker.AsyncMock(spec=aio_pika.RobustChannel)
    mocker.patch("aio_pika.connect", return_value=mocked_connection)
    mocker.patch("aio_pika.connect_robust", return_value=mocked_connection)
    mocked_connection.channel.return_value = mocked_channel()
    yield mocked_connection


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
