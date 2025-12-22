from unittest.mock import MagicMock

import aio_pika
import pytest
from aio_pika import DeliveryMode
from pytest_mock import MockerFixture

import protobunny as pb
from protobunny.backends import deserialize_result_message
from protobunny.backends.rabbitmq.connection import SyncConnection
from protobunny.backends.rabbitmq.queues import configuration
from protobunny.models import get_message_class_from_topic, get_message_class_from_type_url

from . import tests

configuration.mode = "sync"
configuration.backend = "rabbitmq"


@pytest.fixture(autouse=True)
def setup_connections(mocker: MockerFixture, mock_sync_rmq_connection) -> None:
    from protobunny.backends import rabbitmq as rabbitmq_backend
    from protobunny.backends.rabbitmq.queues import configuration

    configuration.mode = "sync"
    configuration.backend = "rabbitmq"
    pb.backend = rabbitmq_backend
    mocker.patch.object(pb, "get_connection", rabbitmq_backend.connection.get_connection)
    mocker.patch.object(pb.base.configuration, "backend", "rabbitmq")
    assert configuration.messages_prefix == "acme"
    assert not configuration.use_async
    queue = pb.get_queue(tests.TestMessage)
    assert isinstance(queue.get_connection_sync(), SyncConnection)


def test_serdeser_result() -> None:
    msg = tests.TestMessage(content="test", number=123)
    result = msg.make_result(return_value={"test": "value"})
    assert result.source_message.value == bytes(msg)
    assert result.return_value == {"test": "value"}
    assert result.source == msg
    message_type = get_message_class_from_topic(msg.topic)
    assert isinstance(msg, message_type)

    # result.source_message is a protobuf.Any instance,
    # (and not an instance of the original Start message)
    # To get an instance of the source message,
    # use get_message_class_from_type_url and pass the Any.type_url as parameter
    message_type = get_message_class_from_type_url(result.source_message.type_url)
    assert isinstance(msg, message_type)
    assert msg == message_type().parse(bytes(msg))

    queue = pb.get_queue(msg)
    assert "acme.tests.TestMessage.result" == queue.result_topic

    # Serialize result
    # The return_value dictionary is first converted
    # to a commons.JsonContent before being serialized
    ser = bytes(result)
    deser = deserialize_result_message(ser)
    assert deser.source_message.type_url == "tests.tests.TestMessage"
    assert deser.source_message.type_url == msg.type_url
    # The return value is a commons.JsonContent object
    # and it transparently returns to a dict once deserialized
    assert deser.return_value == {"test": "value"}
    assert deser.error == "" and deser.error == result.error
    assert deser == result


def test_topics(mock_sync_rmq_connection: MagicMock) -> None:
    msg = tests.TestMessage(content="test", number=123)
    result = msg.make_result(return_value={"test": "value"})
    q = pb.get_queue(result.source)
    assert isinstance(q.get_connection_sync(), SyncConnection)
    assert q.get_connection_sync() == mock_sync_rmq_connection
    assert q.result_topic == "acme.tests.TestMessage.result"
    pb.publish_result_sync(result)
    expected_payload = aio_pika.Message(
        bytes(result),
        correlation_id=None,
        delivery_mode=DeliveryMode.NOT_PERSISTENT,
    )
    mock_sync_rmq_connection.publish.assert_called_once_with(
        "acme.tests.TestMessage.result", expected_payload
    )
