from unittest.mock import MagicMock

import aio_pika
from aio_pika import DeliveryMode

import protobunny as pb
from protobunny.models import get_message_class_from_topic, get_message_class_from_type_url
from protobunny.queues import configuration, deserialize_result_message

from . import tests

configuration.mode = "sync"


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


def test_topics(mock_sync_connection: MagicMock) -> None:
    msg = tests.TestMessage(content="test", number=123)
    result = msg.make_result(return_value={"test": "value"})
    q = pb.get_queue(result.source)
    assert q.result_topic == "acme.tests.TestMessage.result"
    pb.publish_result_sync(result)
    expected_payload = aio_pika.Message(
        bytes(result),
        correlation_id=None,
        delivery_mode=DeliveryMode.NOT_PERSISTENT,
    )
    mock_sync_connection.publish.assert_called_once_with(
        "acme.tests.TestMessage.result", expected_payload
    )
