from unittest.mock import ANY, MagicMock

import aio_pika
from aio_pika import DeliveryMode

import protobunny as pb


def test_send_message(mock_connection_obj: MagicMock) -> None:
    msg = pb.tests.TestMessage(content="test", number=123, color=pb.tests.Color.GREEN)
    pb.publish(msg)
    expected_payload = aio_pika.Message(
        bytes(msg),
        correlation_id=None,
        delivery_mode=DeliveryMode.PERSISTENT,
    )
    mock_connection_obj.publish.assert_called_once_with("pb.tests.TestMessage", expected_payload)


def test_subscribe(mock_connection_obj: MagicMock) -> None:
    msg = pb.tests.TestMessage()
    func = lambda x: print(x)  # noqa: E731
    pb.subscribe(msg, func)
    mock_connection_obj.subscribe.assert_called_with("pb.tests.TestMessage", ANY, shared=False)
    pb.subscribe(pb.tests.tasks.TaskMessage, func)
    mock_connection_obj.subscribe.assert_called_with("pb.tests.tasks.TaskMessage", ANY, shared=True)
    pb.subscribe_results(pb.tests.tasks.TaskMessage, func)
    mock_connection_obj.subscribe.assert_called_with(
        "pb.tests.tasks.TaskMessage.result", ANY, shared=False
    )
