from unittest.mock import ANY, MagicMock

import aio_pika
from aio_pika import DeliveryMode

import protobunny as pb

from . import tests


def test_sync_send_message(mock_sync_connection: MagicMock) -> None:
    msg = tests.TestMessage(content="test", number=123, color=tests.Color.GREEN)
    pb.publish_sync(msg)
    expected_payload = aio_pika.Message(
        bytes(msg),
        correlation_id=None,
        delivery_mode=DeliveryMode.PERSISTENT,
    )
    mock_sync_connection.publish.assert_called_once_with("acme.tests.TestMessage", expected_payload)


def test_sync_subscribe(mock_sync_connection: MagicMock) -> None:
    msg = tests.TestMessage()
    func = lambda x: print(x)  # noqa: E731
    pb.subscribe_sync(msg, func)
    mock_sync_connection.subscribe.assert_called_with("acme.tests.TestMessage", ANY, shared=False)
    pb.subscribe_sync(tests.tasks.TaskMessage, func)
    mock_sync_connection.subscribe.assert_called_with(
        "acme.tests.tasks.TaskMessage", ANY, shared=True
    )
    pb.subscribe_results_sync(tests.tasks.TaskMessage, func)
    mock_sync_connection.subscribe.assert_called_with(
        "acme.tests.tasks.TaskMessage.result", ANY, shared=False
    )
