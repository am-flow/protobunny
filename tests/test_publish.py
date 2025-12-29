from unittest.mock import ANY, MagicMock

import aio_pika
import pytest
from aio_pika import DeliveryMode

import protobunny as pb

from . import tests


@pytest.fixture(autouse=True)
def setup_config(mocker, test_config) -> None:
    test_config.mode = "sync"
    mocker.patch.object(pb.config, "default_configuration", test_config)
    mocker.patch.object(pb.models, "default_configuration", test_config)
    mocker.patch.object(pb.backends, "default_configuration", test_config)
    mocker.patch.object(pb.helpers, "default_configuration", test_config)


def test_sync_send_message(mock_sync_rmq_connection: MagicMock) -> None:
    msg = tests.TestMessage(content="test", number=123, color=tests.Color.GREEN)
    queue = pb.get_queue(msg, backend_name="rabbitmq")
    queue.publish(msg)
    expected_payload = aio_pika.Message(
        bytes(msg),
        correlation_id=None,
        delivery_mode=DeliveryMode.PERSISTENT,
    )
    mock_sync_rmq_connection.publish.assert_called_once_with(
        "acme.tests.TestMessage", expected_payload
    )


def test_sync_subscribe(mock_sync_rmq_connection: MagicMock) -> None:
    msg = tests.TestMessage()
    func = lambda x: print(x)  # noqa: E731
    pb.subscribe(msg, func)
    mock_sync_rmq_connection.subscribe.assert_called_with(
        "acme.tests.TestMessage", ANY, shared=False
    )
    pb.subscribe(tests.tasks.TaskMessage, func)
    mock_sync_rmq_connection.subscribe.assert_called_with(
        "acme.tests.tasks.TaskMessage", ANY, shared=True
    )
    pb.subscribe_results(tests.tasks.TaskMessage, func)
    mock_sync_rmq_connection.subscribe.assert_called_with(
        "acme.tests.tasks.TaskMessage.result", ANY, shared=False
    )
