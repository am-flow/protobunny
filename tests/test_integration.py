from typing import Any

import aio_pika
import betterproto
import pytest
from waiting import wait

import protobunny as pb


@pytest.mark.integration
class TestIntegration:
    """Integration tests (to run with RabbitMQ up)"""

    received = None
    log_msg = None
    msg = pb.tests.TestMessage(content="test", number=123, color=pb.tests.Color.GREEN)

    def teardown_method(self):
        self.received = None
        self.log_msg = None
        pb.unsubscribe_all()

    def setup_method(self):
        self.simple_queue = pb.subscribe(self.msg, self.callback)
        self.task_queue = pb.subscribe(pb.tests.tasks.TaskMessage, self.callback)
        self.logger_queue = pb.subscribe_logger(self.log_callback)

    def callback(self, msg: pb.tests.TestMessage) -> Any:
        self.received = msg

    def log_callback(self, _: aio_pika.IncomingMessage, body: str) -> None:
        self.log_msg = body

    def test_publish(self, integration_test: None) -> None:
        pb.publish(self.msg)
        assert wait(lambda: self.received == self.msg, timeout_seconds=1, sleep_seconds=0.1)
        assert self.received.number == self.msg.number

    def test_to_dict(self, integration_test: None) -> None:
        pb.publish(self.msg)
        assert wait(lambda: self.received == self.msg, timeout_seconds=1, sleep_seconds=0.1)
        assert self.received.to_dict(
            casing=betterproto.Casing.SNAKE, include_default_values=True
        ) == {"content": "test", "number": "123", "detail": None, "options": None, "color": "GREEN"}
        assert (
            self.received.to_json(casing=betterproto.Casing.SNAKE, include_default_values=True)
            == '{"content": "test", "number": 123, "detail": null, "options": null, "color": "GREEN"}'
        )

        msg = pb.tests.tasks.TaskMessage(
            content="test",
            bbox=[1, 2, 3, 4],
        )
        pb.publish(msg)
        assert wait(lambda: self.received == msg, timeout_seconds=1, sleep_seconds=0.1)
        assert self.received.to_dict(
            casing=betterproto.Casing.SNAKE, include_default_values=True
        ) == {
            "content": "test",
            "bbox": ["1", "2", "3", "4"],
            "weights": [],
            "options": None,
        }
        # to_pydict uses enum names and don't stringyfies int64
        assert self.received.to_pydict(
            casing=betterproto.Casing.SNAKE, include_default_values=True
        ) == {
            "content": "test",
            "bbox": [1, 2, 3, 4],
            "weights": [],
            "options": None,
        }

    def test_count_messages(self, integration_test: None) -> None:
        msg = pb.tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4])
        # we subscribe to create the queue in RabbitMQ
        queue = self.task_queue
        queue.purge()  # remove past messages
        # we unsubscribe so the published messages
        # won't be consumed and stay in the queue
        queue.unsubscribe()
        pb.publish(msg)
        pb.publish(msg)
        pb.publish(msg)
        # and we can count them
        assert wait(
            lambda: 3 == queue.get_message_count(), timeout_seconds=1, sleep_seconds=0.1
        ), f"{queue.get_message_count()}"

    def test_logger_int64(self, integration_test: None) -> None:
        # Ensure that uint64/int64 values are not converted to strings in the LoggerQueue callbacks
        pb.publish(
            pb.tests.tasks.TaskMessage(
                content="test", bbox=[1, 2, 3, 4], weights=[1.0, 2.0, -100, -20]
            )
        )
        assert wait(lambda: isinstance(self.log_msg, str), timeout_seconds=1, sleep_seconds=0.1)
        assert isinstance(self.log_msg, str)
        assert (
            self.log_msg
            == '{"content": "test", "weights": [1.0, 2.0, -100.0, -20.0], "bbox": [1, 2, 3, 4], "options": null}'
        )
        pb.publish(pb.tests.TestMessage(number=63, content="test"))
        assert wait(
            lambda: self.log_msg
            == '{"content": "test", "number": 63, "detail": null, "options": null, "color": null}',
            timeout_seconds=1,
            sleep_seconds=0.1,
        ), self.log_msg

    def test_unsubscribe(self, integration_test: None) -> None:
        pb.publish(self.msg)
        assert wait(lambda: self.received is not None, timeout_seconds=1, sleep_seconds=0.1)
        assert self.received == self.msg
        self.received = None
        pb.unsubscribe(pb.tests.TestMessage)
        pb.publish(self.msg)
        assert self.received is None

        # unsubscribe from a package-level topic
        pb.subscribe(pb.tests, self.callback)
        pb.publish(pb.tests.TestMessage(number=63, content="test"))
        assert wait(lambda: self.received is not None, timeout_seconds=1, sleep_seconds=0.1)
        self.received = None
        pb.unsubscribe(pb.tests)
        pb.publish(self.msg)
        assert self.received is None

        # subscribe/unsubscribe two callbacks for two topics
        received = None

        def callback_2(m: pb.ProtoBunnyMessage) -> None:
            nonlocal received
            received = m

        pb.subscribe(pb.tests.TestMessage, self.callback)
        pb.subscribe(pb.tests, callback_2)
        pb.publish(self.msg)  # this will reach callback_2 as well
        assert wait(lambda: self.received and received, timeout_seconds=1, sleep_seconds=0.5)
        assert self.received == received == self.msg
        self.received = None
        received = None
        pb.unsubscribe(pb.tests.TestMessage)
        pb.unsubscribe(pb.tests)
        pb.publish(self.msg)
        assert self.received is None
        assert received is None

    def test_unsubscribe_results(self, integration_test: None) -> None:
        received_result: pb.results.Result | None = None

        def callback(_: pb.tests.TestMessage) -> None:
            # The receiver catches the error in callback and will send a Result.FAILURE message
            # to the result topic
            raise RuntimeError("error in callback")

        def callback_results(m: pb.results.Result) -> None:
            nonlocal received_result
            received_result = m

        pb.unsubscribe_all()
        pb.subscribe(pb.tests.TestMessage, callback)
        # subscribe to the result topic
        pb.subscribe_results(pb.tests.TestMessage, callback_results)
        msg = pb.tests.TestMessage(number=63, content="test")
        pb.publish(msg)
        assert wait(lambda: received_result is not None, timeout_seconds=1, sleep_seconds=0.1)
        assert received_result.source == msg
        assert received_result.return_code == pb.results.ReturnCode.FAILURE
        received_result = None
        pb.unsubscribe_results(pb.tests.TestMessage)
        pb.publish(msg)
        assert received_result is None

    def test_unsubscribe_all(self, integration_test: None) -> None:
        received_message: pb.tests.tasks.TaskMessage | None = None
        received_result: pb.results.Result | None = None

        def callback_1(_: pb.tests.TestMessage) -> None:
            # The receiver catches the error in callback and will send a Result.FAILURE message
            # to the result topic
            raise RuntimeError("error in callback")

        def callback_2(m: pb.tests.tasks.TaskMessage) -> None:
            nonlocal received_message
            received_message = m

        def callback_results(m: pb.results.Result) -> None:
            nonlocal received_result
            received_result = m

        pb.unsubscribe_all()
        pb.subscribe(pb.tests.TestMessage, callback_1)
        pb.subscribe(pb.tests.tasks.TaskMessage, callback_2)
        # subscribe to a result topic
        pb.subscribe_results(pb.tests.TestMessage, callback_results)

        pb.publish(pb.tests.TestMessage(number=2, content="test"))
        pb.publish(pb.tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4]))
        assert wait(lambda: received_message is not None, timeout_seconds=1, sleep_seconds=0.1)
        assert wait(lambda: received_result is not None, timeout_seconds=1, sleep_seconds=0.1)
        assert received_result.source == pb.tests.TestMessage(number=2, content="test")
        received_result = None
        received_message = None

        # Unsubscribe from all
        pb.unsubscribe_all()
        pb.publish(pb.tests.TestMessage(number=2, content="test"))
        pb.publish(pb.tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4]))
        assert received_message is None
        assert received_result is None
