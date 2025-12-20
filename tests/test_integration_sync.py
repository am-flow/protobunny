import typing as tp

import aio_pika
import betterproto
import pytest
from waiting import wait

import protobunny as pb

from . import tests

if tp.TYPE_CHECKING:
    from protobunny.models import ProtoBunnyMessage

from protobunny.queues import configuration


@pytest.mark.integration
class TestIntegration:
    """Integration tests (to run with RabbitMQ up)"""

    received = None
    log_msg = None
    msg = tests.TestMessage(content="test", number=123, color=tests.Color.GREEN)

    @pytest.fixture(autouse=True)
    def setup_connections(self):
        configuration.mode = "sync"
        conn = pb.get_connection_sync()
        self.simple_queue = pb.subscribe_sync(self.msg, self.callback)
        self.task_queue = pb.subscribe_sync(tests.tasks.TaskMessage, self.callback)
        self.logger_queue = pb.subscribe_logger_sync(self.log_callback)
        yield
        pb.unsubscribe_all_sync()
        conn.disconnect()

    def callback(self, msg: "ProtoBunnyMessage") -> tp.Any:
        self.received = msg

    def log_callback(self, message: aio_pika.IncomingMessage, body: str) -> None:
        corr_id = message.correlation_id
        log_msg = (
            f"{message.routing_key}(cid:{corr_id}): {body}"
            if corr_id
            else f"{message.routing_key}: {body}"
        )
        self.log_msg = log_msg

    def test_publish(self) -> None:
        pb.publish_sync(self.msg)
        assert wait(lambda: self.received == self.msg, timeout_seconds=1, sleep_seconds=0.1)
        assert self.received.number == self.msg.number

    def test_to_dict(self) -> None:
        pb.publish_sync(self.msg)
        assert wait(lambda: self.received == self.msg, timeout_seconds=1, sleep_seconds=0.1)
        assert self.received.to_dict(
            casing=betterproto.Casing.SNAKE, include_default_values=True
        ) == {"content": "test", "number": "123", "detail": None, "options": None, "color": "GREEN"}
        assert (
            self.received.to_json(casing=betterproto.Casing.SNAKE, include_default_values=True)
            == '{"content": "test", "number": 123, "detail": null, "options": null, "color": "GREEN"}'
        )

        msg = tests.tasks.TaskMessage(
            content="test",
            bbox=[1, 2, 3, 4],
        )
        pb.publish_sync(msg)
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

    def test_count_messages(self) -> None:
        msg = tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4])
        # we subscribe to create the queue in RabbitMQ
        queue = self.task_queue
        queue.purge()  # remove past messages
        # we unsubscribe so the published messages
        # won't be consumed and stay in the queue
        queue.unsubscribe()
        pb.publish_sync(msg)
        pb.publish_sync(msg)
        pb.publish_sync(msg)
        # and we can count them
        assert wait(
            lambda: 3 == queue.get_message_count(), timeout_seconds=1, sleep_seconds=0.1
        ), f"{queue.get_message_count()}"

    def test_logger_body(self) -> None:
        pb.publish_sync(self.msg)
        assert wait(lambda: isinstance(self.log_msg, str), timeout_seconds=1, sleep_seconds=0.1)
        assert (
            self.log_msg
            == 'acme.tests.TestMessage: {"content": "test", "number": 123, "detail": null, "options": null, "color": "GREEN"}'
        )
        self.log_msg = None
        result = self.msg.make_result()
        pb.publish_result_sync(result)
        assert wait(lambda: isinstance(self.log_msg, str), timeout_seconds=1, sleep_seconds=0.1)
        assert (
            self.log_msg
            == 'acme.tests.TestMessage.result: SUCCESS - {"content": "test", "number": 123, "detail": null, "options": null, "color": "GREEN"}'
        )
        result = self.msg.make_result(
            return_code=pb.results.ReturnCode.FAILURE, return_value={"test": "value"}
        )
        self.log_msg = None
        pb.publish_result_sync(result)
        assert wait(lambda: isinstance(self.log_msg, str), timeout_seconds=1, sleep_seconds=0.1)
        assert (
            self.log_msg
            == 'acme.tests.TestMessage.result: FAILURE - error: [] - {"content": "test", "number": 123, "detail": null, "options": null, "color": "GREEN"}'
        )

    def test_logger_int64(self) -> None:
        # Ensure that uint64/int64 values are not converted to strings in the LoggerQueue callbacks
        pb.publish_sync(
            tests.tasks.TaskMessage(
                content="test", bbox=[1, 2, 3, 4], weights=[1.0, 2.0, -100, -20]
            )
        )
        assert wait(lambda: isinstance(self.log_msg, str), timeout_seconds=1, sleep_seconds=0.1)
        assert isinstance(self.log_msg, str)
        assert (
            self.log_msg
            == 'acme.tests.tasks.TaskMessage: {"content": "test", "weights": [1.0, 2.0, -100.0, -20.0], "bbox": [1, 2, 3, 4], "options": null}'
        )
        self.log_msg = None
        pb.publish_sync(tests.TestMessage(number=63, content="test"))
        assert wait(
            lambda: self.log_msg
            == 'acme.tests.TestMessage: {"content": "test", "number": 63, "detail": null, "options": null, "color": null}',
            timeout_seconds=1,
            sleep_seconds=0.1,
        ), self.log_msg

    def test_unsubscribe(self) -> None:
        pb.publish_sync(self.msg)
        assert wait(lambda: self.received is not None, timeout_seconds=1, sleep_seconds=0.1)
        assert self.received == self.msg
        self.received = None
        pb.unsubscribe_sync(tests.TestMessage, if_unused=False, if_empty=False)
        pb.publish_sync(self.msg)
        assert self.received is None

        # unsubscribe from a package-level topic
        pb.subscribe_sync(tests, self.callback)
        pb.publish_sync(tests.TestMessage(number=63, content="test"))
        assert wait(lambda: self.received is not None, timeout_seconds=1, sleep_seconds=0.1)
        self.received = None
        pb.unsubscribe_sync(tests, if_unused=False, if_empty=False)
        pb.publish_sync(self.msg)
        assert self.received is None

        # subscribe/unsubscribe two callbacks for two topics
        received = None

        def callback_2(m: "ProtoBunnyMessage") -> None:
            nonlocal received
            received = m

        pb.subscribe_sync(tests.TestMessage, self.callback)
        pb.subscribe_sync(tests, callback_2)
        pb.publish_sync(self.msg)  # this will reach callback_2 as well
        assert wait(lambda: self.received and received, timeout_seconds=1, sleep_seconds=0.5)
        assert self.received == received == self.msg
        pb.unsubscribe_all_sync()
        self.received = None
        received = None
        pb.publish_sync(self.msg)
        assert self.received is None
        assert received is None

    def test_unsubscribe_results(self) -> None:
        received_result: pb.results.Result | None = None

        def callback(_: tests.TestMessage) -> None:
            # The receiver catches the error in callback and will send a Result.FAILURE message
            # to the result topic
            raise RuntimeError("error in callback")

        def callback_results(m: pb.results.Result) -> None:
            nonlocal received_result
            received_result = m

        pb.unsubscribe_all_sync()
        pb.subscribe_sync(tests.TestMessage, callback)
        # subscribe to the result topic
        pb.subscribe_results_sync(tests.TestMessage, callback_results)
        msg = tests.TestMessage(number=63, content="test")
        pb.publish_sync(msg)
        assert wait(lambda: received_result is not None, timeout_seconds=1, sleep_seconds=0.1)
        assert received_result.source == msg
        assert received_result.return_code == pb.results.ReturnCode.FAILURE
        pb.unsubscribe_results_sync(tests.TestMessage)
        received_result = None
        pb.publish_sync(msg)
        assert received_result is None

    def test_unsubscribe_all(self) -> None:
        received_message: tests.tasks.TaskMessage | None = None
        received_result: pb.results.Result | None = None

        def callback_1(_: tests.TestMessage) -> None:
            # The receiver catches the error in callback and will send a Result.FAILURE message
            # to the result topic
            raise RuntimeError("error in callback")

        def callback_2(m: tests.tasks.TaskMessage) -> None:
            nonlocal received_message
            received_message = m

        def callback_results(m: pb.results.Result) -> None:
            nonlocal received_result
            received_result = m

        pb.unsubscribe_all_sync()
        q1 = pb.subscribe_sync(tests.TestMessage, callback_1)
        q2 = pb.subscribe_sync(tests.tasks.TaskMessage, callback_2)
        assert q1.topic == "acme.tests.TestMessage"
        assert q2.topic == "acme.tests.tasks.TaskMessage"
        assert q1.subscription is not None
        assert q2.subscription is not None
        # subscribe to a result topic
        pb.subscribe_results_sync(tests.TestMessage, callback_results)
        pb.publish_sync(tests.TestMessage(number=2, content="test"))
        pb.publish_sync(tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4]))
        assert wait(lambda: received_message is not None, timeout_seconds=1, sleep_seconds=0.1)
        assert wait(lambda: received_result is not None, timeout_seconds=1, sleep_seconds=0.1)
        assert received_result.source == tests.TestMessage(number=2, content="test")

        pb.unsubscribe_all_sync()
        received_result = None
        received_message = None
        pb.publish_sync(tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4]))
        pb.publish_sync(tests.TestMessage(number=2, content="test"))
        assert received_message is None
        assert received_result is None
