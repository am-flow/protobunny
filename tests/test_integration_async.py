import asyncio
import time
import typing as tp

import aio_pika
import betterproto
import pytest

import protobunny as pb
from protobunny.models import ProtoBunnyMessage
from protobunny.queues import AsyncQueue, configuration

from . import tests


async def async_wait(condition_func, timeout_seconds=1.0, sleep_seconds=0.1):
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        res = await condition_func()
        if res:
            return True
        await asyncio.sleep(sleep_seconds)  # This yields control to the loop!
    return False


@pytest.mark.integration
class TestAsyncIntegration:
    """Integration tests (to run with RabbitMQ up)"""

    received = None
    log_msg = None
    msg = tests.TestMessage(content="test", number=123, color=tests.Color.GREEN)

    @pytest.fixture(autouse=True)
    async def setup_connections(self):
        configuration.mode = "async"
        connection = await pb.get_connection()
        queue = pb.get_queue(self.msg)
        assert queue.topic == "acme.tests.TestMessage"
        assert isinstance(queue, AsyncQueue)

        self.simple_queue = await pb.subscribe(self.msg, self.callback)
        self.task_queue = await pb.subscribe(tests.tasks.TaskMessage, self.callback)
        self.logger_queue = await pb.subscribe_logger(self.log_callback)
        yield
        await pb.unsubscribe_all()
        await connection.disconnect()

    async def callback(self, msg: "ProtoBunnyMessage") -> tp.Any:
        self.received = msg

    def log_callback(self, _: aio_pika.IncomingMessage, body: str) -> None:
        self.log_msg = body

    @pytest.mark.asyncio(loop_scope="function")
    async def test_publish(self) -> None:
        await pb.publish(self.msg)

        async def predicate() -> bool:
            return self.received == self.msg

        assert await async_wait(
            predicate, timeout_seconds=5, sleep_seconds=0.5
        ), f"Received was {self.received}"
        assert self.received.number == self.msg.number
        assert self.received.content == "test"

    @pytest.mark.asyncio
    async def test_to_dict(self) -> None:
        await pb.publish(self.msg)

        async def predicate() -> bool:
            return self.received == self.msg

        assert await async_wait(predicate, timeout_seconds=1, sleep_seconds=0.1)
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
        await pb.publish(msg)

        async def predicate() -> bool:
            return self.received == msg

        assert await async_wait(predicate, timeout_seconds=1, sleep_seconds=0.1)
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

    @pytest.mark.asyncio
    async def test_count_messages(self) -> None:
        msg = tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4])
        # we subscribe to create the queue in RabbitMQ
        queue = self.task_queue
        await queue.purge()  # remove past messages
        # we unsubscribe so the published messages
        # won't be consumed and stay in the queue
        await queue.unsubscribe()
        await pb.publish(msg)
        await pb.publish(msg)
        await pb.publish(msg)

        # and we can count them
        async def predicate() -> bool:
            return 3 == await queue.get_message_count()

        assert await async_wait(
            predicate, timeout_seconds=1, sleep_seconds=0.1
        ), f"Message count was not 3: {await queue.get_message_count()}"

    async def test_logger_int64(self) -> None:
        # Ensure that uint64/int64 values are not converted to strings in the LoggerQueue callbacks
        await pb.publish(
            tests.tasks.TaskMessage(
                content="test", bbox=[1, 2, 3, 4], weights=[1.0, 2.0, -100, -20]
            )
        )

        async def predicate() -> bool:
            return self.log_msg is not None

        assert await async_wait(predicate, timeout_seconds=1, sleep_seconds=0.1)
        assert isinstance(self.log_msg, str)
        assert (
            self.log_msg
            == '{"content": "test", "weights": [1.0, 2.0, -100.0, -20.0], "bbox": [1, 2, 3, 4], "options": null}'
        )
        self.log_msg = None
        await pb.publish(tests.TestMessage(number=63, content="test"))
        assert await async_wait(
            predicate,
            timeout_seconds=1,
            sleep_seconds=0.1,
        ), self.log_msg
        assert isinstance(self.log_msg, str)
        assert (
            self.log_msg
            == '{"content": "test", "number": 63, "detail": null, "options": null, "color": null}'
        )

    @pytest.mark.asyncio
    async def test_unsubscribe(self) -> None:
        await pb.publish(self.msg)

        async def predicate() -> bool:
            return self.received is not None

        assert await async_wait(predicate, timeout_seconds=1, sleep_seconds=0.1)
        assert self.received == self.msg
        self.received = None
        await pb.unsubscribe(tests.TestMessage, if_unused=False, if_empty=False)
        await pb.publish(self.msg)
        assert self.received is None

        # unsubscribe from a package-level topic
        await pb.subscribe(tests, self.callback)
        await pb.publish(tests.TestMessage(number=63, content="test"))
        assert await async_wait(predicate, timeout_seconds=1, sleep_seconds=0.1)
        self.received = None
        await pb.unsubscribe(tests, if_unused=False, if_empty=False)
        await pb.publish(self.msg)
        assert self.received is None

        # subscribe/unsubscribe two callbacks for two topics
        received = None

        def callback_2(m: "ProtoBunnyMessage") -> None:
            nonlocal received
            received = m

        await pb.subscribe(tests.TestMessage, self.callback)
        await pb.subscribe(tests, callback_2)
        await pb.publish(self.msg)  # this will reach callback_2 as well

        async def predicate() -> bool:
            return self.received is not None and received is not None

        assert await async_wait(predicate, timeout_seconds=1, sleep_seconds=0.5)
        assert self.received == received == self.msg
        await pb.unsubscribe_all()
        self.received = None
        received = None
        await pb.publish(self.msg)
        assert self.received is None
        assert received is None

    @pytest.mark.asyncio
    async def test_unsubscribe_results(self) -> None:
        received_result: pb.results.Result | None = None

        def callback(_: tests.TestMessage) -> None:
            # The receiver catches the error in callback and will send a Result.FAILURE message
            # to the result topic
            raise RuntimeError("error in callback")

        def callback_results(m: pb.results.Result) -> None:
            nonlocal received_result
            received_result = m

        await pb.unsubscribe_all()
        await pb.subscribe(tests.TestMessage, callback)
        # subscribe to the result topic
        await pb.subscribe_results(tests.TestMessage, callback_results)
        msg = tests.TestMessage(number=63, content="test")
        await pb.publish(msg)

        async def predicate() -> bool:
            return received_result is not None

        assert await async_wait(predicate, timeout_seconds=1, sleep_seconds=0.1)
        assert received_result.source == msg
        assert received_result.return_code == pb.results.ReturnCode.FAILURE
        await pb.unsubscribe_results(tests.TestMessage)
        received_result = None
        await pb.publish(msg)
        assert received_result is None

    @pytest.mark.asyncio
    async def test_unsubscribe_all(self) -> None:
        received_message: tests.tasks.TaskMessage | None = None
        received_result: pb.results.Result | None = None

        async def callback_1(_: tests.TestMessage) -> None:
            # The receiver catches the error in callback and will send a Result.FAILURE message
            # to the result topic
            raise RuntimeError("error in callback")

        async def callback_2(m: tests.tasks.TaskMessage) -> None:
            nonlocal received_message
            received_message = m

        async def callback_results(m: pb.results.Result) -> None:
            nonlocal received_result
            received_result = m

        await pb.unsubscribe_all()
        q1 = await pb.subscribe(tests.TestMessage, callback_1)
        q2 = await pb.subscribe(tests.tasks.TaskMessage, callback_2)
        assert q1.topic == "acme.tests.TestMessage"
        assert q2.topic == "acme.tests.tasks.TaskMessage"
        assert q1.subscription is not None
        assert q2.subscription is not None
        # subscribe to a result topic
        await pb.subscribe_results(tests.TestMessage, callback_results)
        await pb.publish(tests.TestMessage(number=2, content="test"))
        await pb.publish(tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4]))

        async def predicate() -> bool:
            return received_message is not None and received_result is not None

        assert await async_wait(predicate, timeout_seconds=2, sleep_seconds=0.2)
        assert received_result.source == tests.TestMessage(number=2, content="test")

        await pb.unsubscribe_all()
        received_result = None
        received_message = None
        await pb.publish(tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4]))
        await pb.publish(tests.TestMessage(number=2, content="test"))
        assert received_message is None
        assert received_result is None
