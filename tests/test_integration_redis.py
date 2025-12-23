import time
import typing as tp

import aio_pika
import betterproto
import pytest
from pytest_mock import MockerFixture
from waiting import wait

import protobunny as pb
from protobunny import get_backend, get_queue
from protobunny.backends.redis.connection import AsyncRedisConnection, SyncRedisConnection
from protobunny.backends.redis.queues import AsyncQueue, SyncQueue, configuration
from protobunny.models import IncomingMessageProtocol, ProtoBunnyMessage

from . import tests
from .utils import async_wait


@pytest.mark.integration
class TestIntegration:
    """Integration tests (to run with RabbitMQ up)"""

    received = None
    log_msg = None
    msg = tests.TestMessage(content="test", number=123, color=tests.Color.GREEN)

    @pytest.fixture(autouse=True)
    async def setup_connections(self, mocker: MockerFixture) -> tp.AsyncGenerator[None, None]:
        from protobunny.backends import redis as redis_backend

        configuration.mode = "async"
        configuration.backend = "rabbitmq"
        pb.backend = redis_backend
        mocker.patch("protobunny.backends.get_backend", return_value=redis_backend)
        mocker.patch("protobunny.base.get_backend", return_value=redis_backend)
        mocker.patch.object(pb, "get_connection", redis_backend.connection.get_connection)
        mocker.patch.object(pb, "disconnect", redis_backend.connection.disconnect)
        connection = await pb.get_connection()
        assert isinstance(connection, AsyncRedisConnection)
        queue = pb.get_queue(self.msg)
        assert queue.topic == "acme.tests.TestMessage"
        assert isinstance(queue, AsyncQueue)
        assert isinstance(await queue.get_connection(), AsyncRedisConnection)
        await pb.unsubscribe_all(if_unused=False, if_empty=False)
        await connection.purge(queue.topic, reset_groups=True)
        yield
        await redis_backend.connection.disconnect()
        assert not await connection.is_connected()
        AsyncRedisConnection.instance_by_vhost.clear()

    async def callback(self, msg: "ProtoBunnyMessage") -> tp.Any:
        self.received = msg

    def log_callback(self, message: aio_pika.IncomingMessage, body: str) -> None:
        self.log_msg = f"{message.routing_key} - {body}"

    @pytest.mark.asyncio(loop_scope="function")
    async def test_publish(self) -> None:
        await pb.subscribe(self.msg, self.callback)
        await pb.publish(self.msg)

        async def predicate() -> bool:
            return self.received == self.msg

        assert await async_wait(
            predicate, timeout_seconds=1, sleep_seconds=0.1
        ), f"Received was {self.received}"
        assert self.received.number == self.msg.number
        assert self.received.content == "test"

    async def test_to_dict(self) -> None:
        await pb.subscribe(self.msg, self.callback)
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
        await pb.subscribe(tests.tasks.TaskMessage, self.callback)
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

    async def test_count_messages(self) -> None:
        task_queue = await pb.subscribe(tests.tasks.TaskMessage, self.callback)
        msg = tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4])
        # we subscribe to create the queue in RabbitMQ
        connection = await pb.get_connection()
        await connection.purge(task_queue.topic, reset_groups=True)
        # await task_queue.purge()  # remove past messages

        async def predicate() -> bool:
            return 0 == await task_queue.get_message_count()

        assert await async_wait(
            predicate, timeout_seconds=1, sleep_seconds=0.1
        ), f"Messages were not in the queue: {await task_queue.get_message_count()}"
        # we unsubscribe so the published messages
        # won't be consumed and stay in the queue
        await task_queue.unsubscribe(if_unused=False, if_empty=False)

        async def predicate() -> bool:
            return 0 == await task_queue.get_consumer_count()

        assert await async_wait(
            predicate, timeout_seconds=1, sleep_seconds=0.1
        ), f"Consumers were not 0: {await task_queue.get_consumer_count()}"
        await pb.publish(msg)
        await pb.publish(msg)
        await pb.publish(msg)

        # and we can count them
        async def predicate() -> bool:
            return 3 == await task_queue.get_message_count()

        assert await async_wait(
            predicate, timeout_seconds=1, sleep_seconds=0.1
        ), f"Message count was not 3: {await task_queue.get_message_count()}"

    async def test_logger_int64(self) -> None:
        await pb.subscribe_logger(self.log_callback)
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
            == 'acme.tests.tasks.TaskMessage - {"content": "test", "weights": [1.0, 2.0, -100.0, -20.0], "bbox": [1, 2, 3, 4], "options": null}'
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
            == 'acme.tests.TestMessage - {"content": "test", "number": 63, "detail": null, "options": null, "color": null}'
        )

    async def test_unsubscribe(self) -> None:
        await pb.subscribe(self.msg, self.callback)
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

        async def callback_2(m: "ProtoBunnyMessage") -> None:
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

    async def test_unsubscribe_results(self) -> None:
        received_result: pb.results.Result | None = None

        def callback(_: tests.TestMessage) -> None:
            # The receiver catches the error in callback and will send a Result.FAILURE message
            # to the result topic
            raise RuntimeError("error in callback")

        def callback_results(m: pb.results.Result) -> None:
            nonlocal received_result
            received_result = m

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


@pytest.mark.integration
class TestIntegrationSync:
    """Integration tests (to run with RabbitMQ up)"""

    received = None
    received_2 = None
    log_msg = None
    msg = None
    task_received = None

    @pytest.fixture(autouse=True)
    def setup_connections(self, mocker: MockerFixture) -> tp.Generator[None, None, None]:
        from protobunny.backends import redis as redis_backend

        self.msg = tests.TestMessage(content="test", number=123, color=tests.Color.GREEN)
        configuration.mode = "sync"
        configuration.backend = "redis"

        self.received = None
        self.task_received = None
        # This ensures that we use the rabbitmq backend
        mocker.patch.object(pb, "get_connection_sync", redis_backend.connection.get_connection_sync)
        mocker.patch.object(pb, "disconnect_sync", redis_backend.connection.disconnect_sync)
        mocker.patch.object(pb.base.configuration, "backend", "redis")
        mocker.patch("protobunny.base.get_backend", return_value=redis_backend)
        backend = get_backend()
        assert backend is redis_backend
        conn = pb.get_connection_sync()
        assert isinstance(conn, SyncRedisConnection)
        queue = get_queue(tests.TestMessage)
        assert queue.topic == "acme.tests.TestMessage"
        assert conn.is_connected()
        conn.purge(queue.topic)
        pb.unsubscribe_all_sync(if_unused=False, if_empty=False)
        yield
        redis_backend.connection.disconnect_sync()
        assert not conn.is_connected()
        assert len(conn._instance_by_vhost) == 0
        AsyncRedisConnection.instance_by_vhost.clear()

    def log_callback(self, message: IncomingMessageProtocol, body: str) -> None:
        corr_id = message.correlation_id
        log_msg = (
            f"{message.routing_key}(cid:{corr_id}): {body}"
            if corr_id
            else f"{message.routing_key}: {body}"
        )
        self.log_msg = log_msg

    def callback(self, msg: "ProtoBunnyMessage") -> tp.Any:
        print("---------DEBUG: CALL 1", msg)
        self.received = msg

    def callback_2(self, msg: "ProtoBunnyMessage") -> tp.Any:
        print("---------DEBUG: CALL 2", msg)
        self.received_2 = msg

    def callback_task(self, msg: "ProtoBunnyMessage") -> tp.Any:
        print("---------DEBUG: CALL TASK", msg)
        self.task_received = msg

    def test_publish(self) -> None:
        queue = pb.subscribe_sync(tests.TestMessage, self.callback)
        assert isinstance(queue, SyncQueue)
        time.sleep(1.5)
        pb.publish_sync(self.msg)

        assert wait(lambda: self.received is not None, timeout_seconds=5, sleep_seconds=0.1)
        assert self.received.number == self.msg.number

    def test_to_dict(self) -> None:
        pb.subscribe_sync(tests.TestMessage, self.callback)
        task_queue = pb.subscribe_sync(tests.tasks.TaskMessage, self.callback_task)
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
        assert wait(lambda: self.task_received == msg, timeout_seconds=1, sleep_seconds=0.1)
        assert self.task_received.to_dict(
            casing=betterproto.Casing.SNAKE, include_default_values=True
        ) == {
            "content": "test",
            "bbox": ["1", "2", "3", "4"],
            "weights": [],
            "options": None,
        }
        # to_pydict uses enum names and don't stringyfies int64
        assert self.task_received.to_pydict(
            casing=betterproto.Casing.SNAKE, include_default_values=True
        ) == {
            "content": "test",
            "bbox": [1, 2, 3, 4],
            "weights": [],
            "options": None,
        }
        task_queue.unsubscribe()

    def test_count_messages(self) -> None:
        task_queue = pb.subscribe_sync(tests.tasks.TaskMessage, self.callback_task)
        msg = tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4])
        # we subscribe to create the queue in RabbitMQ
        pb.get_connection_sync().purge(task_queue.topic, reset_groups=True)  # remove past messages
        # we unsubscribe so the published messages
        # won't be consumed and stay in the queue
        task_queue.unsubscribe()
        assert wait(
            lambda: 0 == task_queue.get_consumer_count(), timeout_seconds=1, sleep_seconds=0.1
        )
        pb.publish_sync(msg)
        pb.publish_sync(msg)
        pb.publish_sync(msg)
        # and we can count them
        assert wait(
            lambda: 3 == task_queue.get_message_count(), timeout_seconds=1, sleep_seconds=0.1
        )

    def test_logger_body(self) -> None:
        logger_queue = pb.subscribe_logger_sync(self.log_callback)
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
        logger_queue = pb.subscribe_logger_sync(self.log_callback)
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
        pb.subscribe_sync(tests.TestMessage, self.callback)
        pb.publish_sync(self.msg)
        assert wait(lambda: self.received is not None, timeout_seconds=1, sleep_seconds=0.1)
        assert self.received == self.msg
        self.received = None
        pb.unsubscribe_sync(tests.TestMessage, if_unused=False, if_empty=False)
        pb.publish_sync(self.msg)
        assert self.received is None

        # unsubscribe from a package-level topic
        pb.subscribe_sync(tests, self.callback_2)
        pb.publish_sync(tests.TestMessage(number=63, content="test"))
        assert wait(lambda: self.received_2 is not None, timeout_seconds=1, sleep_seconds=0.1)
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
