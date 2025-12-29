import gc
import logging
import typing as tp

import aio_pika
import betterproto
import pytest
from pytest_mock import MockerFixture

import protobunny as pb_sync
from protobunny import asyncio as pb
from protobunny.asyncio.backends import mosquitto as mosquitto_backend_aio
from protobunny.asyncio.backends import python as python_backend_aio
from protobunny.asyncio.backends import rabbitmq as rabbitmq_backend_aio
from protobunny.asyncio.backends import redis as redis_backend_aio
from protobunny.backends import mosquitto as mosquitto_backend
from protobunny.backends import python as python_backend
from protobunny.backends import rabbitmq as rabbitmq_backend
from protobunny.backends import redis as redis_backend
from protobunny.config import Config, backend_configs
from protobunny.models import ProtoBunnyMessage

from . import tests
from .utils import async_wait, sync_wait

log = logging.getLogger(__name__)

received: dict[str, "ProtoBunnyMessage | str | None"] = {
    "message": None,
    "log": None,
    "result": None,
    "task": None,
}


# Callbacks for tests
async def callback(msg: "ProtoBunnyMessage") -> None:
    global received
    log.debug("CALLBACK: %s", msg)
    received["message"] = msg


def callback_sync(msg: "ProtoBunnyMessage") -> None:
    global received
    log.debug("CALLBACK: %s", msg)
    received["message"] = msg


# Callbacks for tests
async def callback_task(msg: "ProtoBunnyMessage") -> None:
    global received
    log.debug("CALLBACK TASK: %s", msg)
    received["task"] = msg


def callback_task_sync(msg: "ProtoBunnyMessage") -> None:
    global received
    log.debug("CALLBACK TASK: %s", msg)
    received["task"] = msg


async def callback_result(msg: pb.results.Result) -> None:
    global received
    log.debug("CALLBACK RESULT: %s", msg)
    received["result"] = msg


def callback_result_sync(msg: pb.results.Result) -> None:
    global received
    log.debug("CALLBACK RESULT: %s", msg)
    received["result"] = msg


def log_callback(message: aio_pika.IncomingMessage, body: str) -> str:
    global received
    log.debug("LOG CALLBACK: %s", body)
    corr_id = message.correlation_id
    log_msg = (
        f"{message.routing_key}(cid:{corr_id}) - {body}"
        if corr_id
        else f"{message.routing_key}: {body}"
    )
    received["log"] = log_msg
    return log_msg


@pytest.mark.integration
@pytest.mark.parametrize(
    "backend", [rabbitmq_backend_aio, redis_backend_aio, python_backend_aio, mosquitto_backend_aio]
)
class TestIntegration:
    """Integration tests (to run with RabbitMQ up)"""

    msg = tests.TestMessage(content="test", number=123, color=tests.Color.GREEN)

    @pytest.fixture(autouse=True)
    async def setup_test_env(
        self, mocker: MockerFixture, test_config: Config, backend
    ) -> tp.AsyncGenerator[None, None]:
        backend_name = backend.__name__.split(".")[-1]
        test_config.mode = "async"
        test_config.backend = backend_name
        test_config.log_task_in_redis = True
        test_config.backend_config = backend_configs[backend_name]
        self.topic_delimiter = test_config.backend_config.topic_delimiter
        # Patch global configuration for all modules that use it
        mocker.patch.object(pb_sync.config, "default_configuration", test_config)
        mocker.patch.object(pb_sync.models, "default_configuration", test_config)
        mocker.patch.object(pb_sync.helpers, "default_configuration", test_config)

        # patch the asyncio modules
        mocker.patch.object(pb.backends, "default_configuration", test_config)
        mocker.patch.object(pb, "default_configuration", test_config)
        if hasattr(backend.connection, "default_configuration"):
            mocker.patch.object(backend.connection, "default_configuration", test_config)
        if hasattr(backend.queues, "default_configuration"):
            mocker.patch.object(backend.queues, "default_configuration", test_config)

        pb.backend = backend
        mocker.patch("protobunny.asyncio.backends.get_backend", return_value=backend)
        mocker.patch.object(pb, "get_connection", backend.connection.get_connection)
        mocker.patch.object(pb, "disconnect", backend.connection.disconnect)
        mocker.patch.object(pb, "get_backend", return_value=backend)

        # Assert the patching is working for setting the backend
        connection = await pb.get_connection()
        assert isinstance(connection, backend.connection.Connection)
        queue = pb.get_queue(self.msg)
        assert queue.topic == "acme.tests.TestMessage".replace(
            ".", test_config.backend_config.topic_delimiter
        )
        assert isinstance(queue, backend.queues.AsyncQueue)
        assert isinstance(await queue.get_connection(), backend.connection.Connection)
        # start without pending subscriptions
        await pb.unsubscribe_all(if_unused=False, if_empty=False)
        yield
        # reset the variables holding the messages received
        global received
        received = {
            "message": None,
            "log": None,
            "result": None,
            "task": None,
        }
        await connection.disconnect()
        backend.connection.Connection.instance_by_vhost.clear()
        gc.collect()

    @pytest.mark.flaky(max_runs=3)
    async def test_publish(self, backend) -> None:
        global received
        await pb.subscribe(self.msg.__class__, callback)
        await pb.publish(self.msg)

        async def predicate() -> bool:
            return received["message"] == self.msg

        assert await async_wait(predicate), f"Received was {received['message']}"
        assert received["message"].number == self.msg.number
        assert received["message"].content == "test"

    @pytest.mark.flaky(max_runs=3)
    async def test_to_dict(self, backend) -> None:
        global received
        await pb.subscribe(self.msg.__class__, callback)
        await pb.publish(self.msg)

        async def predicate() -> bool:
            return received["message"] == self.msg

        assert await async_wait(predicate)
        assert received["message"].to_dict(
            casing=betterproto.Casing.SNAKE, include_default_values=True
        ) == {"content": "test", "number": "123", "detail": None, "options": None, "color": "GREEN"}
        assert (
            received["message"].to_json(
                casing=betterproto.Casing.SNAKE, include_default_values=True
            )
            == '{"content": "test", "number": 123, "detail": null, "options": null, "color": "GREEN"}'
        )
        await pb.subscribe(tests.tasks.TaskMessage, callback)
        msg = tests.tasks.TaskMessage(
            content="test",
            bbox=[1, 2, 3, 4],
        )
        await pb.publish(msg)

        async def predicate() -> bool:
            return received["message"] == msg

        assert await async_wait(predicate, timeout=1, sleep=0.1)
        assert received["message"].to_dict(
            casing=betterproto.Casing.SNAKE, include_default_values=True
        ) == {
            "content": "test",
            "bbox": ["1", "2", "3", "4"],
            "weights": [],
            "options": None,
        }
        # to_pydict uses enum names and don't stringyfies int64
        assert received["message"].to_pydict(
            casing=betterproto.Casing.SNAKE, include_default_values=True
        ) == {
            "content": "test",
            "bbox": [1, 2, 3, 4],
            "weights": [],
            "options": None,
        }

    @pytest.mark.flaky(max_runs=3)
    async def test_count_messages(self, backend) -> None:
        backend_name = backend.__name__.split(".")[-1]
        if backend_name == "mosquitto":
            pytest.skip("mosquitto backend doesn't support message counts")
        task_queue = await pb.subscribe(tests.tasks.TaskMessage, callback)
        msg = tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4])
        # we subscribe to create the queue in RabbitMQ
        connection = await pb.get_connection()
        # remove past messages
        await connection.purge(task_queue.topic, reset_groups=True)

        async def predicate() -> bool:
            return 0 == await task_queue.get_message_count()

        assert await async_wait(
            predicate
        ), f"Messages were not in the queue: {await task_queue.get_message_count()}"
        # we unsubscribe so the published messages
        # won't be consumed and stay in the queue
        await pb.unsubscribe(tests.tasks.TaskMessage, if_unused=False, if_empty=False)

        async def predicate() -> bool:
            return 0 == await task_queue.get_consumer_count()

        assert await async_wait(
            predicate
        ), f"Consumers were not 0: {await task_queue.get_consumer_count()}"
        await pb.publish(msg)
        await pb.publish(msg)
        await pb.publish(msg)

        # and we can count them
        async def predicate() -> bool:
            return 3 == await task_queue.get_message_count()

        assert await async_wait(
            predicate
        ), f"Message count was not 3: {await task_queue.get_message_count()}"

    @pytest.mark.flaky(max_runs=3)
    async def test_logger_body(self, backend) -> None:
        topic = "acme.tests.TestMessage".replace(".", self.topic_delimiter)
        results_topic = f"{topic}.result".replace(".", self.topic_delimiter)
        await pb.subscribe_logger(log_callback)
        await pb.publish(self.msg)

        async def predicate() -> bool:
            return received["log"] is not None

        assert await async_wait(predicate)
        assert (
            received["log"]
            == f'{topic}: {{"content": "test", "number": 123, "detail": null, "options": null, "color": "GREEN"}}'
        )
        received["log"] = None
        result = self.msg.make_result()
        await pb.publish_result(result)

        async def predicate() -> bool:
            return received["log"] is not None

        assert await async_wait(predicate)
        assert (
            received["log"]
            == f'{results_topic}: SUCCESS - {{"content": "test", "number": 123, "detail": null, "options": null, "color": "GREEN"}}'
        )
        result = self.msg.make_result(
            return_code=pb.results.ReturnCode.FAILURE, return_value={"test": "value"}
        )
        received["log"] = None
        await pb.publish_result(result)

        assert await async_wait(predicate)
        assert (
            received["log"]
            == f'{results_topic}: FAILURE - error: [] - {{"content": "test", "number": 123, "detail": null, "options": null, "color": "GREEN"}}'
        )

    @pytest.mark.flaky(max_runs=3)
    async def test_logger_int64(self, backend) -> None:
        global received
        topic = "acme.tests.TestMessage".replace(".", self.topic_delimiter)
        topic_task = "acme.tests.tasks.TaskMessage".replace(".", self.topic_delimiter)
        log_msg: str | None = None

        def callback_log(message, body: str) -> None:
            nonlocal log_msg
            log_msg = log_callback(message, body)

        await pb.subscribe_logger(callback_log)
        await pb.publish(tests.TestMessage(number=63, content="test"))

        async def predicate() -> bool:
            return log_msg is not None

        assert await async_wait(
            predicate,
        )

        assert (
            log_msg
            == f'{topic}: {{"content": "test", "number": 63, "detail": null, "options": null, "color": null}}'
        ), log_msg
        # Ensure that uint64/int64 values are not converted to strings in the LoggerQueue callbacks
        log_msg = None
        await pb.publish(
            tests.tasks.TaskMessage(
                content="test", bbox=[1, 2, 3, 4], weights=[1.0, 2.0, -100, -20]
            )
        )
        assert await async_wait(predicate)
        assert isinstance(log_msg, str)
        assert (
            log_msg
            == f'{topic_task}: {{"content": "test", "weights": [1.0, 2.0, -100.0, -20.0], "bbox": [1, 2, 3, 4], "options": null}}'
        )

    @pytest.mark.flaky(max_runs=3)
    async def test_unsubscribe(self, backend) -> None:
        global received
        await pb.subscribe(self.msg.__class__, callback)
        await pb.publish(self.msg)

        async def predicate() -> bool:
            return received["message"] is not None

        assert await async_wait(predicate, timeout=1, sleep=0.1)
        assert received["message"] == self.msg
        received["message"] = None
        await pb.unsubscribe(tests.TestMessage, if_unused=False, if_empty=False)
        await pb.publish(self.msg)
        assert received["message"] is None

        # unsubscribe from a package-level topic
        await pb.subscribe(tests, callback)
        await pb.publish(tests.TestMessage(number=63, content="test"))
        assert await async_wait(predicate, timeout=1, sleep=0.1)
        received["message"] = None
        await pb.unsubscribe(tests, if_unused=False, if_empty=False)
        await pb.publish(self.msg)
        assert received["message"] is None

        # subscribe/unsubscribe two callbacks for two topics
        received2 = None

        async def callback_2(m: "ProtoBunnyMessage") -> None:
            nonlocal received2
            log.debug(f"CALLBACK 2 callback_2: {m}")
            received2 = m

        await pb.subscribe(tests.TestMessage, callback)
        await pb.subscribe(tests, callback_2)
        await pb.publish(self.msg)  # this will reach callback_2 as well

        async def predicate() -> bool:
            nonlocal received2
            return received["message"] is not None and received2 is not None

        assert await async_wait(predicate, timeout=2, sleep=0.1)
        assert received["message"] == received2 == self.msg
        await pb.unsubscribe_all()
        received["message"] = None
        received2 = None
        await pb.publish(self.msg)
        assert received["message"] is None
        assert received2 is None

    @pytest.mark.flaky(max_runs=3)
    async def test_unsubscribe_results(self, backend) -> None:
        received_result: pb.results.Result | None = None

        def callback_test(_: tests.TestMessage) -> None:
            # The receiver catches the error in callback and will send a Result.FAILURE message
            # to the result topic
            raise RuntimeError("error in callback")

        def callback_results(m: pb.results.Result) -> None:
            nonlocal received_result
            received_result = m

        await pb.subscribe(tests.TestMessage, callback_test)
        # subscribe to the result topic
        await pb.subscribe_results(tests.TestMessage, callback_results)
        msg = tests.TestMessage(number=63, content="test")
        await pb.publish(msg)

        async def predicate() -> bool:
            return received_result is not None

        assert await async_wait(predicate, timeout=1, sleep=0.1)
        assert received_result.source == msg
        assert received_result.return_code == pb.results.ReturnCode.FAILURE
        await pb.unsubscribe_results(tests.TestMessage)
        received_result = None
        await pb.publish(msg)
        assert received_result is None

    @pytest.mark.flaky(max_runs=3)
    async def test_unsubscribe_all(self, backend) -> None:
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
        assert q1.topic == "acme.tests.TestMessage".replace(".", self.topic_delimiter)
        assert q2.topic == "acme.tests.tasks.TaskMessage".replace(".", self.topic_delimiter)
        assert q1.subscription is not None
        assert q2.subscription is not None
        # subscribe to a result topic
        await pb.subscribe_results(tests.TestMessage, callback_results)
        await pb.publish(tests.TestMessage(number=2, content="test"))
        await pb.publish(tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4]))

        async def predicate() -> bool:
            return received_message is not None and received_result is not None

        assert await async_wait(predicate, timeout=2, sleep=0.2)
        assert received_result.source == tests.TestMessage(number=2, content="test")

        await pb.unsubscribe_all()
        received_result = None
        received_message = None
        await pb.publish(tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4]))
        await pb.publish(tests.TestMessage(number=2, content="test"))
        assert received_message is None
        assert received_result is None


@pytest.mark.integration
@pytest.mark.parametrize(
    "backend", [rabbitmq_backend, redis_backend, python_backend, mosquitto_backend]
)
class TestIntegrationSync:
    """Integration tests (to run with the backend server up)"""

    msg = tests.TestMessage(content="test", number=123, color=tests.Color.GREEN)

    @pytest.fixture(autouse=True)
    def setup_test_env(
        self, mocker: MockerFixture, test_config: Config, backend
    ) -> tp.Generator[None, None, None]:
        backend_name = backend.__name__.split(".")[-1]
        test_config.mode = "sync"
        test_config.backend = backend_name
        test_config.log_task_in_redis = True
        test_config.backend_config = backend_configs[backend_name]
        self.topic_delimiter = test_config.backend_config.topic_delimiter
        # Patch global configuration for all modules that use it
        mocker.patch.object(pb_sync.config, "default_configuration", test_config)
        mocker.patch.object(pb_sync.models, "default_configuration", test_config)
        mocker.patch.object(pb_sync.backends, "default_configuration", test_config)
        mocker.patch.object(pb_sync.helpers, "default_configuration", test_config)
        mocker.patch.object(pb.backends.redis.connection, "default_configuration", test_config)
        if hasattr(backend.connection, "default_configuration"):
            mocker.patch.object(backend.connection, "default_configuration", test_config)
        if hasattr(backend.queues, "default_configuration"):
            mocker.patch.object(backend.queues, "default_configuration", test_config)

        pb_sync.backend = backend
        # mocker.patch("protobunny.helpers.get_backend", return_value=backend)
        mocker.patch.object(pb_sync.helpers, "get_backend", return_value=backend)
        mocker.patch.object(pb_sync, "get_connection", backend.connection.get_connection)
        mocker.patch.object(pb_sync, "disconnect", backend.connection.disconnect)
        mocker.patch.object(pb_sync, "get_backend", return_value=backend)

        # Assert the patching is working for setting the backend
        connection = pb_sync.get_connection()
        assert isinstance(connection, backend.connection.Connection)
        queue = pb_sync.get_queue(self.msg)
        assert queue.topic == "acme.tests.TestMessage".replace(
            ".", test_config.backend_config.topic_delimiter
        )
        assert isinstance(queue, backend.queues.SyncQueue)
        assert isinstance(queue.get_connection(), backend.connection.Connection)
        # start without pending subscriptions
        pb_sync.unsubscribe_all(if_unused=False, if_empty=False)
        yield
        # reset the variables holding the messages received
        global received
        received = {
            "message": None,
            "log": None,
            "result": None,
            "task": None,
        }
        connection.disconnect()
        backend.connection.Connection.instance_by_vhost.clear()

    @pytest.mark.flaky(max_runs=3)
    def test_publish(self, backend) -> None:
        global received
        pb_sync.subscribe(tests.TestMessage, callback_sync)
        pb_sync.publish(self.msg)
        assert sync_wait(lambda: received["message"] is not None)
        assert received["message"].number == self.msg.number

    @pytest.mark.flaky(max_runs=3)
    def test_to_dict(self, backend) -> None:
        global received
        pb_sync.subscribe(tests.TestMessage, callback_sync)
        pb_sync.subscribe(tests.tasks.TaskMessage, callback_task_sync)
        pb_sync.publish(self.msg)
        assert sync_wait(lambda: received["message"] == self.msg)
        assert received["message"].to_dict(
            casing=betterproto.Casing.SNAKE, include_default_values=True
        ) == {"content": "test", "number": "123", "detail": None, "options": None, "color": "GREEN"}
        assert (
            received["message"].to_json(
                casing=betterproto.Casing.SNAKE, include_default_values=True
            )
            == '{"content": "test", "number": 123, "detail": null, "options": null, "color": "GREEN"}'
        )

        msg = tests.tasks.TaskMessage(
            content="test",
            bbox=[1, 2, 3, 4],
        )
        pb_sync.publish(msg)
        assert sync_wait(lambda: received["task"] == msg)
        assert received["task"].to_dict(
            casing=betterproto.Casing.SNAKE, include_default_values=True
        ) == {
            "content": "test",
            "bbox": ["1", "2", "3", "4"],
            "weights": [],
            "options": None,
        }
        # to_pydict uses enum names and don't stringyfies int64
        assert received["task"].to_pydict(
            casing=betterproto.Casing.SNAKE, include_default_values=True
        ) == {
            "content": "test",
            "bbox": [1, 2, 3, 4],
            "weights": [],
            "options": None,
        }

    @pytest.mark.flaky(max_runs=3)
    def test_count_messages(self, backend) -> None:
        backend_name = backend.__name__.split(".")[-1]
        if backend_name == "mosquitto":
            pytest.skip("mosquitto backend doesn't support message counts")
        # Subscribe to a tasks topic (shared queue)
        task_queue = pb_sync.subscribe(tests.tasks.TaskMessage, callback_task_sync)
        msg = tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4])
        connection = pb_sync.get_connection()
        # remove past messages
        connection.purge(task_queue.topic, reset_groups=True)
        # we unsubscribe so the published messages
        # won't be consumed and stay in the queue
        task_queue.unsubscribe()
        assert sync_wait(lambda: 0 == task_queue.get_consumer_count())
        pb_sync.publish(msg)
        pb_sync.publish(msg)
        pb_sync.publish(msg)
        # and we can count them
        assert sync_wait(lambda: 3 == task_queue.get_message_count())

    @pytest.mark.flaky(max_runs=3)
    def test_logger_body(self, backend) -> None:
        pb_sync.subscribe_logger(log_callback)
        topic = "acme.tests.TestMessage".replace(".", self.topic_delimiter)
        topic_result = "acme.tests.TestMessage.result".replace(".", self.topic_delimiter)

        pb_sync.publish(self.msg)
        assert sync_wait(lambda: isinstance(received["log"], str))
        assert (
            received["log"]
            == f'{topic}: {{"content": "test", "number": 123, "detail": null, "options": null, "color": "GREEN"}}'
        )
        received["log"] = None
        result = self.msg.make_result()
        pb_sync.publish_result(result)
        assert sync_wait(lambda: isinstance(received["log"], str))
        assert (
            received["log"]
            == f'{topic_result}: SUCCESS - {{"content": "test", "number": 123, "detail": null, "options": null, "color": "GREEN"}}'
        )
        result = self.msg.make_result(
            return_code=pb.results.ReturnCode.FAILURE, return_value={"test": "value"}
        )
        received["log"] = None
        pb_sync.publish_result(result)
        assert sync_wait(lambda: isinstance(received["log"], str))
        assert (
            received["log"]
            == f'{topic_result}: FAILURE - error: [] - {{"content": "test", "number": 123, "detail": null, "options": null, "color": "GREEN"}}'
        )

    @pytest.mark.flaky(max_runs=3)
    def test_logger_int64(self, backend) -> None:
        global received
        topic = "acme.tests.TestMessage".replace(".", self.topic_delimiter)
        topic_task = "acme.tests.tasks.TaskMessage".replace(".", self.topic_delimiter)
        log_msg: str | None = None

        def callback_log(message, body: str) -> None:
            nonlocal log_msg
            log_msg = log_callback(message, body)

        pb_sync.subscribe_logger(callback_log)
        pb_sync.publish(tests.TestMessage(number=63, content="test"))

        def predicate() -> bool:
            return log_msg is not None

        assert sync_wait(
            predicate,
        )

        assert (
            log_msg
            == f'{topic}: {{"content": "test", "number": 63, "detail": null, "options": null, "color": null}}'
        ), log_msg
        # Ensure that uint64/int64 values are not converted to strings in the LoggerQueue callbacks
        log_msg = None
        pb_sync.publish(
            tests.tasks.TaskMessage(
                content="test", bbox=[1, 2, 3, 4], weights=[1.0, 2.0, -100, -20]
            )
        )
        assert sync_wait(predicate)
        assert isinstance(log_msg, str)
        assert (
            log_msg
            == f'{topic_task}: {{"content": "test", "weights": [1.0, 2.0, -100.0, -20.0], "bbox": [1, 2, 3, 4], "options": null}}'
        )

    @pytest.mark.flaky(max_runs=3)
    def test_unsubscribe(self, backend) -> None:
        global received
        pb_sync.subscribe(tests.TestMessage, callback_sync)
        pb_sync.publish(self.msg)
        assert sync_wait(lambda: received["message"] is not None)
        assert received["message"] == self.msg
        received["message"] = None
        pb_sync.unsubscribe(tests.TestMessage, if_unused=False, if_empty=False)
        pb_sync.publish(self.msg)
        assert received["message"] is None

        # unsubscribe from a package-level topic
        pb_sync.subscribe(tests, callback_sync)
        pb_sync.publish(tests.TestMessage(number=63, content="test"))
        assert sync_wait(lambda: received["message"] is not None)
        received["message"] = None
        pb_sync.unsubscribe(tests, if_unused=False, if_empty=False)
        pb_sync.publish(self.msg)
        assert received["message"] is None

        # subscribe/unsubscribe two callbacks for two topics
        received2 = None

        def callback_2(m: "ProtoBunnyMessage") -> None:
            nonlocal received2
            received2 = m

        pb_sync.subscribe(tests.TestMessage, callback_sync)
        pb_sync.subscribe(tests, callback_2)
        pb_sync.publish(self.msg)  # this will reach callback_2 as well
        assert sync_wait(lambda: received["message"] and received2)
        assert received["message"] == received2 == self.msg
        pb_sync.unsubscribe_all()
        received["message"] = None
        received2 = None
        pb_sync.publish(self.msg)
        assert received["message"] is None
        assert received2 is None

    @pytest.mark.flaky(max_runs=3)
    def test_unsubscribe_results(self, backend) -> None:
        received_result: pb.results.Result | None = None

        def callback_2(_: tests.TestMessage) -> None:
            # The receiver catches the error in callback and will send a Result.FAILURE message
            # to the result topic
            raise RuntimeError("error in callback")

        def callback_results_2(m: pb.results.Result) -> None:
            nonlocal received_result
            received_result = m

        pb_sync.unsubscribe_all()
        pb_sync.subscribe(tests.TestMessage, callback_2)
        # subscribe to the result topic
        pb_sync.subscribe_results(tests.TestMessage, callback_results_2)
        msg = tests.TestMessage(number=63, content="test")
        pb_sync.publish(msg)
        assert sync_wait(lambda: received_result is not None)
        assert received_result.source == msg
        assert received_result.return_code == pb.results.ReturnCode.FAILURE
        pb_sync.unsubscribe_results(tests.TestMessage)
        received_result = None
        pb_sync.publish(msg)
        assert received_result is None

    @pytest.mark.flaky(max_runs=3)
    def test_unsubscribe_all(self, backend) -> None:
        received_message: tests.tasks.TaskMessage | None = None
        received_result: pb.results.Result | None = None

        def callback_1(_: tests.TestMessage) -> None:
            # The receiver catches the error in callback and will send a Result.FAILURE message
            # to the result topic
            raise RuntimeError("error in callback")

        def callback_2(m: tests.tasks.TaskMessage) -> None:
            nonlocal received_message
            received_message = m

        def callback_results_2(m: pb.results.Result) -> None:
            nonlocal received_result
            received_result = m

        pb_sync.unsubscribe_all()
        q1 = pb_sync.subscribe(tests.TestMessage, callback_1)
        q2 = pb_sync.subscribe(tests.tasks.TaskMessage, callback_2)
        assert q1.topic == "acme.tests.TestMessage".replace(".", self.topic_delimiter)
        assert q2.topic == "acme.tests.tasks.TaskMessage".replace(".", self.topic_delimiter)
        assert q1.subscription is not None
        assert q2.subscription is not None
        # subscribe to a result topic
        pb_sync.subscribe_results(tests.TestMessage, callback_results_2)
        pb_sync.publish(tests.TestMessage(number=2, content="test"))
        pb_sync.publish(tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4]))
        assert sync_wait(lambda: received_message is not None)
        assert sync_wait(lambda: received_result is not None)
        assert received_result.source == tests.TestMessage(number=2, content="test")

        pb_sync.unsubscribe_all()
        received_result = None
        received_message = None
        pb_sync.publish(tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4]))
        pb_sync.publish(tests.TestMessage(number=2, content="test"))
        assert received_message is None
        assert received_result is None
