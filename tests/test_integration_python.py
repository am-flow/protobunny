"""
Integration tests for asynchronous Python multiprocessing Queue.

This module contains integration tests for verifying the functionality
of asynchronous message queues, using the Protobunny framework. These
tests cover message publishing, subscribing, logging, and message
conversion to dictionaries and JSON.


Classes:
    TestIntegrationAsync: Represents the suite of integration tests for
    asynchronous queue connections and messaging functionalities.

Dependencies:
    - asyncio
    - logging
    - typing
    - betterproto
    - pytest
    - pytest_mock
    - waiting
    - protobunny
    - custom utils and test modules from the same package
"""


import asyncio
import logging
import typing as tp

import betterproto
import pytest
from pytest_mock import MockerFixture
from waiting import wait

import protobunny as pb
from protobunny import get_queue
from protobunny.backends import LoggingSyncQueue
from protobunny.backends.python.connection import (
    Connection,
    SyncConnection,
)
from protobunny.backends.python.queues import SyncQueue
from protobunny.models import Envelope, ProtoBunnyMessage

from . import tests
from .utils import async_wait, tear_down

logging.basicConfig(level=logging.DEBUG)


@pytest.mark.integration
@pytest.mark.asyncio
class TestIntegration:
    """Integration tests for python multiprocessing Queue"""

    received = None
    received2 = None
    log_msg = None
    msg = tests.TestMessage(content="test", number=123, color=tests.Color.GREEN)

    @pytest.fixture(autouse=True)
    async def setup_connections(self, mocker: MockerFixture):
        from protobunny.backends import configuration
        from protobunny.backends import python as python_backend

        configuration.mode = "async"
        configuration.backend = "python"
        original_connection_sync = pb.get_connection
        pb.backend = python_backend
        mocker.patch.object(pb, "get_connection", python_backend.connection.get_connection)
        mocker.patch.object(pb, "disconnect", python_backend.connection.disconnect)
        mocker.patch.object(pb.base.configuration, "backend", "python")
        conn = await pb.get_connection()
        assert isinstance(conn, Connection)
        await pb.unsubscribe_all()
        queue = get_queue(self.msg)
        assert isinstance(queue, python_backend.queues.AsyncQueue)
        self.received = None
        self.received2 = None
        self.log_msg = None
        yield
        await pb.disconnect()
        pb.get_connection_sync = original_connection_sync
        # Manually clear the singleton registry
        # to prevent loop leakage between tests
        Connection._instances_by_vhost.clear()
        # cancel pending tasks to avoid warnings in output
        event_loop = asyncio.get_running_loop()
        await tear_down(event_loop)

    async def callback(self, msg: "ProtoBunnyMessage") -> tp.Any:
        self.received = msg

    async def callback2(self, msg: "ProtoBunnyMessage") -> tp.Any:
        self.received2 = msg

    def log_callback(self, message: Envelope, body: str) -> None:
        corr_id = message.correlation_id
        log_msg = (
            f"{message.routing_key}(cid:{corr_id}): {body}"
            if corr_id
            else f"{message.routing_key}: {body}"
        )
        self.log_msg = log_msg

    async def test_publish(self) -> None:
        await pb.subscribe(tests.TestMessage, self.callback)
        await pb.publish(self.msg)

        async def predicate() -> bool:
            return self.received == self.msg

        assert await async_wait(predicate, timeout_seconds=1, sleep_seconds=0.1)
        assert self.received.number == self.msg.number

    async def test_to_dict(self) -> None:
        await pb.subscribe(tests.TestMessage, self.callback)
        assert self.received is None
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
        queue = await pb.subscribe(tests.tasks.TaskMessage, self.callback)
        msg = tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4])
        # we subscribe to create the queue in RabbitMQ
        await queue.purge()  # remove past messages
        # we unsubscribe so the published messages
        # won't be consumed and stay in the queue
        await queue.unsubscribe(if_unused=False, if_empty=False)

        async def predicate() -> bool:
            return await queue.get_message_count() == 0

        assert await async_wait(predicate, timeout_seconds=1, sleep_seconds=0.1)

        await pb.publish(msg)
        await pb.publish(msg)
        await pb.publish(msg)

        # and we can count them
        async def predicate() -> bool:
            return await queue.get_message_count() == 0

        assert await async_wait(predicate, timeout_seconds=1, sleep_seconds=0.1)
        await queue.purge()

    async def test_logger_body(self) -> None:
        await pb.subscribe_logger(self.log_callback)
        await pb.publish(self.msg)

        async def predicate() -> bool:
            return self.log_msg is not None

        assert await async_wait(
            predicate, timeout_seconds=1, sleep_seconds=0.1
        ), f"log msg not set {self.log_msg}"
        assert (
            self.log_msg
            == 'acme.tests.TestMessage: {"content": "test", "number": 123, "detail": null, "options": null, "color": "GREEN"}'
        )
        self.log_msg = None
        result = self.msg.make_result()
        await pb.publish_result(result)
        assert await async_wait(predicate, timeout_seconds=1, sleep_seconds=0.1)
        assert (
            self.log_msg
            == 'acme.tests.TestMessage.result: SUCCESS - {"content": "test", "number": 123, "detail": null, "options": null, "color": "GREEN"}'
        )
        result = self.msg.make_result(
            return_code=pb.results.ReturnCode.FAILURE, return_value={"test": "value"}
        )
        self.log_msg = None
        await pb.publish_result(result)
        assert await async_wait(predicate, timeout_seconds=1, sleep_seconds=0.1)
        assert (
            self.log_msg
            == 'acme.tests.TestMessage.result: FAILURE - error: [] - {"content": "test", "number": 123, "detail": null, "options": null, "color": "GREEN"}'
        )

    async def test_logger_int64(self) -> None:
        # Ensure that uint64/int64 values are not converted to strings in the logger
        await pb.subscribe_logger(self.log_callback)
        await pb.publish(
            tests.tasks.TaskMessage(
                content="test", bbox=[1, 2, 3, 4], weights=[1.0, 2.0, -100, -20]
            )
        )

        async def predicate() -> bool:
            return isinstance(self.log_msg, str)

        assert await async_wait(predicate, timeout_seconds=1, sleep_seconds=0.1)
        assert isinstance(self.log_msg, str)
        assert (
            self.log_msg
            == 'acme.tests.tasks.TaskMessage: {"content": "test", "weights": [1.0, 2.0, -100.0, -20.0], "bbox": [1, 2, 3, 4], "options": null}'
        )
        self.log_msg = None
        await pb.publish(tests.TestMessage(number=63, content="test"))
        assert await async_wait(predicate, timeout_seconds=1, sleep_seconds=0.1)
        assert (
            self.log_msg
            == 'acme.tests.TestMessage: {"content": "test", "number": 63, "detail": null, "options": null, "color": null}'
        )

    async def test_unsubscribe(self) -> None:
        await pb.subscribe(tests.TestMessage, self.callback)
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
        await pb.publish(tests.TestMessage(number=123, content="test"))
        assert await async_wait(predicate, timeout_seconds=1, sleep_seconds=0.1)

        await pb.unsubscribe(tests, if_unused=False, if_empty=False)
        await asyncio.sleep(0.1)
        self.received = None
        await pb.publish(self.msg)
        assert self.received is None

        # subscribe/unsubscribe two callbacks for two topics

        await pb.subscribe(tests.TestMessage, self.callback)
        await pb.subscribe(tests, self.callback2)
        assert self.received is None
        await pb.publish(self.msg)  # this will reach callback_2 as well

        async def predicate2() -> bool:
            return self.received2 is not None

        assert await async_wait(predicate, timeout_seconds=1, sleep_seconds=0.5)
        assert await async_wait(predicate2, timeout_seconds=1, sleep_seconds=0.5)
        assert self.received == self.msg
        assert self.received2 == self.msg
        await pb.unsubscribe(tests, if_unused=False, if_empty=False)
        await pb.unsubscribe(tests.TestMessage, if_unused=False, if_empty=False)
        self.received = None
        self.received2 = None
        await pb.publish(self.msg)
        assert self.received is None

    async def test_unsubscribe_results(self) -> None:
        received_result: pb.results.Result | None = None

        async def callback(_: tests.TestMessage) -> None:
            # The receiver catches the error in callback and will send a Result.FAILURE message
            # to the result topic
            raise RuntimeError("error in callback")

        async def callback_results(m: pb.results.Result) -> None:
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

        assert await async_wait(predicate, timeout_seconds=1, sleep_seconds=0.1)
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
    """Integration tests for python multiprocessing Queue"""

    received = None
    log_msg = None
    msg = tests.TestMessage(content="test", number=123, color=tests.Color.GREEN)

    @pytest.fixture(autouse=True)
    def setup_connections(self, mocker: MockerFixture):
        from protobunny.backends import configuration
        from protobunny.backends import python as python_backend

        configuration.mode = "sync"
        configuration.backend = "python"
        pb.backend = python_backend
        mocker.patch("protobunny.backends.get_backend", return_value=python_backend)
        mocker.patch("protobunny.base.get_backend", return_value=python_backend)
        mocker.patch.object(
            pb, "get_connection_sync", python_backend.connection.get_connection_sync
        )
        mocker.patch.object(pb, "disconnect_sync", python_backend.connection.disconnect_sync)
        conn = pb.get_connection_sync()
        queue = get_queue(tests.TestMessage)
        assert isinstance(queue, python_backend.queues.SyncQueue)
        assert isinstance(conn, SyncConnection)
        pb.unsubscribe_all_sync()
        yield
        pb.disconnect_sync()
        SyncConnection._instances_by_vhost.clear()

    def callback(self, msg: "ProtoBunnyMessage") -> tp.Any:
        self.received = msg

    def log_callback(self, message: Envelope, body: str) -> None:
        corr_id = message.correlation_id
        log_msg = (
            f"{message.routing_key}(cid:{corr_id}): {body}"
            if corr_id
            else f"{message.routing_key}: {body}"
        )
        self.log_msg = log_msg

    def test_publish(self) -> None:
        pb.subscribe_sync(tests.TestMessage, self.callback)
        pb.publish_sync(self.msg)
        assert wait(lambda: self.received == self.msg, timeout_seconds=1, sleep_seconds=0.1)
        assert self.received.number == self.msg.number

    def test_to_dict(self) -> None:
        pb.subscribe_sync(tests.TestMessage, self.callback)
        q = pb.subscribe_sync(tests.tasks.TaskMessage, self.callback)
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
        q.unsubscribe(if_unused=False, if_empty=False)

    def test_count_messages(self) -> None:
        queue = pb.subscribe_sync(tests.tasks.TaskMessage, self.callback)
        msg = tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4])
        # we subscribe to create the queue in RabbitMQ
        queue.purge()  # remove past messages
        # we unsubscribe so the published messages
        # won't be consumed and stay in the queue
        queue.unsubscribe()
        assert wait(lambda: 0 == queue.get_consumer_count(), timeout_seconds=1, sleep_seconds=0.1)
        pb.publish_sync(msg)
        pb.publish_sync(msg)
        pb.publish_sync(msg)
        # and we can count them
        assert wait(lambda: 3 == queue.get_message_count(), timeout_seconds=1, sleep_seconds=0.1)
        queue.purge()

    def test_logger_body(self) -> None:
        log_queue = pb.subscribe_logger_sync(self.log_callback)
        assert isinstance(log_queue, LoggingSyncQueue)
        assert isinstance(log_queue.get_connection_sync(), SyncConnection)
        assert log_queue.topic == "acme.#"
        pb.publish_sync(self.msg)
        assert wait(lambda: self.log_msg is not None, timeout_seconds=1, sleep_seconds=0.1)
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
        # Ensure that uint64/int64 values are not converted to strings in the logger
        pb.subscribe_logger_sync(self.log_callback)
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

        # pb.unsubscribe_all_sync()
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

        q1 = pb.subscribe_sync(tests.TestMessage, callback_1)
        q2 = pb.subscribe_sync(tests.tasks.TaskMessage, callback_2)
        # subscribe to a result topic too, to receive the result error message for the callback_1
        pb.subscribe_results_sync(tests.TestMessage, callback_results)

        assert isinstance(q1, SyncQueue) and isinstance(q2, SyncQueue)
        assert q1.topic == "acme.tests.TestMessage"
        assert q2.topic == "acme.tests.tasks.TaskMessage"
        assert q1.subscription is not None
        assert q2.subscription is not None

        pb.publish_sync(tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4]))
        assert wait(lambda: received_message is not None, timeout_seconds=1, sleep_seconds=0.1)
        pb.publish_sync(tests.TestMessage(number=2, content="test"))
        assert wait(lambda: received_result is not None, timeout_seconds=1, sleep_seconds=0.1)
        assert received_result.source == tests.TestMessage(number=2, content="test")

        pb.unsubscribe_all_sync()
        received_result = None
        received_message = None
        pb.publish_sync(tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4]))
        pb.publish_sync(tests.TestMessage(number=2, content="test"))
        assert received_message is None
        assert received_result is None
