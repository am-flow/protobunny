import asyncio
import logging
import time
import typing as tp

import pytest

import protobunny as pb_sync
from protobunny import asyncio as pb
from protobunny.asyncio.backends import python as python_backend_aio
from protobunny.asyncio.backends import rabbitmq as rabbitmq_backend_aio
from protobunny.asyncio.backends import redis as redis_backend_aio
from protobunny.backends import python as python_backend
from protobunny.backends import rabbitmq as rabbitmq_backend
from protobunny.backends import redis as redis_backend
from protobunny.config import backend_configs
from protobunny.models import ProtoBunnyMessage

from . import tests
from .utils import async_wait, sync_wait

log = logging.getLogger(__name__)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("backend", [rabbitmq_backend_aio, redis_backend_aio, python_backend_aio])
class TestTasks:
    msg = tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4])
    received = {
        "task_1": None,
        "task_2": None,
    }

    @pytest.fixture(autouse=True)
    async def setup_test_env(self, mocker, test_config, backend) -> tp.AsyncGenerator[None, None]:
        backend_name = backend.__name__.split(".")[-1]
        test_config.mode = "async"
        test_config.backend = backend_name
        test_config.backend_config = backend_configs[backend_name]
        self.topic_delimiter = test_config.backend_config.topic_delimiter
        # Patch global configuration for all modules that use it
        mocker.patch.object(pb_sync.config, "default_configuration", test_config)
        mocker.patch.object(pb_sync.models, "default_configuration", test_config)

        mocker.patch.object(pb_sync.helpers, "default_configuration", test_config)
        mocker.patch.object(pb.backends, "default_configuration", test_config)

        if hasattr(backend.connection, "default_configuration"):
            mocker.patch.object(backend.connection, "default_configuration", test_config)
        if hasattr(backend.queues, "default_configuration"):
            mocker.patch.object(backend.queues, "default_configuration", test_config)

        pb.backend = backend
        mocker.patch("protobunny.helpers.get_backend", return_value=backend)
        mocker.patch.object(pb, "get_connection", backend.connection.get_connection)
        mocker.patch.object(pb, "disconnect", backend.connection.disconnect)
        mocker.patch.object(pb, "get_backend", return_value=backend)

        # Assert the patching is working for setting the backend
        connection = await pb.get_connection()
        assert isinstance(connection, backend.connection.Connection)
        queue = pb.get_queue(self.msg)
        assert queue.topic == "acme.tests.tasks.TaskMessage".replace(
            ".", test_config.backend_config.topic_delimiter
        )
        assert isinstance(queue, backend.queues.AsyncQueue)
        assert isinstance(await queue.get_connection(), backend.connection.Connection)
        await queue.purge(reset_groups=True)
        # start without pending subscriptions
        await pb.unsubscribe_all(if_unused=False, if_empty=False)
        # reset the variables holding the messages received
        self.received = {}
        yield

        await connection.disconnect()
        backend.connection.Connection.instance_by_vhost.clear()

    async def test_tasks(self, backend) -> None:
        async def predicate_1() -> bool:
            return self.received.get("task_1") is not None

        async def predicate_2() -> bool:
            return self.received.get("task_2") is not None

        async def callback_task_1(msg: "ProtoBunnyMessage") -> None:
            log.debug("CALLBACK TASK 1 %s", msg)
            await asyncio.sleep(0.1)  # simulate some work
            self.received["task_1"] = msg

        async def callback_task_2(msg: "ProtoBunnyMessage") -> None:
            log.debug("CALLBACK TASK 2 %s", msg)
            await asyncio.sleep(0.1)  # simulate some work
            self.received["task_2"] = msg

        await pb.subscribe(tests.tasks.TaskMessage, callback_task_1)
        await pb.subscribe(tests.tasks.TaskMessage, callback_task_2)
        await pb.publish(self.msg)
        assert await async_wait(predicate_1)

        assert self.received.get("task_2") is None
        self.received["task_1"] = None
        await pb.publish(self.msg)
        await pb.publish(self.msg)
        await pb.publish(self.msg)
        assert await async_wait(predicate_1, timeout=2, sleep=0.1)
        assert await async_wait(predicate_2)
        assert self.received["task_1"] == self.msg
        assert self.received["task_2"] == self.msg
        self.received["task_1"] = None
        self.received["task_2"] = None
        await pb.publish(self.msg)
        await pb.publish(self.msg)
        assert await async_wait(predicate_1)
        assert await async_wait(predicate_2)
        await pb.unsubscribe(tests.tasks.TaskMessage, if_unused=False, if_empty=False)


@pytest.mark.integration
@pytest.mark.parametrize("backend", [rabbitmq_backend, redis_backend, python_backend])
class TestTasksSync:
    msg = tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4])
    received = {
        "task_1": None,
        "task_2": None,
    }

    @pytest.fixture(autouse=True)
    def setup_test_env(self, mocker, test_config, backend) -> tp.Generator[None, None, None]:
        backend_name = backend.__name__.split(".")[-1]
        test_config.mode = "sync"
        test_config.backend = backend_name
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
        mocker.patch("protobunny.backends.get_backend", return_value=backend)
        mocker.patch("protobunny.helpers.get_backend", return_value=backend)
        mocker.patch.object(pb_sync, "get_connection", backend.connection.get_connection)
        mocker.patch.object(pb_sync, "disconnect", backend.connection.disconnect)

        # Assert the patching is working for setting the backend
        connection = pb_sync.get_connection()
        assert isinstance(connection, backend.connection.Connection)
        task_queue = pb_sync.get_queue(self.msg)
        assert task_queue.topic == "acme.tests.tasks.TaskMessage".replace(
            ".", test_config.backend_config.topic_delimiter
        )
        assert isinstance(task_queue, backend.queues.SyncQueue)
        assert isinstance(task_queue.get_connection(), backend.connection.Connection)
        task_queue.purge(reset_groups=True)
        # start without pending subscriptions
        pb_sync.unsubscribe_all(if_unused=False, if_empty=False)
        pb_sync.disconnect()
        # reset the variables holding the messages received
        self.received = {}

        yield

        pb_sync.disconnect()
        backend.connection.Connection.instance_by_vhost.clear()

    def test_tasks(self, backend) -> None:
        def predicate_1() -> bool:
            return self.received.get("task_1") is not None

        def predicate_2() -> bool:
            return self.received.get("task_2") is not None

        def callback_task_1(msg: "ProtoBunnyMessage") -> None:
            log.debug("SYNC CALLBACK TASK 1 %s", msg)
            time.sleep(0.1)
            self.received["task_1"] = msg

        def callback_task_2(msg: "ProtoBunnyMessage") -> None:
            log.debug("SYNC CALLBACK TASK 2 %s", msg)
            time.sleep(0.1)
            self.received["task_2"] = msg

        pb_sync.subscribe(tests.tasks.TaskMessage, callback_task_1)
        pb_sync.subscribe(tests.tasks.TaskMessage, callback_task_2)

        pb_sync.publish(self.msg)
        assert sync_wait(predicate_1)

        assert self.received.get("task_2") is None
        self.received["task_1"] = None
        pb_sync.publish(self.msg)
        pb_sync.publish(self.msg)
        pb_sync.publish(self.msg)
        assert sync_wait(predicate_1)
        assert sync_wait(predicate_2)
        assert self.received["task_1"] == self.msg
        assert self.received["task_2"] == self.msg
        self.received["task_1"] = None
        self.received["task_2"] = None
        pb_sync.publish(self.msg)
        pb_sync.publish(self.msg)
        assert sync_wait(predicate_1)
        assert sync_wait(predicate_2)
        self.received["task_2"] = None
        self.received["task_1"] = None
