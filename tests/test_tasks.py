import asyncio
import importlib
import logging
import time
import typing as tp

import pytest

import protobunny as pb_sync
from protobunny import asyncio as pb
from protobunny import get_backend
from protobunny.conf import Config, backend_configs

from . import tests
from .utils import async_wait, sync_wait

log = logging.getLogger(__name__)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["rabbitmq", "redis", "python", "mosquitto", "nats"])
class TestTasks:
    msg = tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4])
    received = {
        "task_1": None,
        "task_2": None,
    }

    @pytest.fixture(autouse=True)
    async def setup_test_env(
        self, mocker, test_config: Config, backend: str
    ) -> tp.AsyncGenerator[None, None]:
        test_config.mode = "async"
        test_config.backend = backend
        test_config.backend_config = backend_configs[backend]
        self.topic_delimiter = test_config.backend_config.topic_delimiter
        # Patch global configuration for all modules that use it
        mocker.patch.object(pb_sync.conf, "config", test_config)
        mocker.patch.object(pb_sync.models, "config", test_config)
        mocker.patch.object(pb_sync.helpers, "config", test_config)
        mocker.patch.object(pb_sync.backends, "config", test_config)
        mocker.patch.object(pb.backends, "config", test_config)
        backend_module = get_backend()
        if hasattr(backend_module.connection, "config"):
            mocker.patch.object(backend_module.connection, "config", test_config)
        if hasattr(backend_module.queues, "config"):
            mocker.patch.object(backend_module.queues, "config", test_config)

        pb.backend = backend_module
        mocker.patch("protobunny.helpers.get_backend", return_value=backend_module)
        mocker.patch.object(pb, "get_backend", return_value=backend_module)

        # Assert the patching is working for setting the backend
        connection = await pb.connect()
        assert isinstance(connection, backend_module.connection.Connection)
        queue = pb.get_queue(self.msg)
        assert queue.topic == "acme.tests.tasks.TaskMessage".replace(
            ".", test_config.backend_config.topic_delimiter
        )
        assert isinstance(queue, backend_module.queues.AsyncQueue)
        assert isinstance(await queue.get_connection(), backend_module.connection.Connection)
        await queue.purge(reset_groups=True)
        # start without pending subscriptions
        await pb.unsubscribe_all(if_unused=False, if_empty=False)
        # reset the variables holding the messages received
        self.received = {}
        yield

        await connection.disconnect()
        backend_module.connection.Connection.instance_by_vhost.clear()

    async def test_tasks(self, backend) -> None:
        async def predicate_1() -> bool:
            return self.received.get("task_1") is not None

        async def predicate_2() -> bool:
            return self.received.get("task_2") is not None

        async def callback_task_1(msg: tests.tasks.TaskMessage) -> None:
            log.debug("CALLBACK TASK 1 %s", msg)
            await asyncio.sleep(0.1)  # simulate some work
            self.received["task_1"] = msg

        async def callback_task_2(msg: tests.tasks.TaskMessage) -> None:
            log.debug("CALLBACK TASK 2 %s", msg)
            await asyncio.sleep(0.1)  # simulate some work
            self.received["task_2"] = msg

        await pb.subscribe(tests.tasks.TaskMessage, callback_task_1)
        await pb.subscribe(tests.tasks.TaskMessage, callback_task_2)
        await pb.publish(self.msg)
        assert await async_wait(predicate_1) or await async_wait(predicate_2)
        assert self.received.get("task_2") is None or self.received.get("task_1") is None
        self.received["task_1"] = None
        self.received["task_2"] = None

        await pb.publish(self.msg)
        await pb.publish(self.msg)
        await pb.publish(self.msg)
        assert await async_wait(predicate_1) or await async_wait(predicate_2)
        assert self.received["task_1"] == self.msg or self.received["task_2"] == self.msg

        await pb.unsubscribe(tests.tasks.TaskMessage, if_unused=False, if_empty=False)
        self.received["task_1"] = None
        self.received["task_2"] = None
        await pb.publish(self.msg)
        await pb.publish(self.msg)
        assert self.received["task_1"] is None
        assert self.received["task_2"] is None


@pytest.mark.integration
@pytest.mark.parametrize("backend", ["rabbitmq", "redis", "python", "mosquitto", "nats"])
class TestTasksSync:
    msg = tests.tasks.TaskMessage(content="test", bbox=[1, 2, 3, 4])
    received = {
        "task_1": None,
        "task_2": None,
    }

    @pytest.fixture(autouse=True)
    def setup_test_env(
        self, mocker, test_config: Config, backend: str
    ) -> tp.Generator[None, None, None]:
        test_config.mode = "sync"
        test_config.backend = backend
        test_config.backend_config = backend_configs[backend]
        self.topic_delimiter = test_config.backend_config.topic_delimiter
        # Patch global configuration for all modules that use it
        mocker.patch.object(pb_sync.conf, "config", test_config)
        mocker.patch.object(pb_sync.models, "config", test_config)
        mocker.patch.object(pb_sync.backends, "config", test_config)
        mocker.patch.object(pb_sync.helpers, "config", test_config)
        mocker.patch.object(pb.backends, "config", test_config)

        backend_module = get_backend()
        async_backend_module = importlib.import_module(f"protobunny.asyncio.backends.{backend}")
        if hasattr(backend_module.connection, "config"):
            mocker.patch.object(backend_module.connection, "config", test_config)
        if hasattr(backend_module.queues, "config"):
            mocker.patch.object(backend_module.queues, "config", test_config)
        if hasattr(async_backend_module.connection, "config"):
            # The sync connection is often implemented as a wrapper of the relative async module.
            # Patch the config of the async module as well
            mocker.patch.object(async_backend_module.connection, "config", test_config)

        mocker.patch("protobunny.helpers.get_backend", return_value=backend_module)

        # Test the patching is working for setting the backend
        connection = pb_sync.connect()
        assert isinstance(connection, backend_module.connection.Connection)
        assert sync_wait(connection.is_connected)
        task_queue = pb_sync.get_queue(self.msg)
        assert task_queue.topic == "acme.tests.tasks.TaskMessage".replace(
            ".", test_config.backend_config.topic_delimiter
        )
        assert isinstance(task_queue, backend_module.queues.SyncQueue)
        assert isinstance(task_queue.get_connection(), backend_module.connection.Connection)
        task_queue.purge(reset_groups=True)
        # start without pending subscriptions
        pb_sync.unsubscribe_all(if_unused=False, if_empty=False)
        pb_sync.disconnect()
        # reset the variables holding the messages received
        self.received = {}
        yield

        pb_sync.disconnect()
        backend_module.connection.Connection.instance_by_vhost.clear()

    @pytest.mark.flaky(max_runs=3)
    def test_tasks(self, backend) -> None:
        def predicate_1() -> bool:
            return self.received.get("task_1") is not None

        def predicate_2() -> bool:
            return self.received.get("task_2") is not None

        def callback_task_1(msg: tests.tasks.TaskMessage) -> None:
            time.sleep(0.1)
            self.received["task_1"] = msg

        def callback_task_2(msg: tests.tasks.TaskMessage) -> None:
            time.sleep(0.1)
            self.received["task_2"] = msg

        pb_sync.subscribe(tests.tasks.TaskMessage, callback_task_1)
        pb_sync.subscribe(tests.tasks.TaskMessage, callback_task_2)

        pb_sync.publish(self.msg)
        assert sync_wait(predicate_1) or sync_wait(predicate_2)

        assert self.received.get("task_2") is None or self.received.get("task_1") is None
        self.received["task_1"] = None
        self.received["task_2"] = None
        pb_sync.publish(self.msg)
        pb_sync.publish(self.msg)
        pb_sync.publish(self.msg)
        assert sync_wait(predicate_1) or sync_wait(predicate_2)
        assert sync_wait(predicate_2) or sync_wait(predicate_1)
        assert self.received["task_1"] == self.msg
        assert self.received["task_2"] == self.msg
        self.received["task_1"] = None
        self.received["task_2"] = None
        pb_sync.publish(self.msg)
        assert sync_wait(predicate_1) or sync_wait(predicate_2)
