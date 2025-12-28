import asyncio
import time
from types import ModuleType
from unittest.mock import ANY, AsyncMock

from aio_pika import Message
from redis import asyncio as redis
from waiting import TimeoutExpired, wait

from protobunny.backends import BaseConnection
from protobunny.models import Envelope


async def async_wait(condition_func, timeout=1.0, sleep=0.1) -> bool:
    start_time = time.time()
    while time.time() - start_time < timeout:
        res = await condition_func()
        if res:
            return True
        await asyncio.sleep(sleep)  # This yields control to the loop!
    return False


def sync_wait(condition_func, timeout=1.0, sleep=0.1) -> bool:
    try:
        wait(condition_func, timeout_seconds=timeout, sleep_seconds=sleep)
    except TimeoutExpired:
        return False
    return True


async def tear_down(event_loop):
    # Collect all tasks and cancel those that are not 'done'.
    tasks = asyncio.all_tasks(event_loop)
    tasks = [t for t in tasks if not t.done()]
    for task in tasks:
        task.cancel()
    # Wait for all tasks to complete, ignoring any CancelledErrors
    try:
        await asyncio.wait(tasks)
    except asyncio.exceptions.CancelledError:
        pass


def incoming_message_factory(backend, body: bytes = b"Hello"):
    backend_name = backend.__name__.split(".")[-1]
    if backend_name == "rabbitmq":
        return Message(body=body)
    elif backend_name == "redis":
        return Envelope(body=body, correlation_id="123")
    return Envelope(body=body)


async def assert_backend_publish(
    backend: ModuleType,
    internal_mock: dict | redis.Redis | AsyncMock,
    mock_connection: "BaseConnection",
    backend_msg,
    topic,
    count_in_queue: int = 1,
    shared_queue: bool = False,
):
    backend_name = backend.__name__.split(".")[-1]
    match backend_name:
        case "rabbitmq":
            internal_mock["exchange"].publish.assert_awaited_with(
                backend_msg, routing_key=topic, mandatory=True, immediate=False
            )

        case "redis":
            key = mock_connection.build_topic_key(topic)
            if shared_queue:
                # Read the last message from the stream
                incoming = await internal_mock.xread({key: 0})

                topic, messages = incoming[0]

                msg_id, msg = messages[0]
                assert (
                    len(messages) == count_in_queue
                ), f"Expected {count_in_queue} message in stream {key}, got {len(messages)}"
                assert (
                    msg[b"body"] == backend_msg.body
                ), f"Expected body {backend_msg.body}, got {msg[b'body']}"

            else:
                # retest the flow here
                pubsub = internal_mock.pubsub()
                await pubsub.subscribe(key)
                await mock_connection.publish(topic, backend_msg)
                msg: dict[str, bytes] | None = None

                async def predicate():
                    nonlocal msg
                    msg = await pubsub.get_message(timeout=1, ignore_subscribe_messages=True)

                    return msg is not None

                assert await async_wait(predicate)

                assert msg is not None
                assert (
                    msg["channel"].decode() == key
                ), f"Expected {topic}, got {msg['channel'].decode()}"
                assert (
                    msg["data"] == backend_msg.body
                ), f"Expected {backend_msg.body}, got {msg['data']}"

        case "python":
            assert internal_mock._exclusive_queues[topic] is not None
            assert (
                await internal_mock.get_message_count(topic) == count_in_queue
            ), f"count was {await internal_mock.get_message_count(topic)}"

        case "mosquitto":
            internal_mock.publish.assert_awaited_once()


async def assert_backend_setup_queue(
    backend, internal_mock, topic: str, shared: bool, mock_connection
) -> None:
    backend_name = backend.__name__.split(".")[-1]
    if backend_name == "rabbitmq":
        internal_mock["channel"].declare_queue.assert_called_with(
            topic, exclusive=not shared, durable=True, auto_delete=False, arguments=ANY
        )
    elif backend_name == "redis":
        if shared:
            streams = await internal_mock.xinfo_groups(f"protobunny:{topic}")
            assert len(streams) == 1
            assert (
                streams[0]["name"].decode() == "shared_group"
            ), f"Expected 'shared_group', got '{streams[0]['name']}'"
        else:
            assert mock_connection.queues[topic] == {"is_shared": False}

    elif backend_name == "python":
        if not shared:
            assert len(internal_mock._exclusive_queues.get(topic)) == 1
        else:
            assert internal_mock._shared_queues.get(topic)
    elif backend_name == "mosquitto":
        if not shared:
            assert mock_connection.queues[topic] == {
                "is_shared": False,
                "group_name": "",
                "sub_key": f"$share/shared_group/test/{topic}",
                "tag": ANY,
                "topic": topic,
                "topic_key": f"test/{topic}",
            }
        else:
            assert list(mock_connection.queues.values())[0] == {
                "is_shared": True,
                "group_name": "shared_group",
                "sub_key": f"$share/shared_group/protobunny/{topic}",
                "tag": ANY,
                "topic": topic,
                "topic_key": f"protobunny/{topic}",
            }, mock_connection.queues


async def assert_backend_connection(backend, internal_mock):
    backend_name = backend.__name__.split(".")[-1]
    if backend_name == "rabbitmq":
        # Verify aio_pika calls
        internal_mock["connect"].assert_awaited_once()
        assert internal_mock["channel"].set_qos.called
        # Check if main and DLX exchanges were declared
        assert internal_mock["channel"].declare_exchange.call_count == 2
    elif backend_name == "redis":
        assert await internal_mock.ping()
    elif backend_name == "mosquitto":
        internal_mock.__aenter__.assert_awaited_once()
    assert True


def get_mocked_connection(backend, redis_client, mock_aio_pika, mocker, mock_mosquitto):
    backend_name = backend.__name__.split(".")[-1]
    if backend_name == "redis":
        real_conn_with_fake_redis = backend.connection.Connection(url="redis://localhost:6379/0")
        assert (
            real_conn_with_fake_redis._exchange == "protobunny"
        ), real_conn_with_fake_redis._exchange
        real_conn_with_fake_redis._connection = redis_client

        def check_connected() -> bool:
            return real_conn_with_fake_redis._connection is not None

        # Patch is_connected logic
        mocker.patch.object(real_conn_with_fake_redis, "is_connected", side_effect=check_connected)

        return real_conn_with_fake_redis
    elif backend_name == "rabbitmq":
        real_conn_with_fake_aio_pika = backend.connection.Connection(
            url="amqp://guest:guest@localhost:5672/"
        )
        real_conn_with_fake_aio_pika._connection = mock_aio_pika["connection"]
        real_conn_with_fake_aio_pika.is_connected_event.set()
        real_conn_with_fake_aio_pika._channel = mock_aio_pika["channel"]
        real_conn_with_fake_aio_pika._exchange = mock_aio_pika["exchange"]
        real_conn_with_fake_aio_pika._queue = mock_aio_pika["queue"]

        return real_conn_with_fake_aio_pika
    elif backend_name == "python":
        python_conn = backend.connection.Connection()
        python_conn.is_connected_event.set()
        return python_conn
    elif backend_name == "mosquitto":
        real_conn_with_fake_aiomqtt = backend.connection.Connection()
        real_conn_with_fake_aiomqtt._connection = mock_mosquitto
        # real_conn_with_fake_aiomqtt.is_connected_event.set()
        return real_conn_with_fake_aiomqtt
