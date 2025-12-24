import asyncio
import time
from unittest.mock import ANY

from aio_pika import Message

from protobunny.models import Envelope


async def async_wait(condition_func, timeout_seconds=1.0, sleep_seconds=0.1) -> bool:
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        res = await condition_func()
        if res:
            return True
        await asyncio.sleep(sleep_seconds)  # This yields control to the loop!
    return False


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


def assert_backend_publish(backend, mock_connection, backend_msg, topic, count_in_queue: int = 1):
    backend_name = backend.__name__.split(".")[-1]
    mock = mock_connection[backend.__name__.split(".")[-1]]
    if backend_name == "rabbitmq":
        mock["exchange"].publish.assert_awaited_with(
            backend_msg, routing_key=topic, mandatory=True, immediate=False
        )
    elif backend_name == "redis":
        mock.xadd.assert_awaited_with(
            name="protobunny:test.routing.key",
            fields=dict(
                body=backend_msg.body, correlation_id=backend_msg.correlation_id, topic=topic
            ),
            maxlen=1000,
        )
    elif backend_name == "python":
        assert mock._exclusive_queues[topic] is not None
        assert (
            mock.get_message_count(topic) == count_in_queue
        ), f"count was {mock.get_message_count(topic)}"


def assert_backend_setup_queue(backend, mock_connection, topic: str, shared: bool) -> None:
    backend_name = backend.__name__.split(".")[-1]
    mock = mock_connection[backend.__name__.split(".")[-1]]
    if backend_name == "rabbitmq":
        mock["channel"].declare_queue.assert_called_with(
            topic, exclusive=not shared, durable=True, auto_delete=False, arguments=ANY
        )
    elif backend_name == "redis":
        mock.xgroup_create.assert_awaited_with(
            name=f"protobunny:{topic}",
            groupname="shared_group" if shared else ANY,
            id="0" if shared else "$",
            mkstream=True,
        )
    elif backend_name == "python":
        if not shared:
            assert len(mock._exclusive_queues.get(topic)) == 1
        else:
            assert mock._shared_queues.get(topic)


def assert_backend_connection(backend, mock_connection):
    backend_name = backend.__name__.split(".")[-1]
    mock = mock_connection[backend_name]
    if backend_name == "rabbitmq":
        # Verify aio_pika calls
        mock["connect"].assert_awaited_once()
        assert mock["channel"].set_qos.called
        # Check if main and DLX exchanges were declared
        assert mock["channel"].declare_exchange.call_count == 2
    elif backend_name == "redis":
        mock.ping.assert_awaited_once()
