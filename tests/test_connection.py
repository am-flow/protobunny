from unittest.mock import AsyncMock, MagicMock, call

import pytest

import protobunny as pb
from protobunny.connection import Connection


class TestConnection:
    def test_get_connection(self, mock_connection_obj: MagicMock) -> None:
        conn = pb.get_connection()
        assert conn is mock_connection_obj
        conn.purge()
        mock_connection_obj.purge.assert_called_once()
        pb.stop_connection()
        with pytest.raises(RuntimeError) as exc:
            pb.get_connection()
        assert str(exc.value) == "Trying to get connection but it was stopped"

    def test_reset_connection(self, mock_connection_obj: MagicMock) -> None:
        conn = pb.reset_connection()
        assert conn is mock_connection_obj
        assert not pb.is_stopped()
        pb.stop_connection()
        assert pb.is_stopped()

    async def test_properties(self, mock_aiopika_connection: AsyncMock) -> None:
        conn = Connection()
        with pytest.raises(ValueError) as exc:
            _ = conn.connection
        assert str(exc.value) == "Connection not initialized. Call connect first."
        with pytest.raises(ValueError) as exc:
            _ = conn.channel
        assert str(exc.value) == "Channel not initialized. Call connect first."
        with pytest.raises(ValueError) as exc:
            _ = conn.exchange
        assert str(exc.value) == "Exchange not initialized. Call connect first."
        conn.init_connection()
        assert conn.connection is mock_aiopika_connection

    async def test_publish(self, mock_aiopika_connection: AsyncMock, pika_message) -> None:
        conn = Connection()
        conn.init_connection()
        msg = pika_message(bytes())
        conn.publish("test", msg)
        conn.exchange.publish.assert_called_with(msg, "test")

    def test_init(self, mock_connection_obj: MagicMock) -> None:
        conn = Connection()
        assert (
            conn._url
            == "amqp://guest:guest@127.0.0.1:5672//?heartbeat=1200&timeout=1500&fail_fast=no"
        )

    async def test_connect(self, mock_aiopika_connection: AsyncMock) -> None:
        conn = Connection()
        conn.init_connection()
        assert conn.connection is mock_aiopika_connection
        assert conn.connection_ready.is_set()
        conn.channel.declare_exchange.assert_has_calls(
            [
                call("amq.topic", "topic", durable=True, auto_delete=False),
                call("protobunny-dlx", "fanout", durable=True, auto_delete=False),
            ]
        )
        conn.channel.declare_queue.assert_called_once_with(
            "protobunny-dlq", exclusive=False, durable=True, auto_delete=False
        )
        conn.stop()
        assert conn.stopped.is_set()

    async def test_setup_queue(self, mock_aiopika_connection: AsyncMock) -> None:
        conn = Connection()
        conn.init_connection()
        await conn.setup_queue("test", shared=False)
        conn.channel.declare_queue.assert_called_with(
            "",
            exclusive=True,
            durable=False,
            auto_delete=True,
            arguments={"x-dead-letter-exchange": "protobunny-dlx"},
        )
        q2 = await conn.setup_queue("test", shared=True)
        conn.channel.declare_queue.assert_called_with(
            "test",
            exclusive=False,
            durable=True,
            auto_delete=False,
            arguments={"x-dead-letter-exchange": "protobunny-dlx"},
        )
        assert q2.name in conn.queues
