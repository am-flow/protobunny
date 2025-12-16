"""Implements a RabbitMQ Connection object using aio_pika."""
import asyncio
import functools
import logging
import threading
import typing as tp
import urllib
from concurrent.futures import ThreadPoolExecutor

import aio_pika
from aio_pika.abc import (
    AbstractRobustChannel,
    AbstractRobustConnection,
    AbstractRobustExchange,
    AbstractRobustQueue,
)

log = logging.getLogger(__name__)
__CONNECTION: "Connection | None" = None
__STOPPED = False


def is_connected() -> bool:
    return __CONNECTION is not None


def is_stopped() -> bool:
    """Check whether connection is stopped.

    Returns
    -------
    stopped : bool
        whether the connection is stopped
    """
    return __STOPPED


def set_stopped(stopped: bool):
    """Set whether connection is stopped.

    Returns
    -------
    stopped : bool
        whether the connection is stopped
    """
    global __STOPPED
    __STOPPED = stopped


def get_connection(vhost: str = "/") -> "Connection":
    """Get the singleton connection.

    Returns
    -------
    `Connection`
        the RabbitMQ connection
    """
    global __CONNECTION
    if is_stopped():
        raise RuntimeError("Trying to get connection but it was stopped")
    if __CONNECTION is None:
        __CONNECTION = Connection(vhost=vhost)
        __CONNECTION.init_connection()
    return __CONNECTION


def reset_connection(vhost: str = "/") -> "Connection":
    """Reset the singleton connection.

    Returns
    -------
    `Connection`
        the RabbitMQ connection
    """
    if not is_stopped():
        stop_connection()
    set_stopped(False)
    return get_connection(vhost=vhost)


def stop_connection() -> None:
    """Stop the singleton connection."""
    global __CONNECTION
    if __CONNECTION is None:
        return
    if __CONNECTION.is_alive():
        __CONNECTION.stop()
        __CONNECTION.join()
    del __CONNECTION
    __CONNECTION = None  # noqa
    set_stopped(True)


def run_in_loop(async_func):
    """Sync to async translator.

    Decorator that returns a wrapper to synchronously run a function in
    the async event loop and await its result.

    Args:
        async_func: the asynchronous function to wrap

    Returns: sync to async wrapper function

    """

    @functools.wraps(async_func)
    def wrapper(cls, *args, **kwargs):
        in_loop = threading.current_thread().name == "pika_connection"
        assert not in_loop, "Should not be called from loop thread"
        func = async_func(cls, *args, **kwargs)
        return asyncio.run_coroutine_threadsafe(func, cls.loop).result()

    return wrapper


class RequeueMessage(Exception):
    """Raise when a message could not be handled but should be requeued."""

    ...


class Connection(threading.Thread):
    """RabbitMQ Connection wrapper."""

    def __init__(
        self,
        username: str = "guest",
        password: str = "guest",
        ip: str = "127.0.0.1",
        worker_threads: int = 2,
        prefetch_count: int = 1,
        requeue_delay: int = 3,
        vhost: str = "/",
    ):
        """Initialize RabbitMQ connection.

        Args:
            worker_threads: number of concurrent callback workers to use
            prefetch_count: how many messages to prefetch from the queue
            requeue_delay: how long to wait before re-queueing a message
            vhost: RabbitMQ virtual host
        """
        super().__init__(name="pika_connection")
        vhost = urllib.parse.quote(vhost)
        self._url = f"amqp://{username}:{password}@{ip}:5672/{vhost}?heartbeat=1200&timeout=1500&fail_fast=no"
        self._exchange_name = "amq.topic"
        self._dl_exchange = "amvision-dlx"
        self._dl_queue = "amvision-dlq"
        self._exchange: AbstractRobustExchange | None = None
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractRobustChannel | None = None
        self.prefetch_count = prefetch_count
        self.requeue_delay = requeue_delay
        self.queues: dict[str, AbstractRobustQueue] = {}
        self.consumers: dict[str, str] = {}
        self.connection_ready = threading.Event()
        self.daemon = True
        self.executor = ThreadPoolExecutor(max_workers=worker_threads)
        self.loop: asyncio.AbstractEventLoop | None = None
        self.stopped: asyncio.Event | None = None

    def init_connection(self):
        # Start connection thread
        # and wait for connection to be ready
        self.start()
        self.connection_ready.wait()

    @property
    def connection(self) -> AbstractRobustConnection:
        if not self._connection:
            raise ValueError("Connection not initialized. Call connect first.")
        return self._connection

    @property
    def channel(self) -> AbstractRobustChannel:
        if not self._channel:
            raise ValueError("Channel not initialized. Call connect first.")
        return self._channel

    @property
    def exchange(self) -> AbstractRobustExchange:
        if not self._exchange:
            raise ValueError("Exchange not initialized. Call connect first.")
        return self._exchange

    async def connect(self):
        """
        Establish RabbitMQ connection.
        """
        self._connection = await aio_pika.connect_robust(self._url)
        self._channel = await self.connection.channel()
        await self._channel.set_qos(prefetch_count=self.prefetch_count)
        self._exchange = await self._channel.declare_exchange(
            self._exchange_name, "topic", durable=True, auto_delete=False
        )
        await self._channel.declare_exchange(
            self._dl_exchange, "fanout", durable=True, auto_delete=False
        )
        dlq = await self._channel.declare_queue(
            self._dl_queue, exclusive=False, durable=True, auto_delete=False
        )
        await dlq.bind(self._dl_exchange)
        self.connection_ready.set()

    async def setup_queue(self, topic: str, shared: bool = False) -> AbstractRobustQueue:
        """
        Set up a RabbitMQ queue.

        Args:
            topic: the queue/mqtt topic
            shared: if set, all clients share the same queue and receive messages
            a la round robin. Use for task queues.
            if not set, each client has its own anonymous queue and they
            all get copies of each message. Use for pub/sub queues.

        Returns: the setup queue

        """
        queue_name = topic if shared else ""
        log.debug("Setting up queue for topic %s", topic)
        if shared and queue_name in self.queues:
            return self.queues[queue_name]
        args = {"x-dead-letter-exchange": self._dl_exchange}
        queue = await self.channel.declare_queue(
            queue_name,
            exclusive=not shared,
            durable=shared,
            auto_delete=not shared,
            arguments=args,
        )
        await queue.bind(self.exchange, topic)
        self.queues[queue.name] = queue
        return queue

    @run_in_loop
    async def publish(self, topic: str, message: aio_pika.Message) -> None:
        """
        Publish a message to a queue.

        Args:
            topic:
            message:
        """
        log.debug("Publishing msg to topic %s", topic)
        await self.exchange.publish(message, topic)

    async def on_message(self, _: str, callback: tp.Callable, message: aio_pika.IncomingMessage):
        """Handle incoming queue messages.

        Args:
            callback:
            message:
            _: unused injected topic argument
        """
        try:
            # run the callback in a thread
            await asyncio.get_event_loop().run_in_executor(self.executor, callback, message)
            await message.ack()
        except RequeueMessage:
            log.exception("Requeuing failed message %s", message.body)
            await message.reject(requeue=True)
            await asyncio.sleep(self.requeue_delay)

    @run_in_loop
    async def subscribe(self, topic: str, callback: tp.Callable, shared: bool = False) -> str:
        """
        Subscribe to a queue.

        Args:
            topic: mqtt/queue topic
            callback: the function that will handle incoming messages
            shared: if set, all clients share the same queue and receive messages
                a la round robin. Use for task queues.
                if not set, each client has its own anonymous queue and they
                all get copies of each message. Use for pub/sub queues.

        Returns:
            tag: subscription identifier that will be needed to unsubscribe later.
        """

        queue = await self.setup_queue(topic, shared)
        log.info("Subscribing to %s", topic)
        func = functools.partial(self.on_message, topic, callback)
        tag = await queue.consume(func)
        self.consumers[tag] = queue.name
        return tag

    @run_in_loop
    async def purge(self, topic: str) -> None:
        """
        Empty a queue.

        Args:
            topic: mqtt/queue topic
        """
        log.debug("Purging topic %s", topic)
        await self.setup_queue(topic, True)
        await self.queues[topic].purge()

    @run_in_loop
    async def get_message_count(self, topic: str) -> int | None:
        """
        Get queue message count.

        Args:
            topic: mqtt/queue topic

        Returns:
            message count
        """

        log.debug("Getting message count for topic %s", topic)
        queue = await self.channel.declare_queue(
            topic, exclusive=False, durable=True, auto_delete=False, passive=True
        )
        return queue.declaration_result.message_count

    @run_in_loop
    async def unsubscribe(self, tag):
        """
        Unsubscribe an earlier subscription.

        Args:
            tag: the subscription identifier returned from subscribe()
        """
        # TODO(Sem Mulder): We check if the consumer and queues are still there. After upgrading to
        #  pika 7.1.0, sometimes queues are already deleted before this function is called. For now
        #  we just check if the queue still exists, before unsubscribing. If you are reading this
        #  and are encountering issues, that might be worth a look.
        if tag not in self.consumers:
            return
        queue_name = self.consumers[tag]
        del self.consumers[tag]
        if queue_name not in self.queues:
            return
        queue = self.queues[queue_name]

        log.info("Unsubscribing from queue %s", queue.name)
        await queue.cancel(tag)
        if queue.exclusive:
            self.queues.pop(queue.name)
            await queue.delete()

    @run_in_loop
    async def stop(self):
        """Stop the connection."""
        self.stopped.set()

    async def cleanup(self):
        """Clean up connection after stopping."""
        while self.consumers:
            tag, queue_name = self.consumers.popitem()
            queue = self.queues[queue_name]
            await queue.cancel(tag)
            if queue.exclusive:
                self.queues.pop(queue.name)
                await queue.delete()
        self.executor.shutdown(wait=True)
        await self.connection.close()

    def run(self):
        """Run the connection thread."""
        log.info("Starting connection thread")
        asyncio.run(self._run())

    async def _run(self) -> None:
        self.loop = asyncio.get_event_loop()
        self.stopped = asyncio.Event()
        await self.connect()
        await self.stopped.wait()
        log.info("Stopping RabbitMQ connection")
        await self.cleanup()
        log.info("RabbitMQ Connection closed")
