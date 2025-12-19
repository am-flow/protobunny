"""Start the logging service with python -m protobunny.logger"""
import argparse
import asyncio
import logging
import re
import signal
import textwrap
from functools import partial
from types import FrameType

import aio_pika

import protobunny as pb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def log_callback(
    max_length: int, regex: re.Pattern[str], message: aio_pika.IncomingMessage, msg_content: str
) -> None:
    """Log messages to stdout.

    Args:
        max_length: max length to use in textwrap.shorten width parameter
        regex: regex to enable filtering on routing key
        message: the pika incoming message
        msg_content: the message content to log, generated in the LoggingQueue._receive
          method before calling this callback.
    """
    if not regex or regex.search(message.routing_key):
        msg_content = textwrap.shorten(msg_content, width=max_length)
        corr_id = message.correlation_id
        log_msg = (
            f"{message.routing_key}(cid:{corr_id}): {msg_content}"
            if corr_id
            else f"{message.routing_key}: {msg_content}"
        )
        log.info(log_msg)


def _get_parser():
    parser = argparse.ArgumentParser(description="MQTT Logger")
    parser.add_argument(
        "-f", "--filter", type=str, help="filter messages matching this regex", required=False
    )
    parser.add_argument(
        "-l", "--max-length", type=int, default=60, help="cut off messages longer than this"
    )
    parser.add_argument("-m", "--mode", type=str, default="async", help="Set async or sync mode.")
    parser.add_argument(
        "-p",
        "--prefix",
        type=str,
        required=False,
        help="Set the prefix for the logger if different from the configured messages-prefix",
    )
    return parser


def main_sync():
    # If subscribe_logger is called without arguments,
    # it uses a default logger callback
    queue = pb.subscribe_logger_sync(func, prefix)

    def _handler(signum: int, _: FrameType | None) -> None:
        log.info("Received signal %s, shutting down %s", signal.Signals(signum).name, str(queue))
        queue.unsubscribe(if_empty=False, if_unused=False)
        pb.disconnect_sync()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
    signal.pause()


async def main():
    # Setup a "stop event" to keep the loop running
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    queue = await pb.subscribe_logger(func, prefix)

    async def shutdown(signum):
        log.info("Received signal %s, shutting down %s", signal.Signals(signum).name, str(queue))
        await queue.unsubscribe(if_empty=False, if_unused=False)
        await pb.disconnect()
        stop_event.set()

    # Register handlers with the loop
    for sig in (signal.SIGINT, signal.SIGTERM):
        # Note: add_signal_handler requires a callback, so we use a lambda
        # to trigger a Task for the async shutdown
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(s)))

    log.info("Logger service started. Press Ctrl+C to exit.")
    # Wait here forever (non-blocking) until shutdown() is called
    await stop_event.wait()


if __name__ == "__main__":
    args = _get_parser().parse_args()
    filter_regex = re.compile(args.filter) if args.filter else None
    prefix = args.prefix
    func = partial(log_callback, args.max_length, filter_regex)
    if args.mode == "async":
        asyncio.run(main())
    else:
        main_sync()
