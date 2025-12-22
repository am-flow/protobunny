"""
Logging Service
---------------

A program for logging MQTT messages with optional message filtering and length truncation.

This program subscribes to an MQTT queue and logs incoming messages based on user-defined
parameters such as filtering by regex, truncating message content to a maximum length, and
setting logging mode (asynchronous or synchronous). Signal handling is provided to ensure
graceful shutdown.

Modules:
    log_callback: Logs incoming MQTT messages to stdout with optional filtering and truncation.
    main_sync: Entry point for synchronous logging mode with signal handling.
    main: Entry point for asynchronous logging mode with signal handling.

usage: python -m protobunny.logger [-h] [-f FILTER] [-l MAX_LENGTH] [-m MODE] [-p PREFIX]

MQTT Logger

options:
  -h, --help            show this help message and exit
  -f FILTER, --filter FILTER
                        filter messages matching this regex
  -l MAX_LENGTH, --max-length MAX_LENGTH
                        cut off messages longer than this
  -m MODE, --mode MODE  Set async or sync mode. Default is async.
  -p PREFIX, --prefix PREFIX
                        Set the prefix for the logger if different from the configured messages-prefix
"""
import argparse
import asyncio
import logging
import re
import signal
import textwrap
import typing as tp
from functools import partial
from types import FrameType

import protobunny as pb
from protobunny.models import IncomingMessageProtocol, LoggerCallback

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def log_callback(
    max_length: int,
    regex: re.Pattern[tp.Any] | None,
    message: IncomingMessageProtocol,
    msg_content: str,
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


def _get_parser() -> argparse.ArgumentParser:
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


def start_logger_sync(callback: "LoggerCallback", prefix: str) -> None:
    # If subscribe_logger is called without arguments,
    # it uses a default logger callback
    queue = pb.subscribe_logger_sync(callback, prefix)

    def _handler(signum: int, _: FrameType | None) -> None:
        log.info("Received signal %s, shutting down %s", signal.Signals(signum).name, str(queue))
        queue.unsubscribe(if_empty=False, if_unused=False)
        pb.disconnect_sync()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
    signal.pause()


async def start_logger(callback: "LoggerCallback", prefix: str) -> None:
    # Setup a "stop event" to keep the loop running
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    queue = await pb.subscribe_logger(callback, prefix)

    async def shutdown(signum: int) -> None:
        log.info("Received signal %s, shutting down %s", signal.Signals(signum).name, str(queue))
        await queue.unsubscribe(if_empty=False, if_unused=False)
        await pb.disconnect()
        stop_event.set()

    # Register handlers with the loop
    for sig in (signal.SIGINT, signal.SIGTERM):
        # Note: add_signal_handler requires a callback, so we use a lambda
        # to trigger a Task for the async shutdown
        def _handler(s: int) -> asyncio.Task[None]:
            return asyncio.create_task(shutdown(s))

        loop.add_signal_handler(sig, _handler, sig)

    log.info("Logger service started. Press Ctrl+C to exit.")
    # Wait here forever (non-blocking) until shutdown() is called
    await stop_event.wait()


if __name__ == "__main__":
    args = _get_parser().parse_args()
    filter_regex = re.compile(args.filter) if args.filter else None
    func = partial(log_callback, args.max_length, filter_regex)
    if args.mode == "async":
        asyncio.run(start_logger(func, args.prefix))
    else:
        start_logger_sync(func, args.prefix)
