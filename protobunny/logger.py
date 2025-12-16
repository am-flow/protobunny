"""
Example of usage for `amlogic_messages.subscribe_logger`
to provide a logging tool
that prints all messages to stdout.
"""

import argparse
import logging
import re
import signal
import textwrap
from functools import partial
from types import FrameType

import aio_pika

import protobunny as pb

log = logging.getLogger(__name__)


def log_callback(max_length, regex, message: aio_pika.IncomingMessage, msg_content: str) -> None:
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
    parser.add_argument("-f", "--filter", type=str, help="filter messages matching this regex")
    parser.add_argument(
        "-l", "--max-length", type=int, default=60, help="cut off messages longer that this"
    )
    return parser


def cli():
    args = _get_parser().parse_args()
    regex = re.compile(args.filter)
    func = partial(log_callback, args.max_length, regex)

    # If subscribe_logger is called without arguments,
    # it uses a default logger callback
    pb.subscribe_logger(func)

    def _handler(signum: int, _: FrameType | None) -> None:
        log.info("Received signal %s, shutting down", signal.Signals(signum).name)
        pb.stop_connection()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
    signal.pause()


if __name__ == "__main__":
    cli()
