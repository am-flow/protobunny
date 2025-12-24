"""
A module providing support for messaging and communication using RabbitMQ as the backend.

This module includes functionality for publishing, subscribing, and managing message queues,
as well as dynamically managing imports and configurations for RabbitMQ-based communication
logics. It enables both synchronous and asynchronous operations, while also supporting
connection resetting and management.

Modules and functionality are primarily imported from the core RabbitMQ backend, dynamically
generated package-specific configurations, and other base utilities. Exports are adjusted
as per the backend configuration.

"""

__all__ = [
    # from .backends
    "get_backend",
    # from .base
    "get_message_count",
    "get_queue",
    "publish",
    "publish_result",
    "subscribe",
    "subscribe_logger",
    "subscribe_logger_sync",
    "subscribe_results",
    "unsubscribe",
    "unsubscribe_all",
    "unsubscribe_results",
    "publish_sync",
    "publish_result_sync",
    "subscribe_sync",
    "subscribe_results_sync",
    "unsubscribe_sync",
    "unsubscribe_all_sync",
    "unsubscribe_results_sync",
    "get_message_count_sync",
    # from .core
    "commons",
    "results",
    # from .config
    "GENERATED_PACKAGE_NAME",
    "PACKAGE_NAME",
    "ROOT_GENERATED_PACKAGE_NAME",
    "configuration",
    # from .backends.rabbitmq
    "RequeueMessage",
    "ConnectionError",
    "reset_connection_sync",
    "reset_connection",
    "get_connection_sync",
    "get_connection",
    "disconnect",
    "disconnect_sync",
]

from importlib.metadata import version

from .backends import get_backend
from .base import (  # noqa
    get_backend,
    get_message_count,
    get_message_count_sync,
    get_queue,
    publish,
    publish_result,
    publish_result_sync,
    publish_sync,
    subscribe,
    subscribe_logger,
    subscribe_logger_sync,
    subscribe_results,
    subscribe_results_sync,
    subscribe_sync,
    unsubscribe,
    unsubscribe_all,
    unsubscribe_all_sync,
    unsubscribe_results,
    unsubscribe_results_sync,
    unsubscribe_sync,
)

#######################################################
from .config import (  # noqa
    GENERATED_PACKAGE_NAME,
    PACKAGE_NAME,
    ROOT_GENERATED_PACKAGE_NAME,
    configuration,
)

#######################################################
# Dynamically added by post_compile.py
from .core import (  # noqa
    commons,
    results,
)
from .exceptions import ConnectionError, RequeueMessage

backend = get_backend()


reset_connection_sync = backend.connection.reset_connection_sync
reset_connection = backend.connection.reset_connection
get_connection_sync = backend.connection.get_connection_sync
get_connection = backend.connection.get_connection
disconnect = backend.connection.disconnect
disconnect_sync = backend.connection.disconnect_sync

__version__ = version(PACKAGE_NAME)
