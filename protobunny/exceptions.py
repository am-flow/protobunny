class RequeueMessage(Exception):
    """Raise when a message could not be handled but should be requeued."""

    ...


class ConnectionError(Exception):
    """Raised when connection operations fail."""

    ...


class PublishError(Exception):
    """Raised when a callback fails to process a message and doesn't raise the RequeueMessage exception."""

    ...
