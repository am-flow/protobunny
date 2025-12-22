class RequeueMessage(Exception):
    """Raise when a message could not be handled but should be requeued."""

    ...


class ConnectionError(Exception):
    """Raised when connection operations fail."""

    ...
