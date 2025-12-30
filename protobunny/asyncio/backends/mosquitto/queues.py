import logging

from protobunny.asyncio.backends import (
    BaseAsyncQueue,
)

log = logging.getLogger(__name__)


class AsyncQueue(BaseAsyncQueue):
    def get_tag(self) -> str:
        return self.subscription
