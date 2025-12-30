import asyncio
import threading
import typing as tp
from collections import defaultdict

from .models import BaseQueue


class SubscriptionRegistry:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.sync_lock = threading.Lock()
        self.subscriptions: dict[str, "BaseQueue"] = dict()
        self.results_subscriptions: dict[str, "BaseQueue"] = dict()
        self.tasks_subscriptions: dict[str, list["BaseQueue"]] = defaultdict(list)

    @staticmethod
    def get_key(message_type) -> str:
        return str(message_type)

    def register_subscription(self, message_type, queue):
        self.subscriptions[self.get_key(message_type)] = queue

    def register_results(self, message_type, queue):
        self.results_subscriptions[self.get_key(message_type)] = queue

    def register_task(self, message_type, queue):
        self.tasks_subscriptions[self.get_key(message_type)].append(queue)

    def unregister_subscription(self, message_type):
        self.subscriptions.pop(self.get_key(message_type), None)

    def unregister_results(self, message_type) -> "BaseQueue | None":
        return self.results_subscriptions.pop(self.get_key(message_type), None)

    def unregister_tasks(self, message_type):
        return self.tasks_subscriptions.pop(self.get_key(message_type), [])

    def get_subscription(self, message_type) -> "BaseQueue | None":
        return self.subscriptions.get(self.get_key(message_type), None)

    def get_tasks(self, message_type) -> list["BaseQueue"]:
        return self.tasks_subscriptions.get(self.get_key(message_type), [])

    def get_results(self, message_type) -> "BaseQueue | None":
        return self.results_subscriptions.get(self.get_key(message_type), None)

    def get_all_subscriptions(self) -> tp.ValuesView["BaseQueue"]:
        return self.subscriptions.values()

    def get_all_results(self) -> tp.ValuesView["BaseQueue"]:
        return self.results_subscriptions.values()

    def get_all_tasks(
        self, flat: bool = False
    ) -> tp.Generator["BaseQueue", None, None] | list["BaseQueue"]:
        if flat:
            for queues in self.tasks_subscriptions.values():
                for q in queues:
                    yield q
        else:
            yield from self.tasks_subscriptions.values()

    def unregister_all(self):
        self.subscriptions.clear()
        self.tasks_subscriptions.clear()
        self.results_subscriptions.clear()

    def unregister_all_tasks(self):
        self.tasks_subscriptions.clear()

    def unregister_all_subscriptions(self):
        self.subscriptions.clear()

    def unregister_all_results(self):
        self.results_subscriptions.clear()


# Then create a default instance
default_registry = SubscriptionRegistry()
