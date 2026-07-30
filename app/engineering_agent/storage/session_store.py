from __future__ import annotations

from copy import deepcopy
from threading import RLock
from typing import Any


class InMemorySessionStore:
    """Small local store for demo sessions; replace with DynamoDB/Redis in production."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._items: dict[str, dict[str, Any]] = {}

    def get(self, thread_id: str) -> dict[str, Any]:
        with self._lock:
            return deepcopy(self._items.get(thread_id, {}))

    def put(self, thread_id: str, state: dict[str, Any]) -> None:
        with self._lock:
            self._items[thread_id] = deepcopy(state)
