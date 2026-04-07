"""Idempotent operation utilities: deduplication, operation tracking, and conflict resolution."""

from __future__ import annotations

import hashlib
import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "OperationRecord",
    "IdempotentStore",
    "IdempotentExecutor",
    "deduplicate",
]


@dataclass
class OperationRecord:
    """Record of an idempotent operation."""

    key: str
    result: Any
    status: str = "complete"
    created_at: float = field(default_factory=time.time)
    expires_at: float | None = None
    call_count: int = 1


class IdempotentStore:
    """Storage for idempotent operation results."""

    def __init__(self, ttl_seconds: float = 3600) -> None:
        self._store: dict[str, OperationRecord] = {}
        self._lock = threading.RLock()
        self._ttl = ttl_seconds

    def get(self, key: str) -> OperationRecord | None:
        with self._lock:
            record = self._store.get(key)
            if record is None:
                return None
            if record.expires_at and time.time() > record.expires_at:
                del self._store[key]
                return None
            record.call_count += 1
            return record

    def set(self, key: str, result: Any, ttl: float | None = None) -> OperationRecord:
        with self._lock:
            expires = time.time() + (ttl or self._ttl)
            record = OperationRecord(
                key=key,
                result=result,
                expires_at=expires,
            )
            self._store[key] = record
            return record

    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._store:
                del self._store[key]
                return True
            return False

    def clear_expired(self) -> int:
        with self._lock:
            now = time.time()
            expired = [k for k, r in self._store.items() if r.expires_at and now > r.expires_at]
            for k in expired:
                del self._store[k]
            return len(expired)


class IdempotentExecutor:
    """Execute operations idempotently."""

    def __init__(self, store: IdempotentStore | None = None) -> None:
        self._store = store or IdempotentStore()

    def execute(
        self,
        key: str,
        operation: Callable[[], Any],
    ) -> tuple[Any, bool]:
        """Execute operation idempotently.

        Returns (result, was_cached).
        """
        existing = self._store.get(key)
        if existing:
            return existing.result, True

        result = operation()
        self._store.set(key, result)
        return result, False


def deduplicate(items: list[Any], key_fn: Callable[[Any], str]) -> list[Any]:
    """Deduplicate a list using a key function."""
    seen: dict[str, Any] = {}
    for item in items:
        k = key_fn(item)
        if k not in seen:
            seen[k] = item
    return list(seen.values())
