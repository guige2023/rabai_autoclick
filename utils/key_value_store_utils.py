"""Key-value store utilities: simple embedded KV store with TTL and iteration support."""

from __future__ import annotations

import json
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Iterator

__all__ = [
    "KVEntry",
    "KeyValueStore",
    "PersistentKVStore",
]


@dataclass
class KVEntry:
    """A key-value store entry."""

    value: Any
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    ttl: float | None = None
    tags: list[str] = field(default_factory=list)

    def is_expired(self) -> bool:
        if self.ttl is None:
            return False
        return (time.time() - self.created_at) > self.ttl


class KeyValueStore:
    """Thread-safe in-memory key-value store with TTL and ordered iteration."""

    def __init__(self) -> None:
        self._store: OrderedDict[str, KVEntry] = OrderedDict()
        self._lock = threading.RLock()

    def set(
        self,
        key: str,
        value: Any,
        ttl: float | None = None,
        tags: list[str] | None = None,
    ) -> None:
        with self._lock:
            now = time.time()
            existing = self._store.get(key)
            self._store[key] = KVEntry(
                value=value,
                created_at=existing.created_at if existing else now,
                updated_at=now,
                ttl=ttl,
                tags=tags or [],
            )

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return default
            if entry.is_expired():
                del self._store[key]
                return default
            return entry.value

    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._store:
                del self._store[key]
                return True
            return False

    def exists(self, key: str) -> bool:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return False
            if entry.is_expired():
                del self._store[key]
                return False
            return True

    def keys(self, pattern: str = "*") -> Iterator[str]:
        """Iterate over keys matching a glob pattern."""
        import fnmatch
        with self._lock:
            for key in list(self._store.keys()):
                if fnmatch.fnmatch(key, pattern):
                    yield key

    def items(self, pattern: str = "*") -> Iterator[tuple[str, Any]]:
        """Iterate over key-value pairs matching a glob pattern."""
        import fnmatch
        with self._lock:
            for key, entry in list(self._store.items()):
                if fnmatch.fnmatch(key, pattern):
                    if not entry.is_expired():
                        yield key, entry.value

    def clear(self) -> int:
        with self._lock:
            count = len(self._store)
            self._store.clear()
            return count

    def size(self) -> int:
        with self._lock:
            self._cleanup_expired()
            return len(self._store)

    def _cleanup_expired(self) -> None:
        expired = [k for k, e in self._store.items() if e.is_expired()]
        for k in expired:
            del self._store[k]

    def get_metadata(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            return {
                "created_at": entry.created_at,
                "updated_at": entry.updated_at,
                "ttl": entry.ttl,
                "tags": entry.tags,
            }


class PersistentKVStore(KeyValueStore):
    """Key-value store with optional JSON file persistence."""

    def __init__(self, path: str | None = None) -> None:
        super().__init__()
        self._path = path
        if path:
            self._load()

    def _load(self) -> None:
        if not self._path:
            return
        try:
            with open(self._path) as f:
                data = json.load(f)
            for key, entry_data in data.items():
                self._store[key] = KVEntry(
                    value=entry_data["value"],
                    created_at=entry_data.get("created_at", time.time()),
                    updated_at=entry_data.get("updated_at", time.time()),
                    ttl=entry_data.get("ttl"),
                    tags=entry_data.get("tags", []),
                )
        except (FileNotFoundError, json.JSONDecodeError):
            pass

    def save(self) -> None:
        if not self._path:
            return
        with self._lock:
            data = {
                key: {
                    "value": entry.value,
                    "created_at": entry.created_at,
                    "updated_at": entry.updated_at,
                    "ttl": entry.ttl,
                    "tags": entry.tags,
                }
                for key, entry in self._store.items()
            }
            with open(self._path, "w") as f:
                json.dump(data, f)
