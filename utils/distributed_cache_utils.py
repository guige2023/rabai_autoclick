"""Distributed cache utilities: multi-level caching, cache-aside, write-behind patterns."""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "CacheLevel",
    "MultiLevelCache",
    "CacheAside",
    "WriteBehindCache",
]


@dataclass
class CacheLevel:
    """A single cache level with name and in-memory store."""

    name: str
    max_size: int = 1000
    ttl_seconds: float | None = None

    def __post_init__(self) -> None:
        self._store: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self._lock = threading.RLock()

    def get(self, key: str) -> Any | None:
        with self._lock:
            if key not in self._store:
                return None
            value, expiry = self._store[key]
            if expiry and time.time() > expiry:
                del self._store[key]
                return None
            self._store.move_to_end(key)
            return value

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        with self._lock:
            expiry = (time.time() + ttl) if (ttl or self.ttl_seconds) else None
            self._store[key] = (value, expiry)
            self._store.move_to_end(key)
            if len(self._store) > self.max_size:
                self._store.popitem(last=False)

    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._store:
                del self._store[key]
                return True
            return False

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


class MultiLevelCache:
    """Multi-level cache with L1 (local) and L2 (distributed) levels."""

    def __init__(self, levels: list[CacheLevel] | None = None) -> None:
        self.levels = levels or [
            CacheLevel("L1", max_size=100),
            CacheLevel("L2", max_size=1000),
        ]

    def get(self, key: str) -> Any | None:
        for level in self.levels:
            value = level.get(key)
            if value is not None:
                self._promote(key, value, level)
                return value
        return None

    def _promote(self, key: str, value: Any, source: CacheLevel) -> None:
        for level in self.levels:
            if level is not source:
                level.set(key, value)

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        for level in self.levels:
            level.set(key, value, ttl)

    def delete(self, key: str) -> None:
        for level in self.levels:
            level.delete(key)

    def clear(self) -> None:
        for level in self.levels:
            level.clear()


class CacheAside:
    """Cache-aside pattern: check cache first, fetch from source on miss."""

    def __init__(self, cache: MultiLevelCache | Any) -> None:
        self.cache = cache

    def get_or_fetch(
        self,
        key: str,
        fetch_fn: Callable[[], Any],
        ttl: float | None = None,
    ) -> Any:
        """Get from cache or fetch and populate."""
        value = self.cache.get(key)
        if value is not None:
            return value
        value = fetch_fn()
        self.cache.set(key, value, ttl)
        return value


class WriteBehindCache:
    """Write-behind pattern: write to cache, flush to source asynchronously."""

    def __init__(
        self,
        cache: MultiLevelCache | Any,
        write_fn: Callable[[str, Any], None],
        flush_interval: float = 5.0,
    ) -> None:
        self.cache = cache
        self.write_fn = write_fn
        self._dirty: dict[str, Any] = {}
        self._lock = threading.Lock()
        self._running = False
        self._thread: threading.Thread | None = None

    def set(self, key: str, value: Any) -> None:
        self.cache.set(key, value)
        with self._lock:
            self._dirty[key] = value

    def flush(self) -> int:
        with self._lock:
            keys = list(self._dirty.keys())
            count = 0
            for key in keys:
                self.write_fn(key, self._dirty[key])
                del self._dirty[key]
                count += 1
            return count

    def start(self) -> None:
        self._running = True
        def run():
            while self._running:
                time.sleep(5)
                self.flush()
        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        self.flush()
