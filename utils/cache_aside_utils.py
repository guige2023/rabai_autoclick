"""
Cache-aside pattern implementation.

Provides read-through and write-through cache
with invalidation support.
"""

from __future__ import annotations

import threading
import time
from typing import Callable, Generic, TypeVar


T = TypeVar("T")
R = TypeVar("R")


class CacheAside(Generic[T, R]):
    """
    Cache-aside pattern implementation.

    Read strategy: check cache first, fallback to source
    Write strategy: write to source, then update/invalidate cache
    """

    def __init__(
        self,
        source_fn: Callable[[T], R],
        cache_get: Callable[[T], R | None],
        cache_set: Callable[[T, R, float | None], None] | None = None,
        cache_delete: Callable[[T], None] | None = None,
        ttl: float | None = 300.0,
    ):
        self.source_fn = source_fn
        self.cache_get = cache_get
        self.cache_set = cache_set
        self.cache_delete = cache_delete
        self.ttl = ttl

    def get(self, key: T) -> R | None:
        """
        Get value using cache-aside pattern.

        Args:
            key: Cache key

        Returns:
            Cached or source value
        """
        cached = self.cache_get(key)
        if cached is not None:
            return cached
        value = self.source_fn(key)
        if value is not None and self.cache_set:
            self.cache_set(key, value, self.ttl)
        return value

    def put(self, key: T, value: R, ttl: float | None = None) -> None:
        """
        Write value to cache.

        Args:
            key: Cache key
            value: Value to cache
            ttl: TTL override
        """
        if self.cache_set:
            self.cache_set(key, value, ttl if ttl is not None else self.ttl)

    def invalidate(self, key: T) -> None:
        """
        Invalidate cache entry.

        Args:
            key: Cache key
        """
        if self.cache_delete:
            self.cache_delete(key)

    def read_through(self, key: T) -> R | None:
        """Alias for get."""
        return self.get(key)

    def write_through(self, key: T, value: R, ttl: float | None = None) -> None:
        """
        Write to source then cache.

        Args:
            key: Cache key
            value: Value to write
            ttl: Cache TTL
        """
        self.source_fn(key)
        self.put(key, value, ttl)


class InMemoryCache(Generic[R]):
    """Simple in-memory TTL cache for use with CacheAside."""

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self._lock = threading.Lock()
        self._cache: dict[str, tuple[R, float]] = {}

    def get(self, key: str) -> R | None:
        """Get value if not expired."""
        with self._lock:
            if key not in self._cache:
                return None
            value, expiry = self._cache[key]
            if time.time() > expiry:
                del self._cache[key]
                return None
            return value

    def set(self, key: str, value: R, ttl: float | None = None) -> None:
        """Set value with TTL."""
        with self._lock:
            if len(self._cache) >= self.max_size:
                oldest = min(self._cache.items(), key=lambda x: x[1][1])
                del self._cache[oldest[0]]
            expiry = time.time() + (ttl if ttl is not None else 300.0)
            self._cache[key] = (value, expiry)

    def delete(self, key: str) -> None:
        """Delete key."""
        with self._lock:
            self._cache.pop(key, None)

    def clear(self) -> None:
        """Clear all entries."""
        with self._lock:
            self._cache.clear()

    def size(self) -> int:
        """Current cache size."""
        with self._lock:
            return len(self._cache)

    def prune(self) -> int:
        """Remove expired entries. Returns count removed."""
        with self._lock:
            now = time.time()
            expired = [k for k, (_, exp) in self._cache.items() if now > exp]
            for k in expired:
                del self._cache[k]
            return len(expired)


def create_cache_aside(
    source_fn: Callable[[str], R],
    key_prefix: str = "",
) -> CacheAside[str, R]:
    """
    Create cache-aside with in-memory cache.

    Args:
        source_fn: Function to fetch from source
        key_prefix: Prefix for cache keys

    Returns:
        CacheAside instance
    """
    cache = InMemoryCache[str]()

    def prefixed_get(key: str) -> R | None:
        return cache.get(f"{key_prefix}{key}")

    def prefixed_set(key: str, value: R, ttl: float | None) -> None:
        cache.set(f"{key_prefix}{key}", value, ttl)

    def prefixed_delete(key: str) -> None:
        cache.delete(f"{key_prefix}{key}")

    return CacheAside(source_fn, prefixed_get, prefixed_set, prefixed_delete)
