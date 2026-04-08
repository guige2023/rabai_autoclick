"""
Accessibility Cache Utility

Caches and deduplicates accessibility queries for performance.
Reduces repeated AX API calls for stable UI trees.

Example:
    >>> cache = AccessibilityCache(ttl=2.0)
    >>> element = cache.get_or_fetch("button_submit", fetch_fn)
    >>> cache.invalidate()
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, TypeVar, Generic


T = TypeVar("T")

@dataclass
class CacheEntry(Generic[T]):
    """A single cache entry with TTL."""
    value: T
    timestamp: float
    ttl: float
    access_count: int = 0
    last_access: float = field(default_factory=time.time)

    @property
    def is_expired(self) -> bool:
        return time.time() - self.timestamp > self.ttl

    def touch(self) -> None:
        """Update access statistics."""
        self.access_count += 1
        self.last_access = time.time()


class AccessibilityCache(Generic[T]):
    """
    TTL-based cache for accessibility queries.

    Reduces load on accessibility APIs by caching results.

    Args:
        ttl: Time-to-live in seconds (default 2.0).
        max_size: Maximum number of entries.
        cleanup_interval: Seconds between cleanup runs.
    """

    def __init__(
        self,
        ttl: float = 2.0,
        max_size: int = 200,
        cleanup_interval: float = 30.0,
    ) -> None:
        self.ttl = ttl
        self.max_size = max_size
        self.cleanup_interval = cleanup_interval
        self._cache: dict[str, CacheEntry[Any]] = {}
        self._lock = threading.RLock()
        self._last_cleanup = time.time()
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[Any]:
        """
        Get a cached value.

        Args:
            key: Cache key.

        Returns:
            Cached value if present and not expired, None otherwise.
        """
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                self._misses += 1
                return None
            if entry.is_expired:
                del self._cache[key]
                self._misses += 1
                return None
            entry.touch()
            self._hits += 1
            return entry.value

    def set(self, key: str, value: T, ttl: Optional[float] = None) -> None:
        """
        Store a value in the cache.

        Args:
            key: Cache key.
            value: Value to store.
            ttl: Override default TTL.
        """
        with self._lock:
            self._maybe_cleanup()
            if len(self._cache) >= self.max_size:
                self._evict_lru()
            self._cache[key] = CacheEntry(
                value=value,
                timestamp=time.time(),
                ttl=ttl if ttl is not None else self.ttl,
            )

    def get_or_fetch(
        self,
        key: str,
        fetch_fn: Callable[[], T],
        ttl: Optional[float] = None,
    ) -> T:
        """
        Get from cache or fetch if missing/expired.

        Args:
            key: Cache key.
            fetch_fn: Function to call if cache miss.
            ttl: Override default TTL.

        Returns:
            The cached or newly fetched value.
        """
        value = self.get(key)
        if value is None:
            value = fetch_fn()
            self.set(key, value, ttl)
        return value

    def invalidate(self, key: Optional[str] = None) -> None:
        """
        Invalidate cache entries.

        Args:
            key: Specific key to invalidate, or None for all.
        """
        with self._lock:
            if key is None:
                self._cache.clear()
            elif key in self._cache:
                del self._cache[key]

    def touch(self, key: str) -> bool:
        """
        Refresh TTL for an entry.

        Args:
            key: Key to refresh.

        Returns:
            True if key exists and was refreshed.
        """
        with self._lock:
            entry = self._cache.get(key)
            if entry and not entry.is_expired:
                entry.timestamp = time.time()
                return True
        return False

    def stats(self) -> dict[str, Any]:
        """Return cache statistics."""
        with self._lock:
            total = self._hits + self._misses
            hit_rate = self._hits / total if total > 0 else 0.0
            return {
                "size": len(self._cache),
                "max_size": self.max_size,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": hit_rate,
                "ttl": self.ttl,
            }

    def reset_stats(self) -> None:
        """Reset hit/miss counters."""
        self._hits = 0
        self._misses = 0

    def _maybe_cleanup(self) -> None:
        """Run cleanup if interval elapsed."""
        now = time.time()
        if now - self._last_cleanup < self.cleanup_interval:
            return
        self._last_cleanup = now
        expired = [
            k for k, e in self._cache.items()
            if e.is_expired
        ]
        for k in expired:
            del self._cache[k]

    def _evict_lru(self) -> None:
        """Evict least recently used entry."""
        if not self._cache:
            return
        lru_key = min(
            self._cache.keys(),
            key=lambda k: self._cache[k].last_access,
        )
        del self._cache[lru_key]


class AccessibilityQueryCache:
    """
    Specialized cache for accessibility element queries.

    Supports composite keys like "role+name" and "role+index".
    """

    def __init__(
        self,
        ttl: float = 2.0,
        max_size: int = 200,
    ) -> None:
        self.ttl = ttl
        self._cache: dict[str, CacheEntry[Any]] = {}
        self._max_size = max_size
        self._lock = threading.RLock()

    def query_by_role(
        self,
        role: str,
        fetch_fn: Callable[[], list[dict]],
    ) -> list[dict]:
        """Query elements by role with caching."""
        key = f"role:{role}"
        return self.get_or_fetch(key, fetch_fn)

    def query_by_name(
        self,
        name: str,
        fetch_fn: Callable[[], list[dict]],
    ) -> list[dict]:
        """Query elements by name with caching."""
        key = f"name:{name}"
        return self.get_or_fetch(key, fetch_fn)

    def query_by_role_and_name(
        self,
        role: str,
        name: str,
        fetch_fn: Callable[[], list[dict]],
    ) -> list[dict]:
        """Query elements by role and name with caching."""
        key = f"role:{role}:name:{name}"
        return self.get_or_fetch(key, fetch_fn)

    def query_all(
        self,
        fetch_fn: Callable[[], list[dict]],
    ) -> list[dict]:
        """Query all elements with caching."""
        return self.get_or_fetch("__all__", fetch_fn)

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            entry = self._cache.get(key)
            if entry is None or entry.is_expired:
                return None
            entry.touch()
            return entry.value

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            if len(self._cache) >= self._max_size:
                lru_key = min(self._cache.keys(), key=lambda k: self._cache[k].last_access)
                del self._cache[lru_key]
            self._cache[key] = CacheEntry(value=value, timestamp=time.time(), ttl=self.ttl)

    def get_or_fetch(self, key: str, fetch_fn: Callable[[], Any]) -> Any:
        value = self.get(key)
        if value is None:
            value = fetch_fn()
            self.set(key, value)
        return value

    def invalidate(self) -> None:
        with self._lock:
            self._cache.clear()
