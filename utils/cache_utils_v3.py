"""
Cache Utilities v3

Provides advanced caching with TTL, LRU, LFU policies,
write-through/write-back, and cache invalidation.
"""

from __future__ import annotations

import copy
import threading
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")
TKey = TypeVar("TKey")
TValue = TypeVar("TValue")


class CachePolicy(Enum := __import__("enum").Enum):
    """Cache eviction policy."""
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    LIFO = "lifo"
    TTL = "ttl"
    RANDOM = "random"


@dataclass
class CacheEntry(Generic[T]):
    """A single cache entry."""
    key: str
    value: T
    created_at: float = field(default_factory=time.time)
    accessed_at: float = field(default_factory=time.time)
    access_count: int = 0
    ttl_seconds: float | None = None

    def is_expired(self) -> bool:
        """Check if entry has expired based on TTL."""
        if self.ttl_seconds is None:
            return False
        return time.time() - self.created_at > self.ttl_seconds

    def touch(self) -> None:
        """Update access time and count."""
        self.accessed_at = time.time()
        self.access_count += 1


class Cache(ABC, Generic[T]):
    """Abstract cache interface."""

    @abstractmethod
    def get(self, key: str) -> T | None:
        """Get a value from cache."""
        pass

    @abstractmethod
    def set(self, key: str, value: T, ttl: float | None = None) -> None:
        """Set a value in cache."""
        pass

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete a value from cache."""
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clear all cache entries."""
        pass

    @abstractmethod
    def size(self) -> int:
        """Get number of entries in cache."""
        pass


class LRUCache(Cache[T]):
    """
    Least Recently Used cache implementation.
    """

    def __init__(self, max_size: int = 100, ttl_seconds: float | None = None):
        self._max_size = max_size
        self._ttl_seconds = ttl_seconds
        self._cache: OrderedDict[str, CacheEntry[T]] = OrderedDict()
        self._lock = threading.RLock()

    def get(self, key: str) -> T | None:
        """Get a value, updating its access time."""
        with self._lock:
            if key not in self._cache:
                return None

            entry = self._cache[key]

            if entry.is_expired():
                del self._cache[key]
                return None

            # Move to end (most recently used)
            self._cache.move_to_end(key)
            entry.touch()
            return entry.value

    def set(self, key: str, value: T, ttl: float | None = None) -> None:
        """Set a value in cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]

            if len(self._cache) >= self._max_size:
                # Remove oldest entry
                self._cache.popitem(last=False)

            entry = CacheEntry(
                key=key,
                value=value,
                ttl_seconds=ttl or self._ttl_seconds,
            )
            self._cache[key] = entry

    def delete(self, key: str) -> bool:
        """Delete a value from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()

    def size(self) -> int:
        """Get number of entries."""
        with self._lock:
            return len(self._cache)

    def peek(self, key: str) -> T | None:
        """Peek at a value without updating access time."""
        with self._lock:
            if key in self._cache:
                entry = self._cache[key]
                if not entry.is_expired():
                    return entry.value
            return None


class LFUCache(Cache[T]):
    """
    Least Frequently Used cache implementation.
    """

    def __init__(self, max_size: int = 100, ttl_seconds: float | None = None):
        self._max_size = max_size
        self._ttl_seconds = ttl_seconds
        self._cache: dict[str, CacheEntry[T]] = {}
        self._lock = threading.RLock()

    def get(self, key: str) -> T | None:
        """Get a value, updating access count."""
        with self._lock:
            if key not in self._cache:
                return None

            entry = self._cache[key]

            if entry.is_expired():
                del self._cache[key]
                return None

            entry.touch()
            return entry.value

    def set(self, key: str, value: T, ttl: float | None = None) -> None:
        """Set a value in cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]

            if len(self._cache) >= self._max_size:
                # Find least frequently used
                lfu_key = min(self._cache.keys(), key=lambda k: self._cache[k].access_count)
                del self._cache[lfu_key]

            entry = CacheEntry(
                key=key,
                value=value,
                ttl_seconds=ttl or self._ttl_seconds,
            )
            self._cache[key] = entry

    def delete(self, key: str) -> bool:
        """Delete a value from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()

    def size(self) -> int:
        """Get number of entries."""
        with self._lock:
            return len(self._cache)


class TTLCache(Cache[T]):
    """
    Cache that automatically evicts expired entries.
    """

    def __init__(self, default_ttl: float = 300.0, max_size: int = 1000):
        self._default_ttl = default_ttl
        self._max_size = max_size
        self._cache: dict[str, CacheEntry[T]] = {}
        self._lock = threading.RLock()
        self._last_cleanup = time.time()
        self._cleanup_interval = 60.0

    def get(self, key: str) -> T | None:
        """Get a value from cache."""
        with self._lock:
            self._maybe_cleanup()

            if key not in self._cache:
                return None

            entry = self._cache[key]

            if entry.is_expired():
                del self._cache[key]
                return None

            entry.touch()
            return entry.value

    def set(self, key: str, value: T, ttl: float | None = None) -> None:
        """Set a value in cache."""
        with self._lock:
            self._maybe_cleanup()

            if key in self._cache:
                del self._cache[key]
            elif len(self._cache) >= self._max_size:
                # Remove oldest expired first
                expired = [(k, v.created_at) for k, v in self._cache.items() if v.is_expired()]
                if expired:
                    expired.sort(key=lambda x: x[1])
                    del self._cache[expired[0][0]]
                else:
                    # Remove oldest by created_at
                    oldest = min(self._cache.keys(), key=lambda k: self._cache[k].created_at)
                    del self._cache[oldest]

            entry = CacheEntry(
                key=key,
                value=value,
                ttl_seconds=ttl or self._default_ttl,
            )
            self._cache[key] = entry

    def delete(self, key: str) -> bool:
        """Delete a value from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()

    def size(self) -> int:
        """Get number of entries."""
        with self._lock:
            return len(self._cache)

    def _maybe_cleanup(self) -> None:
        """Run cleanup if enough time has passed."""
        if time.time() - self._last_cleanup > self._cleanup_interval:
            self._cleanup()
            self._last_cleanup = time.time()

    def _cleanup(self) -> None:
        """Remove all expired entries."""
        expired = [k for k, v in self._cache.items() if v.is_expired()]
        for key in expired:
            del self._cache[key]


class CacheStats:
    """Statistics for cache performance."""

    def __init__(self):
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.expirations = 0

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0

    @property
    def total_requests(self) -> int:
        return self.hits + self.misses


class MeasuredCache(Cache[T]):
    """
    Cache wrapper that collects statistics.
    """

    def __init__(self, cache: Cache[T]):
        self._cache = cache
        self._stats = CacheStats()

    @property
    def stats(self) -> CacheStats:
        return copy.copy(self._stats)

    def get(self, key: str) -> T | None:
        result = self._cache.get(key)
        if result is not None:
            self._stats.hits += 1
        else:
            self._stats.misses += 1
        return result

    def set(self, key: str, value: T, ttl: float | None = None) -> None:
        self._cache.set(key, value, ttl)

    def delete(self, key: str) -> bool:
        return self._cache.delete(key)

    def clear(self) -> None:
        self._cache.clear()

    def size(self) -> int:
        return self._cache.size()


def create_cache(
    policy: CachePolicy = CachePolicy.LRU,
    max_size: int = 100,
    ttl_seconds: float | None = None,
) -> Cache:
    """
    Create a cache with the specified policy.

    Args:
        policy: The cache eviction policy.
        max_size: Maximum number of entries.
        ttl_seconds: Default TTL for entries.

    Returns:
        Configured cache instance.
    """
    if policy == CachePolicy.LRU:
        return LRUCache(max_size=max_size, ttl_seconds=ttl_seconds)
    elif policy == CachePolicy.LFU:
        return LFUCache(max_size=max_size, ttl_seconds=ttl_seconds)
    elif policy == CachePolicy.TTL:
        return TTLCache(default_ttl=ttl_seconds or 300, max_size=max_size)
    else:
        return LRUCache(max_size=max_size, ttl_seconds=ttl_seconds)
