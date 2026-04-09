"""
UI element caching utilities for automation performance.

This module provides caching mechanisms to reduce redundant
UI queries and improve automation script performance.
"""

from __future__ import annotations

import time
import threading
import hashlib
from dataclasses import dataclass, field
from typing import Callable, Optional, Any, Dict, List, Generic, TypeVar, Type
from enum import Enum, auto


class CacheEvictionPolicy(Enum):
    """Cache eviction policies."""
    LRU = auto()
    LFU = auto()
    FIFO = auto()
    TTL = auto()


T = TypeVar("T")


@dataclass
class CacheEntry(Generic[T]):
    """
    Single cache entry with metadata.

    Attributes:
        key: Cache key.
        value: Cached value.
        created_at: When entry was created.
        last_accessed: When entry was last accessed.
        access_count: Number of times accessed.
        expires_at: When entry expires (0 = never).
    """
    key: str
    value: T
    created_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)
    access_count: int = 0
    expires_at: float = 0.0

    def is_expired(self) -> bool:
        """Check if entry has expired."""
        if self.expires_at <= 0:
            return False
        return time.time() > self.expires_at

    def touch(self) -> None:
        """Update last accessed time."""
        self.last_accessed = time.time()
        self.access_count += 1


@dataclass
class CacheStats:
    """Cache statistics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0

    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate."""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0


class LRUCache(Generic[T]):
    """
    Least Recently Used (LRU) cache implementation.

    Automatically evicts least recently accessed entries
    when capacity is reached.
    """

    def __init__(self, max_size: int = 100) -> None:
        self._max_size = max_size
        self._cache: Dict[str, CacheEntry[T]] = {}
        self._order: List[str] = []  # Most recent at end
        self._lock = threading.RLock()
        self._stats = CacheStats()

    def get(self, key: str) -> Optional[T]:
        """Get value from cache."""
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                self._stats.misses += 1
                return None

            if entry.is_expired():
                self._remove(key)
                self._stats.expirations += 1
                return None

            entry.touch()
            self._move_to_end(key)
            self._stats.hits += 1
            return entry.value

    def put(self, key: str, value: T, ttl: float = 0.0) -> None:
        """Put value into cache."""
        with self._lock:
            if key in self._cache:
                self._remove(key)

            expires_at = time.time() + ttl if ttl > 0 else 0.0
            entry = CacheEntry[T](key=key, value=value, expires_at=expires_at)
            self._cache[key] = entry
            self._order.append(key)
            self._evict_if_needed()

    def invalidate(self, key: str) -> bool:
        """Remove entry from cache."""
        with self._lock:
            if key in self._cache:
                self._remove(key)
                return True
            return False

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()
            self._order.clear()

    def _remove(self, key: str) -> None:
        """Remove entry from cache."""
        if key in self._cache:
            del self._cache[key]
        if key in self._order:
            self._order.remove(key)

    def _move_to_end(self, key: str) -> None:
        """Move key to end of order list (most recent)."""
        if key in self._order:
            self._order.remove(key)
        self._order.append(key)

    def _evict_if_needed(self) -> None:
        """Evict oldest entries if over capacity."""
        while len(self._cache) > self._max_size:
            if self._order:
                oldest = self._order.pop(0)
                self._remove(oldest)
                self._stats.evictions += 1

    @property
    def stats(self) -> CacheStats:
        """Get cache statistics."""
        return self._stats


class TTLCache(Generic[T]):
    """
    Time-To-Live (TTL) cache implementation.

    All entries expire after a fixed duration.
    """

    def __init__(self, default_ttl: float = 60.0, cleanup_interval: float = 10.0) -> None:
        self._default_ttl = default_ttl
        self._cache: Dict[str, CacheEntry[T]] = {}
        self._lock = threading.RLock()
        self._stats = CacheStats()
        self._cleanup_interval = cleanup_interval
        self._last_cleanup = time.time()

    def get(self, key: str) -> Optional[T]:
        """Get value from cache."""
        with self._lock:
            self._maybe_cleanup()

            entry = self._cache.get(key)
            if entry is None:
                self._stats.misses += 1
                return None

            if entry.is_expired():
                del self._cache[key]
                self._stats.expirations += 1
                return None

            entry.touch()
            self._stats.hits += 1
            return entry.value

    def put(self, key: str, value: T, ttl: Optional[float] = None) -> None:
        """Put value into cache with TTL."""
        with self._lock:
            actual_ttl = ttl if ttl is not None else self._default_ttl
            expires_at = time.time() + actual_ttl if actual_ttl > 0 else 0.0
            entry = CacheEntry[T](key=key, value=value, expires_at=expires_at)
            self._cache[key] = entry

    def invalidate(self, key: str) -> bool:
        """Remove entry from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()

    def _maybe_cleanup(self) -> None:
        """Periodically remove expired entries."""
        now = time.time()
        if now - self._last_cleanup < self._cleanup_interval:
            return

        self._last_cleanup = now
        expired = [k for k, e in self._cache.items() if e.is_expired()]
        for key in expired:
            del self._cache[key]
            self._stats.expirations += 1

    @property
    def stats(self) -> CacheStats:
        """Get cache statistics."""
        return self._stats


class ElementCache:
    """
    Specialized cache for UI element lookups.

    Caches accessibility snapshots and element queries
    with automatic invalidation.
    """

    def __init__(self, max_size: int = 50, ttl: float = 30.0) -> None:
        self._lru_cache = LRUCache[Dict[str, Any]](max_size)
        self._ttl_cache = TTLCache[Dict[str, Any]](ttl)

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """Get cached element data."""
        result = self._lru_cache.get(key)
        if result is None:
            result = self._ttl_cache.get(key)
        return result

    def put(self, key: str, value: Dict[str, Any], ttl: Optional[float] = None) -> None:
        """Cache element data."""
        self._lru_cache.put(key, value)
        self._ttl_cache.put(key, value, ttl)

    def invalidate(self, key: str) -> None:
        """Invalidate cached element."""
        self._lru_cache.invalidate(key)
        self._ttl_cache.invalidate(key)

    def invalidate_all(self) -> None:
        """Clear all cached elements."""
        self._lru_cache.clear()
        self._ttl_cache.clear()

    @property
    def stats(self) -> CacheStats:
        """Get combined cache statistics."""
        lru_stats = self._lru_cache.stats
        ttl_stats = self._ttl_cache.stats
        return CacheStats(
            hits=lru_stats.hits + ttl_stats.hits,
            misses=lru_stats.misses + ttl_stats.misses,
            evictions=lru_stats.evictions,
            expirations=lru_stats.expirations + ttl_stats.expirations,
        )


def cache_key(*args: Any, **kwargs: Any) -> str:
    """Generate cache key from arguments."""
    content = str(args) + str(sorted(kwargs.items()))
    return hashlib.md5(content.encode()).hexdigest()
