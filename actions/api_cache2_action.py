"""API Response Cache with TTL.

This module provides API response caching:
- TTL-based expiration
- LRU eviction
- Cache invalidation
- Stats tracking

Example:
    >>> from actions.api_cache2_action import ResponseCache
    >>> cache = ResponseCache(default_ttl=300)
    >>> cache.set("key", response_data)
    >>> data = cache.get("key")
"""

from __future__ import annotations

import time
import logging
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from collections import OrderedDict

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """A cache entry."""
    value: Any
    created_at: float
    expires_at: float
    access_count: int = 0
    last_accessed: float = 0.0
    hit_count: int = 0


class ResponseCache:
    """TTL-based response cache with LRU eviction."""

    def __init__(
        self,
        max_size: int = 1000,
        default_ttl: int = 300,
        enable_stats: bool = True,
    ) -> None:
        """Initialize the response cache.

        Args:
            max_size: Maximum cache entries.
            default_ttl: Default TTL in seconds.
            enable_stats: Whether to track statistics.
        """
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._max_size = max_size
        self._default_ttl = default_ttl
        self._enable_stats = enable_stats
        self._lock = threading.RLock()
        self._stats = {"hits": 0, "misses": 0, "sets": 0, "evictions": 0, "expirations": 0}

    def get(self, key: str) -> Optional[Any]:
        """Get a cached value.

        Args:
            key: Cache key.

        Returns:
            Cached value or None if not found/expired.
        """
        with self._lock:
            entry = self._cache.get(key)

            if entry is None:
                if self._enable_stats:
                    self._stats["misses"] += 1
                return None

            if time.time() > entry.expires_at:
                del self._cache[key]
                if self._enable_stats:
                    self._stats["expirations"] += 1
                    self._stats["misses"] += 1
                return None

            entry.access_count += 1
            entry.last_accessed = time.time()
            entry.hit_count += 1
            self._cache.move_to_end(key)

            if self._enable_stats:
                self._stats["hits"] += 1

            return entry.value

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> None:
        """Set a cache entry.

        Args:
            key: Cache key.
            value: Value to cache.
            ttl: TTL in seconds. None = use default.
        """
        ttl = ttl if ttl is not None else self._default_ttl
        now = time.time()

        entry = CacheEntry(
            value=value,
            created_at=now,
            expires_at=now + ttl,
        )

        with self._lock:
            if key in self._cache:
                del self._cache[key]
            elif len(self._cache) >= self._max_size:
                self._evict_lru()

            self._cache[key] = entry
            if self._enable_stats:
                self._stats["sets"] += 1

    def delete(self, key: str) -> bool:
        """Delete a cache entry.

        Args:
            key: Cache key.

        Returns:
            True if deleted.
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self) -> int:
        """Clear all cache entries.

        Returns:
            Number of entries cleared.
        """
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            return count

    def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate all keys matching a pattern.

        Args:
            pattern: Glob-style pattern (e.g., "user_*").

        Returns:
            Number of keys invalidated.
        """
        import fnmatch

        with self._lock:
            matching = [k for k in self._cache.keys() if fnmatch.fnmatch(k, pattern)]
            for k in matching:
                del self._cache[k]
            return len(matching)

    def cleanup_expired(self) -> int:
        """Remove all expired entries.

        Returns:
            Number of entries removed.
        """
        now = time.time()
        removed = 0

        with self._lock:
            expired = [k for k, e in self._cache.items() if now > e.expires_at]
            for k in expired:
                del self._cache[k]
                removed += 1

            if removed > 0:
                self._stats["expirations"] += removed

        return removed

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dict with hit rate, size, etc.
        """
        with self._lock:
            stats = dict(self._stats)
            stats["size"] = len(self._cache)
            stats["max_size"] = self._max_size

            total_requests = self._stats["hits"] + self._stats["misses"]
            if total_requests > 0:
                stats["hit_rate"] = round(self._stats["hits"] / total_requests, 4)
            else:
                stats["hit_rate"] = 0.0

            return stats

    def _evict_lru(self) -> None:
        """Evict the least recently used entry."""
        if self._cache:
            self._cache.popitem(last=False)
            if self._enable_stats:
                self._stats["evictions"] += 1

    def get_or_compute(
        self,
        key: str,
        compute_fn: Callable[[], Any],
        ttl: Optional[int] = None,
    ) -> Any:
        """Get from cache or compute and store.

        Args:
            key: Cache key.
            compute_fn: Function to compute value if not cached.
            ttl: TTL in seconds.

        Returns:
            Cached or computed value.
        """
        value = self.get(key)
        if value is not None:
            return value

        value = compute_fn()
        self.set(key, value, ttl=ttl)
        return value

    def has(self, key: str) -> bool:
        """Check if a key exists and is not expired.

        Args:
            key: Cache key.

        Returns:
            True if key exists and is valid.
        """
        return self.get(key) is not None
