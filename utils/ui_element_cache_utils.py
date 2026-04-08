"""UI element cache utilities.

This module provides utilities for caching UI element references
to reduce repeated lookups.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar
from dataclasses import dataclass, field
import hashlib

T = TypeVar("T")


@dataclass
class CacheEntry(Generic[T]):
    """A cached element entry."""
    value: T
    created_at: float
    last_accessed: float
    access_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_expired(self, ttl_seconds: float) -> bool:
        return (time.time() - self.created_at) > ttl_seconds

    def touch(self) -> None:
        self.last_accessed = time.time()
        self.access_count += 1


class ElementCache(Generic[T]):
    """Thread-safe cache for UI elements."""

    def __init__(
        self,
        ttl_seconds: float = 60.0,
        max_size: int = 1000,
    ) -> None:
        self._ttl = ttl_seconds
        self._max_size = max_size
        self._cache: Dict[str, CacheEntry[T]] = {}
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[T]:
        """Get a cached element.

        Args:
            key: Cache key.

        Returns:
            Cached value or None if not found/expired.
        """
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                self._misses += 1
                return None
            if entry.is_expired(self._ttl):
                del self._cache[key]
                self._misses += 1
                return None
            entry.touch()
            self._hits += 1
            return entry.value

    def set(self, key: str, value: T, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Set a cached element.

        Args:
            key: Cache key.
            value: Value to cache.
            metadata: Optional metadata.
        """
        with self._lock:
            if len(self._cache) >= self._max_size:
                self._evict_lru()
            now = time.time()
            self._cache[key] = CacheEntry(
                value=value,
                created_at=now,
                last_accessed=now,
                metadata=metadata or {},
            )

    def _evict_lru(self) -> None:
        """Evict least recently used entry."""
        if not self._cache:
            return
        lru_key = min(
            self._cache,
            key=lambda k: self._cache[k].last_accessed,
        )
        del self._cache[lru_key]

    def invalidate(self, key: str) -> bool:
        """Invalidate a cache entry.

        Args:
            key: Cache key.

        Returns:
            True if entry was removed.
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
        return False

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()
            self._hits = 0
            self._misses = 0

    def stats(self) -> Dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dict with hits, misses, size, etc.
        """
        with self._lock:
            total = self._hits + self._misses
            hit_rate = self._hits / total if total > 0 else 0.0
            return {
                "hits": self._hits,
                "misses": self._misses,
                "size": len(self._cache),
                "max_size": self._max_size,
                "hit_rate": hit_rate,
                "ttl_seconds": self._ttl,
            }

    def cleanup_expired(self) -> int:
        """Remove all expired entries.

        Returns:
            Number of entries removed.
        """
        with self._lock:
            expired = [
                k for k, v in self._cache.items()
                if v.is_expired(self._ttl)
            ]
            for k in expired:
                del self._cache[k]
            return len(expired)


def make_cache_key(element_id: str, attributes: Optional[Dict[str, Any]] = None) -> str:
    """Make a cache key from element properties.

    Args:
        element_id: Element identifier.
        attributes: Optional attributes for more specific key.

    Returns:
        Cache key string.
    """
    if not attributes:
        return element_id
    attr_str = str(sorted(attributes.items()))
    return f"{element_id}:{hashlib.md5(attr_str.encode()).hexdigest()[:8]}"


__all__ = [
    "CacheEntry",
    "ElementCache",
    "make_cache_key",
]
