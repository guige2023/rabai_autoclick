"""
Input recognition cache utilities.

This module provides caching utilities for input recognition results,
including LRU cache and TTL-based expiration.
"""

from __future__ import annotations

import time
import hashlib
from typing import Any, Optional, Dict, Tuple, Callable
from dataclasses import dataclass, field
from collections import OrderedDict


@dataclass
class CacheEntry:
    """A cached input recognition result."""
    key: str
    value: Any
    created_at: float
    last_accessed: float
    hit_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CacheStats:
    """Statistics for cache performance."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0

    @property
    def total_requests(self) -> int:
        return self.hits + self.misses

    @property
    def hit_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.hits / self.total_requests


class InputRecognitionCache:
    """LRU cache with TTL expiration for input recognition results."""

    def __init__(self, max_size: int = 1000, ttl_seconds: float = 300.0):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._stats = CacheStats()
        self._access_order: Dict[str, int] = {}  # For LRU tracking

    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve a cached value by key.

        Args:
            key: Cache key.

        Returns:
            Cached value or None if not found/expired.
        """
        entry = self._cache.get(key)
        if entry is None:
            self._stats.misses += 1
            return None

        # Check TTL
        if time.time() - entry.created_at > self.ttl_seconds:
            self._evict(key)
            self._stats.expirations += 1
            self._stats.misses += 1
            return None

        # Update access time and move to end (most recently used)
        entry.last_accessed = time.time()
        entry.hit_count += 1
        self._cache.move_to_end(key)
        self._stats.hits += 1
        return entry.value

    def put(self, key: str, value: Any, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Store a value in the cache.

        Args:
            key: Cache key.
            value: Value to cache.
            metadata: Optional metadata.
        """
        now = time.time()
        if key in self._cache:
            # Update existing
            entry = self._cache[key]
            entry.value = value
            entry.last_accessed = now
            entry.metadata = metadata or {}
            self._cache.move_to_end(key)
        else:
            # Evict if at capacity
            while len(self._cache) >= self.max_size:
                oldest_key = next(iter(self._cache))
                self._evict(oldest_key)

            self._cache[key] = CacheEntry(
                key=key,
                value=value,
                created_at=now,
                last_accessed=now,
                metadata=metadata or {},
            )

    def _evict(self, key: str) -> None:
        """Evict a key from the cache."""
        if key in self._cache:
            del self._cache[key]
            self._stats.evictions += 1

    def invalidate(self, key: str) -> bool:
        """
        Invalidate a specific cache entry.

        Args:
            key: Cache key to invalidate.

        Returns:
            True if key was found and removed.
        """
        if key in self._cache:
            self._evict(key)
            return True
        return False

    def clear(self) -> None:
        """Clear all cache entries."""
        self._cache.clear()

    @property
    def stats(self) -> CacheStats:
        """Get cache statistics."""
        return self._stats

    def __len__(self) -> int:
        return len(self._cache)


def make_cache_key(
    elements: Tuple[str, ...],
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Create a stable cache key from input elements.

    Args:
        elements: Tuple of string identifiers.
        params: Optional parameter dictionary.

    Returns:
        SHA256 hex digest cache key.
    """
    parts = [str(e) for e in elements]
    if params:
        param_str = "|".join(f"{k}={params[k]}" for k in sorted(params.keys()))
        parts.append(param_str)
    canonical = ";".join(parts)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]
