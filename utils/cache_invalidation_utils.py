"""Cache invalidation utilities for automation result caching.

Provides TTL-based caching, cache invalidation strategies,
and memoization decorators for caching expensive
automation computations.

Example:
    >>> from utils.cache_invalidation_utils import cached, invalidate_cache
    >>> @cached(ttl=60)
    ... def expensive_lookup(key):
    ...     return do_lookup(key)
    >>> invalidate_cache('expensive_lookup')
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, Optional

__all__ = [
    "cached",
    "CacheEntry",
    "InMemoryCache",
    "invalidate_cache",
    "CacheStats",
]


@dataclass
class CacheEntry:
    """A single cache entry with TTL."""

    value: Any
    created_at: float
    expires_at: float

    def is_expired(self, ttl: float) -> bool:
        return time.time() > self.expires_at


@dataclass
class CacheStats:
    """Cache statistics."""

    hits: int = 0
    misses: int = 0
    evictions: int = 0

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0


class InMemoryCache:
    """Thread-safe in-memory cache with TTL.

    Example:
        >>> cache = InMemoryCache(max_size=100, default_ttl=60)
        >>> cache.set('key', 'value')
        >>> cache.get('key')
        'value'
    """

    def __init__(self, max_size: int = 1000, default_ttl: float = 300.0):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._store: dict[str, CacheEntry] = {}
        self._lock = __import__("threading").Lock()
        self.stats = CacheStats()

    def get(self, key: str) -> Optional[Any]:
        """Get a value from the cache.

        Returns:
            Cached value, or None if not found or expired.
        """
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self.stats.misses += 1
                return None
            if entry.is_expired(self.default_ttl):
                del self._store[key]
                self.stats.misses += 1
                self.stats.evictions += 1
                return None
            self.stats.hits += 1
            return entry.value

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """Set a value in the cache."""
        with self._lock:
            if len(self._store) >= self.max_size and key not in self._store:
                self._evict_oldest()
            ttl = ttl if ttl is not None else self.default_ttl
            now = time.time()
            self._store[key] = CacheEntry(
                value=value,
                created_at=now,
                expires_at=now + ttl,
            )

    def delete(self, key: str) -> bool:
        """Delete a key from the cache."""
        with self._lock:
            if key in self._store:
                del self._store[key]
                return True
            return False

    def clear(self) -> None:
        """Clear all cached entries."""
        with self._lock:
            self._store.clear()

    def _evict_oldest(self) -> None:
        """Evict the oldest non-expired entry."""
        if not self._store:
            return
        oldest_key = min(self._store, key=lambda k: self._store[k].created_at)
        del self._store[oldest_key]
        self.stats.evictions += 1

    def invalidate_expired(self) -> int:
        """Remove all expired entries.

        Returns:
            Number of entries removed.
        """
        with self._lock:
            expired = [
                k for k, v in self._store.items()
                if v.is_expired(self.default_ttl)
            ]
            for k in expired:
                del self._store[k]
            self.stats.evictions += len(expired)
            return len(expired)


# Global cache registry
_cache_registry: dict[str, InMemoryCache] = {}


def cached(
    ttl: float = 300.0,
    max_size: int = 1000,
    key_prefix: Optional[str] = None,
) -> Callable:
    """Decorator to cache function results.

    Args:
        ttl: Time-to-live in seconds.
        max_size: Maximum cache entries.
        key_prefix: Optional prefix for cache keys.

    Returns:
        Decorated function.

    Example:
        >>> @cached(ttl=60)
        ... def get_data(key):
        ...     return fetch_from_api(key)
    """
    cache = InMemoryCache(max_size=max_size, default_ttl=ttl)
    cache_key = key_prefix or id(cached)
    _cache_registry[cache_key] = cache

    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            # Build cache key from function name and args
            key_parts = [fn.__module__, fn.__name__]
            key_parts.extend(str(a) for a in args)
            key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
            cache_key_str = "|".join(key_parts)
            key_hash = hashlib.md5(cache_key_str.encode()).hexdigest()

            result = cache.get(key_hash)
            if result is not None:
                return result

            result = fn(*args, **kwargs)
            cache.set(key_hash, result, ttl=ttl)
            return result

        wrapper.cache = cache
        wrapper.cache_key = cache_key
        return wrapper

    return decorator


def invalidate_cache(key_prefix: Optional[str] = None) -> None:
    """Invalidate cache entries.

    Args:
        key_prefix: If provided, invalidate entries with this prefix.
            If None, invalidate all caches.
    """
    if key_prefix is None:
        for cache in _cache_registry.values():
            cache.clear()
    else:
        cache = _cache_registry.get(key_prefix)
        if cache:
            cache.clear()


def get_cache_stats(key_prefix: Optional[str] = None) -> CacheStats:
    """Get cache statistics.

    Args:
        key_prefix: Specific cache to query, or None for aggregate.

    Returns:
        CacheStats object.
    """
    if key_prefix:
        cache = _cache_registry.get(key_prefix)
        return cache.stats if cache else CacheStats()

    # Aggregate stats
    total = CacheStats()
    for cache in _cache_registry.values():
        total.hits += cache.stats.hits
        total.misses += cache.stats.misses
        total.evictions += cache.stats.evictions
    return total
