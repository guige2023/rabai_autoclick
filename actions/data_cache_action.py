"""Data caching utilities with TTL and eviction policies.

Supports LRU, LFU, TTL-based expiration, and distributed cache interfaces.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class CacheEntry(Generic[T]):
    """A single cache entry with metadata."""

    key: str
    value: T
    created_at: float = field(default_factory=time.time)
    accessed_at: float = field(default_factory=time.time)
    access_count: int = 0
    ttl: float | None = None
    tags: set[str] = field(default_factory=set)

    def is_expired(self) -> bool:
        """Check if entry has expired based on TTL."""
        if self.ttl is None:
            return False
        return time.time() > self.created_at + self.ttl

    def touch(self) -> None:
        """Update access time and count."""
        self.accessed_at = time.time()
        self.access_count += 1


class CacheEvictionPolicy(ABC):
    """Abstract eviction policy."""

    @abstractmethod
    def select_eviction(self, cache: "TTLCache") -> str | None:
        """Select a key to evict."""
        pass


class LRUEviction(CacheEvictionPolicy):
    """Least Recently Used eviction."""

    def select_eviction(self, cache: "TTLCache") -> str | None:
        """Evict least recently accessed entry."""
        if not cache._data:
            return None
        return min(cache._data.keys(), key=lambda k: cache._data[k].accessed_at)


class LFUEviction(CacheEvictionPolicy):
    """Least Frequently Used eviction."""

    def select_eviction(self, cache: "TTLCache") -> str | None:
        """Evict least frequently accessed entry."""
        if not cache._data:
            return None
        return min(cache._data.keys(), key=lambda k: cache._data[k].access_count)


class TTLEviction(CacheEvictionPolicy):
    """Time-To-Live based eviction."""

    def select_eviction(self, cache: "TTLCache") -> str | None:
        """Evict entry with oldest TTL (closest to expiration)."""
        candidates = {k: v for k, v in cache._data.items() if v.ttl is not None}
        if not candidates:
            return None
        return min(candidates.keys(), key=lambda k: candidates[k].created_at + candidates[k].ttl)


class TTLCache(Generic[T]):
    """In-memory cache with TTL and eviction support.

    Args:
        max_size: Maximum number of entries.
        default_ttl: Default TTL in seconds.
        eviction_policy: Policy for selecting entries to evict.
        on_evict: Callback when entry is evicted.
    """

    def __init__(
        self,
        max_size: int = 1000,
        default_ttl: float | None = 3600.0,
        eviction_policy: CacheEvictionPolicy | None = None,
        on_evict: Callable[[str, T], None] | None = None,
    ) -> None:
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.eviction_policy = eviction_policy or LRUEviction()
        self.on_evict = on_evict
        self._data: dict[str, CacheEntry[T]] = {}
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0

    def get(self, key: str, default: T | None = None) -> T | None:
        """Get value from cache.

        Args:
            key: Cache key.
            default: Default value if not found.

        Returns:
            Cached value or default.
        """
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                self._misses += 1
                return default

            if entry.is_expired():
                self._remove(key)
                self._misses += 1
                return default

            entry.touch()
            return entry.value

    def set(self, key: str, value: T, ttl: float | None = None) -> None:
        """Set value in cache.

        Args:
            key: Cache key.
            value: Value to cache.
            ttl: TTL in seconds (uses default if None).
        """
        with self._lock:
            if key in self._data:
                self._remove(key)

            if len(self._data) >= self.max_size:
                evict_key = self.eviction_policy.select_eviction(self)
                if evict_key:
                    old_value = self._data[evict_key].value
                    self._remove(evict_key)
                    if self.on_evict:
                        self.on_evict(evict_key, old_value)

            entry = CacheEntry(key=key, value=value, ttl=ttl or self.default_ttl)
            self._data[key] = entry

    def delete(self, key: str) -> bool:
        """Delete entry from cache.

        Args:
            key: Cache key.

        Returns:
            True if key was found and deleted.
        """
        with self._lock:
            return self._remove(key)

    def _remove(self, key: str) -> bool:
        """Remove entry without lock."""
        if key in self._data:
            del self._data[key]
            return True
        return False

    def clear(self) -> None:
        """Clear all entries."""
        with self._lock:
            self._data.clear()
            self._hits = 0
            self._misses = 0

    def cleanup(self) -> int:
        """Remove all expired entries.

        Returns:
            Number of entries removed.
        """
        with self._lock:
            expired = [k for k, v in self._data.items() if v.is_expired()]
            for key in expired:
                self._remove(key)
            return len(expired)

    def has(self, key: str) -> bool:
        """Check if key exists and is not expired."""
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return False
            if entry.is_expired():
                self._remove(key)
                return False
            return True

    def size(self) -> int:
        """Get current cache size."""
        return len(self._data)

    def stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        total = self._hits + self._misses
        return {
            "size": len(self._data),
            "max_size": self.max_size,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": self._hits / total if total > 0 else 0.0,
        }

    def get_or_compute(self, key: str, compute_fn: Callable[[], T], ttl: float | None = None) -> T:
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
        self.set(key, value, ttl)
        return value

    def invalidate_tag(self, tag: str) -> int:
        """Invalidate all entries with a given tag.

        Args:
            tag: Tag to match.

        Returns:
            Number of entries invalidated.
        """
        with self._lock:
            to_remove = [k for k, v in self._data.items() if tag in v.tags]
            for key in to_remove:
                self._remove(key)
            return len(to_remove)

    def get_many(self, keys: list[str]) -> dict[str, T]:
        """Get multiple values at once.

        Args:
            keys: List of cache keys.

        Returns:
            Dict of found key-value pairs.
        """
        result = {}
        for key in keys:
            value = self.get(key)
            if value is not None:
                result[key] = value
        return result

    def set_many(self, entries: dict[str, T], ttl: float | None = None) -> None:
        """Set multiple values at once.

        Args:
            entries: Dict of key-value pairs.
            ttl: TTL in seconds.
        """
        for key, value in entries.items():
            self.set(key, value, ttl)


class CacheKeyBuilder:
    """Build consistent cache keys from components."""

    def __init__(self, prefix: str = "") -> None:
        self.prefix = prefix
        self.parts: list[str] = []

    def add(self, *parts: Any) -> "CacheKeyBuilder":
        """Add parts to key."""
        for part in parts:
            if part is not None:
                self.parts.append(str(part))
        return self

    def add_dict(self, d: dict[str, Any], sort_keys: bool = True) -> "CacheKeyBuilder":
        """Add dict as JSON string."""
        parts = json.dumps(d, sort_keys=sort_keys, default=str)
        self.parts.append(parts)
        return self

    def build(self) -> str:
        """Build final cache key."""
        parts = [self.prefix] + self.parts if self.prefix else self.parts
        return ":".join(parts)

    def build_hash(self) -> str:
        """Build hashed cache key for long keys."""
        raw = self.build()
        if len(raw) > 200:
            h = hashlib.sha256(raw.encode()).hexdigest()
            return f"{self.prefix}:{h}" if self.prefix else h
        return raw


def cached(
    cache: TTLCache,
    key_builder: Callable[..., str] | None = None,
    ttl: float | None = None,
):
    """Decorator to cache function results.

    Usage:
        cache = TTLCache()

        @cached(cache, ttl=300)
        def expensive_func(arg1, arg2):
            return compute(arg1, arg2)
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args: Any, **kwargs: Any) -> T:
            if key_builder:
                cache_key = key_builder(*args, **kwargs)
            else:
                key_parts = [func.__name__] + [str(a) for a in args] + [f"{k}={v}" for k, v in sorted(kwargs.items())]
                cache_key = ":".join(key_parts)

            cached_value = cache.get(cache_key)
            if cached_value is not None:
                return cached_value

            result = func(*args, **kwargs)
            cache.set(cache_key, result, ttl)
            return result

        return wrapper

    return decorator


class TwoLevelCache(Generic[T]):
    """Two-level cache: local memory + distributed cache."""

    def __init__(self, local: TTLCache[T], distributed: "DistributedCache | None" = None) -> None:
        self.local = local
        self.distributed = distributed

    async def get(self, key: str, default: T | None = None) -> T | None:
        """Get from local cache first, then distributed."""
        value = self.local.get(key, default=None)
        if value is not None:
            return value

        if self.distributed:
            value = await self.distributed.get(key, default=None)
            if value is not None:
                self.local.set(key, value)
                return value

        return default

    async def set(self, key: str, value: T, ttl: float | None = None) -> None:
        """Set in both caches."""
        self.local.set(key, value, ttl)
        if self.distributed:
            await self.distributed.set(key, value, ttl)

    async def delete(self, key: str) -> None:
        """Delete from both caches."""
        self.local.delete(key)
        if self.distributed:
            await self.distributed.delete(key)


class DistributedCache(ABC):
    """Abstract distributed cache interface."""

    @abstractmethod
    async def get(self, key: str, default: T | None = None) -> T | None:
        pass

    @abstractmethod
    async def set(self, key: str, value: T, ttl: float | None = None) -> None:
        pass

    @abstractmethod
    async def delete(self, key: str) -> None:
        pass
