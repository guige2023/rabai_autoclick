"""
API response caching middleware with TTL and invalidation.

This module provides intelligent caching for API responses with support
for TTL-based expiration, manual invalidation, and cache-aside patterns.

Author: RabAiBot
License: MIT
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from collections import OrderedDict
import threading

logger = logging.getLogger(__name__)


class CacheStrategy(Enum):
    """Cache invalidation strategy."""
    LRU = auto()      # Least Recently Used
    LFU = auto()      # Least Frequently Used
    FIFO = auto()     # First In First Out
    TTL = auto()      # Time To Live based


@dataclass
class CacheEntry:
    """Represents a cached item."""
    key: str
    value: Any
    created_at: float
    last_accessed: float
    access_count: int = 0
    ttl: Optional[float] = None
    tags: List[str] = field(default_factory=list)
    size_bytes: int = 0

    @property
    def is_expired(self) -> bool:
        """Check if entry has expired."""
        if self.ttl is None:
            return False
        return time.time() - self.created_at > self.ttl

    @property
    def age(self) -> float:
        """Get age of entry in seconds."""
        return time.time() - self.created_at

    def touch(self) -> None:
        """Update access metadata."""
        self.last_accessed = time.time()
        self.access_count += 1


@dataclass
class CacheStats:
    """Cache statistics."""
    hits: int = 0
    misses: int = 0
    sets: int = 0
    evictions: int = 0
    expirations: int = 0
    invalidations: int = 0

    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate."""
        total = self.hits + self.misses
        return (self.hits / total) if total > 0 else 0.0


class Cache:
    """
    In-memory cache with multiple eviction strategies.

    Features:
    - LRU, LFU, FIFO, and TTL-based eviction
    - Configurable max size and TTL
    - Tag-based invalidation
    - Thread-safe operations
    - Statistics tracking
    - Async support

    Example:
        >>> cache = Cache(max_size=1000, default_ttl=300)
        >>> cache.set("user:123", user_data, tags=["user", "premium"])
        >>> data = cache.get("user:123")
        >>> cache.invalidate_tags("premium")
    """

    def __init__(
        self,
        max_size: int = 1000,
        default_ttl: Optional[float] = None,
        strategy: CacheStrategy = CacheStrategy.LRU,
        on_evict: Optional[Callable[[str, Any], None]] = None,
    ):
        """
        Initialize cache.

        Args:
            max_size: Maximum number of entries
            default_ttl: Default time-to-live in seconds
            strategy: Eviction strategy
            on_evict: Optional callback when entries are evicted
        """
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.strategy = strategy
        self.on_evict = on_evict

        self._store: OrderedDict[str, CacheEntry] = OrderedDict()
        self._tag_index: Dict[str, set] = {}
        self._lock = threading.RLock()
        self._stats = CacheStats()
        self._total_size = 0

        logger.info(
            f"Cache initialized (max_size={max_size}, ttl={default_ttl}s, "
            f"strategy={strategy.name})"
        )

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
        tags: Optional[List[str]] = None,
    ) -> None:
        """
        Set a cache entry.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Optional TTL override
            tags: Optional tags for invalidation
        """
        with self._lock:
            if key in self._store:
                old_entry = self._store[key]
                self._total_size -= old_entry.size_bytes
                if self.on_evict:
                    self.on_evict(key, old_entry.value)

            size = self._estimate_size(value)
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=time.time(),
                last_accessed=time.time(),
                ttl=ttl or self.default_ttl,
                tags=tags or [],
                size_bytes=size,
            )

            self._store[key] = entry
            self._total_size += size
            self._update_tag_index(key, entry.tags)
            self._stats.sets += 1

            if len(self._store) > self.max_size:
                self._evict_one()

            logger.debug(f"Cache SET: {key} (size={size}b, tags={entry.tags})")

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a cache entry.

        Args:
            key: Cache key
            default: Default value if not found

        Returns:
            Cached value or default
        """
        with self._lock:
            entry = self._store.get(key)

            if entry is None:
                self._stats.misses += 1
                logger.debug(f"Cache MISS: {key}")
                return default

            if entry.is_expired:
                self._remove_entry(key)
                self._stats.misses += 1
                self._stats.expirations += 1
                logger.debug(f"Cache EXPIRED: {key}")
                return default

            entry.touch()
            self._store.move_to_end(key)
            self._stats.hits += 1
            logger.debug(f"Cache HIT: {key}")
            return entry.value

    def get_or_compute(
        self,
        key: str,
        compute_fn: Callable[[], Any],
        ttl: Optional[float] = None,
        tags: Optional[List[str]] = None,
    ) -> Any:
        """
        Get from cache or compute and cache the value.

        Args:
            key: Cache key
            compute_fn: Function to compute value if not cached
            ttl: Optional TTL override
            tags: Optional tags for invalidation

        Returns:
            Cached or computed value
        """
        value = self.get(key)
        if value is not None:
            return value

        computed = compute_fn()
        self.set(key, computed, ttl=ttl, tags=tags)
        return computed

    async def get_or_compute_async(
        self,
        key: str,
        compute_fn: Callable[[], Any],
        ttl: Optional[float] = None,
        tags: Optional[List[str]] = None,
    ) -> Any:
        """Async version of get_or_compute."""
        value = self.get(key)
        if value is not None:
            return value

        loop = asyncio.get_event_loop()
        computed = await loop.run_in_executor(None, compute_fn)
        self.set(key, computed, ttl=ttl, tags=tags)
        return computed

    def delete(self, key: str) -> bool:
        """Delete a cache entry."""
        with self._lock:
            if key in self._store:
                self._remove_entry(key)
                logger.debug(f"Cache DELETE: {key}")
                return True
            return False

    def invalidate_tags(self, *tags: str) -> int:
        """
        Invalidate all entries with given tags.

        Args:
            *tags: Tags to invalidate

        Returns:
            Number of entries invalidated
        """
        with self._lock:
            keys_to_delete = set()
            for tag in tags:
                if tag in self._tag_index:
                    keys_to_delete.update(self._tag_index[tag])

            for key in keys_to_delete:
                self._remove_entry(key)
                self._stats.invalidations += 1

            logger.info(f"Cache INVALIDATE_TAGS: {tags} ({len(keys_to_delete)} entries)")
            return len(keys_to_delete)

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            count = len(self._store)
            if self.on_evict:
                for entry in self._store.values():
                    self.on_evict(entry.key, entry.value)

            self._store.clear()
            self._tag_index.clear()
            self._total_size = 0
            logger.info(f"Cache CLEARED: {count} entries")

    def _remove_entry(self, key: str) -> None:
        """Remove entry from cache."""
        if key in self._store:
            entry = self._store.pop(key)
            self._total_size -= entry.size_bytes

            for tag in entry.tags:
                if tag in self._tag_index:
                    self._tag_index[tag].discard(key)

    def _evict_one(self) -> None:
        """Evict one entry based on strategy."""
        if not self._store:
            return

        if self.strategy == CacheStrategy.LRU:
            key = next(iter(self._store))
        elif self.strategy == CacheStrategy.LFU:
            key = min(self._store, key=lambda k: self._store[k].access_count)
        elif self.strategy == CacheStrategy.FIFO:
            key = min(self._store, key=lambda k: self._store[k].created_at)
        else:
            key = next(iter(self._store))

        entry = self._store.pop(key)
        self._total_size -= entry.size_bytes
        self._stats.evictions += 1

        if self.on_evict:
            self.on_evict(entry.key, entry.value)

        for tag in entry.tags:
            if tag in self._tag_index:
                self._tag_index[tag].discard(key)

        logger.debug(f"Cache EVICT: {key} (strategy={self.strategy.name})")

    def _update_tag_index(self, key: str, tags: List[str]) -> None:
        """Update tag index for a key."""
        for tag in tags:
            if tag not in self._tag_index:
                self._tag_index[tag] = set()
            self._tag_index[tag].add(key)

    def _estimate_size(self, value: Any) -> int:
        """Estimate size of a value in bytes."""
        try:
            return len(json.dumps(value).encode("utf-8"))
        except Exception:
            return len(str(value).encode("utf-8"))

    def cleanup_expired(self) -> int:
        """
        Remove all expired entries.

        Returns:
            Number of entries removed
        """
        with self._lock:
            expired_keys = [
                key for key, entry in self._store.items()
                if entry.is_expired
            ]

            for key in expired_keys:
                self._remove_entry(key)
                self._stats.expirations += 1

            logger.info(f"Cache cleanup: removed {len(expired_keys)} expired entries")
            return len(expired_keys)

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            return {
                "size": len(self._store),
                "max_size": self.max_size,
                "total_bytes": self._total_size,
                "strategy": self.strategy.name,
                "default_ttl": self.default_ttl,
                "hits": self._stats.hits,
                "misses": self._stats.misses,
                "hit_rate": self._stats.hit_rate,
                "sets": self._stats.sets,
                "evictions": self._stats.evictions,
                "expirations": self._stats.expirations,
                "invalidations": self._stats.invalidations,
                "tags": len(self._tag_index),
            }

    def __len__(self) -> int:
        """Get number of entries."""
        return len(self._store)

    def __contains__(self, key: str) -> bool:
        """Check if key exists and is not expired."""
        return self.get(key) is not None


class CachedAPIClient:
    """
    API client wrapper with automatic caching.

    Example:
        >>> client = CachedAPIClient(base_url="https://api.example.com")
        >>> @client.cached(ttl=300, tags=["users"])
        ... def get_user(user_id):
        ...     return client.get(f"/users/{user_id}")
    """

    def __init__(
        self,
        cache: Optional[Cache] = None,
        cache_config: Optional[Dict[str, Any]] = None,
    ):
        """Initialize cached API client."""
        if cache:
            self.cache = cache
        else:
            config = cache_config or {}
            self.cache = Cache(
                max_size=config.get("max_size", 1000),
                default_ttl=config.get("default_ttl", 300),
                strategy=CacheStrategy[config.get("strategy", "LRU").upper()],
            )
        logger.info("CachedAPIClient initialized")

    def cached(
        self,
        key_prefix: str = "",
        ttl: Optional[float] = None,
        tags: Optional[List[str]] = None,
    ):
        """Decorator for caching function results."""
        def decorator(func: Callable):
            def wrapper(*args, **kwargs):
                cache_key = self._make_key(key_prefix or func.__name__, args, kwargs)
                return self.cache.get_or_compute(
                    cache_key,
                    lambda: func(*args, **kwargs),
                    ttl=ttl,
                    tags=tags,
                )
            return wrapper
        return decorator

    def _make_key(self, prefix: str, args: tuple, kwargs: dict) -> str:
        """Generate a cache key from function arguments."""
        key_parts = [prefix, str(args), str(sorted(kwargs.items()))]
        key_str = "|".join(key_parts)
        return hashlib.md5(key_str.encode()).hexdigest()
