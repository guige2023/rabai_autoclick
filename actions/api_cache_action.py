"""
API Cache Action Module.

Caches API responses with TTL, invalidation policies,
stale-while-revalidate, and distributed cache support.

Author: RabAi Team
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple


class CacheStrategy(Enum):
    """Cache invalidation strategies."""
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    TTL = "ttl"
    WRITE_THROUGH = "write_through"
    WRITE_BACK = "write_back"


@dataclass
class CacheEntry:
    """A single cache entry."""
    key: str
    value: Any
    created_at: float
    last_accessed: float
    access_count: int = 0
    size_bytes: int = 0
    ttl_seconds: Optional[float] = None
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_expired(self) -> bool:
        if self.ttl_seconds is None:
            return False
        return time.time() - self.created_at > self.ttl_seconds

    @property
    def is_stale(self) -> bool:
        if self.ttl_seconds is None:
            return False
        return time.time() - self.created_at > self.ttl_seconds * 0.8


@dataclass
class CacheStats:
    """Cache statistics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0
    writes: int = 0
    deletes: int = 0

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0


class APICache:
    """
    API response caching with multiple strategies.

    Supports LRU, LFU, TTL-based eviction, stale-while-revalidate,
    and thread-safe in-memory caching.

    Example:
        >>> cache = APICache(max_size=1000, default_ttl=300)
        >>> cached = cache.get("api:users:123")
        >>> if cached is None:
        >>>     data = api_call()
        >>>     cache.set("api:users:123", data)
    """

    def __init__(
        self,
        max_size: int = 1000,
        default_ttl: Optional[float] = None,
        strategy: CacheStrategy = CacheStrategy.LRU,
        stale_while_revalidate: bool = True,
    ):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.strategy = strategy
        self.stale_while_revalidate = stale_while_revalidate

        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = threading.RLock()
        self._stats = CacheStats()
        self._access_counts: Dict[str, int] = {}

    def get(self, key: str) -> Tuple[Optional[Any], bool]:
        """
        Get value from cache.

        Returns:
            Tuple of (value, is_stale)
            is_stale=True means value exists but should be refreshed
        """
        with self._lock:
            if key not in self._cache:
                self._stats.misses += 1
                return None, False

            entry = self._cache[key]

            if entry.is_expired:
                del self._cache[key]
                self._stats.expirations += 1
                self._stats.misses += 1
                return None, False

            entry.last_accessed = time.time()
            entry.access_count += 1
            self._stats.hits += 1

            if self.strategy == CacheStrategy.LRU:
                self._cache.move_to_end(key)
            elif self.strategy == CacheStrategy.LFU:
                self._access_counts[key] = entry.access_count

            return entry.value, entry.is_stale

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
        tags: Optional[List[str]] = None,
    ) -> None:
        """Set value in cache."""
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            else:
                if len(self._cache) >= self.max_size:
                    self._evict()

            entry = CacheEntry(
                key=key,
                value=value,
                created_at=time.time(),
                last_accessed=time.time(),
                ttl_seconds=ttl or self.default_ttl,
                tags=tags or [],
            )

            self._cache[key] = entry
            self._stats.writes += 1

    def delete(self, key: str) -> bool:
        """Delete a key from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                self._stats.deletes += 1
                return True
            return False

    def invalidate_by_tags(self, tags: List[str]) -> int:
        """Invalidate all entries with matching tags."""
        count = 0
        with self._lock:
            keys_to_delete = [
                key for key, entry in self._cache.items()
                if any(tag in entry.tags for tag in tags)
            ]
            for key in keys_to_delete:
                del self._cache[key]
                count += 1
            self._stats.deletes += count
        return count

    def clear(self) -> int:
        """Clear all cache entries."""
        count = len(self._cache)
        with self._lock:
            self._cache.clear()
            self._access_counts.clear()
        return count

    def get_or_compute(
        self,
        key: str,
        compute_fn: Callable[[], Any],
        ttl: Optional[float] = None,
        tags: Optional[List[str]] = None,
    ) -> Any:
        """Get from cache or compute and cache if missing."""
        value, is_stale = self.get(key)

        if value is not None and not is_stale:
            return value

        if value is not None and is_stale and self.stale_while_revalidate:
            thread = threading.Thread(target=lambda: self.set(key, compute_fn(), ttl, tags))
            thread.start()
            return value

        computed = compute_fn()
        self.set(key, computed, ttl, tags)
        return computed

    def cached_call(
        self,
        key_prefix: str,
        ttl: Optional[float] = None,
    ) -> Callable:
        """Decorator for caching function calls."""
        def decorator(fn: Callable) -> Callable:
            def wrapper(*args, **kwargs):
                key = f"{key_prefix}:{hashlib.md5(str(args).encode()).hexdigest()}"
                return self.get_or_compute(key, lambda: fn(*args, **kwargs), ttl)
            return wrapper
        return decorator

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            return {
                "size": len(self._cache),
                "max_size": self.max_size,
                "hit_rate": self._stats.hit_rate,
                "hits": self._stats.hits,
                "misses": self._stats.misses,
                "evictions": self._stats.evictions,
                "expirations": self._stats.expirations,
                "writes": self._stats.writes,
                "deletes": self._stats.deletes,
                "strategy": self.strategy.value,
            }

    def _evict(self) -> None:
        """Evict entry based on strategy."""
        if not self._cache:
            return

        if self.strategy == CacheStrategy.LRU or self.strategy == CacheStrategy.FIFO:
            self._cache.popitem(last=False)
        elif self.strategy == CacheStrategy.LFU:
            if self._access_counts:
                min_key = min(self._access_counts, key=self._access_counts.get)
                del self._cache[min_key]
                del self._access_counts[min_key]
        elif self.strategy == CacheStrategy.TTL:
            now = time.time()
            expired_keys = [
                k for k, e in self._cache.items()
                if e.ttl_seconds and now - e.created_at > e.ttl_seconds
            ]
            for key in expired_keys:
                del self._cache[key]
                self._stats.expirations += 1

        self._stats.evictions += 1


def create_api_cache(
    max_size: int = 1000,
    ttl: float = 300,
    strategy: str = "lru",
) -> APICache:
    """Factory to create an API cache."""
    return APICache(
        max_size=max_size,
        default_ttl=ttl,
        strategy=CacheStrategy(strategy),
    )
