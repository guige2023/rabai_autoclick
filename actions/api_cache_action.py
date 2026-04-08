"""
API Cache Action Module.

Provides caching layer for API responses with TTL, invalidation,
 and cache-aside patterns for improved performance.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class CacheStrategy(Enum):
    """Caching strategy."""
    CACHE_ASIDE = "cache_aside"
    READ_THROUGH = "read_through"
    WRITE_THROUGH = "write_through"
    WRITE_BACK = "write_back"


@dataclass
class CacheEntry:
    """A single cache entry."""
    key: str
    value: Any
    created_at: float
    expires_at: float
    hit_count: int = 0
    size_bytes: int = 0


@dataclass
class CacheStats:
    """Cache statistics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    writes: int = 0
    hit_rate: float = 0.0
    size_bytes: int = 0
    item_count: int = 0


@dataclass
class CacheConfig:
    """Cache configuration."""
    max_size_bytes: int = 100 * 1024 * 1024
    max_items: int = 10000
    default_ttl: float = 300.0
    eviction_policy: str = "lru"
    enable_stats: bool = True


class APICacheAction:
    """
    API response caching with flexible strategies.

    Provides caching for API responses with TTL, invalidation,
    and multiple caching strategies for different use cases.

    Example:
        cache = APICacheAction(config=CacheConfig(default_ttl=600))
        cached_response = await cache.get_or_fetch("users", fetch_func)
    """

    def __init__(
        self,
        config: Optional[CacheConfig] = None,
    ) -> None:
        self.config = config or CacheConfig()
        self._cache: dict[str, CacheEntry] = {}
        self._access_order: list[str] = []
        self._stats = CacheStats()
        self._lock = asyncio.Lock()

    async def get(
        self,
        key: str,
    ) -> Optional[Any]:
        """Get a value from cache."""
        async with self._lock:
            entry = self._cache.get(key)

            if entry is None:
                self._stats.misses += 1
                self._update_hit_rate()
                return None

            if time.time() > entry.expires_at:
                del self._cache[key]
                self._stats.misses += 1
                self._update_hit_rate()
                return None

            entry.hit_count += 1
            self._stats.hits += 1
            self._update_hit_rate()
            self._update_access_order(key)

            return entry.value

    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
    ) -> None:
        """Set a value in cache."""
        import sys
        ttl = ttl or self.config.default_ttl

        size = self._estimate_size(value)
        now = time.time()

        entry = CacheEntry(
            key=key,
            value=value,
            created_at=now,
            expires_at=now + ttl,
            size_bytes=size,
        )

        async with self._lock:
            old_entry = self._cache.get(key)
            if old_entry:
                self._stats.size_bytes -= old_entry.size_bytes

            self._cache[key] = entry
            self._stats.size_bytes += size
            self._stats.writes += 1
            self._update_access_order(key)

            await self._evict_if_needed()

    async def delete(self, key: str) -> bool:
        """Delete a value from cache."""
        async with self._lock:
            if key in self._cache:
                entry = self._cache.pop(key)
                self._stats.size_bytes -= entry.size_bytes
                self._access_order.remove(key)
                return True
            return False

    async def clear(self) -> None:
        """Clear all cache entries."""
        async with self._lock:
            self._cache.clear()
            self._access_order.clear()
            self._stats = CacheStats()

    async def get_or_fetch(
        self,
        key: str,
        fetch_func: Callable[..., Any],
        ttl: Optional[float] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Get from cache or fetch and cache the result."""
        cached = await self.get(key)
        if cached is not None:
            return cached

        if asyncio.iscoroutinefunction(fetch_func):
            value = await fetch_func(*args, **kwargs)
        else:
            value = fetch_func(*args, **kwargs)

        await self.set(key, value, ttl)
        return value

    async def invalidate_pattern(
        self,
        pattern: str,
    ) -> int:
        """Invalidate all keys matching a pattern."""
        import re
        regex = self._pattern_to_regex(pattern)
        to_delete: list[str] = []

        async with self._lock:
            for key in self._cache.keys():
                if regex.match(key):
                    to_delete.append(key)

            for key in to_delete:
                entry = self._cache.pop(key, None)
                if entry:
                    self._stats.size_bytes -= entry.size_bytes
                    if key in self._access_order:
                        self._access_order.remove(key)

            return len(to_delete)

    async def refresh(self, key: str) -> bool:
        """Refresh the TTL of a cache entry."""
        async with self._lock:
            entry = self._cache.get(key)
            if entry:
                entry.expires_at = time.time() + self.config.default_ttl
                return True
            return False

    def get_stats(self) -> CacheStats:
        """Get cache statistics."""
        stats = self._stats
        stats.size_bytes = self._stats.size_bytes
        stats.item_count = len(self._cache)
        return stats

    def _update_hit_rate(self) -> None:
        """Update hit rate statistic."""
        total = self._stats.hits + self._stats.misses
        if total > 0:
            self._stats.hit_rate = self._stats.hits / total

    def _update_access_order(self, key: str) -> None:
        """Update access order for LRU."""
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)

    async def _evict_if_needed(self) -> None:
        """Evict entries if cache exceeds limits."""
        evicted = 0

        while (self._stats.size_bytes > self.config.max_size_bytes or
               len(self._cache) > self.config.max_items):

            if not self._access_order:
                break

            if self.config.eviction_policy == "lru":
                key_to_evict = self._access_order.pop(0)
            else:
                key_to_evict = self._access_order[0]

            entry = self._cache.pop(key_to_evict, None)
            if entry:
                self._stats.size_bytes -= entry.size_bytes
                self._stats.evictions += 1
                evicted += 1

            if evicted > 1000:
                break

    def _pattern_to_regex(self, pattern: str) -> re.Pattern:
        """Convert a glob pattern to regex."""
        import re
        regex_pattern = pattern.replace(".", r"\.").replace("*", ".*").replace("?", ".")
        return re.compile(f"^{regex_pattern}$")

    def _estimate_size(self, value: Any) -> int:
        """Estimate size of a value in bytes."""
        import sys
        try:
            return len(str(value).encode('utf-8'))
        except Exception:
            return 0

    def _generate_key(self, *args: Any, **kwargs: Any) -> str:
        """Generate a cache key from arguments."""
        key_parts = [str(a) for a in args]
        key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
        key_string = ":".join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()
