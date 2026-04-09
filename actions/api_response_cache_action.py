"""API response caching and invalidation action."""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Optional


class CacheStrategy(str, Enum):
    """Caching strategy."""

    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    TTL = "ttl"


@dataclass
class CacheEntry:
    """A cached response entry."""

    key: str
    value: Any
    created_at: float
    accessed_at: float
    access_count: int = 0
    size_bytes: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class CacheStats:
    """Cache statistics."""

    hits: int = 0
    misses: int = 0
    evictions: int = 0
    insertions: int = 0
    invalidations: int = 0
    total_size_bytes: int = 0


@dataclass
class CacheConfig:
    """Configuration for cache."""

    max_entries: int = 1000
    max_size_bytes: int = 100 * 1024 * 1024  # 100MB
    default_ttl_seconds: float = 300
    strategy: CacheStrategy = CacheStrategy.LRU
    on_evict: Optional[Callable[[str, Any], None]] = None


class APIResponseCacheAction:
    """Caches API responses with various strategies."""

    def __init__(self, config: Optional[CacheConfig] = None):
        """Initialize response cache.

        Args:
            config: Cache configuration.
        """
        self._config = config or CacheConfig()
        self._cache: dict[str, CacheEntry] = {}
        self._stats = CacheStats()
        self._access_order: list[str] = []
        self._access_counts: dict[str, int] = {}

    def _generate_key(
        self,
        method: str,
        url: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> str:
        """Generate cache key from request components."""
        key_parts = [method.upper(), url]
        if params:
            sorted_params = sorted(params.items())
            key_parts.append(str(sorted_params))
        if headers:
            relevant_headers = {
                k: v for k, v in headers.items() if k.lower() in ("authorization", "accept", "content-type")
            }
            key_parts.append(str(sorted(relevant_headers.items())))
        key_str = "|".join(key_parts)
        return hashlib.sha256(key_str.encode()).hexdigest()[:32]

    def _is_expired(self, entry: CacheEntry) -> bool:
        """Check if entry is expired."""
        age = time.time() - entry.created_at
        ttl = entry.metadata.get("ttl", self._config.default_ttl_seconds)
        return age > ttl

    def _evict_if_needed(self) -> None:
        """Evict entries if cache is full."""
        while len(self._cache) >= self._config.max_entries:
            self._evict_one()

        while self._stats.total_size_bytes >= self._config.max_size_bytes and self._cache:
            self._evict_one()

    def _evict_one(self) -> None:
        """Evict one entry based on strategy."""
        if not self._cache:
            return

        if self._config.strategy == CacheStrategy.LRU:
            if self._access_order:
                oldest = self._access_order.pop(0)
            else:
                oldest = next(iter(self._cache))

        elif self._config.strategy == CacheStrategy.LFU:
            oldest = min(self._access_counts, key=self._access_counts.get)
            self._access_counts.pop(oldest, None)

        elif self._config.strategy == CacheStrategy.FIFO:
            oldest = min(self._cache, key=lambda k: self._cache[k].created_at)

        else:  # TTL - evict oldest
            oldest = min(self._cache, key=lambda k: self._cache[k].created_at)

        entry = self._cache.pop(oldest, None)
        if entry:
            self._stats.total_size_bytes -= entry.size_bytes
            self._stats.evictions += 1
            if self._config.on_evict:
                self._config.on_evict(oldest, entry.value)

        if oldest in self._access_order:
            self._access_order.remove(oldest)

    def get(
        self,
        method: str,
        url: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> Optional[Any]:
        """Get cached response.

        Args:
            method: HTTP method.
            url: Request URL.
            params: Query parameters.
            headers: Request headers.

        Returns:
            Cached value or None.
        """
        key = self._generate_key(method, url, params, headers)
        entry = self._cache.get(key)

        if not entry:
            self._stats.misses += 1
            return None

        if self._is_expired(entry):
            self.invalidate(key)
            self._stats.misses += 1
            return None

        entry.accessed_at = time.time()
        entry.access_count += 1
        self._stats.hits += 1

        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)
        self._access_counts[key] = self._access_counts.get(key, 0) + 1

        return entry.value

    def put(
        self,
        method: str,
        url: str,
        value: Any,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
        ttl: Optional[float] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """Store response in cache.

        Args:
            method: HTTP method.
            url: Request URL.
            value: Response value.
            params: Query parameters.
            headers: Request headers.
            ttl: Time-to-live in seconds.
            metadata: Additional metadata.
        """
        key = self._generate_key(method, url, params, headers)

        if key in self._cache:
            old_entry = self._cache[key]
            self._stats.total_size_bytes -= old_entry.size_bytes

        import sys

        size = len(str(value).encode("utf-8"))

        entry = CacheEntry(
            key=key,
            value=value,
            created_at=time.time(),
            accessed_at=time.time(),
            size_bytes=size,
            metadata=metadata or {"ttl": ttl or self._config.default_ttl_seconds},
        )

        self._evict_if_needed()

        self._cache[key] = entry
        self._stats.total_size_bytes += size
        self._stats.insertions += 1

        if key not in self._access_order:
            self._access_order.append(key)
        self._access_counts[key] = 1

    def invalidate(self, key: str) -> bool:
        """Invalidate a cache entry."""
        entry = self._cache.pop(key, None)
        if entry:
            self._stats.total_size_bytes -= entry.size_bytes
            self._stats.invalidations += 1
            if key in self._access_order:
                self._access_order.remove(key)
            self._access_counts.pop(key, None)
            return True
        return False

    def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate all entries matching pattern."""
        count = 0
        keys_to_remove = [k for k in self._cache if pattern in k]
        for key in keys_to_remove:
            if self.invalidate(key):
                count += 1
        return count

    def invalidate_all(self) -> int:
        """Invalidate all cache entries."""
        count = len(self._cache)
        self._cache.clear()
        self._access_order.clear()
        self._access_counts.clear()
        self._stats.total_size_bytes = 0
        self._stats.invalidations += count
        return count

    def get_stats(self) -> CacheStats:
        """Get cache statistics."""
        total = self._stats.hits + self._stats.misses
        hit_rate = self._stats.hits / total if total > 0 else 0
        return CacheStats(
            hits=self._stats.hits,
            misses=self._stats.misses,
            evictions=self._stats.evictions,
            insertions=self._stats.insertions,
            invalidations=self._stats.invalidations,
            total_size_bytes=self._stats.total_size_bytes,
        )

    def get_hit_rate(self) -> float:
        """Get cache hit rate."""
        total = self._stats.hits + self._stats.misses
        return self._stats.hits / total if total > 0 else 0.0
