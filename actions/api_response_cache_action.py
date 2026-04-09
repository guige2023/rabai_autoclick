"""API Response Cache Action.

Caches API responses with TTL, LRU eviction, compression,
invalidation patterns, and cache-aside/read-through support.
"""
from __future__ import annotations

import hashlib
import json
import time
import zlib
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class CacheStrategy(Enum):
    """Cache strategies."""
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
    last_accessed: float
    ttl: Optional[float] = None
    hit_count: int = 0
    size_bytes: int = 0
    compressed: bool = False


@dataclass
class CacheStats:
    """Cache statistics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0
    writes: int = 0
    current_size: int = 0
    max_size: int = 0


class APIResponseCacheAction:
    """High-performance cache for API responses."""

    def __init__(
        self,
        max_size: int = 1000,
        default_ttl: float = 300.0,
        compression_threshold: int = 1024,
        strategy: CacheStrategy = CacheStrategy.CACHE_ASIDE,
    ) -> None:
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.compression_threshold = compression_threshold
        self.strategy = strategy

        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._stats = CacheStats(max_size=max_size)
        self._lock = __import__("threading").Lock()
        self._write_buffer: Dict[str, Any] = {}

    def get(self, key: str) -> Optional[Any]:
        """Get a value from cache."""
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                self._stats.misses += 1
                return None

            if self._is_expired(entry):
                del self._cache[key]
                self._stats.expirations += 1
                self._stats.misses += 1
                return None

            entry.last_accessed = time.time()
            entry.hit_count += 1
            self._cache.move_to_end(key)
            self._stats.hits += 1

            return self._decompress(entry.value, entry.compressed)

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
    ) -> None:
        """Set a value in cache."""
        with self._lock:
            compressed = False
            serialized = self._serialize(value)
            size = len(serialized)

            if size > self.compression_threshold:
                compressed_value = zlib.compress(serialized)
                if len(compressed_value) < size:
                    serialized = compressed_value
                    compressed = True

            entry = CacheEntry(
                key=key,
                value=serialized,
                created_at=time.time(),
                last_accessed=time.time(),
                ttl=ttl or self.default_ttl,
                size_bytes=len(serialized),
                compressed=compressed,
            )

            if key in self._cache:
                del self._cache[key]

            while len(self._cache) >= self.max_size:
                self._evict_lru()

            self._cache[key] = entry
            self._stats.writes += 1
            self._update_size()

    def delete(self, key: str) -> bool:
        """Delete a key from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                self._update_size()
                return True
            return False

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()
            self._write_buffer.clear()
            self._update_size()

    def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate all keys matching a pattern."""
        import re
        compiled = re.compile(pattern)
        with self._lock:
            to_delete = [k for k in self._cache if compiled.search(k)]
            for k in to_delete:
                del self._cache[k]
            self._update_size()
            return len(to_delete)

    def get_or_compute(
        self,
        key: str,
        compute_fn: Callable[[], Any],
        ttl: Optional[float] = None,
    ) -> Any:
        """Get from cache or compute and store."""
        value = self.get(key)
        if value is not None:
            return value

        value = compute_fn()

        if self.strategy == CacheStrategy.CACHE_ASIDE:
            self.set(key, value, ttl)
        elif self.strategy == CacheStrategy.READ_THROUGH:
            self.set(key, value, ttl)

        return value

    def _is_expired(self, entry: CacheEntry) -> bool:
        """Check if an entry has expired."""
        if entry.ttl is None:
            return False
        return (time.time() - entry.created_at) > entry.ttl

    def _evict_lru(self) -> None:
        """Evict the least recently used entry."""
        if self._cache:
            self._cache.popitem(last=False)
            self._stats.evictions += 1

    def _serialize(self, value: Any) -> bytes:
        """Serialize a value to bytes."""
        return json.dumps(value, default=str).encode("utf-8")

    def _deserialize(self, data: bytes) -> Any:
        """Deserialize bytes to a value."""
        return json.loads(data.decode("utf-8"))

    def _decompress(self, data: Any, compressed: bool) -> Any:
        """Decompress if needed and deserialize."""
        if compressed:
            data = zlib.decompress(data)
        return self._deserialize(data)

    def _update_size(self) -> None:
        """Update current cache size."""
        self._stats.current_size = len(self._cache)

    def get_stats(self) -> CacheStats:
        """Get cache statistics."""
        with self._lock:
            hit_rate = 0.0
            total = self._stats.hits + self._stats.misses
            if total > 0:
                hit_rate = self._stats.hits / total
            return CacheStats(
                hits=self._stats.hits,
                misses=self._stats.misses,
                evictions=self._stats.evictions,
                expirations=self._stats.expirations,
                writes=self._stats.writes,
                current_size=self._stats.current_size,
                max_size=self._stats.max_size,
            )

    def get_hit_rate(self) -> float:
        """Get cache hit rate."""
        stats = self.get_stats()
        total = stats.hits + stats.misses
        if total == 0:
            return 0.0
        return stats.hits / total

    def cleanup_expired(self) -> int:
        """Remove all expired entries. Returns count removed."""
        with self._lock:
            now = time.time()
            expired_keys = [
                k for k, entry in self._cache.items()
                if entry.ttl and (now - entry.created_at) > entry.ttl
            ]
            for k in expired_keys:
                del self._cache[k]
                self._stats.expirations += 1
            self._update_size()
            return len(expired_keys)
