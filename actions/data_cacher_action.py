"""Data Cacher Action Module.

Provides multi-tier caching with memory, disk, and TTL-based
eviction policies for data objects.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Optional, TypeVar
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class CacheEntry:
    """Cache entry with metadata."""
    key: str
    value: Any
    created_at: float
    accessed_at: float
    ttl: Optional[float] = None
    hits: int = 0
    size_bytes: int = 0


class DataCacherAction:
    """Multi-tier data cacher.

    Example:
        cacher = DataCacherAction(
            max_memory_mb=100,
            disk_cache_dir="/tmp/cache"
        )

        value = cacher.get_or_compute(
            "key",
            compute_fn=lambda: expensive_operation()
        )
    """

    def __init__(
        self,
        max_memory_items: int = 1000,
        max_memory_mb: float = 100.0,
        disk_cache_dir: Optional[str] = None,
        default_ttl: float = 3600.0,
        eviction_policy: str = "lru",
    ) -> None:
        self.max_memory_items = max_memory_items
        self.max_memory_bytes = int(max_memory_mb * 1024 * 1024)
        self.disk_cache_dir = disk_cache_dir
        self.default_ttl = default_ttl
        self.eviction_policy = eviction_policy

        self._memory_cache: Dict[str, CacheEntry] = {}
        self._access_order: list = []
        self._current_size_bytes = 0
        self._hits = 0
        self._misses = 0

        if disk_cache_dir:
            Path(disk_cache_dir).mkdir(parents=True, exist_ok=True)

    def get(
        self,
        key: str,
        default: Optional[Any] = None,
    ) -> Optional[Any]:
        """Get value from cache.

        Args:
            key: Cache key
            default: Default value if not found

        Returns:
            Cached value or default
        """
        entry = self._memory_cache.get(key)

        if entry and self._is_valid(entry):
            entry.accessed_at = time.time()
            entry.hits += 1
            self._hits += 1
            self._touch_access(key)
            return entry.value

        if self.disk_cache_dir:
            disk_value = self._read_from_disk(key)
            if disk_value is not None:
                self._hits += 1
                self.set(key, disk_value)
                return disk_value

        self._misses += 1
        return default

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
    ) -> None:
        """Set value in cache.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds
        """
        now = time.time()
        size = self._estimate_size(value)

        if size > self.max_memory_bytes:
            logger.warning(f"Value too large for memory cache: {size} bytes")
            if self.disk_cache_dir:
                self._write_to_disk(key, value)
            return

        while (
            len(self._memory_cache) >= self.max_memory_items
            or self._current_size_bytes + size > self.max_memory_bytes
        ):
            self._evict()

        entry = CacheEntry(
            key=key,
            value=value,
            created_at=now,
            accessed_at=now,
            ttl=ttl or self.default_ttl,
            size_bytes=size,
        )

        self._memory_cache[key] = entry
        self._current_size_bytes += size
        self._touch_access(key)

        if self.disk_cache_dir:
            self._write_to_disk(key, value)

    def get_or_compute(
        self,
        key: str,
        compute_fn: Callable[[], T],
        ttl: Optional[float] = None,
    ) -> T:
        """Get from cache or compute if not present.

        Args:
            key: Cache key
            compute_fn: Function to compute value if not cached
            ttl: Optional TTL override

        Returns:
            Cached or computed value
        """
        value = self.get(key)
        if value is not None:
            return value

        computed = compute_fn()
        self.set(key, computed, ttl)
        return computed

    def delete(self, key: str) -> bool:
        """Delete key from cache.

        Returns:
            True if key was deleted, False if not found
        """
        if key in self._memory_cache:
            entry = self._memory_cache.pop(key)
            self._current_size_bytes -= entry.size_bytes
            if key in self._access_order:
                self._access_order.remove(key)
            return True

        if self.disk_cache_dir:
            disk_path = self._disk_path(key)
            if disk_path.exists():
                disk_path.unlink()
                return True

        return False

    def clear(self) -> None:
        """Clear all cache entries."""
        self._memory_cache.clear()
        self._access_order.clear()
        self._current_size_bytes = 0

        if self.disk_cache_dir:
            for path in Path(self.disk_cache_dir).glob("*.cache"):
                path.unlink()

    def _is_valid(self, entry: CacheEntry) -> bool:
        """Check if entry is still valid."""
        if entry.ttl is None:
            return True
        return time.time() - entry.created_at < entry.ttl

    def _evict(self) -> None:
        """Evict entry based on policy."""
        if not self._memory_cache:
            return

        if self.eviction_policy == "lru":
            key = self._access_order.pop(0)
        elif self.eviction_policy == "fifo":
            key = next(iter(self._memory_cache))
        elif self.eviction_policy == "lfu":
            key = min(self._memory_cache, key=lambda k: self._memory_cache[k].hits)
        else:
            key = next(iter(self._memory_cache))

        entry = self._memory_cache.pop(key, None)
        if entry:
            self._current_size_bytes -= entry.size_bytes

    def _touch_access(self, key: str) -> None:
        """Update access order."""
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)

    def _estimate_size(self, value: Any) -> int:
        """Estimate size of value in bytes."""
        try:
            return len(json.dumps(value).encode())
        except:
            return 64

    def _disk_path(self, key: str) -> Path:
        """Get disk path for key."""
        key_hash = hashlib.sha256(key.encode()).hexdigest()
        return Path(self.disk_cache_dir) / f"{key_hash}.cache"

    def _write_to_disk(self, key: str, value: Any) -> None:
        """Write value to disk cache."""
        try:
            path = self._disk_path(key)
            with open(path, "w") as f:
                json.dump(value, f)
        except Exception as e:
            logger.error(f"Failed to write to disk cache: {e}")

    def _read_from_disk(self, key: str) -> Optional[Any]:
        """Read value from disk cache."""
        try:
            path = self._disk_path(key)
            if path.exists():
                with open(path) as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Failed to read from disk cache: {e}")
        return None

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_requests = self._hits + self._misses
        hit_rate = self._hits / total_requests if total_requests > 0 else 0.0

        return {
            "memory_items": len(self._memory_cache),
            "memory_bytes": self._current_size_bytes,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": hit_rate,
            "eviction_policy": self.eviction_policy,
        }
