"""API Caching Action Module.

Provides intelligent caching layer for API responses with support for
TTL-based expiration, cache invalidation strategies, compression,
Etag support, and distributed cache backends.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
import zlib
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class CacheStrategy(Enum):
    """Cache invalidation strategies."""
    TTL = "ttl"
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    ADAPTIVE = "adaptive"


class CacheBackend(Enum):
    """Cache storage backend types."""
    MEMORY = "memory"
    DISK = "disk"
    DISTRIBUTED = "distributed"


@dataclass
class CacheEntry:
    """A cached response entry."""
    key: str
    value: Any
    created_at: datetime
    expires_at: Optional[datetime] = None
    last_accessed: datetime = field(default_factory=datetime.now)
    access_count: int = 0
    hit_count: int = 0
    etag: Optional[str] = None
    compressed: bool = False
    size_bytes: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CacheStats:
    """Cache performance statistics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0
    total_size_bytes: int = 0
    entry_count: int = 0
    hit_rate: float = 0.0
    avg_ttl_seconds: float = 0.0


@dataclass
class CacheConfig:
    """Configuration for API caching."""
    strategy: CacheStrategy = CacheStrategy.TTL
    backend: CacheBackend = CacheBackend.MEMORY
    max_entries: int = 1000
    max_size_mb: int = 100
    default_ttl_seconds: int = 300
    compression_enabled: bool = True
    compression_threshold_bytes: int = 1024
    use_etags: bool = True
    adaptive_ttl: bool = True
    stale_while_revalidate: bool = False
    stale_ttl_seconds: int = 60


class CacheKeyGenerator:
    """Generate cache keys from request parameters."""

    @staticmethod
    def generate(
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> str:
        """Generate a deterministic cache key."""
        key_parts = [method.upper(), path]

        if params:
            sorted_params = json.dumps(params, sort_keys=True)
            key_parts.append(sorted_params)

        if headers:
            cache_headers = {k: v for k, v in headers.items() if k.lower().startswith("accept")}
            if cache_headers:
                key_parts.append(json.dumps(cache_headers, sort_keys=True))

        key_string = "|".join(key_parts)
        return hashlib.sha256(key_string.encode()).hexdigest()[:32]

    @staticmethod
    def generate_etag(data: Any) -> str:
        """Generate ETag from response data."""
        try:
            serialized = json.dumps(data, sort_keys=True)
            return hashlib.md5(serialized.encode()).hexdigest()
        except Exception:
            return hashlib.md5(str(data).encode()).hexdigest()


class MemoryCache:
    """In-memory cache implementation."""

    def __init__(self, max_entries: int, max_size_bytes: int):
        self._max_entries = max_entries
        self._max_size_bytes = max_size_bytes
        self._entries: Dict[str, CacheEntry] = {}
        self._lock = threading.RLock()
        self._access_order: List[str] = []

    def get(self, key: str) -> Optional[CacheEntry]:
        """Get entry from cache."""
        with self._lock:
            if key not in self._entries:
                return None

            entry = self._entries[key]
            entry.last_accessed = datetime.now()
            entry.access_count += 1
            entry.hit_count += 1

            self._access_order.remove(key)
            self._access_order.append(key)

            return entry

    def set(self, key: str, entry: CacheEntry):
        """Set entry in cache."""
        with self._lock:
            if key in self._entries:
                old_entry = self._entries[key]
                self._evict_if_needed(entry.size_bytes - old_entry.size_bytes)
            else:
                self._evict_if_needed(entry.size_bytes)

            self._entries[key] = entry
            self._access_order.append(key)

    def delete(self, key: str) -> bool:
        """Delete entry from cache."""
        with self._lock:
            if key in self._entries:
                del self._entries[key]
                if key in self._access_order:
                    self._access_order.remove(key)
                return True
            return False

    def _evict_if_needed(self, additional_bytes: int = 0):
        """Evict entries if cache exceeds limits."""
        current_size = sum(e.size_bytes for e in self._entries.values())
        current_entries = len(self._entries)

        while (current_size + additional_bytes > self._max_size_bytes
               or current_entries + (1 if additional_bytes > 0 else 0) > self._max_entries):
            if not self._access_order:
                break
            oldest_key = self._access_order.pop(0)
            if oldest_key in self._entries:
                current_size -= self._entries[oldest_key].size_bytes
                del self._entries[oldest_key]
                current_entries -= 1

    def clear(self):
        """Clear all cache entries."""
        with self._lock:
            self._entries.clear()
            self._access_order.clear()

    def get_stats(self) -> CacheStats:
        """Get cache statistics."""
        with self._lock:
            total_hits = sum(e.hit_count for e in self._entries.values())
            total_requests = total_hits + len(self._entries)
            hit_rate = total_hits / total_requests if total_requests > 0 else 0.0

            return CacheStats(
                entry_count=len(self._entries),
                total_size_bytes=sum(e.size_bytes for e in self._entries.values()),
                hit_rate=hit_rate
            )

    def items(self) -> Dict[str, CacheEntry]:
        """Get all cache entries."""
        with self._lock:
            return dict(self._entries)


class ApiCachingAction(BaseAction):
    """Action for API response caching."""

    def __init__(self):
        super().__init__(name="api_caching")
        self._config = CacheConfig()
        self._cache = MemoryCache(
            self._config.max_entries,
            self._config.max_size_mb * 1024 * 1024
        )
        self._lock = threading.Lock()
        self._stats = CacheStats()

    def configure(self, config: CacheConfig):
        """Configure caching settings."""
        self._config = config
        self._cache = MemoryCache(
            config.max_entries,
            config.max_size_mb * 1024 * 1024
        )

    def get(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Tuple[Optional[Any], Optional[str], bool]:
        """Get cached response if available."""
        key = CacheKeyGenerator.generate(method, path, params, headers)

        entry = self._cache.get(key)
        if not entry:
            with self._lock:
                self._stats.misses += 1
            return None, None, False

        if entry.expires_at and datetime.now() > entry.expires_at:
            with self._lock:
                self._stats.expirations += 1
            self._cache.delete(key)
            return None, None, False

        with self._lock:
            self._stats.hits += 1

        value = entry.value
        if entry.compressed:
            try:
                value = json.loads(zlib.decompress(entry.value).decode())
            except Exception:
                pass

        return value, entry.etag, True

    def set(
        self,
        method: str,
        path: str,
        value: Any,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        ttl_seconds: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> ActionResult:
        """Cache a response."""
        try:
            key = CacheKeyGenerator.generate(method, path, params, headers)

            cached_value = value
            compressed = False

            if self._config.compression_enabled:
                try:
                    serialized = json.dumps(value)
                    if len(serialized) >= self._config.compression_threshold_bytes:
                        cached_value = zlib.compress(serialized.encode())
                        compressed = True
                except Exception:
                    pass

            size_bytes = len(str(cached_value))
            ttl = ttl_seconds or self._config.default_ttl_seconds
            expires_at = datetime.now() + timedelta(seconds=ttl)

            etag = None
            if self._config.use_etags:
                etag = CacheKeyGenerator.generate_etag(value)

            entry = CacheEntry(
                key=key,
                value=cached_value,
                created_at=datetime.now(),
                expires_at=expires_at,
                etag=etag,
                compressed=compressed,
                size_bytes=size_bytes,
                metadata=metadata or {}
            )

            self._cache.set(key, entry)

            return ActionResult(success=True, data={
                "key": key,
                "ttl_seconds": ttl,
                "compressed": compressed,
                "size_bytes": size_bytes
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def invalidate(
        self,
        pattern: Optional[str] = None,
        key: Optional[str] = None
    ) -> ActionResult:
        """Invalidate cache entries matching pattern or key."""
        try:
            deleted = 0

            if key:
                if self._cache.delete(key):
                    deleted = 1
            elif pattern:
                entries = self._cache.items()
                for k in list(entries.keys()):
                    if pattern in k:
                        if self._cache.delete(k):
                            deleted += 1

            return ActionResult(success=True, data={"deleted": deleted})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def invalidate_all(self) -> ActionResult:
        """Clear all cache entries."""
        try:
            self._cache.clear()
            return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def get_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics."""
        with self._lock:
            total = self._stats.hits + self._stats.misses
            hit_rate = self._stats.hits / total if total > 0 else 0.0

            entries = self._cache.items()
            total_size = sum(e.size_bytes for e in entries.values())

            return {
                "hits": self._stats.hits,
                "misses": self._stats.misses,
                "evictions": self._stats.evictions,
                "expirations": self._stats.expirations,
                "hit_rate": hit_rate,
                "entry_count": len(entries),
                "total_size_bytes": total_size,
                "strategy": self._config.strategy.value,
                "backend": self._config.backend.value
            }

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute caching action."""
        try:
            action = params.get("action", "get")

            if action == "get":
                value, etag, found = self.get(
                    params["method"],
                    params["path"],
                    params.get("params"),
                    params.get("headers")
                )
                return ActionResult(
                    success=found,
                    data={"value": value, "etag": etag, "cached": found}
                )
            elif action == "set":
                return self.set(
                    params["method"],
                    params["path"],
                    params["value"],
                    params.get("params"),
                    params.get("headers"),
                    params.get("ttl_seconds")
                )
            elif action == "invalidate":
                return self.invalidate(
                    params.get("pattern"),
                    params.get("key")
                )
            elif action == "stats":
                return ActionResult(success=True, data=self.get_stats())
            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))
