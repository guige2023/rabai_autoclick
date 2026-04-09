"""API Response Cache Action Module.

Cache API responses with invalidation and TTL support.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Generic, TypeVar

T = TypeVar("T")


@dataclass
class CacheEntry:
    """Cache entry."""
    key: str
    value: Any
    created_at: float
    expires_at: float | None = None
    tags: list[str] = field(default_factory=list)
    hit_count: int = 0


@dataclass
class CacheConfig:
    """Cache configuration."""
    max_size: int = 1000
    default_ttl: float = 300.0
    eviction_policy: str = "lru"


class APIResponseCache(Generic[T]):
    """Cache for API responses."""

    def __init__(self, config: CacheConfig | None = None) -> None:
        self.config = config or CacheConfig()
        self._cache: dict[str, CacheEntry] = {}
        self._access_order: deque[str] = deque()
        self._lock = asyncio.Lock()

    def generate_key(
        self,
        method: str,
        path: str,
        query_params: dict | None = None,
        headers: dict | None = None
    ) -> str:
        """Generate cache key from request."""
        content = {
            "method": method.upper(),
            "path": path,
            "query": sorted(query_params.items()) if query_params else [],
            "headers": {k: v for k, v in (headers or {}).items() if k.lower().startswith("accept")}
        }
        json_str = json.dumps(content, sort_keys=True)
        return hashlib.sha256(json_str.encode()).hexdigest()

    async def get(self, key: str) -> tuple[Any | None, bool]:
        """Get value from cache. Returns (value, found)."""
        async with self._lock:
            entry = self._cache.get(key)
            if not entry:
                return None, False
            if entry.expires_at and time.time() > entry.expires_at:
                del self._cache[key]
                return None, False
            entry.hit_count += 1
            self._access_order.append(key)
            return entry.value, True

    async def set(
        self,
        key: str,
        value: Any,
        ttl: float | None = None,
        tags: list[str] | None = None
    ) -> None:
        """Set value in cache."""
        async with self._lock:
            if len(self._cache) >= self.config.max_size:
                await self._evict()
            expires_at = time.time() + (ttl or self.config.default_ttl)
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=time.time(),
                expires_at=expires_at,
                tags=tags or []
            )
            self._cache[key] = entry
            self._access_order.append(key)

    async def invalidate(self, key: str) -> bool:
        """Invalidate a cache entry."""
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    async def invalidate_by_tag(self, tag: str) -> int:
        """Invalidate all entries with a tag."""
        async with self._lock:
            to_remove = [k for k, v in self._cache.items() if tag in v.tags]
            for k in to_remove:
                del self._cache[k]
            return len(to_remove)

    async def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate entries matching pattern."""
        import fnmatch
        async with self._lock:
            to_remove = [k for k in self._cache.keys() if fnmatch.fnmatch(k, pattern)]
            for k in to_remove:
                del self._cache[k]
            return len(to_remove)

    async def clear(self) -> None:
        """Clear entire cache."""
        async with self._lock:
            self._cache.clear()
            self._access_order.clear()

    async def _evict(self) -> None:
        """Evict LRU entry."""
        if self._access_order:
            oldest = self._access_order.popleft()
            self._cache.pop(oldest, None)

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        total_hits = sum(e.hit_count for e in self._cache.values())
        return {
            "size": len(self._cache),
            "max_size": self.config.max_size,
            "total_hits": total_hits,
        }
