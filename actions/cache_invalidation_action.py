"""Cache Invalidation Action Module.

Multi-strategy cache invalidation with dependency tracking.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class InvalidationStrategy(Enum):
    """Cache invalidation strategies."""
    IMMEDIATE = "immediate"
    LAZY = "lazy"
    TTL = "ttl"
    DEPENDENCY = "dependency"


@dataclass
class CacheEntry(Generic[T]):
    """Cache entry with metadata."""
    key: str
    value: T
    created_at: float
    accessed_at: float
    access_count: int = 0
    ttl_seconds: float | None = None
    tags: set[str] = field(default_factory=set)


class CacheInvalidationError(Exception):
    """Cache invalidation error."""
    pass


class CacheManager:
    """Cache with multi-strategy invalidation."""

    def __init__(self, default_ttl: float | None = 300.0) -> None:
        self._cache: dict[str, CacheEntry] = {}
        self._index_by_tag: dict[str, set[str]] = defaultdict(set)
        self._dependencies: dict[str, set[str]] = defaultdict(set)
        self._default_ttl = default_ttl
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> tuple[T | None, bool]:
        """Get value from cache. Returns (value, found)."""
        async with self._lock:
            entry = self._cache.get(key)
            if not entry:
                return None, False
            if entry.ttl_seconds:
                if time.time() - entry.created_at > entry.ttl_seconds:
                    await self._evict(key)
                    return None, False
            entry.accessed_at = time.time()
            entry.access_count += 1
            return entry.value, True

    async def set(
        self,
        key: str,
        value: T,
        ttl: float | None = None,
        tags: list[str] | None = None
    ) -> None:
        """Set value in cache."""
        async with self._lock:
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=time.time(),
                accessed_at=time.time(),
                ttl_seconds=ttl if ttl is not None else self._default_ttl,
                tags=set(tags) if tags else set()
            )
            self._cache[key] = entry
            if tags:
                for tag in tags:
                    self._index_by_tag[tag].add(key)

    async def invalidate(self, key: str) -> bool:
        """Invalidate a single cache entry."""
        async with self._lock:
            return await self._evict(key)

    async def invalidate_by_tag(self, tag: str) -> int:
        """Invalidate all entries with a tag."""
        async with self._lock:
            keys = list(self._index_by_tag.get(tag, set()))
            for key in keys:
                await self._evict(key)
            return len(keys)

    async def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate entries matching a key pattern."""
        import fnmatch
        async with self._lock:
            keys = [k for k in self._cache.keys() if fnmatch.fnmatch(k, pattern)]
            for key in keys:
                await self._evict(key)
            return len(keys)

    async def invalidate_dependencies(self, key: str) -> int:
        """Invalidate all entries that depend on a key."""
        async with self._lock:
            affected = list(self._dependencies.get(key, set()))
            for k in affected:
                await self._evict(k)
            return len(affected)

    async def add_dependency(self, dependent: str, dependency: str) -> None:
        """Add dependency relationship between keys."""
        async with self._lock:
            self._dependencies[dependency].add(dependent)

    async def clear(self) -> None:
        """Clear entire cache."""
        async with self._lock:
            self._cache.clear()
            self._index_by_tag.clear()
            self._dependencies.clear()

    async def _evict(self, key: str) -> bool:
        """Evict a cache entry."""
        if key in self._cache:
            entry = self._cache.pop(key)
            for tag in entry.tags:
                self._index_by_tag.get(tag, set()).discard(key)
            return True
        return False

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        total_accesses = sum(e.access_count for e in self._cache.values())
        return {
            "size": len(self._cache),
            "total_accesses": total_accesses,
            "tags": len(self._index_by_tag),
            "dependencies": len(self._dependencies),
        }
