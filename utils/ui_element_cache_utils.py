"""
UI element cache utilities for caching element information.

Provides element caching with TTL, LRU eviction,
and cache invalidation for UI automation.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class CachedElement:
    """A cached element with metadata."""
    element_id: str
    data: dict
    cached_at_ms: float
    access_count: int = 0
    last_access_ms: float = 0.0
    tags: set = field(default_factory=set)

    def is_expired(self, ttl_ms: float) -> bool:
        """Check if the cache entry has expired."""
        if ttl_ms <= 0:
            return False  # No TTL
        return (time.time() * 1000 - self.cached_at_ms) > ttl_ms


class ElementCache:
    """LRU cache for UI elements with TTL support."""

    def __init__(
        self,
        max_size: int = 1000,
        default_ttl_ms: float = 5000.0,
        on_evict: Optional[Callable[[str, dict], None]] = None,
    ):
        self.max_size = max_size
        self.default_ttl_ms = default_ttl_ms
        self.on_evict = on_evict
        self._cache: dict[str, CachedElement] = {}
        self._access_order: list[str] = []  # LRU tracking

    def put(
        self,
        element_id: str,
        data: dict,
        ttl_ms: Optional[float] = None,
        tags: Optional[set] = None,
    ) -> None:
        """Add or update an element in the cache."""
        now = time.time() * 1000

        if element_id in self._cache:
            # Update existing
            cached = self._cache[element_id]
            cached.data = data
            cached.cached_at_ms = now
            cached.last_access_ms = now
            cached.access_count += 1
            if tags is not None:
                cached.tags = tags
        else:
            # Add new
            if len(self._cache) >= self.max_size:
                self._evict_lru()

            self._cache[element_id] = CachedElement(
                element_id=element_id,
                data=data,
                cached_at_ms=now,
                last_access_ms=now,
                tags=tags or set(),
            )

        self._update_access_order(element_id)

    def get(
        self,
        element_id: str,
        ttl_ms: Optional[float] = None,
    ) -> Optional[dict]:
        """Get an element from the cache."""
        cached = self._cache.get(element_id)
        if not cached:
            return None

        ttl = ttl_ms if ttl_ms is not None else self.default_ttl_ms
        if cached.is_expired(ttl):
            self.remove(element_id)
            return None

        # Update access
        cached.last_access_ms = time.time() * 1000
        cached.access_count += 1
        self._update_access_order(element_id)

        return cached.data

    def get_or_compute(
        self,
        element_id: str,
        compute_fn: Callable[[], dict],
        ttl_ms: Optional[float] = None,
    ) -> dict:
        """Get from cache or compute if not present."""
        result = self.get(element_id, ttl_ms)
        if result is not None:
            return result

        result = compute_fn()
        self.put(element_id, result, ttl_ms)
        return result

    def remove(self, element_id: str) -> None:
        """Remove an element from the cache."""
        cached = self._cache.pop(element_id, None)
        if cached and self.on_evict:
            self.on_evict(element_id, cached.data)
        if element_id in self._access_order:
            self._access_order.remove(element_id)

    def invalidate_by_tags(self, tags: set) -> int:
        """Invalidate all cached elements with any of the given tags."""
        to_remove = []
        for element_id, cached in self._cache.items():
            if cached.tags & tags:  # Intersection
                to_remove.append(element_id)

        for element_id in to_remove:
            self.remove(element_id)

        return len(to_remove)

    def clear(self) -> None:
        """Clear all cached elements."""
        if self.on_evict:
            for element_id, cached in self._cache.items():
                self.on_evict(element_id, cached.data)
        self._cache.clear()
        self._access_order.clear()

    def _evict_lru(self) -> None:
        """Evict the least recently used element."""
        if not self._access_order:
            return

        lru_id = self._access_order.pop(0)
        cached = self._cache.pop(lru_id, None)
        if cached and self.on_evict:
            self.on_evict(lru_id, cached.data)

    def _update_access_order(self, element_id: str) -> None:
        """Update LRU order."""
        if element_id in self._access_order:
            self._access_order.remove(element_id)
        self._access_order.append(element_id)

    def size(self) -> int:
        """Get current cache size."""
        return len(self._cache)

    def stats(self) -> dict:
        """Get cache statistics."""
        total_access = sum(c.access_count for c in self._cache.values())
        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "total_accesses": total_access,
            "avg_access": total_access / len(self._cache) if self._cache else 0,
        }


__all__ = ["ElementCache", "CachedElement"]
