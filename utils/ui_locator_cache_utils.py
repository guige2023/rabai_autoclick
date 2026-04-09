"""UI Locator Cache Utilities.

Caches UI element locators to speed up repeated element lookups.

Example:
    >>> from ui_locator_cache_utils import LocatorCache
    >>> cache = LocatorCache(ttl=60)
    >>> cache.put("btn_ok", element, selector="id:ok")
    >>> elem = cache.get("btn_ok")
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class CacheEntry:
    """A cached locator entry."""
    element: Any
    selector: str
    timestamp: float
    hit_count: int = 0


class LocatorCache:
    """LRU-style cache for UI element locators."""

    def __init__(self, max_size: int = 100, ttl: float = 60.0):
        """Initialize cache.

        Args:
            max_size: Maximum number of entries.
            ttl: Time-to-live in seconds.
        """
        self.max_size = max_size
        self.ttl = ttl
        self._cache: Dict[str, CacheEntry] = {}

    def put(self, key: str, element: Any, selector: str = "") -> None:
        """Store an element in the cache.

        Args:
            key: Cache key.
            element: Element to cache.
            selector: Selector used to find element.
        """
        if len(self._cache) >= self.max_size and key not in self._cache:
            self._evict_oldest()

        self._cache[key] = CacheEntry(
            element=element,
            selector=selector,
            timestamp=time.time(),
            hit_count=0,
        )

    def get(self, key: str) -> Optional[Any]:
        """Retrieve an element from cache.

        Args:
            key: Cache key.

        Returns:
            Cached element or None if not found/expired.
        """
        entry = self._cache.get(key)
        if entry is None:
            return None

        if time.time() - entry.timestamp > self.ttl:
            del self._cache[key]
            return None

        entry.hit_count += 1
        entry.timestamp = time.time()
        return entry.element

    def invalidate(self, key: str) -> None:
        """Remove an entry from cache.

        Args:
            key: Cache key to invalidate.
        """
        self._cache.pop(key, None)

    def clear(self) -> None:
        """Clear all cached entries."""
        self._cache.clear()

    def _evict_oldest(self) -> None:
        """Evict the oldest entry."""
        if not self._cache:
            return
        oldest_key = min(self._cache, key=lambda k: self._cache[k].timestamp)
        del self._cache[oldest_key]

    def stats(self) -> Dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dict with cache stats.
        """
        if not self._cache:
            return {"size": 0, "total_hits": 0}
        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "total_hits": sum(e.hit_count for e in self._cache.values()),
        }
