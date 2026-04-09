"""
UI state cache for efficient element retrieval.

Caches UI element states to reduce accessibility
API calls and improve performance.

Author: AutoClick Team
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class CachedElement:
    """
    A cached UI element with metadata.

    Attributes:
        element: The element data
        timestamp: When element was cached
        access_count: Number of times element was accessed
        hash_value: Element content hash for change detection
    """

    element: dict[str, Any]
    timestamp: float
    access_count: int = 0
    hash_value: str = ""


@dataclass
class CacheConfig:
    """Configuration for state cache behavior."""

    max_size: int = 500
    ttl_seconds: float = 5.0
    eviction_policy: str = "lru"
    stale_refresh: bool = True


class UIStateCache:
    """
    LRU cache for UI element states.

    Reduces redundant accessibility API calls by
    caching element data with automatic invalidation.

    Example:
        cache = UIStateCache(max_size=200, ttl_seconds=3.0)
        cache.put("btn_submit", element_data)

        cached = cache.get("btn_submit")
        if cached:
            use_element(cached)
        else:
            fresh = fetch_element("btn_submit")
            cache.put("btn_submit", fresh)
    """

    def __init__(self, config: CacheConfig | None = None) -> None:
        """
        Initialize UI state cache.

        Args:
            config: Cache configuration
        """
        self._config = config or CacheConfig()
        self._cache: dict[str, CachedElement] = {}
        self._access_order: list[str] = []

    def get(self, key: str, allow_stale: bool = True) -> dict[str, Any] | None:
        """
        Retrieve cached element.

        Args:
            key: Element cache key
            allow_stale: Return stale data if fresh unavailable

        Returns:
            Cached element or None
        """
        if key not in self._cache:
            return None

        cached = self._cache[key]

        if not allow_stale and self._is_stale(cached):
            return None

        cached.access_count += 1
        self._update_access_order(key)

        return cached.element

    def put(self, key: str, element: dict[str, Any]) -> None:
        """
        Store element in cache.

        Args:
            key: Cache key for element
            element: Element data to cache
        """
        if key in self._cache:
            cached = self._cache[key]
            cached.element = element
            cached.timestamp = time.time()
            self._update_access_order(key)
            return

        if len(self._cache) >= self._config.max_size:
            self._evict()

        hash_val = self._hash_element(element)

        self._cache[key] = CachedElement(
            element=element,
            timestamp=time.time(),
            hash_value=hash_val,
        )
        self._access_order.append(key)

    def invalidate(self, key: str) -> None:
        """Remove specific key from cache."""
        if key in self._cache:
            del self._cache[key]
            self._access_order.remove(key)

    def invalidate_pattern(self, pattern: str) -> None:
        """Invalidate all keys matching pattern."""
        keys_to_remove = [k for k in self._cache if pattern in k]
        for key in keys_to_remove:
            self.invalidate(key)

    def clear(self) -> None:
        """Clear entire cache."""
        self._cache.clear()
        self._access_order.clear()

    def refresh(self, key: str, element: dict[str, Any]) -> bool:
        """
        Force refresh of cached element.

        Args:
            key: Key to refresh
            element: Fresh element data

        Returns:
            True if key existed and was refreshed
        """
        if key not in self._cache:
            return False

        self.put(key, element)
        return True

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        total_accesses = sum(c.access_count for c in self._cache.values())
        return {
            "size": len(self._cache),
            "max_size": self._config.max_size,
            "total_accesses": total_accesses,
            "eviction_policy": self._config.eviction_policy,
        }

    def _is_stale(self, cached: CachedElement) -> bool:
        """Check if cached element has expired."""
        age = time.time() - cached.timestamp
        return age > self._config.ttl_seconds

    def _hash_element(self, element: dict[str, Any]) -> str:
        """Generate hash for element content."""
        import hashlib
        import json

        content = json.dumps(element, sort_keys=True)
        return hashlib.md5(content.encode()).hexdigest()

    def _update_access_order(self, key: str) -> None:
        """Update LRU access order."""
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)

    def _evict(self) -> None:
        """Evict least recently used element."""
        if not self._access_order:
            return

        if self._config.eviction_policy == "lru":
            oldest = self._access_order.pop(0)
            if oldest in self._cache:
                del self._cache[oldest]
        else:
            oldest_key = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].access_count,
            )
            del self._cache[oldest_key]
            self._access_order.remove(oldest_key)


def cached_property(
    ttl_seconds: float = 5.0,
) -> Callable:
    """
    Decorator for caching function results.

    Args:
        ttl_seconds: Time to live for cached values

    Returns:
        Decorated function with caching
    """
    def decorator(func: Callable) -> Callable:
        _cache: dict[str, tuple[Any, float]] = {}

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            cache_key = f"{func.__name__}:{str(args)}:{str(kwargs)}"

            if cache_key in _cache:
                result, timestamp = _cache[cache_key]
                if time.time() - timestamp < ttl_seconds:
                    return result

            result = func(*args, **kwargs)
            _cache[cache_key] = (result, time.time())
            return result

        return wrapper

    return decorator
