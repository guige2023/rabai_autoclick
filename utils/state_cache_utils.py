"""
State Cache Utilities

Provides utilities for caching UI state
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any
import time


class StateCache:
    """
    Caches UI state for quick access.
    
    Provides TTL-based caching with
    invalidation support.
    """

    def __init__(self, ttl_seconds: float = 30.0) -> None:
        self._ttl = ttl_seconds
        self._cache: dict[str, tuple[Any, float]] = {}

    def get(self, key: str) -> Any | None:
        """Get cached state."""
        if key in self._cache:
            value, timestamp = self._cache[key]
            if time.time() - timestamp < self._ttl:
                return value
            del self._cache[key]
        return None

    def set(self, key: str, value: Any) -> None:
        """Set cached state."""
        self._cache[key] = (value, time.time())

    def invalidate(self, key: str) -> None:
        """Invalidate cache entry."""
        self._cache.pop(key, None)

    def invalidate_prefix(self, prefix: str) -> None:
        """Invalidate all keys with prefix."""
        to_remove = [k for k in self._cache if k.startswith(prefix)]
        for k in to_remove:
            del self._cache[k]

    def clear(self) -> None:
        """Clear all cache."""
        self._cache.clear()

    def size(self) -> int:
        """Get cache size."""
        return len(self._cache)
