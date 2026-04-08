"""
Selector Cache Utilities

Provides utilities for caching selector lookups
in UI automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any
import time


class SelectorCache:
    """
    Caches selector lookup results.
    
    Provides TTL-based caching for
    element selector queries.
    """

    def __init__(self, ttl_seconds: float = 60.0) -> None:
        self._ttl = ttl_seconds
        self._cache: dict[str, tuple[Any, float]] = {}

    def get(self, selector: str) -> Any | None:
        """
        Get cached result for selector.
        
        Args:
            selector: Selector string.
            
        Returns:
            Cached result or None if expired/missing.
        """
        if selector in self._cache:
            result, timestamp = self._cache[selector]
            if time.time() - timestamp < self._ttl:
                return result
            else:
                del self._cache[selector]
        return None

    def set(self, selector: str, result: Any) -> None:
        """
        Cache result for selector.
        
        Args:
            selector: Selector string.
            result: Result to cache.
        """
        self._cache[selector] = (result, time.time())

    def invalidate(self, selector: str) -> None:
        """Invalidate cache entry."""
        self._cache.pop(selector, None)

    def clear(self) -> None:
        """Clear all cached entries."""
        self._cache.clear()

    def size(self) -> int:
        """Get number of cached entries."""
        return len(self._cache)
