"""
Data Cache Action - Caches data for improved performance.

This module provides data caching capabilities including
TTL-based expiration, cache invalidation, and cache warming.
"""

from __future__ import annotations

import time
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class CacheEntry:
    """A cache entry."""
    key: str
    value: Any
    created_at: float
    expires_at: float | None = None
    hit_count: int = 0


@dataclass
class CacheStats:
    """Cache statistics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    size: int = 0


class DataCache:
    """In-memory data cache."""
    
    def __init__(self, default_ttl: float = 300.0, max_size: int = 1000) -> None:
        self.default_ttl = default_ttl
        self.max_size = max_size
        self._cache: dict[str, CacheEntry] = {}
        self._stats = CacheStats()
    
    def _make_key(self, data: Any) -> str:
        """Generate cache key from data."""
        content = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(content.encode()).hexdigest()
    
    def get(self, key: str) -> Any | None:
        """Get value from cache."""
        if key not in self._cache:
            self._stats.misses += 1
            return None
        
        entry = self._cache[key]
        
        if entry.expires_at and time.time() > entry.expires_at:
            del self._cache[key]
            self._stats.misses += 1
            self._stats.evictions += 1
            return None
        
        entry.hit_count += 1
        self._stats.hits += 1
        return entry.value
    
    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        """Set value in cache."""
        if len(self._cache) >= self.max_size:
            self._evict_oldest()
        
        expires_at = None
        if ttl is not None:
            expires_at = time.time() + ttl
        elif self.default_ttl > 0:
            expires_at = time.time() + self.default_ttl
        
        self._cache[key] = CacheEntry(
            key=key,
            value=value,
            created_at=time.time(),
            expires_at=expires_at,
        )
    
    def _evict_oldest(self) -> None:
        """Evict oldest entry."""
        if not self._cache:
            return
        
        oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k].created_at)
        del self._cache[oldest_key]
        self._stats.evictions += 1
    
    def invalidate(self, key: str) -> bool:
        """Invalidate a cache entry."""
        if key in self._cache:
            del self._cache[key]
            return True
        return False
    
    def clear(self) -> None:
        """Clear all cache entries."""
        self._cache.clear()
    
    def get_stats(self) -> CacheStats:
        """Get cache statistics."""
        self._stats.size = len(self._cache)
        return self._stats


class DataCacheAction:
    """Data cache action for automation workflows."""
    
    def __init__(self, ttl: float = 300.0, max_size: int = 1000) -> None:
        self.cache = DataCache(default_ttl=ttl, max_size=max_size)
    
    def get(self, key: str) -> Any | None:
        """Get cached value."""
        return self.cache.get(key)
    
    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        """Set cached value."""
        self.cache.set(key, value, ttl)
    
    def invalidate(self, key: str) -> bool:
        """Invalidate cache entry."""
        return self.cache.invalidate(key)
    
    def clear(self) -> None:
        """Clear cache."""
        self.cache.clear()
    
    def get_stats(self) -> CacheStats:
        """Get cache statistics."""
        return self.cache.get_stats()


__all__ = ["CacheEntry", "CacheStats", "DataCache", "DataCacheAction"]
