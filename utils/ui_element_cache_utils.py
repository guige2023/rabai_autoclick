"""UI element caching utilities for UI automation.

Provides utilities for caching UI element references,
invalidating stale cache entries, and managing element lookup caches.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar, Generic


T = TypeVar('T')


@dataclass
class CacheEntry(Generic[T]):
    """A cached element entry with metadata."""
    value: T
    timestamp_ms: float
    access_count: int = 0
    last_access_ms: float = 0.0
    ttl_ms: float = 0.0
    tags: Set[str] = field(default_factory=set)
    
    def is_expired(self, current_time_ms: float) -> bool:
        """Check if the entry is expired.
        
        Args:
            current_time_ms: Current time in milliseconds.
            
        Returns:
            True if expired.
        """
        if self.ttl_ms <= 0:
            return False
        return current_time_ms - self.timestamp_ms > self.ttl_ms
    
    def touch(self, current_time_ms: float) -> None:
        """Update access time and count.
        
        Args:
            current_time_ms: Current time in milliseconds.
        """
        self.access_count += 1
        self.last_access_ms = current_time_ms


@dataclass
class CacheStats:
    """Statistics for a cache."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0
    total_requests: int = 0
    
    @property
    def hit_rate(self) -> float:
        """Get cache hit rate.
        
        Returns:
            Hit rate as a percentage.
        """
        if self.total_requests == 0:
            return 0.0
        return (self.hits / self.total_requests) * 100


class ElementCache(Generic[T]):
    """Cache for UI element references.
    
    Provides LRU-style caching with TTL support,
    statistics tracking, and cache invalidation.
    """
    
    def __init__(
        self,
        max_size: int = 100,
        default_ttl_ms: float = 30000.0,
        track_stats: bool = True
    ) -> None:
        """Initialize the element cache.
        
        Args:
            max_size: Maximum number of cached entries.
            default_ttl_ms: Default TTL in milliseconds.
            track_stats: Whether to track statistics.
        """
        self.max_size = max_size
        self.default_ttl_ms = default_ttl_ms
        self.track_stats = track_stats
        self._cache: Dict[str, CacheEntry[T]] = {}
        self._access_order: List[str] = []
        self._stats = CacheStats()
    
    def get(self, key: str) -> Optional[T]:
        """Get a value from the cache.
        
        Args:
            key: Cache key.
            
        Returns:
            Cached value or None.
        """
        current_time = time.time() * 1000
        
        if self.track_stats:
            self._stats.total_requests += 1
        
        if key not in self._cache:
            if self.track_stats:
                self._stats.misses += 1
            return None
        
        entry = self._cache[key]
        
        if entry.is_expired(current_time):
            self._remove(key)
            if self.track_stats:
                self._stats.expirations += 1
                self._stats.misses += 1
            return None
        
        entry.touch(current_time)
        self._move_to_end(key)
        
        if self.track_stats:
            self._stats.hits += 1
        
        return entry.value
    
    def put(
        self,
        key: str,
        value: T,
        ttl_ms: Optional[float] = None,
        tags: Optional[Set[str]] = None
    ) -> None:
        """Put a value into the cache.
        
        Args:
            key: Cache key.
            value: Value to cache.
            ttl_ms: TTL in milliseconds (uses default if None).
            tags: Tags for the entry.
        """
        current_time = time.time() * 1000
        
        if key in self._cache:
            self._remove(key)
        
        if len(self._cache) >= self.max_size:
            self._evict_lru()
        
        entry = CacheEntry(
            value=value,
            timestamp_ms=current_time,
            ttl_ms=ttl_ms if ttl_ms is not None else self.default_ttl_ms,
            tags=tags or set()
        )
        
        self._cache[key] = entry
        self._access_order.append(key)
    
    def invalidate(self, key: str) -> bool:
        """Invalidate a cache entry.
        
        Args:
            key: Cache key.
            
        Returns:
            True if entry was removed.
        """
        if key in self._cache:
            self._remove(key)
            return True
        return False
    
    def invalidate_by_tags(self, tags: Set[str]) -> int:
        """Invalidate all entries with any of the given tags.
        
        Args:
            tags: Tags to match.
            
        Returns:
            Number of entries invalidated.
        """
        to_remove = []
        for key, entry in self._cache.items():
            if entry.tags & tags:
                to_remove.append(key)
        
        for key in to_remove:
            self._remove(key)
        
        return len(to_remove)
    
    def invalidate_by_prefix(self, prefix: str) -> int:
        """Invalidate all entries with keys starting with prefix.
        
        Args:
            prefix: Key prefix.
            
        Returns:
            Number of entries invalidated.
        """
        to_remove = [k for k in self._cache.keys() if k.startswith(prefix)]
        
        for key in to_remove:
            self._remove(key)
        
        return len(to_remove)
    
    def clear(self) -> None:
        """Clear all cache entries."""
        self._cache.clear()
        self._access_order.clear()
    
    def get_stats(self) -> CacheStats:
        """Get cache statistics.
        
        Returns:
            Cache statistics.
        """
        return self._stats
    
    def _remove(self, key: str) -> None:
        """Remove an entry from the cache.
        
        Args:
            key: Cache key.
        """
        if key in self._cache:
            del self._cache[key]
        if key in self._access_order:
            self._access_order.remove(key)
    
    def _move_to_end(self, key: str) -> None:
        """Move key to end of access order (most recent).
        
        Args:
            key: Cache key.
        """
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)
    
    def _evict_lru(self) -> None:
        """Evict the least recently used entry."""
        if not self._access_order:
            return
        
        lru_key = self._access_order[0]
        self._remove(lru_key)
        
        if self.track_stats:
            self._stats.evictions += 1
    
    def size(self) -> int:
        """Get current cache size.
        
        Returns:
            Number of entries.
        """
        return len(self._cache)
    
    def keys(self) -> List[str]:
        """Get all cache keys.
        
        Returns:
            List of keys.
        """
        return list(self._cache.keys())


class TwoLevelCache(Generic[T]):
    """Two-level cache with L1 (memory) and L2 (persistent) tiers.
    
    L1 is a fast in-memory cache; L2 is a slower persistent cache.
    Entries are checked in L1 first, then L2.
    """
    
    def __init__(
        self,
        l1_size: int = 50,
        l2_size: int = 500,
        l1_ttl_ms: float = 5000.0,
        l2_ttl_ms: float = 60000.0
    ) -> None:
        """Initialize the two-level cache.
        
        Args:
            l1_size: L1 cache size.
            l2_size: L2 cache size.
            l1_ttl_ms: L1 TTL in milliseconds.
            l2_ttl_ms: L2 TTL in milliseconds.
        """
        self.l1 = ElementCache[T](max_size=l1_size, default_ttl_ms=l1_ttl_ms)
        self.l2 = ElementCache[T](max_size=l2_size, default_ttl_ms=l2_ttl_ms)
    
    def get(self, key: str) -> Optional[T]:
        """Get a value from the cache.
        
        Args:
            key: Cache key.
            
        Returns:
            Cached value or None.
        """
        value = self.l1.get(key)
        if value is not None:
            return value
        
        value = self.l2.get(key)
        if value is not None:
            self.l1.put(key, value)
            return value
        
        return None
    
    def put(
        self,
        key: str,
        value: T,
        ttl_ms: Optional[float] = None,
        tags: Optional[Set[str]] = None
    ) -> None:
        """Put a value into both cache levels.
        
        Args:
            key: Cache key.
            value: Value to cache.
            ttl_ms: TTL in milliseconds.
            tags: Tags for the entry.
        """
        self.l1.put(key, value, ttl_ms, tags)
        self.l2.put(key, value, ttl_ms, tags)
    
    def invalidate(self, key: str) -> None:
        """Invalidate a key in both levels.
        
        Args:
            key: Cache key.
        """
        self.l1.invalidate(key)
        self.l2.invalidate(key)
    
    def clear(self) -> None:
        """Clear both cache levels."""
        self.l1.clear()
        self.l2.clear()


class ElementLookupCache(Generic[T]):
    """Cached element lookup with lazy evaluation.
    
    Caches the result of element lookups and can
    invalidate based on page/screen changes.
    """
    
    def __init__(
        self,
        finder: Callable[[str], Optional[T]],
        ttl_ms: float = 10000.0
    ) -> None:
        """Initialize the lookup cache.
        
        Args:
            finder: Function to find element by key.
            ttl_ms: Cache TTL in milliseconds.
        """
        self.finder = finder
        self._cache: ElementCache[Optional[T]] = ElementCache(
            max_size=100,
            default_ttl_ms=ttl_ms
        )
        self._current_context: str = ""
    
    def find(self, key: str, force_refresh: bool = False) -> Optional[T]:
        """Find an element, using cache if available.
        
        Args:
            key: Element key/selector.
            force_refresh: Force re-lookup even if cached.
            
        Returns:
            Found element or None.
        """
        if not force_refresh:
            cached = self._cache.get(key)
            if cached is not None or self._cache.size() > 0:
                return cached
        
        value = self.finder(key)
        self._cache.put(key, value)
        return value
    
    def invalidate_context(self) -> None:
        """Invalidate all entries when context changes."""
        self._cache.clear()
    
    def set_context(self, context: str) -> None:
        """Set the current context and invalidate if changed.
        
        Args:
            context: New context identifier.
        """
        if self._current_context != context:
            self.invalidate_context()
            self._current_context = context
    
    def get_stats(self) -> CacheStats:
        """Get cache statistics.
        
        Returns:
            Cache statistics.
        """
        return self._cache.get_stats()


class CacheWarmer:
    """Warms up caches by pre-populating with likely needed entries.
    
    Analyzes usage patterns and pre-fetches elements
    that are likely to be needed.
    """
    
    def __init__(self, cache: ElementCache) -> None:
        """Initialize the cache warmer.
        
        Args:
            cache: Cache to warm.
        """
        self.cache = cache
        self._access_patterns: Dict[str, int] = {}
    
    def record_access(self, key: str) -> None:
        """Record an access to a key.
        
        Args:
            key: Accessed key.
        """
        self._access_patterns[key] = self._access_patterns.get(key, 0) + 1
    
    def get_warm_keys(self, top_n: int = 10) -> List[str]:
        """Get keys that should be warmed.
        
        Args:
            top_n: Number of top keys to return.
            
        Returns:
            List of keys to pre-cache.
        """
        sorted_keys = sorted(
            self._access_patterns.items(),
            key=lambda x: x[1],
            reverse=True
        )
        return [k for k, _ in sorted_keys[:top_n]]
    
    def warm_cache(
        self,
        fetcher: Callable[[str], Any]
    ) -> int:
        """Warm the cache with top accessed keys.
        
        Args:
            fetcher: Function to fetch values.
            
        Returns:
            Number of entries warmed.
        """
        warm_keys = self.get_warm_keys()
        count = 0
        
        for key in warm_keys:
            if self.cache.get(key) is None:
                value = fetcher(key)
                self.cache.put(key, value)
                count += 1
        
        return count
