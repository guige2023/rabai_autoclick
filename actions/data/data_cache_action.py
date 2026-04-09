"""Data Cache Action Module.

Provides caching capabilities for data operations including
TTL-based expiration, LRU eviction, and cache invalidation.

Example:
    >>> from actions.data.data_cache_action import DataCacheAction
    >>> action = DataCacheAction()
    >>> action.set("key", data, ttl=300)
    >>> cached = action.get("key")
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
import threading


class EvictionPolicy(Enum):
    """Cache eviction policies."""
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    TTL = "ttl"


@dataclass
class CacheEntry:
    """Cache entry.
    
    Attributes:
        key: Cache key
        value: Cached value
        created_at: Creation timestamp
        accessed_at: Last access timestamp
        access_count: Number of accesses
        ttl: Time to live
        expires_at: Expiration timestamp
    """
    key: str
    value: Any
    created_at: float
    accessed_at: float
    access_count: int = 0
    ttl: Optional[float] = None
    expires_at: Optional[float] = None


@dataclass
class CacheConfig:
    """Configuration for cache.
    
    Attributes:
        max_size: Maximum cache entries
        default_ttl: Default TTL in seconds
        eviction_policy: Eviction policy
        enable_stats: Enable cache statistics
        cleanup_interval: Cleanup interval in seconds
    """
    max_size: int = 1000
    default_ttl: float = 300.0
    eviction_policy: EvictionPolicy = EvictionPolicy.LRU
    enable_stats: bool = True
    cleanup_interval: float = 60.0


@dataclass
class CacheStats:
    """Cache statistics.
    
    Attributes:
        hits: Number of cache hits
        misses: Number of cache misses
        evictions: Number of evictions
        current_size: Current cache size
        hit_rate: Hit rate percentage
    """
    hits: int
    misses: int
    evictions: int
    current_size: int
    hit_rate: float


class DataCacheAction:
    """Data cache for operations.
    
    Provides in-memory caching with configurable eviction
    policies and TTL support.
    
    Attributes:
        config: Cache configuration
        _cache: Cache storage
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[CacheConfig] = None,
    ) -> None:
        """Initialize cache action.
        
        Args:
            config: Cache configuration
        """
        self.config = config or CacheConfig()
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
        self._evictions = 0
    
    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
    ) -> None:
        """Set cache entry.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds
        """
        ttl = ttl or self.config.default_ttl
        current_time = time.time()
        
        entry = CacheEntry(
            key=key,
            value=value,
            created_at=current_time,
            accessed_at=current_time,
            access_count=0,
            ttl=ttl,
            expires_at=current_time + ttl if ttl > 0 else None,
        )
        
        with self._lock:
            if key not in self._cache and len(self._cache) >= self.config.max_size:
                self._evict_one()
            
            self._cache[key] = entry
    
    def get(
        self,
        key: str,
        default: Any = None,
    ) -> Any:
        """Get cache entry.
        
        Args:
            key: Cache key
            default: Default value if not found
        
        Returns:
            Cached value or default
        """
        with self._lock:
            if key not in self._cache:
                if self.config.enable_stats:
                    self._misses += 1
                return default
            
            entry = self._cache[key]
            
            if entry.expires_at and time.time() > entry.expires_at:
                del self._cache[key]
                if self.config.enable_stats:
                    self._misses += 1
                return default
            
            entry.accessed_at = time.time()
            entry.access_count += 1
            
            if self.config.enable_stats:
                self._hits += 1
            
            return entry.value
    
    def get_or_compute(
        self,
        key: str,
        compute_func: Callable[[], Any],
        ttl: Optional[float] = None,
    ) -> Any:
        """Get from cache or compute and cache.
        
        Args:
            key: Cache key
            compute_func: Function to compute value
            ttl: Time to live
        
        Returns:
            Cached or computed value
        """
        value = self.get(key)
        
        if value is not None:
            return value
        
        computed = compute_func()
        self.set(key, computed, ttl)
        
        return computed
    
    def delete(self, key: str) -> bool:
        """Delete cache entry.
        
        Args:
            key: Cache key
        
        Returns:
            True if deleted
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False
    
    def clear(self) -> int:
        """Clear all cache entries.
        
        Returns:
            Number of entries cleared
        """
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            return count
    
    def has(self, key: str) -> bool:
        """Check if key exists and is not expired.
        
        Args:
            key: Cache key
        
        Returns:
            True if exists
        """
        with self._lock:
            if key not in self._cache:
                return False
            
            entry = self._cache[key]
            
            if entry.expires_at and time.time() > entry.expires_at:
                del self._cache[key]
                return False
            
            return True
    
    def get_many(
        self,
        keys: List[str],
    ) -> Dict[str, Any]:
        """Get multiple cache entries.
        
        Args:
            keys: List of cache keys
        
        Returns:
            Dictionary of found entries
        """
        result = {}
        
        for key in keys:
            value = self.get(key)
            if value is not None:
                result[key] = value
        
        return result
    
    def set_many(
        self,
        items: Dict[str, Any],
        ttl: Optional[float] = None,
    ) -> None:
        """Set multiple cache entries.
        
        Args:
            items: Dictionary of key-value pairs
            ttl: Time to live
        """
        for key, value in items.items():
            self.set(key, value, ttl)
    
    def invalidate_prefix(self, prefix: str) -> int:
        """Invalidate all keys with prefix.
        
        Args:
            prefix: Key prefix
        
        Returns:
            Number of keys invalidated
        """
        with self._lock:
            to_delete = [
                key for key in self._cache.keys()
                if key.startswith(prefix)
            ]
            
            for key in to_delete:
                del self._cache[key]
            
            return len(to_delete)
    
    def _evict_one(self) -> Optional[str]:
        """Evict one entry based on policy.
        
        Returns:
            Evicted key or None
        """
        if not self._cache:
            return None
        
        if self.config.eviction_policy == EvictionPolicy.LRU:
            evict_key = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].accessed_at,
            )
        elif self.config.eviction_policy == EvictionPolicy.LFU:
            evict_key = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].access_count,
            )
        elif self.config.eviction_policy == EvictionPolicy.FIFO:
            evict_key = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].created_at,
            )
        else:
            evict_key = next(iter(self._cache))
        
        if evict_key:
            del self._cache[evict_key]
            self._evictions += 1
        
        return evict_key
    
    def cleanup_expired(self) -> int:
        """Remove all expired entries.
        
        Returns:
            Number of entries removed
        """
        current_time = time.time()
        removed = 0
        
        with self._lock:
            to_delete = [
                key for key, entry in self._cache.items()
                if entry.expires_at and current_time > entry.expires_at
            ]
            
            for key in to_delete:
                del self._cache[key]
                removed += 1
        
        return removed
    
    def get_stats(self) -> CacheStats:
        """Get cache statistics.
        
        Returns:
            CacheStats
        """
        with self._lock:
            total = self._hits + self._misses
            hit_rate = (self._hits / total * 100) if total > 0 else 0.0
            
            return CacheStats(
                hits=self._hits,
                misses=self._misses,
                evictions=self._evictions,
                current_size=len(self._cache),
                hit_rate=hit_rate,
            )
    
    def reset_stats(self) -> None:
        """Reset cache statistics."""
        with self._lock:
            self._hits = 0
            self._misses = 0
            self._evictions = 0
