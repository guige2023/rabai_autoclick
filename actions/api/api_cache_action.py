"""API Cache Action Module.

Provides caching capabilities for API responses with support for
TTL, cache invalidation, and various eviction policies.

Example:
    >>> from actions.api.api_cache_action import APICacheAction
    >>> cache = APICacheAction()
    >>> result = cache.get_or_fetch("users", fetch_fn, ttl=300)
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union
import asyncio


class CacheStrategy(Enum):
    """Cache strategy types."""
    LRU = "lru"           # Least Recently Used
    LFU = "lfu"           # Least Frequently Used
    FIFO = "fifo"         # First In First Out
    TTL = "ttl"           # Time To Live
    RANDOM = "random"     # Random eviction


@dataclass
class CacheEntry:
    """A single cache entry.
    
    Attributes:
        key: Cache key
        value: Cached value
        created_at: When the entry was created
        accessed_at: When the entry was last accessed
        access_count: Number of times the entry was accessed
        ttl: Time to live in seconds (None for no expiry)
        metadata: Additional entry metadata
    """
    key: str
    value: Any
    created_at: float = field(default_factory=time.time)
    accessed_at: float = field(default_factory=time.time)
    access_count: int = 0
    ttl: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CacheStats:
    """Cache statistics.
    
    Attributes:
        hits: Number of cache hits
        misses: Number of cache misses
        evictions: Number of evicted entries
        hit_rate: Cache hit rate percentage
    """
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    hit_rate: float = 0.0
    size: int = 0
    max_size: int = 0


class APICacheAction:
    """Handles caching of API responses.
    
    Provides thread-safe caching with support for TTL, various
    eviction strategies, and cache statistics.
    
    Attributes:
        max_size: Maximum number of entries to cache
        default_ttl: Default TTL in seconds
    
    Example:
        >>> cache = APICacheAction(max_size=1000, default_ttl=300)
        >>> result = cache.get_or_fetch("key", fetch_fn, ttl=60)
    """
    
    def __init__(
        self,
        max_size: int = 1000,
        default_ttl: float = 300.0,
        strategy: CacheStrategy = CacheStrategy.LRU,
        enable_stats: bool = True
    ):
        """Initialize the API cache action.
        
        Args:
            max_size: Maximum number of entries
            default_ttl: Default time-to-live in seconds
            strategy: Cache eviction strategy
            enable_stats: Whether to track cache statistics
        """
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.strategy = strategy
        self.enable_stats = enable_stats
        
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.RLock()
        
        # Statistics
        self._hits = 0
        self._misses = 0
        self._evictions = 0
    
    def get(self, key: str) -> Optional[Any]:
        """Get a value from the cache.
        
        Args:
            key: Cache key
        
        Returns:
            Cached value or None if not found
        """
        with self._lock:
            entry = self._cache.get(key)
            
            if entry is None:
                if self.enable_stats:
                    self._misses += 1
                return None
            
            # Check TTL
            if entry.ttl is not None:
                if time.time() - entry.created_at > entry.ttl:
                    del self._cache[key]
                    if self.enable_stats:
                        self._misses += 1
                    return None
            
            # Update access statistics
            entry.accessed_at = time.time()
            entry.access_count += 1
            
            if self.enable_stats:
                self._hits += 1
            
            return entry.value
    
    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None
    ) -> None:
        """Set a value in the cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Optional TTL in seconds
        """
        with self._lock:
            # Check if we need to evict
            if len(self._cache) >= self.max_size and key not in self._cache:
                self._evict()
            
            self._cache[key] = CacheEntry(
                key=key,
                value=value,
                ttl=ttl or self.default_ttl
            )
    
    def delete(self, key: str) -> bool:
        """Delete a value from the cache.
        
        Args:
            key: Cache key
        
        Returns:
            True if the key was found and deleted
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False
    
    def clear(self) -> None:
        """Clear all cached entries."""
        with self._lock:
            self._cache.clear()
            self._hits = 0
            self._misses = 0
            self._evictions = 0
    
    def get_or_fetch(
        self,
        key: str,
        fetch_fn: Callable[[], Any],
        ttl: Optional[float] = None
    ) -> Any:
        """Get from cache or fetch if not present.
        
        Args:
            key: Cache key
            fetch_fn: Function to fetch data if not cached
            ttl: Optional TTL in seconds
        
        Returns:
            Cached or fetched value
        """
        cached = self.get(key)
        if cached is not None:
            return cached
        
        value = fetch_fn()
        self.set(key, value, ttl)
        return value
    
    async def get_or_fetch_async(
        self,
        key: str,
        fetch_fn: Callable[[], Any],
        ttl: Optional[float] = None
    ) -> Any:
        """Async version of get_or_fetch.
        
        Args:
            key: Cache key
            fetch_fn: Async function to fetch data if not cached
            ttl: Optional TTL in seconds
        
        Returns:
            Cached or fetched value
        """
        cached = self.get(key)
        if cached is not None:
            return cached
        
        if asyncio.iscoroutinefunction(fetch_fn):
            value = await fetch_fn()
        else:
            value = await asyncio.get_event_loop().run_in_executor(None, fetch_fn)
        
        self.set(key, value, ttl)
        return value
    
    def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate all keys matching a pattern.
        
        Args:
            pattern: Pattern to match (supports * wildcard)
        
        Returns:
            Number of keys invalidated
        """
        import fnmatch
        
        with self._lock:
            keys_to_delete = [
                key for key in self._cache.keys()
                if fnmatch.fnmatch(key, pattern)
            ]
            
            for key in keys_to_delete:
                del self._cache[key]
            
            return len(keys_to_delete)
    
    def refresh(self, key: str, ttl: Optional[float] = None) -> bool:
        """Refresh the TTL of a cached entry.
        
        Args:
            key: Cache key
            ttl: Optional new TTL
        
        Returns:
            True if the key was found
        """
        with self._lock:
            if key in self._cache:
                entry = self._cache[key]
                entry.created_at = time.time()
                if ttl is not None:
                    entry.ttl = ttl
                return True
            return False
    
    def _evict(self) -> None:
        """Evict entries based on the configured strategy."""
        if not self._cache:
            return
        
        if self.strategy == CacheStrategy.LRU:
            # Evict least recently used
            key_to_evict = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].accessed_at
            )
        elif self.strategy == CacheStrategy.LFU:
            # Evict least frequently used
            key_to_evict = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].access_count
            )
        elif self.strategy == CacheStrategy.FIFO:
            # Evict oldest
            key_to_evict = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].created_at
            )
        elif self.strategy == CacheStrategy.TTL:
            # Evict expired or oldest TTL
            current_time = time.time()
            expired = [
                (k, self._cache[k].created_at + self._cache[k].ttl)
                for k in self._cache.keys()
                if self._cache[k].ttl and
                current_time - self._cache[k].created_at > self._cache[k].ttl
            ]
            
            if expired:
                key_to_evict = min(expired, key=lambda x: x[1])[0]
            else:
                key_to_evict = min(
                    self._cache.keys(),
                    key=lambda k: self._cache[k].created_at
                )
        else:  # RANDOM
            import random
            key_to_evict = random.choice(list(self._cache.keys()))
        
        del self._cache[key_to_evict]
        self._evictions += 1
    
    def get_stats(self) -> CacheStats:
        """Get cache statistics.
        
        Returns:
            CacheStats object with current statistics
        """
        with self._lock:
            total = self._hits + self._misses
            hit_rate = (self._hits / total * 100) if total > 0 else 0.0
            
            return CacheStats(
                hits=self._hits,
                misses=self._misses,
                evictions=self._evictions,
                hit_rate=hit_rate,
                size=len(self._cache),
                max_size=self.max_size
            )
    
    def cleanup_expired(self) -> int:
        """Remove all expired entries from the cache.
        
        Returns:
            Number of entries removed
        """
        with self._lock:
            current_time = time.time()
            expired_keys = [
                key for key, entry in self._cache.items()
                if entry.ttl and current_time - entry.created_at > entry.ttl
            ]
            
            for key in expired_keys:
                del self._cache[key]
            
            return len(expired_keys)
    
    def generate_key(self, *args: Any, **kwargs: Any) -> str:
        """Generate a cache key from arguments.
        
        Args:
            *args: Positional arguments
            **kwargs: Keyword arguments
        
        Returns:
            Generated cache key
        """
        key_data = {
            "args": args,
            "kwargs": sorted(kwargs.items())
        }
        key_str = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def export_cache(self) -> Dict[str, Any]:
        """Export cache data for debugging.
        
        Returns:
            Dictionary with cache contents and statistics
        """
        with self._lock:
            return {
                "size": len(self._cache),
                "max_size": self.max_size,
                "strategy": self.strategy.value,
                "stats": {
                    "hits": self._hits,
                    "misses": self._misses,
                    "evictions": self._evictions,
                    "hit_rate": self.get_stats().hit_rate
                },
                "entries": {
                    key: {
                        "created_at": entry.created_at,
                        "accessed_at": entry.accessed_at,
                        "access_count": entry.access_count,
                        "ttl": entry.ttl
                    }
                    for key, entry in self._cache.items()
                }
            }
