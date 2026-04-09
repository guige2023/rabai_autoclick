"""
Cache Management Utilities for UI Automation.

This module provides utilities for caching data, elements, and results
to improve automation performance and reduce redundant operations.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
import hashlib
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class CacheEvictionPolicy(Enum):
    """Cache eviction policies."""
    LRU = auto()      # Least Recently Used
    LFU = auto()      # Least Frequently Used
    FIFO = auto()     # First In First Out
    TTL = auto()      # Time To Live based
    RANDOM = auto()


@dataclass
class CacheEntry:
    """
    A cache entry.
    
    Attributes:
        key: Cache key
        value: Cached value
        created_at: Creation timestamp
        accessed_at: Last access timestamp
        access_count: Number of times accessed
        size_bytes: Approximate size in bytes
    """
    key: str
    value: Any
    created_at: float = field(default_factory=time.time)
    accessed_at: float = field(default_factory=time.time)
    access_count: int = 0
    size_bytes: int = 0
    
    def touch(self) -> None:
        """Update access time and count."""
        self.accessed_at = time.time()
        self.access_count += 1
    
    @property
    def age_seconds(self) -> float:
        """Get age of entry in seconds."""
        return time.time() - self.created_at
    
    @property
    def idle_seconds(self) -> float:
        """Get time since last access in seconds."""
        return time.time() - self.accessed_at


class Cache:
    """
    General-purpose cache with configurable eviction.
    
    Example:
        cache = Cache(max_size=100, eviction_policy=CacheEvictionPolicy.LRU)
        cache.set("key", "value")
        value = cache.get("key")
    """
    
    def __init__(
        self,
        max_size: int = 1000,
        max_memory_bytes: Optional[int] = None,
        eviction_policy: CacheEvictionPolicy = CacheEvictionPolicy.LRU,
        default_ttl: Optional[float] = None
    ):
        self.max_size = max_size
        self.max_memory_bytes = max_memory_bytes
        self.eviction_policy = eviction_policy
        self.default_ttl = default_ttl
        
        self._cache: dict[str, CacheEntry] = {}
        self._current_memory = 0
        self._hits = 0
        self._misses = 0
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get a value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found
        """
        entry = self._cache.get(key)
        
        if entry is None:
            self._misses += 1
            return None
        
        # Check TTL
        if self.default_ttl and entry.age_seconds > self.default_ttl:
            self.delete(key)
            self._misses += 1
            return None
        
        # Update access info
        entry.touch()
        
        self._hits += 1
        return entry.value
    
    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None
    ) -> None:
        """
        Set a value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Optional TTL override
        """
        # Check if we need to evict
        if key not in self._cache and len(self._cache) >= self.max_size:
            self._evict()
        
        # Calculate size
        size = self._estimate_size(value)
        
        # Check memory limit
        if self.max_memory_bytes:
            while (self._current_memory + size > self.max_memory_bytes 
                   and self._cache):
                self._evict()
        
        # Remove old entry if updating
        if key in self._cache:
            self._current_memory -= self._cache[key].size_bytes
            del self._cache[key]
        
        # Add new entry
        entry = CacheEntry(
            key=key,
            value=value,
            size_bytes=size
        )
        
        self._cache[key] = entry
        self._current_memory += size
    
    def delete(self, key: str) -> bool:
        """
        Delete a key from cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if deleted, False if not found
        """
        if key not in self._cache:
            return False
        
        self._current_memory -= self._cache[key].size_bytes
        del self._cache[key]
        return True
    
    def clear(self) -> None:
        """Clear all cached entries."""
        self._cache.clear()
        self._current_memory = 0
    
    def _evict(self) -> None:
        """Evict an entry based on eviction policy."""
        if not self._cache:
            return
        
        if self.eviction_policy == CacheEvictionPolicy.LRU:
            # Evict least recently used
            lru_key = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].accessed_at
            )
        elif self.eviction_policy == CacheEvictionPolicy.LFU:
            # Evict least frequently used
            lfu_key = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].access_count
            )
            lru_key = lfu_key
        elif self.eviction_policy == CacheEvictionPolicy.FIFO:
            # Evict oldest
            fifo_key = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].created_at
            )
            lru_key = fifo_key
        elif self.eviction_policy == CacheEvictionPolicy.TTL:
            # Evict oldest by age
            oldest_key = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].age_seconds
            )
            lru_key = oldest_key
        else:  # RANDOM
            import random
            lru_key = random.choice(list(self._cache.keys()))
        
        self.delete(lru_key)
    
    def _estimate_size(self, value: Any) -> int:
        """Estimate size of a value in bytes."""
        import sys
        try:
            return sys.getsizeof(value)
        except TypeError:
            return 0
    
    @property
    def size(self) -> int:
        """Get number of entries in cache."""
        return len(self._cache)
    
    @property
    def hit_rate(self) -> float:
        """Get cache hit rate."""
        total = self._hits + self._misses
        return self._hits / total if total > 0 else 0.0
    
    @property
    def memory_usage_bytes(self) -> int:
        """Get current memory usage."""
        return self._current_memory
    
    def keys(self) -> list[str]:
        """Get all cache keys."""
        return list(self._cache.keys())


class MemoizedFunction:
    """
    A memoized wrapper for functions.
    
    Example:
        @MemoizedFunction(max_size=100)
        def expensive_computation(x, y):
            return x + y
    """
    
    def __init__(self, func: Callable, max_size: int = 1000):
        self.func = func
        self._cache = Cache(max_size=max_size)
    
    def __call__(self, *args, **kwargs) -> Any:
        """Call the function with memoization."""
        key = self._make_key(args, kwargs)
        
        result = self._cache.get(key)
        if result is not None:
            return result
        
        result = self.func(*args, **kwargs)
        self._cache.set(key, result)
        
        return result
    
    def _make_key(self, args: tuple, kwargs: dict) -> str:
        """Create a cache key from arguments."""
        key_parts = [str(arg) for arg in args]
        key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
        key_str = "|".join(key_parts)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def clear_cache(self) -> None:
        """Clear the memoization cache."""
        self._cache.clear()
    
    @property
    def cache(self) -> Cache:
        """Access the underlying cache."""
        return self._cache
