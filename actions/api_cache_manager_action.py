"""API Cache Manager Action Module.

Manages API response caching with:
- Multiple eviction policies (LRU, LFU, FIFO, TTL)
- Distributed cache support
- Cache invalidation strategies
- Compression support
- Metrics and monitoring

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class EvictionPolicy(Enum):
    """Cache eviction policies."""
    LRU = auto()   # Least Recently Used
    LFU = auto()   # Least Frequently Used
    FIFO = auto()  # First In First Out
    TTL = auto()   # Time To Live based


@dataclass
class CacheEntry:
    """A cache entry."""
    key: str
    value: Any
    created_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)
    access_count: int = 0
    size_bytes: int = 0
    tags: set = field(default_factory=set)
    expires_at: Optional[float] = None


@dataclass
class CacheMetrics:
    """Cache performance metrics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    invalidations: int = 0
    total_size_bytes: int = 0
    items_count: int = 0


class APICacheManager:
    """Manages API response caching with multiple strategies.
    
    Features:
    - LRU, LFU, FIFO, TTL eviction policies
    - Tagged invalidation
    - Compression support
    - Cache-aside and write-through patterns
    - Metrics tracking
    """
    
    def __init__(
        self,
        name: str = "default",
        max_size_mb: float = 100.0,
        default_ttl_seconds: float = 300.0,
        eviction_policy: EvictionPolicy = EvictionPolicy.LRU,
        enable_compression: bool = False
    ):
        self.name = name
        self.max_size_bytes = int(max_size_mb * 1024 * 1024)
        self.default_ttl_seconds = default_ttl_seconds
        self.eviction_policy = eviction_policy
        self.enable_compression = enable_compression
        
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lfu_counts: Dict[str, int] = {}
        self._lock = asyncio.Lock()
        self._metrics = CacheMetrics()
        self._expiry_task: Optional[asyncio.Task] = None
        self._running = False
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None
        """
        async with self._lock:
            if key not in self._cache:
                self._metrics.misses += 1
                return None
            
            entry = self._cache[key]
            
            if entry.expires_at and time.time() > entry.expires_at:
                del self._cache[key]
                self._metrics.misses += 1
                return None
            
            entry.last_accessed = time.time()
            entry.access_count += 1
            self._lfu_counts[key] = self._lfu_counts.get(key, 0) + 1
            
            if self.eviction_policy == EvictionPolicy.LRU:
                self._cache.move_to_end(key)
            
            self._metrics.hits += 1
            return entry.value
    
    async def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[float] = None,
        tags: Optional[set] = None,
        size_bytes: Optional[int] = None
    ) -> None:
        """Set value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl_seconds: Optional TTL override
            tags: Optional tags for invalidation
            size_bytes: Optional size hint
        """
        ttl = ttl_seconds if ttl_seconds is not None else self.default_ttl_seconds
        
        expires_at = None
        if ttl > 0:
            expires_at = time.time() + ttl
        
        entry_size = size_bytes or self._estimate_size(value)
        
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
            
            while self._current_size() + entry_size > self.max_size_bytes and self._cache:
                await self._evict_one()
            
            entry = CacheEntry(
                key=key,
                value=value,
                expires_at=expires_at,
                size_bytes=entry_size,
                tags=tags or set()
            )
            
            self._cache[key] = entry
            self._lfu_counts[key] = 0
            
            if self.eviction_policy == EvictionPolicy.LRU:
                self._cache.move_to_end(key)
    
    async def delete(self, key: str) -> bool:
        """Delete value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if deleted
        """
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                if key in self._lfu_counts:
                    del self._lfu_counts[key]
                return True
            return False
    
    async def invalidate_by_tags(self, tags: set) -> int:
        """Invalidate cache entries by tags.
        
        Args:
            tags: Tags to match
            
        Returns:
            Number of entries invalidated
        """
        count = 0
        
        async with self._lock:
            to_delete = [
                key for key, entry in self._cache.items()
                if entry.tags & tags
            ]
            
            for key in to_delete:
                del self._cache[key]
                if key in self._lfu_counts:
                    del self._lfu_counts[key]
                count += 1
        
        self._metrics.invalidations += count
        return count
    
    async def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate cache entries matching pattern.
        
        Args:
            pattern: Key pattern (supports * wildcard)
            
        Returns:
            Number of entries invalidated
        """
        import fnmatch
        
        async with self._lock:
            to_delete = [
                key for key in self._cache.keys()
                if fnmatch.fnmatch(key, pattern)
            ]
            
            for key in to_delete:
                del self._cache[key]
                if key in self._lfu_counts:
                    del self._lfu_counts[key]
        
        self._metrics.invalidations += len(to_delete)
        return len(to_delete)
    
    async def clear(self) -> None:
        """Clear all cache entries."""
        async with self._lock:
            self._cache.clear()
            self._lfu_counts.clear()
    
    async def _evict_one(self) -> None:
        """Evict one entry based on eviction policy."""
        if not self._cache:
            return
        
        key_to_evict = None
        
        if self.eviction_policy == EvictionPolicy.LRU:
            key_to_evict = next(iter(self._cache))
        
        elif self.eviction_policy == EvictionPolicy.LFU:
            key_to_evict = min(self._lfu_counts, key=self._lfu_counts.get, default=None)
        
        elif self.eviction_policy == EvictionPolicy.FIFO:
            key_to_evict = next(iter(self._cache))
        
        elif self.eviction_policy == EvictionPolicy.TTL:
            now = time.time()
            expired = [
                (key, entry.expires_at) for key, entry in self._cache.items()
                if entry.expires_at and entry.expires_at < now
            ]
            if expired:
                key_to_evict = min(expired, key=lambda x: x[1])[0]
            else:
                key_to_evict = next(iter(self._cache))
        
        if key_to_evict:
            del self._cache[key_to_evict]
            if key_to_evict in self._lfu_counts:
                del self._lfu_counts[key_to_evict]
            self._metrics.evictions += 1
    
    async def _cleanup_expired(self) -> None:
        """Background task to clean up expired entries."""
        while self._running:
            try:
                await asyncio.sleep(10)
                
                now = time.time()
                async with self._lock:
                    to_delete = [
                        key for key, entry in self._cache.items()
                        if entry.expires_at and entry.expires_at < now
                    ]
                    
                    for key in to_delete:
                        del self._cache[key]
                        if key in self._lfu_counts:
                            del self._lfu_counts[key]
                    
                    if to_delete:
                        self._metrics.evictions += len(to_delete)
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cache cleanup error: {e}")
    
    def _current_size(self) -> int:
        """Get current cache size in bytes."""
        return sum(e.size_bytes for e in self._cache.values())
    
    def _estimate_size(self, value: Any) -> int:
        """Estimate size of value in bytes."""
        try:
            return len(json.dumps(value, default=str).encode("utf-8"))
        except Exception:
            return 100
    
    async def start(self) -> None:
        """Start the cache manager."""
        self._running = True
        self._expiry_task = asyncio.create_task(self._cleanup_expired())
        logger.info(f"Cache manager '{self.name}' started")
    
    async def stop(self) -> None:
        """Stop the cache manager."""
        self._running = False
        
        if self._expiry_task:
            self._expiry_task.cancel()
            try:
                await self._expiry_task
            except asyncio.CancelledError:
                pass
        
        logger.info(f"Cache manager '{self.name}' stopped")
    
    async def get_or_compute(
        self,
        key: str,
        compute_fn: Callable[[], Any],
        ttl_seconds: Optional[float] = None,
        tags: Optional[set] = None
    ) -> Any:
        """Get from cache or compute and cache.
        
        Args:
            key: Cache key
            compute_fn: Function to compute value if not cached
            ttl_seconds: Optional TTL
            tags: Optional tags
            
        Returns:
            Cached or computed value
        """
        cached = await self.get(key)
        if cached is not None:
            return cached
        
        value = await compute_fn() if asyncio.iscoroutinefunction(compute_fn) else compute_fn()
        await self.set(key, value, ttl_seconds, tags)
        
        return value
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        hit_rate = (
            self._metrics.hits / (self._metrics.hits + self._metrics.misses)
            if (self._metrics.hits + self._metrics.misses) > 0 else 0.0
        )
        
        return {
            "name": self.name,
            "hits": self._metrics.hits,
            "misses": self._metrics.misses,
            "hit_rate": hit_rate,
            "evictions": self._metrics.evictions,
            "invalidations": self._metrics.invalidations,
            "items_count": len(self._cache),
            "total_size_mb": self._current_size() / (1024 * 1024),
            "max_size_mb": self.max_size_bytes / (1024 * 1024),
            "eviction_policy": self.eviction_policy.name
        }


class DistributedCacheWrapper:
    """Wrapper for distributed cache operations."""
    
    def __init__(self, local_cache: APICacheManager):
        self._local = local_cache
        self._distributed: Dict[str, APICacheManager] = {}
    
    def add_node(self, node_id: str, cache: APICacheManager) -> None:
        """Add a distributed cache node."""
        self._distributed[node_id] = cache
    
    async def get(self, key: str) -> Optional[Any]:
        """Get from local or distributed cache."""
        value = await self._local.get(key)
        if value is not None:
            return value
        
        for node_cache in self._distributed.values():
            value = await node_cache.get(key)
            if value is not None:
                await self._local.set(key, value)
                return value
        
        return None
    
    async def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        """Set in all cache nodes."""
        await self._local.set(key, value, ttl_seconds)
        
        for node_cache in self._distributed.values():
            await node_cache.set(key, value, ttl_seconds)
    
    async def invalidate_everywhere(self, key: str) -> None:
        """Invalidate key in all cache nodes."""
        await self._local.delete(key)
        
        for node_cache in self._distributed.values():
            await node_cache.delete(key)
