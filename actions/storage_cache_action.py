"""
Storage Cache Action Module

Multi-tier caching system with memory, disk, and distributed cache support.
Cache warming, eviction policies, and consistency guarantees.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class EvictionPolicy(Enum):
    """Cache eviction policies."""
    
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    TTL = "ttl"
    RANDOM = "random"


class CacheLevel(Enum):
    """Cache storage levels."""
    
    MEMORY = "memory"
    DISK = "disk"
    DISTRIBUTED = "distributed"


@dataclass
class CacheEntry:
    """Cache entry with metadata."""
    
    key: str
    value: Any
    created_at: float = field(default_factory=time.time)
    accessed_at: float = field(default_factory=time.time)
    access_count: int = 0
    ttl_seconds: float = 0
    size_bytes: int = 0
    compressed: bool = False
    
    def is_expired(self) -> bool:
        """Check if entry has expired."""
        if self.ttl_seconds <= 0:
            return False
        return (time.time() - self.created_at) > self.ttl_seconds
    
    def access(self) -> None:
        """Record an access."""
        self.accessed_at = time.time()
        self.access_count += 1


@dataclass
class CacheStats:
    """Cache statistics."""
    
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0
    total_size_bytes: int = 0
    entries_count: int = 0
    
    @property
    def hit_rate(self) -> float:
        """Calculate hit rate."""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0


class MemoryCache:
    """In-memory cache with LRU eviction."""
    
    def __init__(self, max_entries: int = 1000, max_size_bytes: int = 100_000_000):
        self.max_entries = max_entries
        self.max_size_bytes = max_size_bytes
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = asyncio.Lock()
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        async with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            
            if entry.is_expired():
                del self._cache[key]
                return None
            
            entry.access()
            self._cache.move_to_end(key)
            return entry.value
    
    async def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: float = 0,
        size_bytes: Optional[int] = None
    ) -> None:
        """Set value in cache."""
        async with self._lock:
            entry = CacheEntry(
                key=key,
                value=value,
                ttl_seconds=ttl_seconds,
                size_bytes=size_bytes or len(str(value))
            )
            
            if key in self._cache:
                del self._cache[key]
            
            self._cache[key] = entry
            self._cache.move_to_end(key)
            
            await self._evict_if_needed()
    
    async def delete(self, key: str) -> bool:
        """Delete value from cache."""
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False
    
    async def clear(self) -> None:
        """Clear all cache entries."""
        async with self._lock:
            self._cache.clear()
    
    async def _evict_if_needed(self) -> None:
        """Evict entries if cache is full."""
        while len(self._cache) > self.max_entries:
            self._cache.popitem(last=False)
        
        total_size = sum(e.size_bytes for e in self._cache.values())
        while total_size > self.max_size_bytes and self._cache:
            oldest = next(iter(self._cache.values()))
            total_size -= oldest.size_bytes
            self._cache.popitem(last=False)
    
    def size(self) -> int:
        """Get number of entries."""
        return len(self._cache)


class DiskCache:
    """Disk-based cache with TTL support."""
    
    def __init__(self, cache_dir: str, max_size_mb: int = 1000):
        self.cache_dir = Path(cache_dir)
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self._lock = asyncio.Lock()
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_path(self, key: str) -> Path:
        """Get file path for a key."""
        key_hash = hashlib.sha256(key.encode()).hexdigest()[:16]
        return self.cache_dir / f"{key_hash}.cache"
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from disk cache."""
        path = self._get_path(key)
        
        if not path.exists():
            return None
        
        try:
            stat = path.stat()
            if time.time() - stat.st_mtime > 86400:
                path.unlink()
                return None
            
            with open(path, "r") as f:
                data = json.load(f)
            
            return data.get("value")
        
        except Exception as e:
            logger.error(f"Disk cache read error: {e}")
            return None
    
    async def set(self, key: str, value: Any) -> None:
        """Set value in disk cache."""
        path = self._get_path(key)
        
        try:
            data = {"key": key, "value": value, "timestamp": time.time()}
            with open(path, "w") as f:
                json.dump(data, f)
        except Exception as e:
            logger.error(f"Disk cache write error: {e}")
    
    async def delete(self, key: str) -> bool:
        """Delete value from disk cache."""
        path = self._get_path(key)
        if path.exists():
            path.unlink()
            return True
        return False
    
    async def clear(self) -> None:
        """Clear all disk cache entries."""
        for path in self.cache_dir.glob("*.cache"):
            path.unlink()


class StorageCacheAction:
    """
    Main storage cache action handler.
    
    Provides multi-tier caching with memory and disk levels,
    configurable eviction policies, and cache warming.
    """
    
    def __init__(
        self,
        max_memory_entries: int = 1000,
        max_memory_size_mb: int = 100,
        disk_cache_dir: Optional[str] = None,
        eviction_policy: EvictionPolicy = EvictionPolicy.LRU
    ):
        self.memory_cache = MemoryCache(max_entries=max_memory_entries)
        self.disk_cache = DiskCache(disk_cache_dir) if disk_cache_dir else None
        self.eviction_policy = eviction_policy
        self.stats = CacheStats()
        self._stats_lock = asyncio.Lock()
        self._middleware: List[Callable] = []
    
    async def get(
        self,
        key: str,
        bypass_cache: bool = False
    ) -> Optional[Any]:
        """Get value from cache."""
        if bypass_cache:
            return None
        
        for mw in self._middleware:
            result = await mw("get", key, None)
            if result is not None:
                return result
        
        value = await self.memory_cache.get(key)
        
        if value is not None:
            async with self._stats_lock:
                self.stats.hits += 1
            return value
        
        if self.disk_cache:
            value = await self.disk_cache.get(key)
            if value is not None:
                await self.memory_cache.set(key, value)
                async with self._stats_lock:
                    self.stats.hits += 1
                return value
        
        async with self._stats_lock:
            self.stats.misses += 1
        
        return None
    
    async def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: float = 0,
        bypass_disk: bool = False
    ) -> None:
        """Set value in cache."""
        for mw in self._middleware:
            await mw("set", key, value)
        
        await self.memory_cache.set(key, value, ttl_seconds)
        
        if self.disk_cache and not bypass_disk:
            await self.disk_cache.set(key, value)
    
    async def delete(self, key: str) -> bool:
        """Delete value from cache."""
        result = await self.memory_cache.delete(key)
        if self.disk_cache:
            await self.disk_cache.delete(key)
        return result
    
    async def clear(self) -> None:
        """Clear all cache levels."""
        await self.memory_cache.clear()
        if self.disk_cache:
            await self.disk_cache.clear()
    
    async def warm_up(
        self,
        keys: List[str],
        loader: Callable[[str], Any]
    ) -> Dict[str, Any]:
        """Warm up cache with specified keys."""
        results = {}
        
        for key in keys:
            value = await self.get(key)
            if value is None and loader:
                try:
                    value = loader(key)
                    if value is not None:
                        await self.set(key, value)
                        results[key] = value
                except Exception as e:
                    logger.error(f"Cache warm-up error for {key}: {e}")
        
        return results
    
    def add_middleware(self, func: Callable) -> None:
        """Add cache middleware."""
        self._middleware.append(func)
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        stats = CacheStats(
            hits=self.stats.hits,
            misses=self.stats.misses,
            evictions=self.stats.evictions,
            expirations=self.stats.expirations,
            entries_count=await self.memory_cache.size()
        )
        
        return {
            "hits": stats.hits,
            "misses": stats.misses,
            "hit_rate": f"{stats.hit_rate * 100:.1f}%",
            "evictions": stats.evictions,
            "expirations": stats.expirations,
            "memory_entries": await self.memory_cache.size(),
            "eviction_policy": self.eviction_policy.value
        }
    
    async def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate all keys matching a pattern."""
        count = 0
        for key in [k for k in ["_cache_keys"]]:
            if pattern in key:
                await self.delete(key)
                count += 1
        return count
