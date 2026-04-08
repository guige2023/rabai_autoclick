"""
Data Caching Action Module

Provides multi-level caching, cache invalidation, and cache strategies.
"""
from typing import Any, Optional, Callable, TypeVar
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from collections import OrderedDict
import asyncio
import hashlib
import json


T = TypeVar('T')


class CacheStrategy(Enum):
    """Cache strategies."""
    LRU = "lru"  # Least Recently Used
    LFU = "lfu"  # Least Frequently Used
    FIFO = "fifo"  # First In First Out
    TTL = "ttl"  # Time To Live
    WRITE_THROUGH = "write_through"
    WRITE_BACK = "write_back"


@dataclass
class CacheEntry:
    """A cache entry."""
    key: str
    value: Any
    created_at: datetime
    accessed_at: datetime
    access_count: int = 0
    ttl_seconds: Optional[float] = None
    tags: list[str] = field(default_factory=list)
    size_bytes: int = 0


@dataclass
class CacheStats:
    """Cache statistics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0
    writes: int = 0
    deletes: int = 0


@dataclass
class CacheConfig:
    """Cache configuration."""
    name: str
    max_size: int = 1000
    max_memory_mb: float = 100.0
    strategy: CacheStrategy = CacheStrategy.LRU
    default_ttl_seconds: Optional[float] = 3600.0
    enable_stats: bool = True
    serializer: Optional[Callable[[Any], bytes]] = None
    deserializer: Optional[Callable[[bytes], Any]] = None


class LRUCache:
    """Least Recently Used cache implementation."""
    
    def __init__(self, max_size: int):
        self.max_size = max_size
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
    
    def get(self, key: str) -> Optional[CacheEntry]:
        """Get entry and mark as recently used."""
        if key not in self._cache:
            return None
        
        entry = self._cache[key]
        self._cache.move_to_end(key)
        entry.accessed_at = datetime.now()
        entry.access_count += 1
        return entry
    
    def put(self, key: str, entry: CacheEntry):
        """Put entry in cache."""
        if key in self._cache:
            self._cache.move_to_end(key)
        elif len(self._cache) >= self.max_size:
            # Evict oldest
            self._cache.popitem(last=False)
        
        self._cache[key] = entry
    
    def remove(self, key: str) -> bool:
        """Remove entry from cache."""
        if key in self._cache:
            del self._cache[key]
            return True
        return False
    
    def clear(self):
        """Clear all entries."""
        self._cache.clear()
    
    def get_all(self) -> list[CacheEntry]:
        """Get all entries."""
        return list(self._cache.values())
    
    def get_lru_key(self) -> Optional[str]:
        """Get the least recently used key."""
        if self._cache:
            return next(iter(self._cache))
        return None


class DataCachingAction:
    """Main data caching action handler."""
    
    def __init__(self, default_config: Optional[CacheConfig] = None):
        self.default_config = default_config or CacheConfig(name="default")
        self._caches: dict[str, LRUCache] = {}
        self._entries: dict[str, dict[str, CacheEntry]] = {}
        self._stats: dict[str, CacheStats] = defaultdict(CacheStats)
        self._write_buffer: dict[str, list[tuple[str, Any]]] = defaultdict(list)
    
    def get_cache(self, name: str, config: Optional[CacheConfig] = None) -> "DataCachingAction":
        """Get or create a named cache."""
        if name not in self._caches:
            cfg = config or self.default_config
            self._caches[name] = LRUCache(max_size=cfg.max_size)
            self._entries[name] = {}
            self._stats[name] = CacheStats()
        return self
    
    async def get(
        self,
        key: str,
        cache_name: str = "default"
    ) -> Optional[Any]:
        """
        Get value from cache.
        
        Args:
            key: Cache key
            cache_name: Name of cache
            
        Returns:
            Cached value or None if not found/expired
        """
        if cache_name not in self._caches:
            return None
        
        entry = self._caches[cache_name].get(key)
        
        if entry is None:
            self._stats[cache_name].misses += 1
            return None
        
        # Check TTL
        if entry.ttl_seconds:
            age = (datetime.now() - entry.created_at).total_seconds()
            if age > entry.ttl_seconds:
                self._caches[cache_name].remove(key)
                del self._entries[cache_name][key]
                self._stats[cache_name].expirations += 1
                self._stats[cache_name].misses += 1
                return None
        
        self._stats[cache_name].hits += 1
        return entry.value
    
    async def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[float] = None,
        tags: Optional[list[str]] = None,
        cache_name: str = "default"
    ):
        """
        Set value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl_seconds: Time to live in seconds
            tags: Tags for cache invalidation
            cache_name: Name of cache
        """
        if cache_name not in self._caches:
            self.get_cache(cache_name)
        
        entry = CacheEntry(
            key=key,
            value=value,
            created_at=datetime.now(),
            accessed_at=datetime.now(),
            ttl_seconds=ttl_seconds,
            tags=tags or []
        )
        
        self._caches[cache_name].put(key, entry)
        self._entries[cache_name][key] = entry
        self._stats[cache_name].writes += 1
        
        # Check memory limit
        await self._check_memory_limit(cache_name)
    
    async def delete(
        self,
        key: str,
        cache_name: str = "default"
    ) -> bool:
        """Delete a key from cache."""
        if cache_name not in self._caches:
            return False
        
        removed = self._caches[cache_name].remove(key)
        if removed and key in self._entries[cache_name]:
            del self._entries[cache_name][key]
            self._stats[cache_name].deletes += 1
        
        return removed
    
    async def invalidate_by_tags(
        self,
        tags: list[str],
        cache_name: str = "default"
    ) -> int:
        """
        Invalidate all cache entries with given tags.
        
        Returns:
            Number of entries invalidated
        """
        if cache_name not in self._entries:
            return 0
        
        to_delete = []
        for key, entry in self._entries[cache_name].items():
            if any(tag in entry.tags for tag in tags):
                to_delete.append(key)
        
        for key in to_delete:
            self._caches[cache_name].remove(key)
            del self._entries[cache_name][key]
        
        self._stats[cache_name].evictions += len(to_delete)
        return len(to_delete)
    
    async def invalidate_by_pattern(
        self,
        pattern: str,
        cache_name: str = "default"
    ) -> int:
        """
        Invalidate cache entries matching a pattern.
        
        Args:
            pattern: Key pattern (supports * wildcard)
            cache_name: Name of cache
            
        Returns:
            Number of entries invalidated
        """
        import fnmatch
        
        if cache_name not in self._entries:
            return 0
        
        to_delete = [
            key for key in self._entries[cache_name]
            if fnmatch.fnmatch(key, pattern)
        ]
        
        for key in to_delete:
            self._caches[cache_name].remove(key)
            del self._entries[cache_name][key]
        
        self._stats[cache_name].evictions += len(to_delete)
        return len(to_delete)
    
    async def clear(self, cache_name: str = "default"):
        """Clear all entries from cache."""
        if cache_name in self._caches:
            self._caches[cache_name].clear()
            self._entries[cache_name].clear()
    
    async def get_or_compute(
        self,
        key: str,
        compute_fn: Callable[[], Awaitable[T]],
        ttl_seconds: Optional[float] = None,
        cache_name: str = "default"
    ) -> T:
        """
        Get from cache or compute if not present.
        
        Args:
            key: Cache key
            compute_fn: Async function to compute value if not cached
            ttl_seconds: Time to live
            cache_name: Name of cache
            
        Returns:
            Cached or computed value
        """
        cached = await self.get(key, cache_name)
        if cached is not None:
            return cached
        
        value = await compute_fn()
        await self.set(key, value, ttl_seconds, cache_name=cache_name)
        return value
    
    async def _check_memory_limit(self, cache_name: str):
        """Check and enforce memory limits."""
        cfg = self.default_config
        
        # Calculate total size
        total_size = sum(
            entry.size_bytes for entry in self._entries[cache_name].values()
        )
        
        max_bytes = cfg.max_memory_mb * 1024 * 1024
        
        while total_size > max_bytes:
            lru_key = self._caches[cache_name].get_lru_key()
            if not lru_key:
                break
            
            entry = self._entries[cache_name][lru_key]
            total_size -= entry.size_bytes
            
            self._caches[cache_name].remove(lru_key)
            del self._entries[cache_name][lru_key]
            self._stats[cache_name].evictions += 1
    
    async def cleanup_expired(self, cache_name: str = "default"):
        """Remove all expired entries."""
        if cache_name not in self._entries:
            return
        
        now = datetime.now()
        to_delete = []
        
        for key, entry in self._entries[cache_name].items():
            if entry.ttl_seconds:
                age = (now - entry.created_at).total_seconds()
                if age > entry.ttl_seconds:
                    to_delete.append(key)
        
        for key in to_delete:
            self._caches[cache_name].remove(key)
            del self._entries[cache_name][key]
            self._stats[cache_name].expirations += 1
    
    def get_stats(self, cache_name: str = "default") -> dict[str, Any]:
        """Get cache statistics."""
        stats = self._stats.get(cache_name, CacheStats())
        total = stats.hits + stats.misses
        
        return {
            "name": cache_name,
            "entries": len(self._entries.get(cache_name, {})),
            "hits": stats.hits,
            "misses": stats.misses,
            "hit_rate": stats.hits / total if total > 0 else 0,
            "evictions": stats.evictions,
            "expirations": stats.expirations,
            "writes": stats.writes,
            "deletes": stats.deletes
        }
    
    async def warm_cache(
        self,
        entries: list[tuple[str, Any]],
        ttl_seconds: Optional[float] = None,
        cache_name: str = "default"
    ):
        """Pre-populate cache with entries."""
        for key, value in entries:
            await self.set(key, value, ttl_seconds, cache_name=cache_name)
