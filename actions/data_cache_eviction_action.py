"""Data cache eviction action module for RabAI AutoClick.

Provides cache eviction strategies:
- CacheEvictionPolicy: Configurable eviction policies (LRU, LFU, FIFO, etc.)
- CacheEvictor: Evict items based on policy
- AdaptiveCacheEvictor: Adaptively adjust eviction based on access patterns
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import time
import threading
import logging
import random
from dataclasses import dataclass, field
from enum import Enum
from collections import OrderedDict, defaultdict

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EvictionPolicy(Enum):
    """Cache eviction policies."""
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    LIFO = "lifo"
    MRU = "mru"
    RANDOM = "random"
    TTL = "ttl"
    SIZE = "size"


@dataclass
class CacheEntry:
    """Cache entry with metadata."""
    key: str
    value: Any
    size: int = 1
    created_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)
    access_count: int = 0
    ttl: Optional[float] = None
    
    def is_expired(self) -> bool:
        """Check if entry has expired."""
        if self.ttl is None:
            return False
        return time.time() > (self.created_at + self.ttl)
    
    def touch(self):
        """Update access metadata."""
        self.last_accessed = time.time()
        self.access_count += 1


@dataclass
class CacheEvictionConfig:
    """Configuration for cache eviction."""
    policy: EvictionPolicy = EvictionPolicy.LRU
    max_size: int = 1000
    max_bytes: Optional[int] = None
    eviction_batch_size: int = 10
    check_interval: float = 60.0
    ttl_default: Optional[float] = None
    size_aware: bool = True


class LRUCache:
    """Least Recently Used cache eviction."""
    
    def __init__(self, max_size: int):
        self.max_size = max_size
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = threading.RLock()
    
    def get(self, key: str) -> Optional[Any]:
        """Get item and update recency."""
        with self._lock:
            if key not in self._cache:
                return None
            entry = self._cache[key]
            if entry.is_expired():
                del self._cache[key]
                return None
            self._cache.move_to_end(key)
            entry.touch()
            return entry.value
    
    def put(self, key: str, value: Any, size: int = 1, ttl: Optional[float] = None):
        """Put item in cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
            
            while len(self._cache) >= self.max_size:
                self._cache.popitem(last=False)
            
            entry = CacheEntry(key=key, value=value, size=size, ttl=ttl)
            self._cache[key] = entry
    
    def evict(self, count: int = 1) -> List[str]:
        """Evict least recently used items."""
        evicted = []
        with self._lock:
            for _ in range(min(count, len(self._cache))):
                if self._cache:
                    key, _ = self._cache.popitem(last=False)
                    evicted.append(key)
        return evicted
    
    def remove(self, key: str) -> bool:
        """Remove specific key."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
        return False
    
    def clear(self):
        """Clear all entries."""
        with self._lock:
            self._cache.clear()
    
    def get_all_keys(self) -> List[str]:
        """Get all cache keys."""
        with self._lock:
            return list(self._cache.keys())
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            return {"size": len(self._cache), "max_size": self.max_size}


class LFUCache:
    """Least Frequently Used cache eviction."""
    
    def __init__(self, max_size: int):
        self.max_size = max_size
        self._cache: Dict[str, CacheEntry] = {}
        self._freq_groups: Dict[int, Set[str]] = defaultdict(set)
        self._min_freq = 0
        self._lock = threading.RLock()
    
    def get(self, key: str) -> Optional[Any]:
        """Get item and update frequency."""
        with self._lock:
            if key not in self._cache:
                return None
            entry = self._cache[key]
            if entry.is_expired():
                del self._cache[key]
                self._freq_groups[entry.access_count].discard(key)
                return None
            
            old_freq = entry.access_count
            entry.touch()
            new_freq = entry.access_count
            
            self._freq_groups[old_freq].discard(key)
            self._freq_groups[new_freq].add(key)
            
            while self._min_freq in self._freq_groups and not self._freq_groups[self._min_freq]:
                self._min_freq += 1
            
            return entry.value
    
    def put(self, key: str, value: Any, size: int = 1, ttl: Optional[float] = None):
        """Put item in cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
            
            while len(self._cache) >= self.max_size:
                if self._min_freq in self._freq_groups and self._freq_groups[self._min_freq]:
                    evict_key = next(iter(self._freq_groups[self._min_freq]))
                    del self._cache[evict_key]
                    self._freq_groups[self._min_freq].discard(evict_key)
                else:
                    break
            
            entry = CacheEntry(key=key, value=value, size=size, ttl=ttl)
            entry.access_count = 0
            self._cache[key] = entry
            self._freq_groups[0].add(key)
            self._min_freq = 0
    
    def evict(self, count: int = 1) -> List[str]:
        """Evict least frequently used items."""
        evicted = []
        with self._lock:
            for _ in range(min(count, len(self._cache))):
                if self._min_freq in self._freq_groups and self._freq_groups[self._min_freq]:
                    evict_key = next(iter(self._freq_groups[self._min_freq]))
                    del self._cache[evict_key]
                    self._freq_groups[self._min_freq].discard(evict_key)
                    evicted.append(evict_key)
                    
                    while self._min_freq in self._freq_groups and not self._freq_groups[self._min_freq]:
                        self._min_freq += 1
        return evicted


class CacheEvictor:
    """Manage cache eviction across multiple caches."""
    
    def __init__(self, name: str, config: Optional[CacheEvictionConfig] = None):
        self.name = name
        self.config = config or CacheEvictionConfig()
        self._caches: Dict[str, Any] = {}
        self._lock = threading.RLock()
        self._stats = {"total_evictions": 0, "total_hits": 0, "total_misses": 0}
    
    def get_cache(self, cache_name: str) -> Any:
        """Get or create cache instance."""
        with self._lock:
            if cache_name not in self._caches:
                if self.config.policy == EvictionPolicy.LRU:
                    self._caches[cache_name] = LRUCache(self.config.max_size)
                elif self.config.policy == EvictionPolicy.LFU:
                    self._caches[cache_name] = LFUCache(self.config.max_size)
                else:
                    self._caches[cache_name] = LRUCache(self.config.max_size)
            return self._caches[cache_name]
    
    def get(self, cache_name: str, key: str) -> Optional[Any]:
        """Get item from cache."""
        cache = self.get_cache(cache_name)
        result = cache.get(key)
        
        with self._lock:
            if result is not None:
                self._stats["total_hits"] += 1
            else:
                self._stats["total_misses"] += 1
        
        return result
    
    def put(self, cache_name: str, key: str, value: Any, ttl: Optional[float] = None):
        """Put item in cache."""
        cache = self.get_cache(cache_name)
        cache.put(key, value, ttl=ttl or self.config.ttl_default)
    
    def evict(self, cache_name: str, count: Optional[int] = None) -> List[str]:
        """Evict items from cache."""
        cache = self.get_cache(cache_name)
        count = count or self.config.eviction_batch_size
        evicted = cache.evict(count)
        
        with self._lock:
            self._stats["total_evictions"] += len(evicted)
        
        return evicted
    
    def evict_expired(self, cache_name: str) -> List[str]:
        """Evict expired items from cache."""
        cache = self.get_cache(cache_name)
        all_keys = cache.get_all_keys()
        expired = []
        
        for key in all_keys:
            entry = cache._cache.get(key) if hasattr(cache, '_cache') else None
            if entry and entry.is_expired():
                if cache.remove(key):
                    expired.append(key)
        
        with self._lock:
            self._stats["total_evictions"] += len(expired)
        
        return expired
    
    def get_stats(self) -> Dict[str, Any]:
        """Get eviction statistics."""
        with self._lock:
            cache_stats = {}
            for name, cache in self._caches.items():
                if hasattr(cache, 'get_stats'):
                    cache_stats[name] = cache.get_stats()
            
            return {
                "name": self.name,
                "policy": self.config.policy.value,
                "cache_count": len(self._caches),
                **{k: v for k, v in self._stats.items()},
                "caches": cache_stats,
            }


class DataCacheEvictionAction(BaseAction):
    """Data cache eviction action."""
    action_type = "data_cache_eviction"
    display_name = "数据缓存淘汰"
    description = "数据缓存淘汰策略管理"
    
    def __init__(self):
        super().__init__()
        self._evictors: Dict[str, CacheEvictor] = {}
        self._lock = threading.Lock()
    
    def _get_evictor(self, name: str, config: Optional[CacheEvictionConfig] = None) -> CacheEvictor:
        """Get or create evictor."""
        with self._lock:
            if name not in self._evictors:
                self._evictors[name] = CacheEvictor(name, config)
            return self._evictors[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cache eviction operation."""
        try:
            name = params.get("name", "default")
            command = params.get("command", "get")
            cache_name = params.get("cache", "default")
            
            config = CacheEvictionConfig(
                policy=EvictionPolicy[params.get("policy", "lru").upper()],
                max_size=params.get("max_size", 1000),
                eviction_batch_size=params.get("batch_size", 10),
                ttl_default=params.get("ttl"),
            )
            
            evictor = self._get_evictor(name, config)
            
            if command == "get":
                key = params.get("key")
                value = evictor.get(cache_name, key)
                return ActionResult(success=value is not None, data={"value": value})
            
            elif command == "put":
                key = params.get("key")
                value = params.get("value")
                ttl = params.get("ttl")
                evictor.put(cache_name, key, value, ttl)
                return ActionResult(success=True)
            
            elif command == "evict":
                count = params.get("count")
                evicted = evictor.evict(cache_name, count)
                return ActionResult(success=True, data={"evicted": evicted, "count": len(evicted)})
            
            elif command == "evict_expired":
                evicted = evictor.evict_expired(cache_name)
                return ActionResult(success=True, data={"evicted": evicted, "count": len(evicted)})
            
            elif command == "stats":
                stats = evictor.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            elif command == "clear":
                cache = evictor.get_cache(cache_name)
                cache.clear()
                return ActionResult(success=True)
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"DataCacheEvictionAction error: {str(e)}")
