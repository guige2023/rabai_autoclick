"""Cache V2 action module for RabAI AutoClick.

Provides advanced caching operations including TTL, LRU eviction,
write-through/write-back policies, and cache warming.
"""

import time
import threading
import sys
import os
import hashlib
import json
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import OrderedDict
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CachePolicy(Enum):
    """Cache eviction policies."""
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    TTL = "ttl"


@dataclass
class CacheEntry:
    """A single cache entry.
    
    Attributes:
        key: Cache key.
        value: Cached value.
        created_at: Creation timestamp.
        accessed_at: Last access timestamp.
        access_count: Number of times accessed.
        ttl: Time-to-live in seconds (0 = no expiry).
        size_bytes: Estimated size of the value.
    """
    key: str
    value: Any
    created_at: float
    accessed_at: float
    access_count: int
    ttl: int
    size_bytes: int


class LRUCache:
    """Thread-safe LRU (Least Recently Used) cache implementation."""
    
    def __init__(self, max_size: int, ttl: int = 0):
        """Initialize LRU cache.
        
        Args:
            max_size: Maximum number of entries.
            ttl: Default TTL in seconds (0 = no expiry).
        """
        self.max_size = max_size
        self.default_ttl = ttl
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
    
    def _is_expired(self, entry: CacheEntry) -> bool:
        """Check if an entry has expired."""
        if entry.ttl <= 0:
            return False
        return time.time() - entry.created_at > entry.ttl
    
    def _evict_if_needed(self) -> None:
        """Evict least recently used entry if at capacity."""
        while len(self._cache) >= self.max_size:
            self._cache.popitem(last=False)
    
    def get(self, key: str) -> Tuple[bool, Any]:
        """Get a value from cache.
        
        Args:
            key: Cache key.
        
        Returns:
            Tuple of (found, value).
        """
        with self._lock:
            if key not in self._cache:
                self._misses += 1
                return False, None
            
            entry = self._cache[key]
            
            if self._is_expired(entry):
                del self._cache[key]
                self._misses += 1
                return False, None
            
            entry.accessed_at = time.time()
            entry.access_count += 1
            self._cache.move_to_end(key)
            self._hits += 1
            return True, entry.value
    
    def set(self, key: str, value: Any, ttl: int = None) -> None:
        """Set a value in cache.
        
        Args:
            key: Cache key.
            value: Value to cache.
            ttl: TTL in seconds (uses default if None).
        """
        if ttl is None:
            ttl = self.default_ttl
        
        size = len(str(value)) if value is not None else 0
        
        with self._lock:
            if key in self._cache:
                del self._cache[key]
            
            self._evict_if_needed()
            
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=time.time(),
                accessed_at=time.time(),
                access_count=0,
                ttl=ttl,
                size_bytes=size
            )
            self._cache[key] = entry
    
    def delete(self, key: str) -> bool:
        """Delete a key from cache.
        
        Args:
            key: Cache key.
        
        Returns:
            True if deleted, False if not found.
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False
    
    def clear(self) -> int:
        """Clear all entries.
        
        Returns:
            Number of entries cleared.
        """
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            return count
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            total = self._hits + self._misses
            hit_rate = self._hits / total if total > 0 else 0
            
            return {
                "size": len(self._cache),
                "max_size": self.max_size,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": hit_rate,
                "policy": "lru"
            }
    
    def keys(self) -> List[str]:
        """Get all cache keys."""
        with self._lock:
            return list(self._cache.keys())
    
    def cleanup_expired(self) -> int:
        """Remove all expired entries.
        
        Returns:
            Number of entries removed.
        """
        with self._lock:
            expired = [k for k, e in self._cache.items() if self._is_expired(e)]
            for k in expired:
                del self._cache[k]
            return len(expired)


class LFUCache:
    """Thread-safe LFU (Least Frequently Used) cache implementation."""
    
    def __init__(self, max_size: int, ttl: int = 0):
        """Initialize LFU cache.
        
        Args:
            max_size: Maximum number of entries.
            ttl: Default TTL in seconds.
        """
        self.max_size = max_size
        self.default_ttl = ttl
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
    
    def _is_expired(self, entry: CacheEntry) -> bool:
        if entry.ttl <= 0:
            return False
        return time.time() - entry.created_at > entry.ttl
    
    def _evict_lfu(self) -> None:
        if len(self._cache) >= self.max_size:
            lfu_key = min(self._cache.keys(), key=lambda k: self._cache[k].access_count)
            del self._cache[lfu_key]
    
    def get(self, key: str) -> Tuple[bool, Any]:
        with self._lock:
            if key not in self._cache:
                self._misses += 1
                return False, None
            
            entry = self._cache[key]
            
            if self._is_expired(entry):
                del self._cache[key]
                self._misses += 1
                return False, None
            
            entry.accessed_at = time.time()
            entry.access_count += 1
            self._hits += 1
            return True, entry.value
    
    def set(self, key: str, value: Any, ttl: int = None) -> None:
        if ttl is None:
            ttl = self.default_ttl
        
        size = len(str(value)) if value is not None else 0
        
        with self._lock:
            if key in self._cache:
                del self._cache[key]
            
            self._evict_lfu()
            
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=time.time(),
                accessed_at=time.time(),
                access_count=0,
                ttl=ttl,
                size_bytes=size
            )
            self._cache[key] = entry
    
    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False
    
    def clear(self) -> int:
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            return count
    
    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            total = self._hits + self._misses
            hit_rate = self._hits / total if total > 0 else 0
            
            return {
                "size": len(self._cache),
                "max_size": self.max_size,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": hit_rate,
                "policy": "lfu"
            }
    
    def cleanup_expired(self) -> int:
        with self._lock:
            expired = [k for k, e in self._cache.items() if self._is_expired(e)]
            for k in expired:
                del self._cache[k]
            return len(expired)


# Global cache storage
_caches: Dict[str, Any] = {}
_cache_lock = threading.Lock()


class CacheGetAction(BaseAction):
    """Get a value from a cache."""
    action_type = "cache_get"
    display_name = "缓存获取"
    description = "从缓存读取数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get from cache.
        
        Args:
            context: Execution context.
            params: Dict with keys: cache_name, key.
        
        Returns:
            ActionResult with cached value or miss.
        """
        cache_name = params.get('cache_name', 'default')
        key = params.get('key', '')
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        with _cache_lock:
            if cache_name not in _caches:
                return ActionResult(success=True, message="Cache miss", data={"found": False, "key": key})
            cache = _caches[cache_name]
        
        found, value = cache.get(key)
        
        if found:
            return ActionResult(success=True, message="Cache hit", data={"found": True, "key": key, "value": value})
        else:
            return ActionResult(success=True, message="Cache miss", data={"found": False, "key": key})


class CacheSetAction(BaseAction):
    """Set a value in a cache."""
    action_type = "cache_set"
    display_name = "缓存设置"
    description = "向缓存写入数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Set cache value.
        
        Args:
            context: Execution context.
            params: Dict with keys: cache_name, key, value, ttl, policy.
        
        Returns:
            ActionResult with operation status.
        """
        cache_name = params.get('cache_name', 'default')
        key = params.get('key', '')
        value = params.get('value', None)
        ttl = params.get('ttl', 0)
        policy = params.get('policy', 'lru')
        max_size = params.get('max_size', 1000)
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        with _cache_lock:
            if cache_name not in _caches:
                if policy == 'lfu':
                    _caches[cache_name] = LFUCache(max_size=max_size, ttl=ttl)
                else:
                    _caches[cache_name] = LRUCache(max_size=max_size, ttl=ttl)
            cache = _caches[cache_name]
        
        cache.set(key, value, ttl=ttl)
        
        return ActionResult(success=True, message=f"Set {key} in {cache_name}", data={"key": key, "ttl": ttl})


class CacheDeleteAction(BaseAction):
    """Delete a key from a cache."""
    action_type = "cache_delete"
    display_name = "缓存删除"
    description = "从缓存删除数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Delete from cache.
        
        Args:
            context: Execution context.
            params: Dict with keys: cache_name, key.
        
        Returns:
            ActionResult with deletion status.
        """
        cache_name = params.get('cache_name', 'default')
        key = params.get('key', '')
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        with _cache_lock:
            if cache_name not in _caches:
                return ActionResult(success=True, message="Cache not found", data={"deleted": False})
            cache = _caches[cache_name]
        
        deleted = cache.delete(key)
        
        return ActionResult(success=True, message=f"Deleted {key}" if deleted else f"Key {key} not found", data={"deleted": deleted})


class CacheClearAction(BaseAction):
    """Clear a cache."""
    action_type = "cache_clear"
    display_name = "缓存清空"
    description = "清空缓存内容"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Clear cache.
        
        Args:
            context: Execution context.
            params: Dict with keys: cache_name.
        
        Returns:
            ActionResult with number of entries cleared.
        """
        cache_name = params.get('cache_name', 'default')
        
        with _cache_lock:
            if cache_name not in _caches:
                return ActionResult(success=True, message="Cache not found", data={"cleared": 0})
            cache = _caches[cache_name]
        
        cleared = cache.clear()
        
        return ActionResult(success=True, message=f"Cleared {cleared} entries from {cache_name}", data={"cache_name": cache_name, "cleared": cleared})


class CacheStatsAction(BaseAction):
    """Get cache statistics."""
    action_type = "cache_stats"
    display_name = "缓存统计"
    description = "查看缓存统计信息"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get cache stats.
        
        Args:
            context: Execution context.
            params: Dict with keys: cache_name.
        
        Returns:
            ActionResult with cache statistics.
        """
        cache_name = params.get('cache_name', 'default')
        
        with _cache_lock:
            if cache_name not in _caches:
                return ActionResult(success=True, message="Cache not found", data={"exists": False})
            cache = _caches[cache_name]
        
        stats = cache.get_stats()
        stats["cache_name"] = cache_name
        
        return ActionResult(success=True, message=f"Stats for {cache_name}", data=stats)


class CacheCleanupAction(BaseAction):
    """Remove expired entries from a cache."""
    action_type = "cache_cleanup"
    display_name = "缓存清理"
    description = "清理过期缓存条目"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Cleanup expired entries.
        
        Args:
            context: Execution context.
            params: Dict with keys: cache_name.
        
        Returns:
            ActionResult with number of entries removed.
        """
        cache_name = params.get('cache_name', 'default')
        
        with _cache_lock:
            if cache_name not in _caches:
                return ActionResult(success=True, message="Cache not found", data={"removed": 0})
            cache = _caches[cache_name]
        
        removed = cache.cleanup_expired()
        
        return ActionResult(success=True, message=f"Removed {removed} expired entries", data={"removed": removed})
