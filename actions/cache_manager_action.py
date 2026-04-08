"""Cache manager action module for RabAI AutoClick.

Provides in-memory caching with TTL, LRU eviction, and cache-aside
pattern support for API responses and computed values.
"""

import sys
import os
import time
import hashlib
import pickle
from typing import Any, Dict, Optional, Callable, Union, Tuple
from dataclasses import dataclass, field
from collections import OrderedDict
from threading import Lock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class CacheEntry:
    """A single cache entry with metadata."""
    key: str
    value: Any
    created_at: float
    accessed_at: float
    ttl: Optional[float]
    hit_count: int = 0
    
    def is_expired(self, current_time: float) -> bool:
        """Check if entry has expired."""
        if self.ttl is None:
            return False
        return (current_time - self.created_at) > self.ttl


class CacheStore:
    """Thread-safe in-memory cache with LRU eviction."""
    
    def __init__(
        self,
        max_size: int = 1000,
        default_ttl: Optional[float] = 300.0
    ):
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._max_size = max_size
        self._default_ttl = default_ttl
        self._lock = Lock()
        self._stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'expirations': 0
        }
    
    def get(self, key: str) -> Tuple[Optional[Any], bool]:
        """Get value from cache.
        
        Returns:
            Tuple of (value, found). found=False if key missing or expired.
        """
        with self._lock:
            current_time = time.time()
            if key not in self._cache:
                self._stats['misses'] += 1
                return None, False
            
            entry = self._cache[key]
            
            # Check expiration
            if entry.is_expired(current_time):
                del self._cache[key]
                self._stats['expirations'] += 1
                self._stats['misses'] += 1
                return None, False
            
            # Update access time and move to end (most recently used)
            entry.accessed_at = current_time
            self._cache.move_to_end(key)
            entry.hit_count += 1
            self._stats['hits'] += 1
            
            return entry.value, True
    
    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None
    ) -> None:
        """Set value in cache with optional TTL override."""
        with self._lock:
            current_time = time.time()
            ttl = ttl if ttl is not None else self._default_ttl
            
            # Remove if exists to update position
            if key in self._cache:
                del self._cache[key]
            
            # Evict LRU entries if at capacity
            while len(self._cache) >= self._max_size:
                oldest_key = next(iter(self._cache))
                del self._cache[oldest_key]
                self._stats['evictions'] += 1
            
            # Add new entry
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=current_time,
                accessed_at=current_time,
                ttl=ttl
            )
            self._cache[key] = entry
    
    def delete(self, key: str) -> bool:
        """Delete key from cache. Returns True if key existed."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False
    
    def clear(self) -> int:
        """Clear all entries. Returns count of cleared entries."""
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            return count
    
    def cleanup_expired(self) -> int:
        """Remove all expired entries. Returns count removed."""
        with self._lock:
            current_time = time.time()
            expired_keys = [
                k for k, v in self._cache.items()
                if v.is_expired(current_time)
            ]
            for key in expired_keys:
                del self._cache[key]
                self._stats['expirations'] += 1
            return len(expired_keys)
    
    def stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        with self._lock:
            total = self._stats['hits'] + self._stats['misses']
            return {
                **self._stats,
                'size': len(self._cache),
                'max_size': self._max_size,
                'hit_rate': self._stats['hits'] / total if total > 0 else 0
            }


class CacheManagerAction(BaseAction):
    """Manage caching with TTL, LRU eviction, and cache-aside pattern.
    
    Provides get/set/delete operations with statistics tracking
    and automatic expiration handling.
    """
    action_type = "cache_manager"
    display_name = "缓存管理"
    description = "带TTL和LRU淘汰的内存缓存管理"
    
    def __init__(self):
        super().__init__()
        self._global_cache = CacheStore()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cache operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'get', 'set', 'delete', 'clear', 'stats', 'cleanup'
                - key: Cache key (for get/set/delete)
                - value: Value to cache (for set)
                - ttl: Time-to-live in seconds (for set, default 300)
                - max_size: Maximum cache size (for init)
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', '').lower()
        
        if operation == 'get':
            return self._get(params)
        elif operation == 'set':
            return self._set(params)
        elif operation == 'delete':
            return self._delete(params)
        elif operation == 'clear':
            return self._clear(params)
        elif operation == 'stats':
            return self._stats(params)
        elif operation == 'cleanup':
            return self._cleanup(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _get(self, params: Dict[str, Any]) -> ActionResult:
        """Get value from cache."""
        key = params.get('key')
        if not key:
            return ActionResult(success=False, message="key is required")
        
        value, found = self._global_cache.get(key)
        
        if found:
            return ActionResult(
                success=True,
                message="Cache hit",
                data={'value': value, 'cached': True}
            )
        else:
            return ActionResult(
                success=True,
                message="Cache miss",
                data={'cached': False}
            )
    
    def _set(self, params: Dict[str, Any]) -> ActionResult:
        """Set value in cache."""
        key = params.get('key')
        value = params.get('value')
        ttl = params.get('ttl')
        
        if not key:
            return ActionResult(success=False, message="key is required")
        if value is None:
            return ActionResult(success=False, message="value is required")
        
        self._global_cache.set(key, value, ttl)
        
        return ActionResult(
            success=True,
            message=f"Cached value with ttl={ttl}",
            data={'key': key}
        )
    
    def _delete(self, params: Dict[str, Any]) -> ActionResult:
        """Delete key from cache."""
        key = params.get('key')
        if not key:
            return ActionResult(success=False, message="key is required")
        
        deleted = self._global_cache.delete(key)
        
        return ActionResult(
            success=True,
            message=f"{'Deleted' if deleted else 'Key not found'}",
            data={'deleted': deleted}
        )
    
    def _clear(self, params: Dict[str, Any]) -> ActionResult:
        """Clear all cache entries."""
        count = self._global_cache.clear()
        return ActionResult(
            success=True,
            message=f"Cleared {count} entries",
            data={'cleared': count}
        )
    
    def _stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get cache statistics."""
        stats = self._global_cache.stats()
        return ActionResult(
            success=True,
            message="Cache statistics",
            data=stats
        )
    
    def _cleanup(self, params: Dict[str, Any]) -> ActionResult:
        """Remove expired entries."""
        count = self._global_cache.cleanup_expired()
        return ActionResult(
            success=True,
            message=f"Removed {count} expired entries",
            data={'removed': count}
        )


class CacheAsideAction(BaseAction):
    """Cache-aside pattern: read-through with write-through support.
    
    Automatically manages cache population and invalidation.
    """
    action_type = "cache_aside"
    display_name = "缓存旁路"
    description = "自动缓存填充和失效的缓存旁路模式"
    
    def __init__(self):
        super().__init__()
        self._cache = CacheStore()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cache-aside operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'read', 'write', 'invalidate'
                - key: Cache key
                - fetch_func: Callable to fetch data on cache miss (for read)
                - value: Value to cache (for write)
                - ttl: Optional TTL override
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', '').lower()
        key = params.get('key')
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        if operation == 'read':
            return self._read_aside(params)
        elif operation == 'write':
            return self._write_aside(params)
        elif operation == 'invalidate':
            deleted = self._cache.delete(key)
            return ActionResult(
                success=True,
                message=f"Invalidated: {deleted}",
                data={'key': key, 'found': deleted}
            )
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _read_aside(self, params: Dict[str, Any]) -> ActionResult:
        """Read with cache-aside pattern."""
        key = params.get('key')
        fetch_func = params.get('fetch_func')
        ttl = params.get('ttl')
        
        # Try cache first
        value, found = self._cache.get(key)
        if found:
            return ActionResult(
                success=True,
                message="Cache hit",
                data={'value': value, 'source': 'cache'}
            )
        
        # Cache miss - fetch from source
        if not callable(fetch_func):
            return ActionResult(
                success=False,
                message="fetch_func required on cache miss"
            )
        
        try:
            value = fetch_func()
            # Populate cache
            self._cache.set(key, value, ttl)
            return ActionResult(
                success=True,
                message="Fetched and cached",
                data={'value': value, 'source': 'fetch'}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Fetch failed: {e}",
                data={'error': str(e)}
            )
    
    def _write_aside(self, params: Dict[str, Any]) -> ActionResult:
        """Write with write-through pattern."""
        key = params.get('key')
        value = params.get('value')
        ttl = params.get('ttl')
        
        if value is None:
            return ActionResult(success=False, message="value is required")
        
        self._cache.set(key, value, ttl)
        return ActionResult(
            success=True,
            message="Cached",
            data={'key': key}
        )
