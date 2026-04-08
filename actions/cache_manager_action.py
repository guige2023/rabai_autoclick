"""Cache manager action module for RabAI AutoClick.

Provides caching functionality with TTL, eviction policies,
cache invalidation, and distributed cache support.
"""

import time
import sys
import os
import hashlib
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class EvictionPolicy(Enum):
    """Cache eviction policies."""
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    TTL = "ttl"


@dataclass
class CacheEntry:
    """A single cache entry."""
    key: str
    value: Any
    created_at: float
    accessed_at: float
    access_count: int
    ttl: Optional[float] = None
    tags: List[str] = field(default_factory=list)
    
    def is_expired(self) -> bool:
        """Check if entry has expired."""
        if self.ttl is None:
            return False
        return time.time() - self.created_at > self.ttl
    
    def touch(self) -> None:
        """Update access time and count."""
        self.accessed_at = time.time()
        self.access_count += 1


class CacheManagerAction(BaseAction):
    """Cache manager action with TTL and eviction policies.
    
    Supports LRU, LFU, FIFO eviction, cache tagging,
    invalidation patterns, and statistics tracking.
    """
    action_type = "cache_manager"
    display_name = "缓存管理器"
    description = "缓存管理与TTL过期策略"
    
    def __init__(self):
        super().__init__()
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.RLock()
        self._eviction_policy = EvictionPolicy.LRU
        self._max_size = 1000
        self._default_ttl: Optional[float] = None
    
    def set_policy(self, policy: EvictionPolicy, max_size: int = 1000, default_ttl: Optional[float] = None) -> None:
        """Set cache eviction policy and limits.
        
        Args:
            policy: Eviction policy to use.
            max_size: Maximum number of entries.
            default_ttl: Default TTL in seconds.
        """
        self._eviction_policy = policy
        self._max_size = max_size
        self._default_ttl = default_ttl
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cache operations.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                operation: get|set|delete|clear|invalidate|stats
                key: Cache key (for get/set/delete)
                value: Value to cache (for set)
                ttl: TTL in seconds (for set)
                tags: List of tags (for set/invalidate)
                pattern: Key pattern (for invalidate).
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', 'get')
        
        if operation == 'get':
            return self._get(params)
        elif operation == 'set':
            return self._set(params)
        elif operation == 'delete':
            return self._delete(params)
        elif operation == 'clear':
            return self._clear(params)
        elif operation == 'invalidate':
            return self._invalidate(params)
        elif operation == 'stats':
            return self._stats(params)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")
    
    def _get(self, params: Dict[str, Any]) -> ActionResult:
        """Get value from cache."""
        key = params.get('key')
        
        if not key:
            return ActionResult(success=False, message="Key is required")
        
        with self._lock:
            entry = self._cache.get(key)
            
            if entry is None:
                return ActionResult(
                    success=False,
                    message=f"Key '{key}' not found",
                    data={'hit': False}
                )
            
            if entry.is_expired():
                del self._cache[key]
                return ActionResult(
                    success=False,
                    message=f"Key '{key}' expired",
                    data={'hit': False, 'expired': True}
                )
            
            entry.touch()
            
            return ActionResult(
                success=True,
                message=f"Cache hit for '{key}'",
                data={
                    'hit': True,
                    'value': entry.value,
                    'ttl_remaining': entry.ttl - (time.time() - entry.created_at) if entry.ttl else None
                }
            )
    
    def _set(self, params: Dict[str, Any]) -> ActionResult:
        """Set value in cache."""
        key = params.get('key')
        value = params.get('value')
        ttl = params.get('ttl', self._default_ttl)
        tags = params.get('tags', [])
        
        if not key:
            return ActionResult(success=False, message="Key is required")
        
        with self._lock:
            if len(self._cache) >= self._max_size and key not in self._cache:
                self._evict()
            
            now = time.time()
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=now,
                accessed_at=now,
                access_count=0,
                ttl=ttl,
                tags=tags
            )
            self._cache[key] = entry
            
            return ActionResult(
                success=True,
                message=f"Cached '{key}' with TTL {ttl}s",
                data={
                    'key': key,
                    'ttl': ttl,
                    'tags': tags,
                    'cache_size': len(self._cache)
                }
            )
    
    def _delete(self, params: Dict[str, Any]) -> ActionResult:
        """Delete key from cache."""
        key = params.get('key')
        
        if not key:
            return ActionResult(success=False, message="Key is required")
        
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return ActionResult(
                    success=True,
                    message=f"Deleted '{key}'",
                    data={'deleted': True, 'cache_size': len(self._cache)}
                )
            
            return ActionResult(
                success=False,
                message=f"Key '{key}' not found",
                data={'deleted': False}
            )
    
    def _clear(self, params: Dict[str, Any]) -> ActionResult:
        """Clear all cache entries."""
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            
            return ActionResult(
                success=True,
                message=f"Cleared {count} entries",
                data={'cleared_count': count}
            )
    
    def _invalidate(self, params: Dict[str, Any]) -> ActionResult:
        """Invalidate cache entries by tag or pattern."""
        tags = params.get('tags', [])
        pattern = params.get('pattern')
        older_than = params.get('older_than')
        
        with self._lock:
            removed = 0
            
            if tags:
                keys_to_remove = [
                    k for k, e in self._cache.items()
                    if any(tag in e.tags for tag in tags)
                ]
                for key in keys_to_remove:
                    del self._cache[key]
                    removed += 1
            
            if pattern:
                import fnmatch
                keys_to_remove = [
                    k for k in self._cache.keys()
                    if fnmatch.fnmatch(k, pattern)
                ]
                for key in keys_to_remove:
                    if key not in [k for k, e in self._cache.items() if e.tags]:
                        del self._cache[key]
                        removed += 1
            
            if older_than:
                cutoff = time.time() - older_than
                keys_to_remove = [
                    k for k, e in self._cache.items()
                    if e.created_at < cutoff
                ]
                for key in keys_to_remove:
                    del self._cache[key]
                    removed += 1
            
            return ActionResult(
                success=True,
                message=f"Invalidated {removed} entries",
                data={'invalidated': removed, 'remaining': len(self._cache)}
            )
    
    def _stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get cache statistics."""
        with self._lock:
            now = time.time()
            expired = sum(1 for e in self._cache.values() if e.is_expired())
            
            total_accesses = sum(e.access_count for e in self._cache.values())
            oldest = min((e.created_at for e in self._cache.values()), default=0)
            newest = max((e.created_at for e in self._cache.values()), default=0)
            
            return ActionResult(
                success=True,
                message="Cache statistics",
                data={
                    'size': len(self._cache),
                    'max_size': self._max_size,
                    'policy': self._eviction_policy.value,
                    'expired_entries': expired,
                    'total_accesses': total_accesses,
                    'oldest_entry_age': now - oldest if oldest else 0,
                    'newest_entry_age': now - newest if newest else 0,
                    'default_ttl': self._default_ttl
                }
            )
    
    def _evict(self) -> None:
        """Evict entry based on eviction policy."""
        if not self._cache:
            return
        
        if self._eviction_policy == EvictionPolicy.LRU:
            key_to_remove = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].accessed_at
            )
        elif self._eviction_policy == EvictionPolicy.LFU:
            key_to_remove = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].access_count
            )
        elif self._eviction_policy == EvictionPolicy.FIFO:
            key_to_remove = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].created_at
            )
        elif self._eviction_policy == EvictionPolicy.TTL:
            key_to_remove = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].created_at + (self._cache[k].ttl or float('inf'))
            )
        else:
            key_to_remove = next(iter(self._cache))
        
        del self._cache[key_to_remove]
    
    def get_or_compute(
        self,
        key: str,
        compute_fn: Callable[[], Any],
        ttl: Optional[float] = None
    ) -> Any:
        """Get from cache or compute and cache if not present.
        
        Args:
            key: Cache key.
            compute_fn: Function to compute value if not cached.
            ttl: Optional TTL override.
            
        Returns:
            Cached or computed value.
        """
        with self._lock:
            entry = self._cache.get(key)
            
            if entry and not entry.is_expired():
                entry.touch()
                return entry.value
            
            value = compute_fn()
            
            if len(self._cache) >= self._max_size:
                self._evict()
            
            now = time.time()
            self._cache[key] = CacheEntry(
                key=key,
                value=value,
                created_at=now,
                accessed_at=now,
                access_count=1,
                ttl=ttl or self._default_ttl
            )
            
            return value
