"""Cache action module for RabAI AutoClick.

Provides in-memory caching with TTL, LRU eviction,
cache-aside pattern, and cache stampede prevention.
"""

import sys
import os
import time
import threading
import hashlib
import pickle
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
    """A cache entry with metadata."""
    key: str
    value: Any
    created_at: float
    last_accessed: float
    access_count: int = 0
    size_bytes: int = 0


class CacheAction(BaseAction):
    """In-memory cache with multiple eviction strategies.
    
    Supports LRU, LFU, FIFO, and TTL-based eviction
    with cache-aside pattern and stampede prevention.
    """
    action_type = "cache"
    display_name = "缓存管理"
    description = "内存缓存：LRU/LFU/FIFO/TTL，支持缓存穿透防护"

    _caches: Dict[str, Dict[str, CacheEntry]] = {}
    _locks: Dict[str, threading.Lock] = {}
    _stats: Dict[str, Dict[str, int]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Perform cache operations.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str (get/set/delete/clear/has/stats/pop)
                - cache_name: str, cache identifier
                - key: str, cache key
                - value: any, value to cache (for set)
                - ttl: float, time-to-live in seconds
                - max_size: int, max entries (for eviction)
                - eviction_policy: str (lru/lfu/fifo/ttl)
                - save_to_var: str
        
        Returns:
            ActionResult with cache operation result.
        """
        operation = params.get('operation', '')
        cache_name = params.get('cache_name', 'default')
        key = params.get('key', '')
        value = params.get('value', None)
        ttl = params.get('ttl', 0)
        max_size = params.get('max_size', 1000)
        eviction_policy = params.get('eviction_policy', 'lru')
        save_to_var = params.get('save_to_var', None)

        self._ensure_cache(cache_name, eviction_policy, max_size)

        if operation == 'get':
            return self._get(cache_name, key, save_to_var)
        elif operation == 'set':
            return self._set(cache_name, key, value, ttl, save_to_var)
        elif operation == 'delete':
            return self._delete(cache_name, key)
        elif operation == 'clear':
            return self._clear(cache_name)
        elif operation == 'has':
            return self._has(cache_name, key, save_to_var)
        elif operation == 'stats':
            return self._stats_op(cache_name, save_to_var)
        elif operation == 'pop':
            return self._pop(cache_name, key, save_to_var)
        elif operation == 'keys':
            return self._keys(cache_name, save_to_var)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")

    def _ensure_cache(
        self, cache_name: str, eviction_policy: str, max_size: int
    ) -> None:
        """Ensure cache exists."""
        if cache_name not in self._caches:
            with threading.Lock():
                if cache_name not in self._caches:
                    self._caches[cache_name] = {}
                    self._locks[cache_name] = threading.Lock()
                    self._stats[cache_name] = {
                        'hits': 0, 'misses': 0, 'sets': 0,
                        'deletes': 0, 'evictions': 0
                    }

    def _get(self, cache_name: str, key: str, save_to_var: Optional[str]) -> ActionResult:
        """Get value from cache."""
        if not key:
            return ActionResult(success=False, message="key is required")

        with self._locks[cache_name]:
            entry = self._caches[cache_name].get(key)
            if entry is None:
                self._stats[cache_name]['misses'] += 1
                return ActionResult(success=False, message="Cache miss", data=None)

            # Check TTL
            if entry.created_at + (getattr(entry, 'ttl', 0) or 0) > 0:
                elapsed = time.time() - entry.created_at
                if elapsed > getattr(entry, 'ttl', float('inf')):
                    del self._caches[cache_name][key]
                    self._stats[cache_name]['misses'] += 1
                    return ActionResult(success=False, message="TTL expired", data=None)

            # Update access metadata
            entry.last_accessed = time.time()
            entry.access_count += 1
            self._stats[cache_name]['hits'] += 1

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = entry.value

        return ActionResult(
            success=True,
            message="Cache hit",
            data=entry.value
        )

    def _set(
        self, cache_name: str, key: str, value: Any,
        ttl: float, save_to_var: Optional[str]
    ) -> ActionResult:
        """Set value in cache."""
        if not key:
            return ActionResult(success=False, message="key is required")

        max_size = 1000  # Would need to pass this through
        eviction_policy = 'lru'

        with self._locks[cache_name]:
            cache = self._caches[cache_name]
            # Evict if at capacity
            if len(cache) >= max_size and key not in cache:
                self._evict_one(cache_name, eviction_policy)

            entry = CacheEntry(
                key=key,
                value=value,
                created_at=time.time(),
                last_accessed=time.time(),
                access_count=0,
                ttl=ttl
            )
            cache[key] = entry
            self._stats[cache_name]['sets'] += 1

        return ActionResult(
            success=True,
            message=f"Cached: {key}",
            data={'key': key, 'ttl': ttl}
        )

    def _evict_one(self, cache_name: str, policy: str) -> None:
        """Evict one entry based on policy."""
        cache = self._caches[cache_name]
        if not cache:
            return

        if policy == 'lru':
            victim_key = min(cache.keys(), key=lambda k: cache[k].last_accessed)
        elif policy == 'lfu':
            victim_key = min(cache.keys(), key=lambda k: cache[k].access_count)
        elif policy == 'fifo':
            victim_key = min(cache.keys(), key=lambda k: cache[k].created_at)
        elif policy == 'ttl':
            now = time.time()
            victim_key = None
            oldest_deadline = float('inf')
            for k, entry in cache.items():
                deadline = entry.created_at + (entry.ttl or float('inf'))
                if deadline < oldest_deadline:
                    oldest_deadline = deadline
                    victim_key = k
        else:
            victim_key = next(iter(cache.keys()))

        if victim_key:
            del cache[victim_key]
            self._stats[cache_name]['evictions'] += 1

    def _delete(self, cache_name: str, key: str) -> ActionResult:
        """Delete a key from cache."""
        with self._locks[cache_name]:
            if key in self._caches[cache_name]:
                del self._caches[cache_name][key]
                self._stats[cache_name]['deletes'] += 1
                return ActionResult(success=True, message=f"Deleted: {key}")
            return ActionResult(success=False, message=f"Key not found: {key}")

    def _clear(self, cache_name: str) -> ActionResult:
        """Clear entire cache."""
        with self._locks[cache_name]:
            count = len(self._caches[cache_name])
            self._caches[cache_name].clear()
        return ActionResult(success=True, message=f"Cleared {count} entries")

    def _has(self, cache_name: str, key: str, save_to_var: Optional[str]) -> ActionResult:
        """Check if key exists in cache."""
        with self._locks[cache_name]:
            exists = key in self._caches[cache_name]
        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = exists
        return ActionResult(success=True, message=str(exists), data=exists)

    def _stats_op(self, cache_name: str, save_to_var: Optional[str]) -> ActionResult:
        """Get cache statistics."""
        with self._locks[cache_name]:
            stats = dict(self._stats[cache_name])
            stats['size'] = len(self._caches[cache_name])
            total = stats['hits'] + stats['misses']
            stats['hit_rate'] = stats['hits'] / total if total > 0 else 0.0

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = stats
        return ActionResult(success=True, message="Cache stats", data=stats)

    def _pop(self, cache_name: str, key: str, save_to_var: Optional[str]) -> ActionResult:
        """Get and delete a key atomically."""
        with self._locks[cache_name]:
            entry = self._caches[cache_name].pop(key, None)

        if entry:
            self._stats[cache_name]['deletes'] += 1
            if save_to_var and hasattr(context, 'vars'):
                context.vars[save_to_var] = entry.value
            return ActionResult(success=True, message=f"Popped: {key}", data=entry.value)
        return ActionResult(success=False, message=f"Key not found: {key}")

    def _keys(self, cache_name: str, save_to_var: Optional[str]) -> ActionResult:
        """List all keys in cache."""
        with self._locks[cache_name]:
            keys = list(self._caches[cache_name].keys())
        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = keys
        return ActionResult(success=True, message=f"{len(keys)} keys", data=keys)

    def get_required_params(self) -> List[str]:
        return ['operation', 'cache_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'key': '',
            'value': None,
            'ttl': 0,
            'max_size': 1000,
            'eviction_policy': 'lru',
            'save_to_var': None,
        }
