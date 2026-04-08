"""Cache action module for RabAI AutoClick.

Provides caching functionality with TTL, LRU eviction,
and cache-aside pattern support.
"""

import sys
import os
import time
import threading
import hashlib
import json
from typing import Any, Dict, List, Optional, Callable
from collections import OrderedDict
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class LRUCache:
    """Thread-safe LRU cache implementation."""

    def __init__(self, capacity: int = 100):
        self.capacity = capacity
        self.cache: OrderedDict = OrderedDict()
        self.lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                self.hits += 1
                entry = self.cache[key]
                if entry['expires_at'] and time.time() > entry['expires_at']:
                    del self.cache[key]
                    self.misses += 1
                    return None
                return entry['value']
            self.misses += 1
            return None

    def set(self, key: str, value: Any, ttl: float = 0):
        """Set value in cache with optional TTL."""
        with self.lock:
            expires_at = time.time() + ttl if ttl > 0 else None
            if key in self.cache:
                self.cache.move_to_end(key)
            self.cache[key] = {'value': value, 'expires_at': expires_at}
            while len(self.cache) > self.capacity:
                self.cache.popitem(last=False)

    def delete(self, key: str):
        """Delete key from cache."""
        with self.lock:
            if key in self.cache:
                del self.cache[key]

    def clear(self):
        """Clear entire cache."""
        with self.lock:
            self.cache.clear()
            self.hits = 0
            self.misses = 0

    def stats(self) -> Dict:
        """Get cache statistics."""
        with self.lock:
            total = self.hits + self.misses
            hit_rate = self.hits / total if total > 0 else 0
            return {
                'size': len(self.cache),
                'capacity': self.capacity,
                'hits': self.hits,
                'misses': self.misses,
                'hit_rate': round(hit_rate, 3),
            }


class CacheAction(BaseAction):
    """LRU cache with TTL support.
    
    Thread-safe in-memory cache with configurable
    capacity and automatic expiration.
    """
    action_type = "cache"
    display_name = "缓存"
    description = "LRU缓存，支持TTL过期和自动淘汰"

    _caches: Dict[str, LRUCache] = {}
    _lock = threading.Lock()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cache operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - cache_id: str (cache identifier)
                - action: str (get/set/delete/clear/stats)
                - key: str (for get/set/delete)
                - value: any (for set)
                - ttl: float (seconds, for set)
                - capacity: int (for create)
                - save_to_var: str
        
        Returns:
            ActionResult with cache result.
        """
        cache_id = params.get('cache_id', 'default')
        action = params.get('action', 'get')
        key = params.get('key', '')
        value = params.get('value')
        ttl = params.get('ttl', 0)
        capacity = params.get('capacity', 100)
        save_to_var = params.get('save_to_var', 'cache_result')

        with self._lock:
            if cache_id not in self._caches:
                self._caches[cache_id] = LRUCache(capacity=capacity)

        cache = self._caches[cache_id]

        if action == 'get':
            result_value = cache.get(key)
            result = {
                'cache_id': cache_id,
                'action': 'get',
                'key': key,
                'found': result_value is not None,
                'value': result_value,
            }

        elif action == 'set':
            cache.set(key, value, ttl=ttl)
            result = {
                'cache_id': cache_id,
                'action': 'set',
                'key': key,
                'ttl': ttl,
            }

        elif action == 'delete':
            cache.delete(key)
            result = {
                'cache_id': cache_id,
                'action': 'delete',
                'key': key,
            }

        elif action == 'clear':
            cache.clear()
            result = {
                'cache_id': cache_id,
                'action': 'clear',
            }

        elif action == 'stats':
            stats = cache.stats()
            result = {
                'cache_id': cache_id,
                'action': 'stats',
                **stats,
            }

        else:
            return ActionResult(success=False, message=f"Unknown action: {action}")

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(success=True, data=result, message=f"Cache {action}: {key if key else cache_id}")


class CacheAsideAction(BaseAction):
    """Cache-aside pattern implementation.
    
    Check cache first, load from source on miss,
    and populate cache automatically.
    """
    action_type = "cache_aside"
    display_name = "缓存旁路"
    description = "缓存旁路模式：先查缓存，未命中再查源并回填"

    _caches: Dict[str, LRUCache] = {}
    _lock = threading.Lock()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cache-aside pattern.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - cache_id: str
                - key: str
                - loader: callable (function to load data on miss)
                - loader_params: dict
                - ttl: float (cache TTL in seconds)
                - save_to_var: str
        
        Returns:
            ActionResult with cache-aside result.
        """
        cache_id = params.get('cache_id', 'aside_default')
        key = params.get('key', '')
        loader = params.get('loader', None)
        loader_params = params.get('loader_params', {})
        ttl = params.get('ttl', 300)
        save_to_var = params.get('save_to_var', 'cache_aside_result')

        with self._lock:
            if cache_id not in self._caches:
                self._caches[cache_id] = LRUCache(capacity=1000)

        cache = self._caches[cache_id]

        # Try cache first
        value = cache.get(key)

        if value is not None:
            result = {
                'cache_id': cache_id,
                'key': key,
                'source': 'cache',
                'found': True,
                'value': value,
            }
        else:
            # Load from source
            loaded = None
            if loader and callable(loader):
                try:
                    loaded = loader(loader_params)
                except Exception as e:
                    return ActionResult(success=False, message=f"Loader error: {e}")

            if loaded is not None:
                cache.set(key, loaded, ttl=ttl)
                result = {
                    'cache_id': cache_id,
                    'key': key,
                    'source': 'loader',
                    'found': True,
                    'value': loaded,
                }
            else:
                result = {
                    'cache_id': cache_id,
                    'key': key,
                    'source': None,
                    'found': False,
                    'value': None,
                }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=result['found'],
            data=result,
            message=f"Cache-aside: {result['source']} {'hit' if result['found'] else 'miss'}"
        )


class MemoizeAction(BaseAction):
    """Memoize a function call result.
    
    Cache function results based on arguments
    to avoid repeated expensive computations.
    """
    action_type = "memoize"
    display_name = "函数记忆化"
    description = "记忆化：缓存函数结果避免重复计算"

    _memo: Dict[str, Any] = {}
    _lock = threading.Lock()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Memoize function result.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - func_name: str (identifier for the function)
                - args: list (positional arguments)
                - kwargs: dict (keyword arguments)
                - func: callable (the function to call)
                - func_params: dict (params to pass to func)
                - ttl: float (optional TTL)
                - save_to_var: str
        
        Returns:
            ActionResult with memoize result.
        """
        func_name = params.get('func_name', '')
        args = params.get('args', [])
        kwargs = params.get('kwargs', {})
        func = params.get('func', None)
        func_params = params.get('func_params', {})
        ttl = params.get('ttl', 0)
        save_to_var = params.get('save_to_var', 'memoize_result')

        # Build cache key from function name and args
        key_parts = [func_name]
        if args:
            key_parts.append(json.dumps(args, sort_keys=True))
        if kwargs:
            key_parts.append(json.dumps(kwargs, sort_keys=True))
        cache_key = hashlib.sha256('|'.join(key_parts).encode()).hexdigest()

        with self._lock:
            if cache_key in self._memo:
                entry = self._memo[cache_key]
                if entry['expires_at'] and time.time() > entry['expires_at']:
                    del self._memo[cache_key]
                else:
                    result = {
                        'from_cache': True,
                        'value': entry['value'],
                        'func_name': func_name,
                    }
                    if context and save_to_var:
                        context.variables[save_to_var] = result
                    return ActionResult(
                        success=True,
                        data=result,
                        message=f"Memoized hit: {func_name}"
                    )

        # Call function
        value = None
        if func and callable(func):
            try:
                value = func(func_params)
            except Exception as e:
                return ActionResult(success=False, message=f"Function error: {e}")

        # Cache result
        expires_at = time.time() + ttl if ttl > 0 else None
        with self._lock:
            self._memo[cache_key] = {
                'value': value,
                'expires_at': expires_at,
            }

        result = {
            'from_cache': False,
            'value': value,
            'func_name': func_name,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(success=True, data=result, message=f"Memoized miss: {func_name}, computed value")
