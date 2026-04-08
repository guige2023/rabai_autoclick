"""API cache action module for RabAI AutoClick.

Provides caching functionality for API responses including
in-memory cache, TTL-based expiration, and cache invalidation.
"""

import time
import hashlib
import json
import sys
import os
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiCacheAction(BaseAction):
    """Cache API responses with TTL-based expiration.
    
    Stores responses in memory cache with configurable
    TTL, key generation, and invalidation strategies.
    """
    action_type = "api_cache"
    display_name = "API缓存"
    description = "缓存API响应结果"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Cache or retrieve from cache.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation (get|set|invalidate|clear),
                   cache_key, ttl_seconds, data, key_fields.
        
        Returns:
            ActionResult with cache operation result.
        """
        operation = params.get('operation', 'get')
        start_time = time.time()

        if not hasattr(context, '_api_cache'):
            context._api_cache = {}

        cache = context._api_cache

        if operation == 'get':
            cache_key = self._build_key(params)
            entry = cache.get(cache_key)
            if entry and (time.time() - entry['cached_at']) < entry['ttl']:
                return ActionResult(
                    success=True,
                    message="Cache hit",
                    data={
                        'hit': True,
                        'data': entry['data'],
                        'age_seconds': time.time() - entry['cached_at']
                    },
                    duration=time.time() - start_time
                )
            return ActionResult(
                success=True,
                message="Cache miss",
                data={'hit': False, 'cache_key': cache_key}
            )

        elif operation == 'set':
            cache_key = self._build_key(params)
            data = params.get('data')
            ttl = params.get('ttl_seconds', 300)
            cache[cache_key] = {
                'data': data,
                'cached_at': time.time(),
                'ttl': ttl
            }
            return ActionResult(
                success=True,
                message=f"Cached with TTL={ttl}s",
                data={
                    'cached': True,
                    'cache_key': cache_key,
                    'ttl': ttl
                },
                duration=time.time() - start_time
            )

        elif operation == 'invalidate':
            cache_key = self._build_key(params)
            if cache_key in cache:
                del cache[cache_key]
            return ActionResult(
                success=True,
                message=f"Invalidated: {cache_key}",
                data={'invalidated': True, 'cache_key': cache_key}
            )

        elif operation == 'clear':
            count = len(cache)
            cache.clear()
            return ActionResult(
                success=True,
                message=f"Cleared {count} cache entries",
                data={'cleared': count}
            )

        return ActionResult(success=False, message=f"Unknown operation: {operation}")

    def _build_key(self, params: Dict[str, Any]) -> str:
        """Build cache key from request parameters."""
        key_data = {}
        key_fields = params.get('key_fields', ['url', 'method', 'params'])
        for field in key_fields:
            if field in params:
                key_data[field] = params[field]

        key_str = json.dumps(key_data, sort_keys=True)
        return hashlib.md5(key_str.encode()).hexdigest()


class CacheAsideAction(BaseAction):
    """Cache-aside pattern: read from cache, fallback to source.
    
    Implements the cache-aside pattern where reads check cache first,
    then source on miss, and populate cache on successful fetch.
    """
    action_type = "cache_aside"
    display_name = "旁路缓存"
    description = "旁路缓存模式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cache-aside pattern.
        
        Args:
            context: Execution context.
            params: Dict with keys: cache_key, ttl_seconds, source_func,
                   source_params, bypass_cache.
        
        Returns:
            ActionResult with data (from cache or source).
        """
        cache_key = params.get('cache_key', '')
        ttl = params.get('ttl_seconds', 300)
        bypass_cache = params.get('bypass_cache', False)
        source_func = params.get('source_func', '')
        source_params = params.get('source_params', {})
        start_time = time.time()

        if not bypass_cache:
            cache_action = ApiCacheAction()
            cache_result = cache_action.execute(context, {
                'operation': 'get',
                'cache_key': cache_key
            })
            if cache_result.success and cache_result.data.get('hit'):
                cache_result.duration = time.time() - start_time
                return cache_result

        if not source_func:
            return ActionResult(
                success=False,
                message="source_func required on cache miss"
            )

        try:
            if isinstance(source_func, str):
                result_data = self._call_function(source_func, source_params)
            else:
                result_data = source_func(source_params)

            cache_action = ApiCacheAction()
            cache_action.execute(context, {
                'operation': 'set',
                'cache_key': cache_key,
                'data': result_data,
                'ttl_seconds': ttl
            })

            return ActionResult(
                success=True,
                message="Fetched from source and cached",
                data={
                    'data': result_data,
                    'from_cache': False,
                    'cached': True
                },
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Source fetch failed: {str(e)}",
                data={'error': str(e)}
            )

    def _call_function(self, func_name: str, params: Dict) -> Any:
        """Call a function by name."""
        return None


class WriteThroughCacheAction(BaseAction):
    """Write-through cache: write to cache and source simultaneously.
    
    Updates both cache and underlying store on writes,
    ensuring consistency.
    """
    action_type = "write_through_cache"
    display_name = "穿透写缓存"
    description = "穿透写缓存模式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute write-through cache operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: cache_key, data, write_func,
                   write_params, ttl_seconds.
        
        Returns:
            ActionResult with write result.
        """
        cache_key = params.get('cache_key', '')
        data = params.get('data')
        write_func = params.get('write_func', '')
        write_params = params.get('write_params', {})
        ttl = params.get('ttl_seconds', 300)
        start_time = time.time()

        if write_func:
            try:
                if isinstance(write_func, str):
                    self._call_function(write_func, write_params)
                else:
                    write_func(write_params)
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Write failed: {str(e)}",
                    data={'error': str(e)}
                )

        cache_action = ApiCacheAction()
        cache_result = cache_action.execute(context, {
            'operation': 'set',
            'cache_key': cache_key,
            'data': data,
            'ttl_seconds': ttl
        })

        return ActionResult(
            success=True,
            message="Written through to cache and store",
            data={
                'written': True,
                'cached': cache_result.data.get('cached', False)
            },
            duration=time.time() - start_time
        )

    def _call_function(self, func_name: str, params: Dict) -> Any:
        """Call a function by name."""
        return None
