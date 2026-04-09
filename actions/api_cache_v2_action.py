"""API Cache V2 Action Module.

Provides advanced caching strategies for API responses.
"""

import time
import hashlib
import json
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Callable
from collections import OrderedDict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class APICacheV2Action(BaseAction):
    """Advanced API response cache.
    
    Supports TTL, LRU eviction, and cache invalidation strategies.
    """
    action_type = "api_cache_v2"
    display_name = "API缓存V2"
    description = "支持TTL和LRU驱逐的高级缓存"
    
    def __init__(self):
        super().__init__()
        self._cache: OrderedDict = OrderedDict()
        self._metadata: Dict[str, Dict] = {}
        self._max_size = 1000
        self._default_ttl = 300
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cache operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: action, key, value, ttl.
        
        Returns:
            ActionResult with cache result.
        """
        action = params.get('action', 'get')
        key = params.get('key', '')
        
        if action == 'get':
            return self._get(key)
        elif action == 'set':
            return self._set(key, params)
        elif action == 'delete':
            return self._delete(key)
        elif action == 'clear':
            return self._clear()
        elif action == 'stats':
            return self._stats()
        elif action == 'invalidate':
            return self._invalidate(params)
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _get(self, key: str) -> ActionResult:
        """Get value from cache."""
        if key not in self._cache:
            return ActionResult(
                success=True,
                data={'hit': False, 'key': key},
                error=None
            )
        
        meta = self._metadata.get(key, {})
        
        # Check TTL
        if 'expires_at' in meta and time.time() > meta['expires_at']:
            del self._cache[key]
            del self._metadata[key]
            return ActionResult(
                success=True,
                data={'hit': False, 'key': key, 'reason': 'expired'},
                error=None
            )
        
        # Move to end (most recently used)
        self._cache.move_to_end(key)
        meta['hits'] = meta.get('hits', 0) + 1
        self._metadata[key] = meta
        
        return ActionResult(
            success=True,
            data={
                'hit': True,
                'key': key,
                'value': self._cache[key],
                'hits': meta.get('hits', 1)
            },
            error=None
        )
    
    def _set(self, key: str, params: Dict) -> ActionResult:
        """Set value in cache."""
        value = params.get('value')
        ttl = params.get('ttl', self._default_ttl)
        
        # Evict if necessary
        if key not in self._cache and len(self._cache) >= self._max_size:
            self._evict_lru()
        
        self._cache[key] = value
        self._cache.move_to_end(key)
        
        self._metadata[key] = {
            'created_at': time.time(),
            'expires_at': time.time() + ttl if ttl > 0 else float('inf'),
            'hits': 0
        }
        
        return ActionResult(
            success=True,
            data={'key': key, 'cached': True},
            error=None
        )
    
    def _delete(self, key: str) -> ActionResult:
        """Delete value from cache."""
        if key in self._cache:
            del self._cache[key]
        if key in self._metadata:
            del self._metadata[key]
        
        return ActionResult(
            success=True,
            data={'key': key, 'deleted': True},
            error=None
        )
    
    def _clear(self) -> ActionResult:
        """Clear all cache."""
        count = len(self._cache)
        self._cache.clear()
        self._metadata.clear()
        
        return ActionResult(
            success=True,
            data={'cleared_count': count},
            error=None
        )
    
    def _stats(self) -> ActionResult:
        """Get cache statistics."""
        total_hits = sum(m.get('hits', 0) for m in self._metadata.values())
        total_requests = total_hits + (len(self._cache) - total_hits)
        hit_rate = total_hits / total_requests if total_requests > 0 else 0
        
        return ActionResult(
            success=True,
            data={
                'size': len(self._cache),
                'max_size': self._max_size,
                'total_hits': total_hits,
                'hit_rate': hit_rate,
                'keys': list(self._cache.keys())
            },
            error=None
        )
    
    def _invalidate(self, params: Dict) -> ActionResult:
        """Invalidate cache entries by pattern."""
        pattern = params.get('pattern', '')
        prefix = params.get('prefix', '')
        
        if not pattern and not prefix:
            return ActionResult(
                success=False,
                data=None,
                error="Pattern or prefix required"
            )
        
        keys_to_delete = []
        for key in self._cache.keys():
            if prefix and key.startswith(prefix):
                keys_to_delete.append(key)
        
        for key in keys_to_delete:
            del self._cache[key]
            if key in self._metadata:
                del self._metadata[key]
        
        return ActionResult(
            success=True,
            data={'invalidated_count': len(keys_to_delete)},
            error=None
        )
    
    def _evict_lru(self):
        """Evict least recently used entry."""
        if self._cache:
            oldest_key = next(iter(self._cache))
            del self._cache[oldest_key]
            if oldest_key in self._metadata:
                del self._metadata[oldest_key]


class APIResponseCacheAction(BaseAction):
    """Cache API responses by endpoint and parameters.
    
    Automatically generates cache keys from request details.
    """
    action_type = "api_response_cache"
    display_name = "API响应缓存"
    description = "根据端点和参数自动缓存响应"
    
    def __init__(self):
        super().__init__()
        self._cache: Dict[str, Any] = {}
        self._ttl: Dict[str, float] = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute caching operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, method, params, response, ttl.
        
        Returns:
            ActionResult with caching result.
        """
        url = params.get('url', '')
        method = params.get('method', 'GET')
        request_params = params.get('params', {})
        response = params.get('response')
        ttl = params.get('ttl', 300)
        
        if not url:
            return ActionResult(
                success=False,
                data=None,
                error="URL required"
            )
        
        cache_key = self._generate_key(url, method, request_params)
        
        if response is not None:
            # Set cache
            self._cache[cache_key] = response
            self._ttl[cache_key] = time.time() + ttl
            return ActionResult(
                success=True,
                data={'key': cache_key, 'cached': True},
                error=None
            )
        else:
            # Get from cache
            if cache_key in self._cache:
                if time.time() < self._ttl.get(cache_key, 0):
                    return ActionResult(
                        success=True,
                        data={
                            'hit': True,
                            'response': self._cache[cache_key]
                        },
                        error=None
                    )
                else:
                    del self._cache[cache_key]
                    del self._ttl[cache_key]
            
            return ActionResult(
                success=True,
                data={'hit': False, 'key': cache_key},
                error=None
            )
    
    def _generate_key(self, url: str, method: str, params: Dict) -> str:
        """Generate cache key from request details."""
        content = f"{method}:{url}:{json.dumps(params, sort_keys=True)}"
        return hashlib.sha256(content.encode()).hexdigest()


class APICacheWarmerAction(BaseAction):
    """Warm up API cache with frequently accessed endpoints.
    
    Pre-populates cache with expected requests.
    """
    action_type = "api_cache_warmer"
    display_name = "API缓存预热"
    description = "预填充缓存以提高命中率"
    
    def __init__(self):
        super().__init__()
        self._warm_queue: List[Dict] = []
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cache warming.
        
        Args:
            context: Execution context.
            params: Dict with keys: endpoints, warm_now.
        
        Returns:
            ActionResult with warming result.
        """
        endpoints = params.get('endpoints', [])
        warm_now = params.get('warm_now', False)
        
        if not endpoints:
            return ActionResult(
                success=False,
                data=None,
                error="No endpoints specified"
            )
        
        for endpoint in endpoints:
            self._warm_queue.append({
                'url': endpoint.get('url'),
                'method': endpoint.get('method', 'GET'),
                'params': endpoint.get('params', {}),
                'priority': endpoint.get('priority', 1)
            })
        
        warmed_count = 0
        if warm_now:
            warmed_count = self._warm_batch()
        
        return ActionResult(
            success=True,
            data={
                'queued': len(endpoints),
                'warmed': warmed_count,
                'remaining': len(self._warm_queue)
            },
            error=None
        )
    
    def _warm_batch(self) -> int:
        """Warm a batch of cached endpoints."""
        warmed = 0
        for _ in range(min(10, len(self._warm_queue))):
            if self._warm_queue:
                self._warm_queue.pop(0)
                warmed += 1
        return warmed


def register_actions():
    """Register all API Cache V2 actions."""
    return [
        APICacheV2Action,
        APIResponseCacheAction,
        APICacheWarmerAction,
    ]
