"""API Cache action module for RabAI AutoClick.

Provides API caching operations:
- CacheGetAction: Get cached response
- CacheSetAction: Set cache
- CacheInvalidateAction: Invalidate cache
- CacheStatsAction: Cache statistics
"""

from __future__ import annotations

import sys
import os
import time
import hashlib
import json
from typing import Any, Dict, Optional
from collections import defaultdict

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CacheGetAction(BaseAction):
    """Get cached response."""
    action_type = "cache_get"
    display_name = "缓存获取"
    description = "获取缓存数据"
    version = "1.0"

    def __init__(self):
        super().__init__()
        self._cache = {}
        self._stats = defaultdict(lambda: {'hits': 0, 'misses': 0, 'size': 0})

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cache get."""
        key = params.get('key', '')
        namespace = params.get('namespace', 'default')
        ttl = params.get('ttl', 3600)
        output_var = params.get('output_var', 'cache_result')

        if not key:
            return ActionResult(success=False, message="key is required")

        try:
            resolved_key = context.resolve_value(key) if context else key
            resolved_namespace = context.resolve_value(namespace) if context else namespace

            cache_key = f"{resolved_namespace}:{resolved_key}"
            entry = self._cache.get(cache_key)

            if entry:
                if entry['expires_at'] > time.time():
                    self._stats[resolved_namespace]['hits'] += 1
                    result = {
                        'hit': True,
                        'value': entry['value'],
                        'cached_at': entry['cached_at'],
                        'key': cache_key,
                    }
                    return ActionResult(
                        success=True,
                        data={output_var: result},
                        message="Cache hit"
                    )
                else:
                    del self._cache[cache_key]

            self._stats[resolved_namespace]['misses'] += 1
            result = {
                'hit': False,
                'key': cache_key,
            }
            return ActionResult(
                success=True,
                data={output_var: result},
                message="Cache miss"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache get error: {e}")


class CacheSetAction(BaseAction):
    """Set cache."""
    action_type = "cache_set"
    display_name = "缓存设置"
    description = "设置缓存数据"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cache set."""
        key = params.get('key', '')
        value = params.get('value', None)
        namespace = params.get('namespace', 'default')
        ttl = params.get('ttl', 3600)
        output_var = params.get('output_var', 'cache_result')

        if not key or value is None:
            return ActionResult(success=False, message="key and value are required")

        try:
            resolved_key = context.resolve_value(key) if context else key
            resolved_value = context.resolve_value(value) if context else value
            resolved_ttl = context.resolve_value(ttl) if context else ttl

            cache_key = f"{namespace}:{resolved_key}"

            if hasattr(self.__class__, '_cache'):
                self._cache[cache_key] = {
                    'value': resolved_value,
                    'cached_at': time.time(),
                    'expires_at': time.time() + resolved_ttl,
                }

            result = {
                'key': cache_key,
                'ttl': resolved_ttl,
                'set': True,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Cache set: {cache_key[:30]}... (TTL: {resolved_ttl}s)"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache set error: {e}")


class CacheInvalidateAction(BaseAction):
    """Invalidate cache."""
    action_type = "cache_invalidate"
    display_name = "缓存失效"
    description = "清除缓存"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cache invalidation."""
        key = params.get('key', '')
        namespace = params.get('namespace', 'default')
        pattern = params.get('pattern', None)
        output_var = params.get('output_var', 'invalidate_result')

        try:
            resolved_key = context.resolve_value(key) if context else key
            resolved_namespace = context.resolve_value(namespace) if context else namespace

            if hasattr(self.__class__, '_cache'):
                if resolved_key:
                    cache_key = f"{resolved_namespace}:{resolved_key}"
                    if cache_key in self._cache:
                        del self._cache[cache_key]
                        count = 1
                    else:
                        count = 0
                elif pattern:
                    count = 0
                    to_delete = [k for k in self._cache.keys() if pattern in k and k.startswith(f"{resolved_namespace}:")]
                    for k in to_delete:
                        del self._cache[k]
                        count += 1
                else:
                    to_delete = [k for k in self._cache.keys() if k.startswith(f"{resolved_namespace}:")]
                    for k in to_delete:
                        del self._cache[k]
                    count = len(to_delete)
            else:
                count = 0

            result = {
                'invalidated': count,
                'namespace': resolved_namespace,
                'key': resolved_key,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Invalidated {count} cache entries"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache invalidate error: {e}")


class CacheStatsAction(BaseAction):
    """Cache statistics."""
    action_type = "cache_stats"
    display_name = "缓存统计"
    description = "缓存统计信息"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cache stats."""
        namespace = params.get('namespace', 'default')
        output_var = params.get('output_var', 'cache_stats')

        try:
            resolved_namespace = context.resolve_value(namespace) if context else namespace

            if hasattr(self.__class__, '_stats'):
                stats = self._stats[resolved_namespace]
            else:
                stats = {'hits': 0, 'misses': 0}

            total = stats.get('hits', 0) + stats.get('misses', 0)
            hit_rate = stats.get('hits', 0) / total if total > 0 else 0

            cache_size = 0
            if hasattr(self.__class__, '_cache'):
                cache_size = len([k for k in self._cache.keys() if k.startswith(f"{resolved_namespace}:")])

            result = {
                'namespace': resolved_namespace,
                'hits': stats.get('hits', 0),
                'misses': stats.get('misses', 0),
                'total_requests': total,
                'hit_rate': hit_rate,
                'cache_size': cache_size,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Cache stats: {hit_rate:.1%} hit rate ({stats.get('hits', 0)} hits)"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache stats error: {e}")
