"""Cache action module for RabAI AutoClick.

Provides caching functionality for improving performance
with various cache strategies and expiration policies.
"""

import time
import hashlib
import json
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CacheGetAction(BaseAction):
    """Get value from cache.
    
    Retrieves cached data by key.
    """
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
            params: Dict with keys: key, namespace, default.
        
        Returns:
            ActionResult with cached value.
        """
        key = params.get('key', '')
        namespace = params.get('namespace', 'default')
        default = params.get('default', None)

        if not key:
            return ActionResult(success=False, message="key is required")

        try:
            cache = getattr(context, '_cache', None)
            if cache is None:
                context._cache = {}
                cache = context._cache

            cache_key = f"{namespace}:{key}"

            if cache_key not in cache:
                return ActionResult(
                    success=True,
                    message=f"Cache miss: {key}",
                    data={'hit': False, 'key': key, 'value': default}
                )

            entry = cache[cache_key]

            if entry.get('expires_at') and time.time() > entry['expires_at']:
                del cache[cache_key]
                return ActionResult(
                    success=True,
                    message=f"Cache expired: {key}",
                    data={'hit': False, 'key': key, 'value': default}
                )

            return ActionResult(
                success=True,
                message=f"Cache hit: {key}",
                data={
                    'hit': True,
                    'key': key,
                    'value': entry['value'],
                    'age': time.time() - entry.get('created_at', 0)
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Cache get failed: {str(e)}")


class CacheSetAction(BaseAction):
    """Set value in cache.
    
    Stores data with optional TTL.
    """
    action_type = "cache_set"
    display_name = "缓存设置"
    description = "写入数据到缓存"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Set in cache.
        
        Args:
            context: Execution context.
            params: Dict with keys: key, value, namespace, ttl.
        
        Returns:
            ActionResult with set status.
        """
        key = params.get('key', '')
        value = params.get('value', None)
        namespace = params.get('namespace', 'default')
        ttl = params.get('ttl', 0)

        if not key:
            return ActionResult(success=False, message="key is required")

        try:
            cache = getattr(context, '_cache', None)
            if cache is None:
                context._cache = {}
                cache = context._cache

            cache_key = f"{namespace}:{key}"

            entry = {
                'value': value,
                'created_at': time.time(),
                'namespace': namespace
            }

            if ttl > 0:
                entry['expires_at'] = time.time() + ttl
            else:
                entry['expires_at'] = None

            cache[cache_key] = entry

            return ActionResult(
                success=True,
                message=f"Cache set: {key}",
                data={
                    'key': key,
                    'namespace': namespace,
                    'ttl': ttl,
                    'expires_at': entry.get('expires_at')
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Cache set failed: {str(e)}")


class CacheDeleteAction(BaseAction):
    """Delete value from cache.
    
    Removes cached data by key.
    """
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
            params: Dict with keys: key, namespace, pattern.
        
        Returns:
            ActionResult with deletion status.
        """
        key = params.get('key', '')
        namespace = params.get('namespace', None)
        pattern = params.get('pattern', None)

        try:
            cache = getattr(context, '_cache', None)
            if cache is None:
                return ActionResult(success=True, message="Cache empty")

            deleted = 0

            if pattern:
                import re
                for cache_key in list(cache.keys()):
                    ns = cache[cache_key].get('namespace', 'default')
                    key_part = cache_key.split(':', 1)[-1] if ':' in cache_key else cache_key
                    
                    if namespace is None or ns == namespace:
                        if re.match(pattern, key_part):
                            del cache[cache_key]
                            deleted += 1

            elif key:
                cache_key = f"{namespace}:{key}" if namespace else key
                if cache_key in cache:
                    del cache[cache_key]
                    deleted = 1

            elif namespace:
                for cache_key in list(cache.keys()):
                    if cache[cache_key].get('namespace') == namespace:
                        del cache[cache_key]
                        deleted += 1

            return ActionResult(
                success=True,
                message=f"Deleted {deleted} entries",
                data={'deleted': deleted}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Cache delete failed: {str(e)}")


class CacheClearAction(BaseAction):
    """Clear cache entries.
    
    Removes all or filtered cache entries.
    """
    action_type = "cache_clear"
    display_name = "清空缓存"
    description = "清空缓存"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Clear cache.
        
        Args:
            context: Execution context.
            params: Dict with keys: namespace, older_than.
        
        Returns:
            ActionResult with clear status.
        """
        namespace = params.get('namespace', None)
        older_than = params.get('older_than', 0)

        try:
            cache = getattr(context, '_cache', None)
            if cache is None:
                return ActionResult(success=True, message="Cache already empty")

            if namespace:
                keys_to_delete = [
                    k for k, v in cache.items()
                    if v.get('namespace') == namespace
                ]
            elif older_than > 0:
                cutoff = time.time() - older_than
                keys_to_delete = [
                    k for k, v in cache.items()
                    if v.get('created_at', 0) < cutoff
                ]
            else:
                keys_to_delete = list(cache.keys())

            for key in keys_to_delete:
                del cache[key]

            return ActionResult(
                success=True,
                message=f"Cleared {len(keys_to_delete)} entries",
                data={'cleared': len(keys_to_delete)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Cache clear failed: {str(e)}")


class CacheStatsAction(BaseAction):
    """Get cache statistics.
    
    Returns cache metrics and status.
    """
    action_type = "cache_stats"
    display_name = "缓存统计"
    description = "缓存统计信息"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get cache stats.
        
        Args:
            context: Execution context.
            params: Dict with keys: namespace, detailed.
        
        Returns:
            ActionResult with statistics.
        """
        namespace = params.get('namespace', None)
        detailed = params.get('detailed', False)

        try:
            cache = getattr(context, '_cache', None)
            if cache is None:
                return ActionResult(
                    success=True,
                    message="No cache",
                    data={'entries': 0, 'namespaces': []}
                )

            entries = cache.values()
            
            if namespace:
                entries = [e for e in entries if e.get('namespace') == namespace]

            expired = sum(
                1 for e in entries
                if e.get('expires_at') and time.time() > e['expires_at']
            )

            namespaces = set(e.get('namespace', 'default') for e in cache.values())

            stats = {
                'total_entries': len(cache),
                'namespace_entries': len(entries),
                'expired_entries': expired,
                'namespaces': list(namespaces)
            }

            if detailed:
                stats['entries'] = [
                    {
                        'key': k,
                        'namespace': v.get('namespace'),
                        'age': time.time() - v.get('created_at', 0),
                        'expires_in': v.get('expires_at', 0) - time.time() if v.get('expires_at') else None
                    }
                    for k, v in list(cache.items())[:100]
                ]

            return ActionResult(
                success=True,
                message=f"Cache stats: {len(cache)} entries",
                data=stats
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Cache stats failed: {str(e)}")


class CacheMemoizeAction(BaseAction):
    """Memoize function results in cache.
    
    Caches function output based on arguments.
    """
    action_type = "cache_memoize"
    display_name = "缓存记忆"
    description = "函数结果缓存"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Memoize function.
        
        Args:
            context: Execution context.
            params: Dict with keys: func_name, args, kwargs,
                   namespace, ttl.
        
        Returns:
            ActionResult with memoized result.
        """
        func_name = params.get('func_name', '')
        args = params.get('args', [])
        kwargs = params.get('kwargs', {})
        namespace = params.get('namespace', 'memoize')
        ttl = params.get('ttl', 300)

        if not func_name:
            return ActionResult(success=False, message="func_name is required")

        try:
            cache = getattr(context, '_cache', None)
            if cache is None:
                context._cache = {}
                cache = context._cache

            cache_key_input = json.dumps({'args': args, 'kwargs': kwargs}, sort_keys=True, default=str)
            cache_key_hash = hashlib.md5(cache_key_input.encode()).hexdigest()
            cache_key = f"{namespace}:{func_name}:{cache_key_hash}"

            if cache_key in cache:
                entry = cache[cache_key]
                if not entry.get('expires_at') or time.time() < entry['expires_at']:
                    return ActionResult(
                        success=True,
                        message=f"Memoized hit: {func_name}",
                        data={
                            'hit': True,
                            'value': entry['value'],
                            'func_name': func_name
                        }
                    )

            result = {'status': 'computed', 'function': func_name}
            
            entry = {
                'value': result,
                'created_at': time.time(),
                'namespace': namespace,
                'expires_at': time.time() + ttl if ttl > 0 else None
            }
            cache[cache_key] = entry

            return ActionResult(
                success=True,
                message=f"Memoized miss, computed: {func_name}",
                data={
                    'hit': False,
                    'value': result,
                    'func_name': func_name
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Memoize failed: {str(e)}")
