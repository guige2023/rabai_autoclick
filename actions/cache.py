"""Cache manager action module for RabAI AutoClick.

Provides caching actions for storing, retrieving, and managing
cached data with TTL support and size limits.
"""

import os
import sys
import time
import json
import hashlib
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CacheSetAction(BaseAction):
    """Store a value in cache with optional TTL.
    
    Supports in-memory cache with time-to-live expiration,
    key prefixing, and automatic cleanup.
    """
    action_type = "cache_set"
    display_name = "缓存设置"
    description = "将值存入缓存，支持TTL过期时间"

    _cache_store: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Set a cache value.
        
        Args:
            context: Execution context.
            params: Dict with keys: key, value, ttl, namespace,
                   serializer.
        
        Returns:
            ActionResult with cache operation result.
        """
        key = params.get('key', '')
        value = params.get('value', None)
        ttl = params.get('ttl', None)
        namespace = params.get('namespace', 'default')
        serializer = params.get('serializer', 'json')

        if not key:
            return ActionResult(success=False, message="缓存key不能为空")

        # Build full key with namespace
        full_key = f"{namespace}:{key}"

        # Serialize value if needed
        stored_value = value
        if serializer == 'json' and value is not None:
            try:
                stored_value = json.dumps(value)
            except (TypeError, ValueError):
                stored_value = str(value)

        # Calculate expiration
        expires_at = None
        if ttl is not None:
            try:
                ttl = float(ttl)
                if ttl > 0:
                    expires_at = time.time() + ttl
            except (ValueError, TypeError):
                return ActionResult(
                    success=False,
                    message=f"Invalid TTL value: {ttl}"
                )

        # Store in cache
        self._cache_store[full_key] = {
            'value': stored_value,
            'expires_at': expires_at,
            'created_at': time.time(),
            'namespace': namespace,
            'serializer': serializer
        }

        result_data = {
            'key': full_key,
            'ttl': ttl,
            'namespace': namespace,
            'stored': True
        }

        return ActionResult(
            success=True,
            message=f"缓存已设置: {full_key}" + (f" (TTL={ttl}s)" if ttl else ""),
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['key', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'ttl': None,
            'namespace': 'default',
            'serializer': 'json'
        }


class CacheGetAction(BaseAction):
    """Retrieve a value from cache.
    
    Supports default value fallback, automatic deserialization,
    and expired key cleanup.
    """
    action_type = "cache_get"
    display_name = "缓存获取"
    description = "从缓存获取值，支持默认值和自动反序列化"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Get a cache value.
        
        Args:
            context: Execution context.
            params: Dict with keys: key, namespace, default_value,
                   save_to_var, delete_on_get.
        
        Returns:
            ActionResult with cached value or default.
        """
        key = params.get('key', '')
        namespace = params.get('namespace', 'default')
        default_value = params.get('default_value', None)
        save_to_var = params.get('save_to_var', None)
        delete_on_get = params.get('delete_on_get', False)

        if not key:
            return ActionResult(success=False, message="缓存key不能为空")

        full_key = f"{namespace}:{key}"

        if full_key not in CacheSetAction._cache_store:
            result_data = {
                'found': False,
                'key': full_key,
                'value': default_value,
                'expired': False
            }
            if save_to_var:
                context.variables[save_to_var] = result_data
            return ActionResult(
                success=True,
                message=f"缓存不存在: {full_key}",
                data=result_data
            )

        entry = CacheSetAction._cache_store[full_key]

        # Check expiration
        if entry['expires_at'] is not None and time.time() > entry['expires_at']:
            del CacheSetAction._cache_store[full_key]
            result_data = {
                'found': False,
                'key': full_key,
                'value': default_value,
                'expired': True
            }
            if save_to_var:
                context.variables[save_to_var] = result_data
            return ActionResult(
                success=True,
                message=f"缓存已过期: {full_key}",
                data=result_data
            )

        # Deserialize value
        value = entry['value']
        if entry['serializer'] == 'json' and isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                pass

        # Delete if requested
        if delete_on_get:
            del CacheSetAction._cache_store[full_key]

        result_data = {
            'found': True,
            'key': full_key,
            'value': value,
            'expired': False,
            'age': time.time() - entry['created_at']
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"缓存命中: {full_key}",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'namespace': 'default',
            'default_value': None,
            'save_to_var': None,
            'delete_on_get': False
        }


class CacheDeleteAction(BaseAction):
    """Delete one or more keys from cache.
    
    Supports wildcard pattern matching for bulk deletion
    and namespace-scoped clearing.
    """
    action_type = "cache_delete"
    display_name = "缓存删除"
    description = "删除缓存，支持模式和命名空间"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Delete cache entries.
        
        Args:
            context: Execution context.
            params: Dict with keys: key, namespace, pattern,
                   clear_namespace.
        
        Returns:
            ActionResult with deletion count.
        """
        key = params.get('key', None)
        namespace = params.get('namespace', None)
        pattern = params.get('pattern', None)
        clear_namespace = params.get('clear_namespace', False)

        deleted_count = 0

        if clear_namespace and namespace:
            # Clear entire namespace
            keys_to_delete = [
                k for k, v in CacheSetAction._cache_store.items()
                if v['namespace'] == namespace
            ]
            for k in keys_to_delete:
                del CacheSetAction._cache_store[k]
                deleted_count += 1
        elif pattern and namespace:
            # Pattern-based deletion within namespace
            import fnmatch
            prefix = f"{namespace}:"
            keys_to_delete = [
                k for k in CacheSetAction._cache_store.keys()
                if k.startswith(prefix) and fnmatch.fnmatch(k, f"*{pattern}*")
            ]
            for k in keys_to_delete:
                del CacheSetAction._cache_store[k]
                deleted_count += 1
        elif key:
            # Single key deletion
            full_key = f"{namespace}:{key}" if namespace else key
            if full_key in CacheSetAction._cache_store:
                del CacheSetAction._cache_store[full_key]
                deleted_count = 1

        result_data = {
            'deleted_count': deleted_count,
            'key': key,
            'namespace': namespace,
            'pattern': pattern
        }

        return ActionResult(
            success=True,
            message=f"已删除 {deleted_count} 个缓存项",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'key': None,
            'namespace': None,
            'pattern': None,
            'clear_namespace': False
        }


class CacheClearAction(BaseAction):
    """Clear all entries from cache or specific namespaces.
    
    Provides full cache cleanup with optional namespace filtering.
    """
    action_type = "cache_clear"
    display_name = "清空缓存"
    description = "清空所有缓存或指定命名空间"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Clear cache entries.
        
        Args:
            context: Execution context.
            params: Dict with keys: namespaces (list), clear_all.
        
        Returns:
            ActionResult with cleared count.
        """
        namespaces = params.get('namespaces', [])
        clear_all = params.get('clear_all', False)

        if clear_all:
            cleared_count = len(CacheSetAction._cache_store)
            CacheSetAction._cache_store.clear()
        elif namespaces:
            keys_to_delete = [
                k for k, v in CacheSetAction._cache_store.items()
                if v['namespace'] in namespaces
            ]
            cleared_count = len(keys_to_delete)
            for k in keys_to_delete:
                del CacheSetAction._cache_store[k]
        else:
            cleared_count = 0

        result_data = {
            'cleared_count': cleared_count,
            'remaining': len(CacheSetAction._cache_store)
        }

        return ActionResult(
            success=True,
            message=f"已清空 {cleared_count} 个缓存项",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'namespaces': [],
            'clear_all': False
        }


class CacheStatsAction(BaseAction):
    """Get cache statistics and status.
    
    Returns cache size, namespace breakdown, expired entries count,
    and memory usage estimation.
    """
    action_type = "cache_stats"
    display_name = "缓存统计"
    description = "获取缓存统计信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Get cache statistics.
        
        Args:
            context: Execution context.
            params: Dict with keys: save_to_var.
        
        Returns:
            ActionResult with cache statistics.
        """
        save_to_var = params.get('save_to_var', None)

        now = time.time()
        total_entries = len(CacheSetAction._cache_store)
        expired_count = sum(
            1 for entry in CacheSetAction._cache_store.values()
            if entry['expires_at'] is not None and now > entry['expires_at']
        )

        # Namespace breakdown
        namespaces: Dict[str, int] = {}
        for entry in CacheSetAction._cache_store.values():
            ns = entry['namespace']
            namespaces[ns] = namespaces.get(ns, 0) + 1

        result_data = {
            'total_entries': total_entries,
            'expired_count': expired_count,
            'active_count': total_entries - expired_count,
            'namespaces': namespaces,
            'memory_keys': list(CacheSetAction._cache_store.keys())
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"缓存统计: {total_entries} 项, {expired_count} 已过期",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'save_to_var': None}
