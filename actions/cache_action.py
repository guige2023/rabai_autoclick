"""Cache action module for RabAI AutoClick.

Provides caching operations:
- CacheSetAction: Set cache value
- CacheGetAction: Get cache value
- CacheDeleteAction: Delete cache key
- CacheClearAction: Clear all cache
- CacheHasAction: Check if key exists
- CacheExpireAction: Set TTL on key
"""

from __future__ import annotations

import sys
import time
import json
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CacheSetAction(BaseAction):
    """Set cache value."""
    action_type = "cache_set"
    display_name = "缓存设置"
    description = "设置缓存"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cache set."""
        key = params.get('key', '')
        value = params.get('value', None)
        ttl = params.get('ttl', None)  # seconds
        output_var = params.get('output_var', 'cache_set_result')

        if not key:
            return ActionResult(success=False, message="key is required")

        try:
            resolved_key = context.resolve_value(key) if context else key
            resolved_value = context.resolve_value(value) if context else value
            resolved_ttl = context.resolve_value(ttl) if context else ttl

            if not hasattr(context, '_cache'):
                context._cache = {}
            if not hasattr(context, '_cache_expire'):
                context._cache_expire = {}

            context._cache[resolved_key] = resolved_value
            if resolved_ttl:
                context._cache_expire[resolved_key] = time.time() + resolved_ttl

            result = {'set': True, 'key': resolved_key, 'ttl': resolved_ttl}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Cache set: {resolved_key}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Cache set error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'value': None, 'ttl': None, 'output_var': 'cache_set_result'}


class CacheGetAction(BaseAction):
    """Get cache value."""
    action_type = "cache_get"
    display_name = "缓存获取"
    description = "获取缓存"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cache get."""
        key = params.get('key', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'cache_value')

        if not key:
            return ActionResult(success=False, message="key is required")

        try:
            resolved_key = context.resolve_value(key) if context else key

            if not hasattr(context, '_cache') or resolved_key not in context._cache:
                if context:
                    context.set(output_var, default)
                return ActionResult(success=False, message=f"Cache miss: {resolved_key}", data={'found': False})

            # Check expiration
            if hasattr(context, '_cache_expire') and resolved_key in context._cache_expire:
                if time.time() > context._cache_expire[resolved_key]:
                    del context._cache[resolved_key]
                    del context._cache_expire[resolved_key]
                    if context:
                        context.set(output_var, default)
                    return ActionResult(success=False, message=f"Cache expired: {resolved_key}", data={'found': False})

            value = context._cache[resolved_key]
            result = {'found': True, 'key': resolved_key, 'value': value}
            if context:
                context.set(output_var, value)
            return ActionResult(success=True, message=f"Cache hit: {resolved_key}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Cache get error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': None, 'output_var': 'cache_value'}


class CacheDeleteAction(BaseAction):
    """Delete cache key."""
    action_type = "cache_delete"
    display_name = "缓存删除"
    description = "删除缓存"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cache delete."""
        key = params.get('key', '')
        output_var = params.get('output_var', 'cache_delete_result')

        if not key:
            return ActionResult(success=False, message="key is required")

        try:
            resolved_key = context.resolve_value(key) if context else key

            deleted = False
            if hasattr(context, '_cache') and resolved_key in context._cache:
                del context._cache[resolved_key]
                deleted = True
            if hasattr(context, '_cache_expire') and resolved_key in context._cache_expire:
                del context._cache_expire[resolved_key]

            result = {'deleted': deleted, 'key': resolved_key}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Cache delete: {resolved_key}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Cache delete error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cache_delete_result'}


class CacheClearAction(BaseAction):
    """Clear all cache."""
    action_type = "cache_clear"
    display_name = "缓存清空"
    description = "清空所有缓存"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cache clear."""
        output_var = params.get('output_var', 'cache_clear_result')

        try:
            count = 0
            if hasattr(context, '_cache'):
                count = len(context._cache)
                context._cache.clear()
            if hasattr(context, '_cache_expire'):
                context._cache_expire.clear()

            result = {'cleared': True, 'count': count}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Cache cleared: {count} items", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Cache clear error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cache_clear_result'}


class CacheHasAction(BaseAction):
    """Check if cache key exists."""
    action_type = "cache_has"
    display_name = "缓存存在检查"
    description = "检查缓存是否存在"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cache has."""
        key = params.get('key', '')
        output_var = params.get('output_var', 'cache_has')

        if not key:
            return ActionResult(success=False, message="key is required")

        try:
            resolved_key = context.resolve_value(key) if context else key

            exists = hasattr(context, '_cache') and resolved_key in context._cache
            if exists and hasattr(context, '_cache_expire') and resolved_key in context._cache_expire:
                if time.time() > context._cache_expire[resolved_key]:
                    del context._cache[resolved_key]
                    del context._cache_expire[resolved_key]
                    exists = False

            result = {'exists': exists, 'key': resolved_key}
            if context:
                context.set(output_var, exists)
            return ActionResult(success=True, message=f"Cache {'has' if exists else 'does not have'}: {resolved_key}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Cache has error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cache_has'}


class CacheListAction(BaseAction):
    """List cache keys."""
    action_type = "cache_list"
    display_name = "缓存列表"
    description = "列出缓存键"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cache list."""
        pattern = params.get('pattern', None)
        output_var = params.get('output_var', 'cache_keys')

        try:
            if not hasattr(context, '_cache') or not context._cache:
                keys = []
            else:
                keys = list(context._cache.keys())
                if pattern:
                    import re
                    regex = re.compile(pattern)
                    keys = [k for k in keys if regex.search(k)]

            result = {'keys': keys, 'count': len(keys)}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Cache has {len(keys)} keys", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Cache list error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'pattern': None, 'output_var': 'cache_keys'}
