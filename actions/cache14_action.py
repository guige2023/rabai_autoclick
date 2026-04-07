"""Cache14 action module for RabAI AutoClick.

Provides additional caching operations:
- CacheGetAction: Get cached value
- CacheSetAction: Set cached value
- CacheDeleteAction: Delete cached value
- CacheClearAction: Clear cache
- CacheExistsAction: Check if key exists
- CacheTTLAction: Set cache with TTL
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CacheGetAction(BaseAction):
    """Get cached value."""
    action_type = "cache14_get"
    display_name = "获取缓存"
    description = "获取缓存值"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cache get.

        Args:
            context: Execution context.
            params: Dict with key, default, output_var.

        Returns:
            ActionResult with cached value.
        """
        key = params.get('key', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'cached_value')

        try:
            import time

            resolved_key = context.resolve_value(key) if key else ''

            if not hasattr(context, '_cache'):
                context._cache = {}

            if resolved_key in context._cache:
                entry = context._cache[resolved_key]
                if entry['expires_at'] is None or entry['expires_at'] > time.time():
                    result = entry['value']
                else:
                    del context._cache[resolved_key]
                    result = default
            else:
                result = default

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取缓存: {resolved_key}={result}",
                data={
                    'key': resolved_key,
                    'value': result,
                    'found': result != default,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取缓存失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': None, 'output_var': 'cached_value'}


class CacheSetAction(BaseAction):
    """Set cached value."""
    action_type = "cache14_set"
    display_name = "设置缓存"
    description = "设置缓存值"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cache set.

        Args:
            context: Execution context.
            params: Dict with key, value, ttl, output_var.

        Returns:
            ActionResult with set result.
        """
        key = params.get('key', '')
        value = params.get('value', None)
        ttl = params.get('ttl', None)
        output_var = params.get('output_var', 'cache_result')

        try:
            import time

            resolved_key = context.resolve_value(key) if key else ''
            resolved_value = context.resolve_value(value) if value else None
            resolved_ttl = int(context.resolve_value(ttl)) if ttl else None

            if not hasattr(context, '_cache'):
                context._cache = {}

            expires_at = None
            if resolved_ttl is not None:
                expires_at = time.time() + resolved_ttl

            context._cache[resolved_key] = {
                'value': resolved_value,
                'expires_at': expires_at,
                'created_at': time.time()
            }

            context.set(output_var, resolved_value)

            return ActionResult(
                success=True,
                message=f"设置缓存: {resolved_key}",
                data={
                    'key': resolved_key,
                    'value': resolved_value,
                    'ttl': resolved_ttl,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置缓存失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'ttl': None, 'output_var': 'cache_result'}


class CacheDeleteAction(BaseAction):
    """Delete cached value."""
    action_type = "cache14_delete"
    display_name = "删除缓存"
    description = "删除缓存值"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cache delete.

        Args:
            context: Execution context.
            params: Dict with key, output_var.

        Returns:
            ActionResult with delete result.
        """
        key = params.get('key', '')
        output_var = params.get('output_var', 'delete_result')

        try:
            resolved_key = context.resolve_value(key) if key else ''

            if not hasattr(context, '_cache'):
                context._cache = {}

            deleted = context._cache.pop(resolved_key, None)

            context.set(output_var, deleted is not None)

            return ActionResult(
                success=True,
                message=f"删除缓存: {resolved_key}",
                data={
                    'key': resolved_key,
                    'deleted': deleted is not None,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"删除缓存失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'delete_result'}


class CacheClearAction(BaseAction):
    """Clear cache."""
    action_type = "cache14_clear"
    display_name = "清空缓存"
    description = "清空所有缓存"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cache clear.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with clear result.
        """
        output_var = params.get('output_var', 'clear_result')

        try:
            if not hasattr(context, '_cache'):
                context._cache = {}

            count = len(context._cache)
            context._cache.clear()

            context.set(output_var, count)

            return ActionResult(
                success=True,
                message=f"清空缓存: {count}项",
                data={
                    'cleared_count': count,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清空缓存失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'clear_result'}


class CacheExistsAction(BaseAction):
    """Check if key exists."""
    action_type = "cache14_exists"
    display_name = "缓存存在"
    description = "检查缓存键是否存在"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cache exists.

        Args:
            context: Execution context.
            params: Dict with key, output_var.

        Returns:
            ActionResult with exists result.
        """
        key = params.get('key', '')
        output_var = params.get('output_var', 'exists_result')

        try:
            import time

            resolved_key = context.resolve_value(key) if key else ''

            if not hasattr(context, '_cache'):
                context._cache = {}

            exists = False
            if resolved_key in context._cache:
                entry = context._cache[resolved_key]
                if entry['expires_at'] is None or entry['expires_at'] > time.time():
                    exists = True
                else:
                    del context._cache[resolved_key]

            context.set(output_var, exists)

            return ActionResult(
                success=True,
                message=f"缓存存在: {resolved_key}={exists}",
                data={
                    'key': resolved_key,
                    'exists': exists,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"缓存存在检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'exists_result'}


class CacheTTLAction(BaseAction):
    """Set cache with TTL."""
    action_type = "cache14_ttl"
    display_name = "缓存TTL"
    description = "设置带TTL的缓存"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cache TTL.

        Args:
            context: Execution context.
            params: Dict with key, ttl, output_var.

        Returns:
            ActionResult with TTL result.
        """
        key = params.get('key', '')
        ttl = params.get('ttl', 60)
        output_var = params.get('output_var', 'ttl_result')

        try:
            import time

            resolved_key = context.resolve_value(key) if key else ''
            resolved_ttl = int(context.resolve_value(ttl)) if ttl else 60

            if not hasattr(context, '_cache'):
                context._cache = {}

            if resolved_key in context._cache:
                context._cache[resolved_key]['expires_at'] = time.time() + resolved_ttl
                remaining = resolved_ttl
            else:
                remaining = 0

            context.set(output_var, remaining)

            return ActionResult(
                success=True,
                message=f"缓存TTL: {resolved_key}={resolved_ttl}s",
                data={
                    'key': resolved_key,
                    'ttl': resolved_ttl,
                    'remaining': remaining,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"缓存TTL失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key', 'ttl']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'ttl_result'}