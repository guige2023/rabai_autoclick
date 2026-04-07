"""Cache action module for RabAI AutoClick.

Provides cache operations:
- CacheSetAction: Set cache value
- CacheGetAction: Get cache value
- CacheDeleteAction: Delete cache
- CacheClearAction: Clear all cache
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CacheSetAction(BaseAction):
    """Set cache value."""
    action_type = "cache_set"
    display_name = "设置缓存"
    description = "设置缓存值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set.

        Args:
            context: Execution context.
            params: Dict with key, value, ttl.

        Returns:
            ActionResult indicating set.
        """
        key = params.get('key', '')
        value = params.get('value', None)
        ttl = params.get('ttl', 3600)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)
            resolved_value = context.resolve_value(value)
            resolved_ttl = int(context.resolve_value(ttl))

            context.set(f'_cache_{resolved_key}', resolved_value)
            context.set(f'_cache_{resolved_key}_ttl', resolved_ttl)

            return ActionResult(
                success=True,
                message=f"缓存已设置: {resolved_key}",
                data={
                    'key': resolved_key,
                    'ttl': resolved_ttl
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
        return {'ttl': 3600}


class CacheGetAction(BaseAction):
    """Get cache value."""
    action_type = "cache_get"
    display_name = "获取缓存"
    description = "获取缓存值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get.

        Args:
            context: Execution context.
            params: Dict with key, default, output_var.

        Returns:
            ActionResult with cache value.
        """
        key = params.get('key', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'cache_value')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)

            value = context.get(f'_cache_{resolved_key}')
            found = value is not None

            if not found:
                value = context.resolve_value(default) if default is not None else None

            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"获取缓存: {resolved_key} ({'命中' if found else '未命中'})",
                data={
                    'key': resolved_key,
                    'value': value,
                    'hit': found,
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
        return {'default': None, 'output_var': 'cache_value'}


class CacheDeleteAction(BaseAction):
    """Delete cache."""
    action_type = "cache_delete"
    display_name = "删除缓存"
    description = "删除缓存"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute delete.

        Args:
            context: Execution context.
            params: Dict with key.

        Returns:
            ActionResult indicating deleted.
        """
        key = params.get('key', '')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)

            context.delete(f'_cache_{resolved_key}')
            context.delete(f'_cache_{resolved_key}_ttl')

            return ActionResult(
                success=True,
                message=f"缓存已删除: {resolved_key}",
                data={'key': resolved_key}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"删除缓存失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class CacheClearAction(BaseAction):
    """Clear all cache."""
    action_type = "cache_clear"
    display_name = "清除所有缓存"
    description = "清除所有缓存"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clear.

        Args:
            context: Execution context.
            params: Dict with.

        Returns:
            ActionResult indicating cleared.
        """
        try:
            keys_to_delete = []
            for key in context._variables.keys():
                if key.startswith('_cache_'):
                    keys_to_delete.append(key)

            for key in keys_to_delete:
                context.delete(key)

            return ActionResult(
                success=True,
                message=f"缓存已清除: {len(keys_to_delete)} 项",
                data={'count': len(keys_to_delete)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清除缓存失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {}
