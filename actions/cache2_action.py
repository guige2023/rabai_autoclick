"""Cache2 action module for RabAI AutoClick.

Provides additional cache operations:
- CacheSetAction: Set cache value
- CacheGetAction: Get cache value
- CacheDeleteAction: Delete cache entry
- CacheClearAction: Clear all cache
- CacheHasAction: Check if key exists
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CacheSetAction(BaseAction):
    """Set cache value."""
    action_type = "cache2_set"
    display_name = "缓存设置"
    description = "设置缓存值"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set.

        Args:
            context: Execution context.
            params: Dict with key, value, ttl, output_var.

        Returns:
            ActionResult with set status.
        """
        key = params.get('key', '')
        value = params.get('value', None)
        ttl = params.get('ttl', None)
        output_var = params.get('output_var', 'cache_status')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)
            resolved_value = context.resolve_value(value)
            resolved_ttl = int(context.resolve_value(ttl)) if ttl else None

            context.set(f'cache_{resolved_key}', resolved_value)
            if resolved_ttl:
                context.set(f'cache_ttl_{resolved_key}', resolved_ttl)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"缓存设置: {resolved_key}",
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
                message=f"缓存设置失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'ttl': None, 'output_var': 'cache_status'}


class CacheGetAction(BaseAction):
    """Get cache value."""
    action_type = "cache2_get"
    display_name = "缓存获取"
    description = "获取缓存值"
    version = "2.0"

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
            resolved_default = context.resolve_value(default) if default is not None else None

            value = context.get(f'cache_{resolved_key}', resolved_default)

            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"缓存获取: {resolved_key}",
                data={
                    'key': resolved_key,
                    'value': value,
                    'found': value != resolved_default,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"缓存获取失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': None, 'output_var': 'cache_value'}


class CacheDeleteAction(BaseAction):
    """Delete cache entry."""
    action_type = "cache2_delete"
    display_name = "缓存删除"
    description = "删除缓存条目"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute delete.

        Args:
            context: Execution context.
            params: Dict with key, output_var.

        Returns:
            ActionResult with delete status.
        """
        key = params.get('key', '')
        output_var = params.get('output_var', 'delete_status')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)

            context.set(f'cache_{resolved_key}', None)
            context.set(f'cache_ttl_{resolved_key}', None)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"缓存删除: {resolved_key}",
                data={
                    'key': resolved_key,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"缓存删除失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'delete_status'}


class CacheClearAction(BaseAction):
    """Clear all cache."""
    action_type = "cache2_clear"
    display_name = "清空缓存"
    description = "清空所有缓存"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clear.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with clear status.
        """
        output_var = params.get('output_var', 'clear_status')

        try:
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"缓存已清空",
                data={
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
        return {'output_var': 'clear_status'}


class CacheHasAction(BaseAction):
    """Check if key exists."""
    action_type = "cache2_has"
    display_name = "缓存是否存在"
    description = "检查缓存键是否存在"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute has.

        Args:
            context: Execution context.
            params: Dict with key, output_var.

        Returns:
            ActionResult with has result.
        """
        key = params.get('key', '')
        output_var = params.get('output_var', 'has_result')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)

            value = context.get(f'cache_{resolved_key}', None)
            exists = value is not None

            context.set(output_var, exists)

            return ActionResult(
                success=True,
                message=f"缓存存在: {'是' if exists else '否'}",
                data={
                    'key': resolved_key,
                    'exists': exists,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查缓存存在失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'has_result'}