"""State14 action module for RabAI AutoClick.

Provides additional state management operations:
- StateGetAction: Get state value
- StateSetAction: Set state value
- StateDeleteAction: Delete state value
- StateClearAction: Clear all state
- StateKeysAction: Get all state keys
- StateMergeAction: Merge state values
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StateGetAction(BaseAction):
    """Get state value."""
    action_type = "state14_get"
    display_name = "获取状态"
    description = "获取状态值"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute state get.

        Args:
            context: Execution context.
            params: Dict with key, default, output_var.

        Returns:
            ActionResult with state value.
        """
        key = params.get('key', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'state_value')

        try:
            resolved_key = context.resolve_value(key) if key else ''

            if not hasattr(context, '_state'):
                context._state = {}

            result = context._state.get(resolved_key, default)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取状态: {resolved_key}={result}",
                data={
                    'key': resolved_key,
                    'value': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取状态失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': None, 'output_var': 'state_value'}


class StateSetAction(BaseAction):
    """Set state value."""
    action_type = "state14_set"
    display_name = "设置状态"
    description = "设置状态值"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute state set.

        Args:
            context: Execution context.
            params: Dict with key, value, output_var.

        Returns:
            ActionResult with set result.
        """
        key = params.get('key', '')
        value = params.get('value', None)
        output_var = params.get('output_var', 'state_result')

        try:
            resolved_key = context.resolve_value(key) if key else ''
            resolved_value = context.resolve_value(value) if value else None

            if not hasattr(context, '_state'):
                context._state = {}
            context._state[resolved_key] = resolved_value

            context.set(output_var, resolved_value)

            return ActionResult(
                success=True,
                message=f"设置状态: {resolved_key}={resolved_value}",
                data={
                    'key': resolved_key,
                    'value': resolved_value,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置状态失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'state_result'}


class StateDeleteAction(BaseAction):
    """Delete state value."""
    action_type = "state14_delete"
    display_name = "删除状态"
    description = "删除状态值"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute state delete.

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

            if not hasattr(context, '_state'):
                context._state = {}

            deleted = context._state.pop(resolved_key, None)

            context.set(output_var, deleted)

            return ActionResult(
                success=True,
                message=f"删除状态: {resolved_key}",
                data={
                    'key': resolved_key,
                    'deleted': deleted,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"删除状态失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'delete_result'}


class StateClearAction(BaseAction):
    """Clear all state."""
    action_type = "state14_clear"
    display_name = "清除状态"
    description = "清除所有状态"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute state clear.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with clear result.
        """
        output_var = params.get('output_var', 'clear_result')

        try:
            if not hasattr(context, '_state'):
                context._state = {}

            count = len(context._state)
            context._state.clear()

            context.set(output_var, count)

            return ActionResult(
                success=True,
                message=f"清除状态: {count}项",
                data={
                    'cleared_count': count,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清除状态失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'clear_result'}


class StateKeysAction(BaseAction):
    """Get all state keys."""
    action_type = "state14_keys"
    display_name = "状态键列表"
    description = "获取所有状态键"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute state keys.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with state keys.
        """
        output_var = params.get('output_var', 'state_keys')

        try:
            if not hasattr(context, '_state'):
                context._state = {}

            keys = list(context._state.keys())

            context.set(output_var, keys)

            return ActionResult(
                success=True,
                message=f"状态键列表: {len(keys)}项",
                data={
                    'keys': keys,
                    'count': len(keys),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"状态键列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'state_keys'}


class StateMergeAction(BaseAction):
    """Merge state values."""
    action_type = "state14_merge"
    display_name = "合并状态"
    description = "合并状态值"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute state merge.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with merge result.
        """
        values = params.get('values', {})
        output_var = params.get('output_var', 'merge_result')

        try:
            resolved_values = context.resolve_value(values) if values else {}

            if not hasattr(context, '_state'):
                context._state = {}

            if isinstance(resolved_values, dict):
                context._state.update(resolved_values)
                merged = resolved_values
            else:
                merged = {}

            context.set(output_var, merged)

            return ActionResult(
                success=True,
                message=f"合并状态: {len(merged)}项",
                data={
                    'merged': merged,
                    'count': len(merged),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"合并状态失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'merge_result'}