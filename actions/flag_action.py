"""Flag action module for RabAI AutoClick.

Provides flag operations:
- FlagSetAction: Set flag
- FlagGetAction: Get flag
- FlagToggleAction: Toggle flag
- FlagClearAction: Clear flag
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FlagSetAction(BaseAction):
    """Set flag."""
    action_type = "flag_set"
    display_name = "设置标志"
    description = "设置标志"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set.

        Args:
            context: Execution context.
            params: Dict with name, value.

        Returns:
            ActionResult indicating set.
        """
        name = params.get('name', '')
        value = params.get('value', True)

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_value = bool(context.resolve_value(value))

            context.set(f'_flag_{resolved_name}', resolved_value)

            return ActionResult(
                success=True,
                message=f"标志 {resolved_name} 已{'设置' if resolved_value else '清除'}",
                data={
                    'name': resolved_name,
                    'value': resolved_value
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置标志失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'value': True}


class FlagGetAction(BaseAction):
    """Get flag."""
    action_type = "flag_get"
    display_name = "获取标志"
    description = "获取标志值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get.

        Args:
            context: Execution context.
            params: Dict with name, default, output_var.

        Returns:
            ActionResult with flag value.
        """
        name = params.get('name', '')
        default = params.get('default', False)
        output_var = params.get('output_var', 'flag_value')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)

            value = context.get(f'_flag_{resolved_name}')
            if value is None:
                value = bool(context.resolve_value(default))
                context.set(f'_flag_{resolved_name}', value)

            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"标志 {resolved_name}: {value}",
                data={
                    'name': resolved_name,
                    'value': value,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取标志失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': False, 'output_var': 'flag_value'}


class FlagToggleAction(BaseAction):
    """Toggle flag."""
    action_type = "flag_toggle"
    display_name = "切换标志"
    description = "切换标志值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute toggle.

        Args:
            context: Execution context.
            params: Dict with name.

        Returns:
            ActionResult with new value.
        """
        name = params.get('name', '')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)

            current = context.get(f'_flag_{resolved_name}', False)
            new_value = not current
            context.set(f'_flag_{resolved_name}', new_value)

            return ActionResult(
                success=True,
                message=f"标志 {resolved_name} 切换: {current} -> {new_value}",
                data={
                    'name': resolved_name,
                    'old_value': current,
                    'new_value': new_value
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"切换标志失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class FlagClearAction(BaseAction):
    """Clear flag."""
    action_type = "flag_clear"
    display_name = "清除标志"
    description = "清除标志"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clear.

        Args:
            context: Execution context.
            params: Dict with name.

        Returns:
            ActionResult indicating cleared.
        """
        name = params.get('name', '')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)

            old_value = context.get(f'_flag_{resolved_name}', False)
            context.set(f'_flag_{resolved_name}', False)

            return ActionResult(
                success=True,
                message=f"标志 {resolved_name} 已清除",
                data={
                    'name': resolved_name,
                    'old_value': old_value
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清除标志失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}
