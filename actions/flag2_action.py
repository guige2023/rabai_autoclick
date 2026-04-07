"""Flag2 action module for RabAI AutoClick.

Provides additional flag operations:
- FlagSetAction: Set flag
- FlagGetAction: Get flag value
- FlagToggleAction: Toggle flag
- FlagIsSetAction: Check if flag is set
- FlagClearAction: Clear flag
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FlagSetAction(BaseAction):
    """Set flag."""
    action_type = "flag2_set"
    display_name = "设置标志"
    description = "设置标志为真"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set.

        Args:
            context: Execution context.
            params: Dict with name, value, output_var.

        Returns:
            ActionResult with set status.
        """
        name = params.get('name', '')
        value = params.get('value', True)
        output_var = params.get('output_var', 'flag_status')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_value = bool(context.resolve_value(value)) if value is not None else True

            context.set(f'flag_{resolved_name}', resolved_value)
            context.set(output_var, resolved_value)

            return ActionResult(
                success=True,
                message=f"标志设置: {resolved_name} = {resolved_value}",
                data={
                    'name': resolved_name,
                    'value': resolved_value,
                    'output_var': output_var
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
        return {'value': True, 'output_var': 'flag_status'}


class FlagGetAction(BaseAction):
    """Get flag value."""
    action_type = "flag2_get"
    display_name = "获取标志"
    description = "获取标志值"
    version = "2.0"

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
            resolved_default = bool(context.resolve_value(default)) if default is not None else False

            value = context.get(f'flag_{resolved_name}', resolved_default)

            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"标志获取: {resolved_name} = {value}",
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
    action_type = "flag2_toggle"
    display_name = "切换标志"
    description = "切换标志值"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute toggle.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with toggled value.
        """
        name = params.get('name', '')
        output_var = params.get('output_var', 'toggle_result')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)

            current = context.get(f'flag_{resolved_name}', False)
            new_value = not bool(current)

            context.set(f'flag_{resolved_name}', new_value)
            context.set(output_var, new_value)

            return ActionResult(
                success=True,
                message=f"标志切换: {resolved_name} = {new_value}",
                data={
                    'name': resolved_name,
                    'previous': current,
                    'new_value': new_value,
                    'output_var': output_var
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
        return {'output_var': 'toggle_result'}


class FlagIsSetAction(BaseAction):
    """Check if flag is set."""
    action_type = "flag2_is_set"
    display_name = "判断标志是否设置"
    description = "判断标志是否已设置"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is set.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with is set result.
        """
        name = params.get('name', '')
        output_var = params.get('output_var', 'is_set_result')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)

            value = context.get(f'flag_{resolved_name}', False)
            is_set = bool(value)

            context.set(output_var, is_set)

            return ActionResult(
                success=True,
                message=f"标志已设置: {'是' if is_set else '否'}",
                data={
                    'name': resolved_name,
                    'is_set': is_set,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断标志设置失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_set_result'}


class FlagClearAction(BaseAction):
    """Clear flag."""
    action_type = "flag2_clear"
    display_name = "清除标志"
    description = "清除标志"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clear.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with clear status.
        """
        name = params.get('name', '')
        output_var = params.get('output_var', 'clear_result')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)

            context.set(f'flag_{resolved_name}', False)
            context.set(output_var, False)

            return ActionResult(
                success=True,
                message=f"标志清除: {resolved_name}",
                data={
                    'name': resolved_name,
                    'value': False,
                    'output_var': output_var
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
        return {'output_var': 'clear_result'}