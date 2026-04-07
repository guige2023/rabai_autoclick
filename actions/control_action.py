"""Control action module for RabAI AutoClick.

Provides control flow operations:
- ControlReturnAction: Return value
- ControlBreakAction: Break loop
- ControlContinueAction: Continue loop
- ControlExitAction: Exit execution
- ControlSleepAction: Sleep for duration
- ControlnoopAction: No operation
"""

import time
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ControlReturnAction(BaseAction):
    """Return value."""
    action_type = "control_return"
    display_name = "返回值"
    description = "从工作流返回值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute return.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult indicating return.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'return_value')

        try:
            resolved = context.resolve_value(value) if value is not None else None
            context.set(output_var, resolved)
            context.set('_flow_return', True)
            context.set('_flow_return_value', resolved)

            return ActionResult(
                success=True,
                message=f"返回: {resolved}",
                data={
                    'value': resolved,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"返回值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'value': None, 'output_var': 'return_value'}


class ControlBreakAction(BaseAction):
    """Break loop."""
    action_type = "control_break"
    display_name = "跳出循环"
    description = "跳出当前循环"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute break.

        Args:
            context: Execution context.
            params: Dict with.

        Returns:
            ActionResult indicating break.
        """
        context.set('_loop_break', True)

        return ActionResult(
            success=True,
            message="跳出循环",
            data={'break': True}
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class ControlContinueAction(BaseAction):
    """Continue loop."""
    action_type = "control_continue"
    display_name = "继续循环"
    description = "继续下次循环"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute continue.

        Args:
            context: Execution context.
            params: Dict with.

        Returns:
            ActionResult indicating continue.
        """
        context.set('_loop_continue', True)

        return ActionResult(
            success=True,
            message="继续循环",
            data={'continue': True}
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class ControlExitAction(BaseAction):
    """Exit execution."""
    action_type = "control_exit"
    display_name = "退出执行"
    description = "退出工作流执行"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute exit.

        Args:
            context: Execution context.
            params: Dict with code.

        Returns:
            ActionResult indicating exit.
        """
        code = params.get('code', 0)

        try:
            resolved_code = int(context.resolve_value(code))
            context.set('_flow_exit', True)
            context.set('_flow_exit_code', resolved_code)

            return ActionResult(
                success=True,
                message=f"退出执行: 代码 {resolved_code}",
                data={
                    'exit': True,
                    'code': resolved_code
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"退出执行失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'code': 0}


class ControlSleepAction(BaseAction):
    """Sleep for duration."""
    action_type = "control_sleep"
    display_name = "延时"
    description = "延时执行"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sleep.

        Args:
            context: Execution context.
            params: Dict with seconds.

        Returns:
            ActionResult indicating sleep.
        """
        seconds = params.get('seconds', 1)

        try:
            resolved_seconds = float(context.resolve_value(seconds))
            time.sleep(resolved_seconds)

            return ActionResult(
                success=True,
                message=f"延时: {resolved_seconds} 秒",
                data={
                    'seconds': resolved_seconds
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"延时失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'seconds': 1}


class ControlNoopAction(BaseAction):
    """No operation."""
    action_type = "control_noop"
    display_name = "空操作"
    description = "执行空操作"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute noop.

        Args:
            context: Execution context.
            params: Dict with.

        Returns:
            ActionResult indicating noop.
        """
        return ActionResult(
            success=True,
            message="空操作",
            data={'noop': True}
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {}
