"""Timer action module for RabAI AutoClick.

Provides timer operations:
- TimerStartAction: Start timer
- TimerStopAction: Stop timer
- TimerGetAction: Get elapsed time
- TimerResetAction: Reset timer
"""

import time
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TimerStartAction(BaseAction):
    """Start timer."""
    action_type = "timer_start"
    display_name = "启动计时器"
    description = "启动计时器"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute start.

        Args:
            context: Execution context.
            params: Dict with name.

        Returns:
            ActionResult indicating started.
        """
        name = params.get('name', 'default')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            start_time = time.time()
            context.set(f'_timer_{resolved_name}_start', start_time)
            context.set(f'_timer_{resolved_name}_stopped', False)

            return ActionResult(
                success=True,
                message=f"计时器 {resolved_name} 启动",
                data={
                    'name': resolved_name,
                    'start_time': start_time
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"启动计时器失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'default'}


class TimerStopAction(BaseAction):
    """Stop timer."""
    action_type = "timer_stop"
    display_name = "停止计时器"
    description = "停止计时器"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute stop.

        Args:
            context: Execution context.
            params: Dict with name.

        Returns:
            ActionResult with elapsed time.
        """
        name = params.get('name', 'default')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            start_time = context.get(f'_timer_{resolved_name}_start')

            if start_time is None:
                return ActionResult(
                    success=False,
                    message=f"计时器 {resolved_name} 未启动"
                )

            stop_time = time.time()
            elapsed = stop_time - start_time

            context.set(f'_timer_{resolved_name}_stopped', True)
            context.set(f'_timer_{resolved_name}_stop', stop_time)
            context.set(f'_timer_{resolved_name}_elapsed', elapsed)

            return ActionResult(
                success=True,
                message=f"计时器 {resolved_name} 停止: {elapsed:.4f}秒",
                data={
                    'name': resolved_name,
                    'elapsed': elapsed,
                    'start_time': start_time,
                    'stop_time': stop_time
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"停止计时器失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'default'}


class TimerGetAction(BaseAction):
    """Get elapsed time."""
    action_type = "timer_get"
    display_name = "获取时间"
    description = "获取已过时间"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with elapsed time.
        """
        name = params.get('name', 'default')
        output_var = params.get('output_var', 'elapsed_time')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            start_time = context.get(f'_timer_{resolved_name}_start')

            if start_time is None:
                return ActionResult(
                    success=False,
                    message=f"计时器 {resolved_name} 未启动"
                )

            elapsed = time.time() - start_time
            context.set(output_var, elapsed)

            return ActionResult(
                success=True,
                message=f"计时器 {resolved_name}: {elapsed:.4f}秒",
                data={
                    'name': resolved_name,
                    'elapsed': elapsed,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'default', 'output_var': 'elapsed_time'}


class TimerResetAction(BaseAction):
    """Reset timer."""
    action_type = "timer_reset"
    display_name = "重置计时器"
    description = "重置计时器"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reset.

        Args:
            context: Execution context.
            params: Dict with name.

        Returns:
            ActionResult indicating reset.
        """
        name = params.get('name', 'default')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)

            context.delete(f'_timer_{resolved_name}_start')
            context.delete(f'_timer_{resolved_name}_stopped')
            context.delete(f'_timer_{resolved_name}_stop')
            context.delete(f'_timer_{resolved_name}_elapsed')

            return ActionResult(
                success=True,
                message=f"计时器 {resolved_name} 重置",
                data={'name': resolved_name}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"重置计时器失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'default'}
