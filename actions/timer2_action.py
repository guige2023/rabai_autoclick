"""Timer2 action module for RabAI AutoClick.

Provides additional timer operations:
- TimerStartAction: Start timer
- TimerStopAction: Stop timer
- TimerPauseAction: Pause timer
- TimerResumeAction: Resume timer
- TimerElapsedAction: Get elapsed time
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TimerStartAction(BaseAction):
    """Start timer."""
    action_type = "timer2_start"
    display_name = "启动计时器"
    description = "启动计时器"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute start.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with start status.
        """
        name = params.get('name', 'default')
        output_var = params.get('output_var', 'timer_status')

        try:
            import time

            resolved_name = context.resolve_value(name)

            context.set(f'timer_start_{resolved_name}', time.time())
            context.set(f'timer_paused_{resolved_name}', False)
            context.set(f'timer_paused_at_{resolved_name}', 0)
            context.set(f'timer_elapsed_{resolved_name}', 0)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"计时器启动: {resolved_name}",
                data={
                    'name': resolved_name,
                    'started_at': time.time(),
                    'output_var': output_var
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
        return {'name': 'default', 'output_var': 'timer_status'}


class TimerStopAction(BaseAction):
    """Stop timer."""
    action_type = "timer2_stop"
    display_name = "停止计时器"
    description = "停止计时器"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute stop.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with stop status and elapsed time.
        """
        name = params.get('name', 'default')
        output_var = params.get('output_var', 'timer_result')

        try:
            import time

            resolved_name = context.resolve_value(name)

            start_time = context.get(f'timer_start_{resolved_name}', None)
            if start_time is None:
                return ActionResult(
                    success=False,
                    message=f"停止计时器失败: 计时器 {resolved_name} 未启动"
                )

            elapsed = time.time() - start_time

            context.set(f'timer_elapsed_{resolved_name}', elapsed)
            context.set(f'timer_start_{resolved_name}', None)
            context.set(output_var, elapsed)

            return ActionResult(
                success=True,
                message=f"计时器停止: {resolved_name} = {elapsed:.2f}秒",
                data={
                    'name': resolved_name,
                    'elapsed': elapsed,
                    'output_var': output_var
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
        return {'name': 'default', 'output_var': 'timer_result'}


class TimerPauseAction(BaseAction):
    """Pause timer."""
    action_type = "timer2_pause"
    display_name = "暂停计时器"
    description = "暂停计时器"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pause.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with pause status.
        """
        name = params.get('name', 'default')
        output_var = params.get('output_var', 'pause_result')

        try:
            import time

            resolved_name = context.resolve_value(name)

            start_time = context.get(f'timer_start_{resolved_name}', None)
            if start_time is None:
                return ActionResult(
                    success=False,
                    message=f"暂停计时器失败: 计时器 {resolved_name} 未启动"
                )

            is_paused = context.get(f'timer_paused_{resolved_name}', False)
            if is_paused:
                return ActionResult(
                    success=True,
                    message=f"计时器已暂停: {resolved_name}",
                    data={
                        'name': resolved_name,
                        'paused': True,
                        'output_var': output_var
                    }
                )

            elapsed_so_far = context.get(f'timer_elapsed_{resolved_name}', 0)
            paused_at = time.time()
            total_paused = context.get(f'timer_paused_at_{resolved_name}', 0)

            context.set(f'timer_paused_{resolved_name}', True)
            context.set(f'timer_paused_at_{resolved_name}', total_paused + (paused_at - start_time - elapsed_so_far))
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"计时器暂停: {resolved_name}",
                data={
                    'name': resolved_name,
                    'paused': True,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"暂停计时器失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'default', 'output_var': 'pause_result'}


class TimerResumeAction(BaseAction):
    """Resume timer."""
    action_type = "timer2_resume"
    display_name = "继续计时器"
    description = "继续计时器"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute resume.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with resume status.
        """
        name = params.get('name', 'default')
        output_var = params.get('output_var', 'resume_result')

        try:
            import time

            resolved_name = context.resolve_value(name)

            is_paused = context.get(f'timer_paused_{resolved_name}', False)
            if not is_paused:
                return ActionResult(
                    success=True,
                    message=f"计时器未暂停: {resolved_name}",
                    data={
                        'name': resolved_name,
                        'resumed': False,
                        'output_var': output_var
                    }
                )

            context.set(f'timer_paused_{resolved_name}', False)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"计时器继续: {resolved_name}",
                data={
                    'name': resolved_name,
                    'resumed': True,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"继续计时器失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'default', 'output_var': 'resume_result'}


class TimerElapsedAction(BaseAction):
    """Get elapsed time."""
    action_type = "timer2_elapsed"
    display_name = "获取已用时间"
    description = "获取计时器已用时间"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute elapsed.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with elapsed time.
        """
        name = params.get('name', 'default')
        output_var = params.get('output_var', 'elapsed_time')

        try:
            import time

            resolved_name = context.resolve_value(name)

            start_time = context.get(f'timer_start_{resolved_name}', None)
            if start_time is None:
                elapsed = context.get(f'timer_elapsed_{resolved_name}', 0)
            else:
                is_paused = context.get(f'timer_paused_{resolved_name}', False)
                if is_paused:
                    paused_at = context.get(f'timer_paused_at_{resolved_name}', 0)
                    elapsed = paused_at
                else:
                    elapsed = time.time() - start_time

            context.set(output_var, elapsed)

            return ActionResult(
                success=True,
                message=f"已用时间: {resolved_name} = {elapsed:.2f}秒",
                data={
                    'name': resolved_name,
                    'elapsed': elapsed,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取已用时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'default', 'output_var': 'elapsed_time'}