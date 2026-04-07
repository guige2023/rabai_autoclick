"""Debug14 action module for RabAI AutoClick.

Provides additional debugging operations:
- DebugBreakpointAction: Set breakpoint
- DebugVariableAction: Inspect variable
- DebugStackAction: Get stack trace
- DebugMemoryAction: Check memory usage
- DebugTimingAction: Measure execution time
- DebugDumpAction: Dump context state
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DebugBreakpointAction(BaseAction):
    """Set breakpoint."""
    action_type = "debug14_breakpoint"
    display_name = "断点"
    description = "设置调试断点"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute breakpoint.

        Args:
            context: Execution context.
            params: Dict with message, output_var.

        Returns:
            ActionResult with breakpoint info.
        """
        message = params.get('message', '')
        output_var = params.get('output_var', 'breakpoint_info')

        try:
            import sys

            resolved_message = context.resolve_value(message) if message else 'Breakpoint reached'

            frame = sys._getframe()
            result = {
                'file': frame.f_code.co_filename,
                'line': frame.f_lineno,
                'function': frame.f_code.co_name,
                'message': resolved_message
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"断点: {resolved_message}",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"断点失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'breakpoint_info'}


class DebugVariableAction(BaseAction):
    """Inspect variable."""
    action_type = "debug14_variable"
    display_name = "检查变量"
    description = "检查变量信息"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute variable inspection.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with variable info.
        """
        name = params.get('name', '')
        output_var = params.get('output_var', 'variable_info')

        try:
            resolved_name = context.resolve_value(name) if name else ''

            value = context.get(resolved_name) if hasattr(context, 'get') else None

            result = {
                'name': resolved_name,
                'type': type(value).__name__,
                'value': value,
                'repr': repr(value)
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"检查变量: {resolved_name}",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'variable_info'}


class DebugStackAction(BaseAction):
    """Get stack trace."""
    action_type = "debug14_stack"
    display_name = "堆栈跟踪"
    description = "获取当前堆栈跟踪"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute stack trace.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with stack trace.
        """
        output_var = params.get('output_var', 'stack_trace')

        try:
            import traceback

            stack = traceback.extract_stack()
            result = {
                'stack': [
                    {
                        'file': frame.filename,
                        'line': frame.lineno,
                        'function': frame.name,
                        'code': frame.line
                    }
                    for frame in stack[-10:]  # Last 10 frames
                ]
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"堆栈跟踪: {len(result['stack'])} frames",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"堆栈跟踪失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'stack_trace'}


class DebugMemoryAction(BaseAction):
    """Check memory usage."""
    action_type = "debug14_memory"
    display_name = "内存使用"
    description = "检查内存使用情况"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute memory check.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with memory info.
        """
        output_var = params.get('output_var', 'memory_info')

        try:
            import sys

            result = {
                'python_size': sys.getsizeof({}),
                'object_count': len(gc.get_objects()) if 'gc' in dir() else 0
            }

            try:
                import gc
                result['object_count'] = len(gc.get_objects())
            except:
                pass

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"内存使用: {result}",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"内存使用失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'memory_info'}


class DebugTimingAction(BaseAction):
    """Measure execution time."""
    action_type = "debug14_timing"
    display_name = "计时测量"
    description = "测量执行时间"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute timing.

        Args:
            context: Execution context.
            params: Dict with start, end, output_var.

        Returns:
            ActionResult with timing info.
        """
        start = params.get('start', None)
        end = params.get('end', None)
        output_var = params.get('output_var', 'timing_info')

        try:
            import time

            if start is not None:
                resolved_start = float(context.resolve_value(start)) if start else None
                resolved_end = float(context.resolve_value(end)) if end else time.time()
                elapsed = resolved_end - resolved_start
            else:
                resolved_start = time.time()
                resolved_end = None
                elapsed = None

            result = {
                'start': resolved_start,
                'end': resolved_end,
                'elapsed': elapsed
            }

            context.set(output_var, result)

            if elapsed is not None:
                return ActionResult(
                    success=True,
                    message=f"计时: {elapsed:.4f}秒",
                    data=result
                )
            else:
                return ActionResult(
                    success=True,
                    message=f"计时开始: {resolved_start}",
                    data=result
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计时失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': None, 'end': None, 'output_var': 'timing_info'}


class DebugDumpAction(BaseAction):
    """Dump context state."""
    action_type = "debug14_dump"
    display_name = "导出状态"
    description = "导出上下文状态"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute context dump.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with context dump.
        """
        output_var = params.get('output_var', 'context_dump')

        try:
            result = {
                'context_type': type(context).__name__,
                'has_resolve_value': hasattr(context, 'resolve_value'),
                'has_set': hasattr(context, 'set'),
                'has_get': hasattr(context, 'get')
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"导出状态: {result['context_type']}",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"导出状态失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'context_dump'}