"""Trace action module for RabAI AutoClick.

Provides tracing operations:
- TraceEnterAction: Trace enter
- TraceExitAction: Trace exit
- TraceStepAction: Trace step
- TraceVariableAction: Trace variable
"""

import time
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TraceEnterAction(BaseAction):
    """Trace enter."""
    action_type = "trace_enter"
    display_name = "追踪进入"
    description = "追踪进入函数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute trace enter.

        Args:
            context: Execution context.
            params: Dict with name, args.

        Returns:
            ActionResult indicating enter.
        """
        name = params.get('name', 'function')
        args = params.get('args', {})

        try:
            resolved_name = context.resolve_value(name)
            resolved_args = context.resolve_value(args)

            timestamp = time.time()
            context.set('_trace_stack', context.get('_trace_stack', []) + [{
                'name': resolved_name,
                'args': resolved_args,
                'enter_time': timestamp
            }])

            return ActionResult(
                success=True,
                message=f"进入追踪: {resolved_name}",
                data={
                    'name': resolved_name,
                    'args': resolved_args,
                    'timestamp': timestamp
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"追踪进入失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'function', 'args': {}}


class TraceExitAction(BaseAction):
    """Trace exit."""
    action_type = "trace_exit"
    display_name = "追踪退出"
    description = "追踪退出函数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute trace exit.

        Args:
            context: Execution context.
            params: Dict with result.

        Returns:
            ActionResult indicating exit.
        """
        result = params.get('result', None)

        try:
            stack = context.get('_trace_stack', [])
            if not stack:
                return ActionResult(
                    success=False,
                    message="没有活动的追踪"
                )

            entry = stack[-1]
            resolved_result = context.resolve_value(result) if result is not None else None

            exit_time = time.time()
            duration = exit_time - entry['enter_time']

            context.set('_trace_stack', stack[:-1])

            return ActionResult(
                success=True,
                message=f"退出追踪: {entry['name']} ({duration:.4f}s)",
                data={
                    'name': entry['name'],
                    'duration': duration,
                    'result': resolved_result
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"追踪退出失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'result': None}


class TraceStepAction(BaseAction):
    """Trace step."""
    action_type = "trace_step"
    display_name = "追踪步骤"
    description = "追踪执行步骤"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute trace step.

        Args:
            context: Execution context.
            params: Dict with name, data.

        Returns:
            ActionResult indicating step.
        """
        name = params.get('name', 'step')
        data = params.get('data', None)

        try:
            resolved_name = context.resolve_value(name)
            resolved_data = context.resolve_value(data) if data is not None else None

            timestamp = time.time()
            step_num = context.get('_trace_step_num', 0) + 1
            context.set('_trace_step_num', step_num)

            context.set('_trace_steps', context.get('_trace_steps', []) + [{
                'step': step_num,
                'name': resolved_name,
                'data': resolved_data,
                'timestamp': timestamp
            }])

            return ActionResult(
                success=True,
                message=f"追踪步骤: {resolved_name} (#{step_num})",
                data={
                    'step': step_num,
                    'name': resolved_name,
                    'data': resolved_data
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"追踪步骤失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'step', 'data': None}


class TraceVariableAction(BaseAction):
    """Trace variable."""
    action_type = "trace_variable"
    display_name = "追踪变量"
    description = "追踪变量值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute trace variable.

        Args:
            context: Execution context.
            params: Dict with names.

        Returns:
            ActionResult with variable values.
        """
        names = params.get('names', [])

        try:
            resolved_names = context.resolve_value(names) if names else []

            values = {}
            for name in resolved_names:
                values[name] = context.get(name)

            timestamp = time.time()

            context.set('_trace_variables', context.get('_trace_variables', []) + [{
                'timestamp': timestamp,
                'values': values
            }])

            return ActionResult(
                success=True,
                message=f"追踪变量: {len(values)} 个",
                data={
                    'values': values,
                    'count': len(values)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"追踪变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'names': []}
