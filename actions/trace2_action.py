"""Trace2 action module for RabAI AutoClick.

Provides additional tracing operations:
- TraceStartAction: Start trace
- TraceEndAction: End trace
- TraceEventAction: Record trace event
- TraceGetAction: Get trace data
- TraceClearAction: Clear trace data
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TraceStartAction(BaseAction):
    """Start trace."""
    action_type = "trace2_start"
    display_name = "开始追踪"
    description = "开始追踪"
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
        output_var = params.get('output_var', 'trace_status')

        try:
            import time

            resolved_name = context.resolve_value(name)

            context.set(f'trace_start_{resolved_name}', time.time())
            context.set(f'trace_events_{resolved_name}', [])
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"追踪开始: {resolved_name}",
                data={
                    'name': resolved_name,
                    'started_at': time.time(),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"开始追踪失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'default', 'output_var': 'trace_status'}


class TraceEndAction(BaseAction):
    """End trace."""
    action_type = "trace2_end"
    display_name = "结束追踪"
    description = "结束追踪"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute end.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with end status.
        """
        name = params.get('name', 'default')
        output_var = params.get('output_var', 'trace_result')

        try:
            import time

            resolved_name = context.resolve_value(name)

            start_time = context.get(f'trace_start_{resolved_name}', None)
            if start_time is None:
                return ActionResult(
                    success=False,
                    message=f"结束追踪失败: 追踪 {resolved_name} 未开始"
                )

            events = context.get(f'trace_events_{resolved_name}', [])
            duration = time.time() - start_time

            context.set(f'trace_duration_{resolved_name}', duration)
            context.set(f'trace_start_{resolved_name}', None)
            context.set(output_var, {
                'name': resolved_name,
                'duration': duration,
                'event_count': len(events)
            })

            return ActionResult(
                success=True,
                message=f"追踪结束: {resolved_name} ({duration:.2f}秒, {len(events)}事件)",
                data={
                    'name': resolved_name,
                    'duration': duration,
                    'event_count': len(events),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"结束追踪失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'default', 'output_var': 'trace_result'}


class TraceEventAction(BaseAction):
    """Record trace event."""
    action_type = "trace2_event"
    display_name = "记录追踪事件"
    description = "记录追踪事件"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute event.

        Args:
            context: Execution context.
            params: Dict with name, event, output_var.

        Returns:
            ActionResult with event status.
        """
        name = params.get('name', 'default')
        event = params.get('event', '')
        output_var = params.get('output_var', 'event_status')

        try:
            import time

            resolved_name = context.resolve_value(name)
            resolved_event = context.resolve_value(event) if event else ''

            start_time = context.get(f'trace_start_{resolved_name}', None)
            if start_time is None:
                return ActionResult(
                    success=False,
                    message=f"记录追踪事件失败: 追踪 {resolved_name} 未开始"
                )

            events = context.get(f'trace_events_{resolved_name}', [])
            timestamp = time.time() - start_time

            events.append({
                'timestamp': timestamp,
                'event': resolved_event
            })

            context.set(f'trace_events_{resolved_name}', events)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"追踪事件: {resolved_event}",
                data={
                    'name': resolved_name,
                    'event': resolved_event,
                    'timestamp': timestamp,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"记录追踪事件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['event']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'default', 'output_var': 'event_status'}


class TraceGetAction(BaseAction):
    """Get trace data."""
    action_type = "trace2_get"
    display_name: "获取追踪数据"
    description = "获取追踪数据"
    version = "2.0"

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
            ActionResult with trace data.
        """
        name = params.get('name', 'default')
        output_var = params.get('output_var', 'trace_data')

        try:
            resolved_name = context.resolve_value(name)

            events = context.get(f'trace_events_{resolved_name}', [])
            duration = context.get(f'trace_duration_{resolved_name}', None)

            context.set(output_var, {
                'name': resolved_name,
                'events': events,
                'duration': duration,
                'event_count': len(events)
            })

            return ActionResult(
                success=True,
                message=f"获取追踪数据: {resolved_name} ({len(events)}事件)",
                data={
                    'name': resolved_name,
                    'events': events,
                    'duration': duration,
                    'event_count': len(events),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取追踪数据失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'default', 'output_var': 'trace_data'}


class TraceClearAction(BaseAction):
    """Clear trace data."""
    action_type = "trace2_clear"
    display_name = "清除追踪数据"
    description = "清除追踪数据"
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
        name = params.get('name', 'default')
        output_var = params.get('output_var', 'clear_status')

        try:
            resolved_name = context.resolve_value(name)

            context.set(f'trace_events_{resolved_name}', [])
            context.set(f'trace_start_{resolved_name}', None)
            context.set(f'trace_duration_{resolved_name}', None)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"追踪数据已清除: {resolved_name}",
                data={
                    'name': resolved_name,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清除追踪数据失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'default', 'output_var': 'clear_status'}