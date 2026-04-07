"""Trace14 action module for RabAI AutoClick.

Provides additional tracing operations:
- TraceStartAction: Start trace
- TraceEndAction: End trace
- TraceSpanAction: Create span
- TraceEventAction: Add event
- TraceAnnotationAction: Add annotation
- TraceContextAction: Manage trace context
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TraceStartAction(BaseAction):
    """Start trace."""
    action_type = "trace14_start"
    display_name = "开始追踪"
    description = "开始追踪会话"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute trace start.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with trace info.
        """
        name = params.get('name', 'trace')
        output_var = params.get('output_var', 'trace_info')

        try:
            import time
            import uuid

            resolved_name = context.resolve_value(name) if name else 'trace'
            trace_id = str(uuid.uuid4())
            start_time = time.time()

            if not hasattr(context, '_traces'):
                context._traces = {}
            context._traces['current'] = {
                'name': resolved_name,
                'trace_id': trace_id,
                'start_time': start_time,
                'spans': []
            }

            result = {
                'name': resolved_name,
                'trace_id': trace_id,
                'start_time': start_time
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"开始追踪: {resolved_name} ({trace_id})",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"开始追踪失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'trace_info'}


class TraceEndAction(BaseAction):
    """End trace."""
    action_type = "trace14_end"
    display_name = "结束追踪"
    description = "结束追踪会话"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute trace end.

        Args:
            context: Execution context.
            params: Dict with status, output_var.

        Returns:
            ActionResult with trace result.
        """
        status = params.get('status', 'ok')
        output_var = params.get('output_var', 'trace_result')

        try:
            import time

            resolved_status = context.resolve_value(status) if status else 'ok'

            if not hasattr(context, '_traces') or 'current' not in context._traces:
                return ActionResult(
                    success=False,
                    message="没有活动的追踪会话"
                )

            trace = context._traces['current']
            end_time = time.time()
            duration = end_time - trace['start_time']

            result = {
                'name': trace['name'],
                'trace_id': trace['trace_id'],
                'start_time': trace['start_time'],
                'end_time': end_time,
                'duration': duration,
                'status': resolved_status,
                'span_count': len(trace['spans'])
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"结束追踪: {trace['name']} ({duration:.4f}s)",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"结束追踪失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'status': 'ok', 'output_var': 'trace_result'}


class TraceSpanAction(BaseAction):
    """Create span."""
    action_type = "trace14_span"
    display_name = "创建跨度"
    description = "创建追踪跨度"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute span create.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with span info.
        """
        name = params.get('name', 'span')
        output_var = params.get('output_var', 'span_info')

        try:
            import time
            import uuid

            resolved_name = context.resolve_value(name) if name else 'span'
            span_id = str(uuid.uuid4())[:8]
            start_time = time.time()

            if not hasattr(context, '_traces'):
                context._traces = {}
            if 'current' not in context._traces:
                context._traces['current'] = {
                    'name': 'implicit',
                    'spans': []
                }

            span = {
                'name': resolved_name,
                'span_id': span_id,
                'start_time': start_time,
                'events': []
            }
            context._traces['current']['spans'].append(span)

            result = {
                'name': resolved_name,
                'span_id': span_id,
                'start_time': start_time
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"创建跨度: {resolved_name} ({span_id})",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建跨度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'span_info'}


class TraceEventAction(BaseAction):
    """Add event."""
    action_type = "trace14_event"
    display_name = "添加事件"
    description = "添加追踪事件"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute event add.

        Args:
            context: Execution context.
            params: Dict with name, message, output_var.

        Returns:
            ActionResult with event info.
        """
        name = params.get('name', 'event')
        message = params.get('message', '')
        output_var = params.get('output_var', 'event_info')

        try:
            import time

            resolved_name = context.resolve_value(name) if name else 'event'
            resolved_message = context.resolve_value(message) if message else ''

            if not hasattr(context, '_traces'):
                context._traces = {}
            if 'current' not in context._traces or not context._traces['current']['spans']:
                return ActionResult(
                    success=False,
                    message="没有活动的跨度"
                )

            event = {
                'name': resolved_name,
                'message': resolved_message,
                'timestamp': time.time()
            }

            context._traces['current']['spans'][-1]['events'].append(event)

            result = {
                'name': resolved_name,
                'message': resolved_message,
                'timestamp': event['timestamp']
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"添加事件: {resolved_name}",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"添加事件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'message': '', 'output_var': 'event_info'}


class TraceAnnotationAction(BaseAction):
    """Add annotation."""
    action_type = "trace14_annotation"
    display_name = "添加注释"
    description = "添加追踪注释"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute annotation add.

        Args:
            context: Execution context.
            params: Dict with key, value, output_var.

        Returns:
            ActionResult with annotation info.
        """
        key = params.get('key', '')
        value = params.get('value', '')
        output_var = params.get('output_var', 'annotation_info')

        try:
            resolved_key = context.resolve_value(key) if key else ''
            resolved_value = context.resolve_value(value) if value else ''

            if not hasattr(context, '_traces'):
                context._traces = {}
            if 'annotations' not in context._traces:
                context._traces['annotations'] = {}
            context._traces['annotations'][resolved_key] = resolved_value

            result = {
                'key': resolved_key,
                'value': resolved_value
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"添加注释: {resolved_key}={resolved_value}",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"添加注释失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'annotation_info'}


class TraceContextAction(BaseAction):
    """Manage trace context."""
    action_type = "trace14_context"
    display_name = "追踪上下文"
    description = "管理追踪上下文"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute context manage.

        Args:
            context: Execution context.
            params: Dict with action, output_var.

        Returns:
            ActionResult with context info.
        """
        action = params.get('action', 'get')
        output_var = params.get('output_var', 'trace_context')

        try:
            resolved_action = context.resolve_value(action) if action else 'get'

            if not hasattr(context, '_traces'):
                context._traces = {}

            if resolved_action == 'get':
                result = context._traces.get('current', {})
            elif resolved_action == 'clear':
                context._traces = {}
                result = {'cleared': True}
            elif resolved_action == 'list':
                result = {
                    'current': context._traces.get('current', {}).get('name'),
                    'trace_id': context._traces.get('current', {}).get('trace_id'),
                    'span_count': len(context._traces.get('current', {}).get('spans', [])),
                    'annotations': context._traces.get('annotations', {})
                }
            else:
                result = {'error': f'Unknown action: {resolved_action}'}

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"追踪上下文: {resolved_action}",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"追踪上下文失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['action']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'trace_context'}