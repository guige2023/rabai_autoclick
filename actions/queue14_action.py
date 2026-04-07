"""Queue14 action module for RabAI AutoClick.

Provides additional queue operations:
- QueueEnqueueAction: Add to queue
- QueueDequeueAction: Remove from queue
- QueuePeekAction: View front item
- QueueSizeAction: Get queue size
- QueueEmptyAction: Check if empty
- QueueClearAction: Clear queue
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class QueueEnqueueAction(BaseAction):
    """Add to queue."""
    action_type = "queue14_enqueue"
    display_name = "入队"
    description = "添加到队列"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute enqueue.

        Args:
            context: Execution context.
            params: Dict with queue_name, value, output_var.

        Returns:
            ActionResult with enqueue result.
        """
        queue_name = params.get('queue_name', 'default')
        value = params.get('value', None)
        output_var = params.get('output_var', 'enqueue_result')

        try:
            resolved_queue = context.resolve_value(queue_name) if queue_name else 'default'
            resolved_value = context.resolve_value(value) if value else None

            if not hasattr(context, '_queues'):
                context._queues = {}

            if resolved_queue not in context._queues:
                context._queues[resolved_queue] = []

            context._queues[resolved_queue].append(resolved_value)

            context.set(output_var, len(context._queues[resolved_queue]))

            return ActionResult(
                success=True,
                message=f"入队: {resolved_value} -> {resolved_queue} ({len(context._queues[resolved_queue])}项)",
                data={
                    'queue': resolved_queue,
                    'value': resolved_value,
                    'size': len(context._queues[resolved_queue]),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"入队失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue_name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'enqueue_result'}


class QueueDequeueAction(BaseAction):
    """Remove from queue."""
    action_type = "queue14_dequeue"
    display_name = "出队"
    description = "从队列移除"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dequeue.

        Args:
            context: Execution context.
            params: Dict with queue_name, output_var.

        Returns:
            ActionResult with dequeued value.
        """
        queue_name = params.get('queue_name', 'default')
        output_var = params.get('output_var', 'dequeue_result')

        try:
            resolved_queue = context.resolve_value(queue_name) if queue_name else 'default'

            if not hasattr(context, '_queues'):
                context._queues = {}

            if resolved_queue not in context._queues or not context._queues[resolved_queue]:
                return ActionResult(
                    success=False,
                    message=f"队列为空: {resolved_queue}"
                )

            value = context._queues[resolved_queue].pop(0)

            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"出队: {value} <- {resolved_queue} ({len(context._queues[resolved_queue])}项)",
                data={
                    'queue': resolved_queue,
                    'value': value,
                    'size': len(context._queues[resolved_queue]),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"出队失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dequeue_result'}


class QueuePeekAction(BaseAction):
    """View front item."""
    action_type = "queue14_peek"
    display_name = "查看队首"
    description = "查看队列前端项目"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute peek.

        Args:
            context: Execution context.
            params: Dict with queue_name, output_var.

        Returns:
            ActionResult with front item.
        """
        queue_name = params.get('queue_name', 'default')
        output_var = params.get('output_var', 'peek_result')

        try:
            resolved_queue = context.resolve_value(queue_name) if queue_name else 'default'

            if not hasattr(context, '_queues'):
                context._queues = {}

            if resolved_queue not in context._queues or not context._queues[resolved_queue]:
                return ActionResult(
                    success=False,
                    message=f"队列为空: {resolved_queue}"
                )

            value = context._queues[resolved_queue][0]

            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"查看队首: {value}",
                data={
                    'queue': resolved_queue,
                    'value': value,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查看队首失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'peek_result'}


class QueueSizeAction(BaseAction):
    """Get queue size."""
    action_type = "queue14_size"
    display_name = "队列大小"
    description = "获取队列大小"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute size.

        Args:
            context: Execution context.
            params: Dict with queue_name, output_var.

        Returns:
            ActionResult with queue size.
        """
        queue_name = params.get('queue_name', 'default')
        output_var = params.get('output_var', 'size_result')

        try:
            resolved_queue = context.resolve_value(queue_name) if queue_name else 'default'

            if not hasattr(context, '_queues'):
                context._queues = {}

            size = len(context._queues.get(resolved_queue, []))

            context.set(output_var, size)

            return ActionResult(
                success=True,
                message=f"队列大小: {resolved_queue} = {size}",
                data={
                    'queue': resolved_queue,
                    'size': size,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"队列大小失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'size_result'}


class QueueEmptyAction(BaseAction):
    """Check if empty."""
    action_type = "queue14_empty"
    display_name = "队列是否为空"
    description = "检查队列是否为空"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute empty check.

        Args:
            context: Execution context.
            params: Dict with queue_name, output_var.

        Returns:
            ActionResult with empty status.
        """
        queue_name = params.get('queue_name', 'default')
        output_var = params.get('output_var', 'empty_result')

        try:
            resolved_queue = context.resolve_value(queue_name) if queue_name else 'default'

            if not hasattr(context, '_queues'):
                context._queues = {}

            is_empty = resolved_queue not in context._queues or not context._queues[resolved_queue]

            context.set(output_var, is_empty)

            return ActionResult(
                success=True,
                message=f"队列为空: {resolved_queue} = {is_empty}",
                data={
                    'queue': resolved_queue,
                    'is_empty': is_empty,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"队列是否为空检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'empty_result'}


class QueueClearAction(BaseAction):
    """Clear queue."""
    action_type = "queue14_clear"
    display_name = "清空队列"
    description = "清空队列"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clear.

        Args:
            context: Execution context.
            params: Dict with queue_name, output_var.

        Returns:
            ActionResult with clear result.
        """
        queue_name = params.get('queue_name', 'default')
        output_var = params.get('output_var', 'clear_result')

        try:
            resolved_queue = context.resolve_value(queue_name) if queue_name else 'default'

            if not hasattr(context, '_queues'):
                context._queues = {}

            if resolved_queue in context._queues:
                count = len(context._queues[resolved_queue])
                context._queues[resolved_queue] = []
            else:
                count = 0

            context.set(output_var, count)

            return ActionResult(
                success=True,
                message=f"清空队列: {resolved_queue} ({count}项)",
                data={
                    'queue': resolved_queue,
                    'cleared_count': count,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清空队列失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'clear_result'}