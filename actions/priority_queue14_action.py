"""PriorityQueue14 action module for RabAI AutoClick.

Provides additional priority queue operations:
- PriorityQueueEnqueueAction: Add to priority queue
- PriorityQueueDequeueAction: Remove highest priority
- PriorityQueuePeekAction: View highest priority
- PriorityQueueSizeAction: Get queue size
- PriorityQueueEmptyAction: Check if empty
- PriorityQueueClearAction: Clear queue
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PriorityQueueEnqueueAction(BaseAction):
    """Add to priority queue."""
    action_type = "priority_queue14_enqueue"
    display_name = "优先级入队"
    description = "添加到优先级队列"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute enqueue.

        Args:
            context: Execution context.
            params: Dict with queue_name, value, priority, output_var.

        Returns:
            ActionResult with enqueue result.
        """
        queue_name = params.get('queue_name', 'default')
        value = params.get('value', None)
        priority = params.get('priority', 0)
        output_var = params.get('output_var', 'enqueue_result')

        try:
            import heapq

            resolved_queue = context.resolve_value(queue_name) if queue_name else 'default'
            resolved_value = context.resolve_value(value) if value else None
            resolved_priority = int(context.resolve_value(priority)) if priority else 0

            if not hasattr(context, '_priority_queues'):
                context._priority_queues = {}

            if resolved_queue not in context._priority_queues:
                context._priority_queues[resolved_queue] = []

            heapq.heappush(context._priority_queues[resolved_queue], (resolved_priority, resolved_value))

            context.set(output_var, len(context._priority_queues[resolved_queue]))

            return ActionResult(
                success=True,
                message=f"优先级入队: {resolved_value} (优先级={resolved_priority}) -> {resolved_queue}",
                data={
                    'queue': resolved_queue,
                    'value': resolved_value,
                    'priority': resolved_priority,
                    'size': len(context._priority_queues[resolved_queue]),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"优先级入队失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue_name', 'value', 'priority']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'enqueue_result'}


class PriorityQueueDequeueAction(BaseAction):
    """Remove highest priority."""
    action_type = "priority_queue14_dequeue"
    display_name = "优先级出队"
    description = "从优先级队列移除最高优先级"
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
            import heapq

            resolved_queue = context.resolve_value(queue_name) if queue_name else 'default'

            if not hasattr(context, '_priority_queues'):
                context._priority_queues = {}

            if resolved_queue not in context._priority_queues or not context._priority_queues[resolved_queue]:
                return ActionResult(
                    success=False,
                    message=f"优先级队列为空: {resolved_queue}"
                )

            priority, value = heapq.heappop(context._priority_queues[resolved_queue])

            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"优先级出队: {value} (优先级={priority}) <- {resolved_queue}",
                data={
                    'queue': resolved_queue,
                    'value': value,
                    'priority': priority,
                    'size': len(context._priority_queues[resolved_queue]),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"优先级出队失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dequeue_result'}


class PriorityQueuePeekAction(BaseAction):
    """View highest priority."""
    action_type = "priority_queue14_peek"
    display_name = "查看最高优先级"
    description = "查看优先级队列最高优先级项目"
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
            ActionResult with highest priority item.
        """
        queue_name = params.get('queue_name', 'default')
        output_var = params.get('output_var', 'peek_result')

        try:
            resolved_queue = context.resolve_value(queue_name) if queue_name else 'default'

            if not hasattr(context, '_priority_queues'):
                context._priority_queues = {}

            if resolved_queue not in context._priority_queues or not context._priority_queues[resolved_queue]:
                return ActionResult(
                    success=False,
                    message=f"优先级队列为空: {resolved_queue}"
                )

            priority, value = context._priority_queues[resolved_queue][0]

            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"查看最高优先级: {value} (优先级={priority})",
                data={
                    'queue': resolved_queue,
                    'value': value,
                    'priority': priority,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查看最高优先级失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'peek_result'}


class PriorityQueueSizeAction(BaseAction):
    """Get queue size."""
    action_type = "priority_queue14_size"
    display_name = "优先级队列大小"
    description = "获取优先级队列大小"
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

            if not hasattr(context, '_priority_queues'):
                context._priority_queues = {}

            size = len(context._priority_queues.get(resolved_queue, []))

            context.set(output_var, size)

            return ActionResult(
                success=True,
                message=f"优先级队列大小: {resolved_queue} = {size}",
                data={
                    'queue': resolved_queue,
                    'size': size,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"优先级队列大小失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'size_result'}


class PriorityQueueEmptyAction(BaseAction):
    """Check if empty."""
    action_type = "priority_queue14_empty"
    display_name = "优先级队列是否为空"
    description = "检查优先级队列是否为空"
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

            if not hasattr(context, '_priority_queues'):
                context._priority_queues = {}

            is_empty = resolved_queue not in context._priority_queues or not context._priority_queues[resolved_queue]

            context.set(output_var, is_empty)

            return ActionResult(
                success=True,
                message=f"优先级队列是否为空: {resolved_queue} = {is_empty}",
                data={
                    'queue': resolved_queue,
                    'is_empty': is_empty,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"优先级队列是否为空检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'empty_result'}


class PriorityQueueClearAction(BaseAction):
    """Clear queue."""
    action_type = "priority_queue14_clear"
    display_name = "清空优先级队列"
    description = "清空优先级队列"
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

            if not hasattr(context, '_priority_queues'):
                context._priority_queues = {}

            if resolved_queue in context._priority_queues:
                count = len(context._priority_queues[resolved_queue])
                context._priority_queues[resolved_queue] = []
            else:
                count = 0

            context.set(output_var, count)

            return ActionResult(
                success=True,
                message=f"清空优先级队列: {resolved_queue} ({count}项)",
                data={
                    'queue': resolved_queue,
                    'cleared_count': count,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清空优先级队列失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'clear_result'}