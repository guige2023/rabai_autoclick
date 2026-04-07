"""PriorityQueue2 action module for RabAI AutoClick.

Provides additional priority queue operations:
- PriorityQueueEnqueueAction: Enqueue with priority
- PriorityQueueDequeueAction: Dequeue highest priority
- PriorityQueuePeekAction: Peek at highest priority
- PriorityQueueSizeAction: Get queue size
- PriorityQueueIsEmptyAction: Check if empty
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PriorityQueueEnqueueAction(BaseAction):
    """Enqueue with priority."""
    action_type = "priority_queue2_enqueue"
    display_name = "优先级入队"
    description = "将元素加入优先级队列"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute enqueue.

        Args:
            context: Execution context.
            params: Dict with queue, item, priority, output_var.

        Returns:
            ActionResult with updated queue.
        """
        queue = params.get('queue', [])
        item = params.get('item', None)
        priority = params.get('priority', 0)
        output_var = params.get('output_var', 'updated_queue')

        try:
            import heapq

            resolved_queue = context.resolve_value(queue)
            resolved_item = context.resolve_value(item)
            resolved_priority = int(context.resolve_value(priority)) if priority else 0

            if not isinstance(resolved_queue, (list, tuple)):
                resolved_queue = [resolved_queue]

            result = list(resolved_queue)
            heapq.heappush(result, (resolved_priority, resolved_item))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"优先级入队: 优先级{resolved_priority}",
                data={
                    'item': resolved_item,
                    'priority': resolved_priority,
                    'queue': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"优先级入队失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['item', 'priority']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'queue': [], 'output_var': 'updated_queue'}


class PriorityQueueDequeueAction(BaseAction):
    """Dequeue highest priority."""
    action_type = "priority_queue2_dequeue"
    display_name: "优先级出队"
    description = "从优先级队列取出最高优先级"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dequeue.

        Args:
            context: Execution context.
            params: Dict with queue, output_var.

        Returns:
            ActionResult with dequeued item.
        """
        queue = params.get('queue', [])
        output_var = params.get('output_var', 'dequeue_result')

        try:
            import heapq

            resolved_queue = context.resolve_value(queue)

            if not isinstance(resolved_queue, (list, tuple)):
                resolved_queue = [resolved_queue]

            if len(resolved_queue) == 0:
                return ActionResult(
                    success=False,
                    message="优先级出队失败: 队列为空"
                )

            result_queue = list(resolved_queue)
            priority, item = heapq.heappop(result_queue)

            context.set(output_var, {
                'item': item,
                'priority': priority,
                'queue': result_queue
            })

            return ActionResult(
                success=True,
                message=f"优先级出队: {item}, 优先级{priority}",
                data={
                    'dequeued_item': item,
                    'priority': priority,
                    'queue': result_queue,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"优先级出队失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dequeue_result'}


class PriorityQueuePeekAction(BaseAction):
    """Peek at highest priority."""
    action_type = "priority_queue2_peek"
    display_name: "查看最高优先级"
    description = "查看优先级队列的最高优先级"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute peek.

        Args:
            context: Execution context.
            params: Dict with queue, output_var.

        Returns:
            ActionResult with highest priority item.
        """
        queue = params.get('queue', [])
        output_var = params.get('output_var', 'peek_result')

        try:
            import heapq

            resolved_queue = context.resolve_value(queue)

            if not isinstance(resolved_queue, (list, tuple)):
                resolved_queue = [resolved_queue]

            if len(resolved_queue) == 0:
                return ActionResult(
                    success=False,
                    message="查看最高优先级失败: 队列为空"
                )

            priority, item = resolved_queue[0]

            context.set(output_var, {
                'item': item,
                'priority': priority
            })

            return ActionResult(
                success=True,
                message=f"最高优先级: {item}, 优先级{priority}",
                data={
                    'item': item,
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
        return ['queue']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'peek_result'}


class PriorityQueueSizeAction(BaseAction):
    """Get queue size."""
    action_type = "priority_queue2_size"
    display_name: "获取队列大小"
    description = "获取优先级队列大小"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute size.

        Args:
            context: Execution context.
            params: Dict with queue, output_var.

        Returns:
            ActionResult with queue size.
        """
        queue = params.get('queue', [])
        output_var = params.get('output_var', 'queue_size')

        try:
            resolved_queue = context.resolve_value(queue)

            if not isinstance(resolved_queue, (list, tuple)):
                resolved_queue = [resolved_queue]

            size = len(resolved_queue)

            context.set(output_var, size)

            return ActionResult(
                success=True,
                message=f"队列大小: {size}",
                data={
                    'size': size,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取队列大小失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'queue_size'}


class PriorityQueueIsEmptyAction(BaseAction):
    """Check if empty."""
    action_type = "priority_queue2_is_empty"
    display_name: "判断队列为空"
    description = "判断优先级队列是否为空"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is empty.

        Args:
            context: Execution context.
            params: Dict with queue, output_var.

        Returns:
            ActionResult with is empty result.
        """
        queue = params.get('queue', [])
        output_var = params.get('output_var', 'is_empty')

        try:
            resolved_queue = context.resolve_value(queue)

            if not isinstance(resolved_queue, (list, tuple)):
                resolved_queue = [resolved_queue]

            is_empty = len(resolved_queue) == 0

            context.set(output_var, is_empty)

            return ActionResult(
                success=True,
                message=f"队列为空: {'是' if is_empty else '否'}",
                data={
                    'is_empty': is_empty,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断队列为空失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_empty'}