"""PriorityQueue action module for RabAI AutoClick.

Provides priority queue operations:
- PriorityQueueCreateAction: Create priority queue
- PriorityQueueEnqueueAction: Enqueue item with priority
- PriorityQueueDequeueAction: Dequeue highest priority item
- PriorityQueuePeekAction: Peek highest priority item
- PriorityQueueSizeAction: Get priority queue size
"""

import heapq
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PriorityQueueCreateAction(BaseAction):
    """Create priority queue."""
    action_type = "priority_queue_create"
    display_name = "创建优先队列"
    description = "创建优先队列"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create.

        Args:
            context: Execution context.
            params: Dict with name, max_priority.

        Returns:
            ActionResult indicating created.
        """
        name = params.get('name', 'priority_queue')
        max_priority = params.get('max_priority', True)

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_max = bool(context.resolve_value(max_priority))

            context.set(f'_pq_{resolved_name}_items', [])
            context.set(f'_pq_{resolved_name}_max', resolved_max)

            return ActionResult(
                success=True,
                message=f"优先队列 {resolved_name} 创建",
                data={
                    'name': resolved_name,
                    'max_priority': resolved_max
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建优先队列失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'priority_queue', 'max_priority': True}


class PriorityQueueEnqueueAction(BaseAction):
    """Enqueue item with priority."""
    action_type = "priority_queue_enqueue"
    display_name = "优先队列入队"
    description = "将项目加入优先队列"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute enqueue.

        Args:
            context: Execution context.
            params: Dict with name, item, priority.

        Returns:
            ActionResult indicating enqueued.
        """
        name = params.get('name', 'priority_queue')
        item = params.get('item', None)
        priority = params.get('priority', 0)

        try:
            resolved_name = context.resolve_value(name)
            resolved_item = context.resolve_value(item)
            resolved_priority = float(context.resolve_value(priority))

            items = context.get(f'_pq_{resolved_name}_items', [])
            max_priority = context.get(f'_pq_{resolved_name}_max', True)

            if max_priority:
                heapq.heappush(items, (-resolved_priority, resolved_item))
            else:
                heapq.heappush(items, (resolved_priority, resolved_item))

            context.set(f'_pq_{resolved_name}_items', items)

            return ActionResult(
                success=True,
                message=f"入队: 优先级 {resolved_priority}",
                data={
                    'name': resolved_name,
                    'item': resolved_item,
                    'priority': resolved_priority,
                    'size': len(items)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"入队失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['item', 'priority']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'priority_queue'}


class PriorityQueueDequeueAction(BaseAction):
    """Dequeue highest priority item."""
    action_type = "priority_queue_dequeue"
    display_name = "优先队列出队"
    description = "从优先队列取出最高优先级项目"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dequeue.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with dequeued item.
        """
        name = params.get('name', 'priority_queue')
        output_var = params.get('output_var', 'dequeued_item')

        try:
            resolved_name = context.resolve_value(name)

            items = context.get(f'_pq_{resolved_name}_items', [])

            if not items:
                return ActionResult(
                    success=False,
                    message=f"优先队列 {resolved_name} 为空"
                )

            priority, item = heapq.heappop(items)
            context.set(f'_pq_{resolved_name}_items', items)

            actual_priority = -priority if context.get(f'_pq_{resolved_name}_max', True) else priority

            result = {'item': item, 'priority': actual_priority}
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"出队: {item}, 优先级 {actual_priority}",
                data={
                    'name': resolved_name,
                    'item': item,
                    'priority': actual_priority,
                    'remaining': len(items),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"出队失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'priority_queue', 'output_var': 'dequeued_item'}


class PriorityQueuePeekAction(BaseAction):
    """Peek highest priority item."""
    action_type = "priority_queue_peek"
    display_name = "查看优先队列"
    description = "查看最高优先级项目"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute peek.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with front item.
        """
        name = params.get('name', 'priority_queue')
        output_var = params.get('output_var', 'peek_item')

        try:
            resolved_name = context.resolve_value(name)

            items = context.get(f'_pq_{resolved_name}_items', [])

            if not items:
                return ActionResult(
                    success=False,
                    message=f"优先队列 {resolved_name} 为空"
                )

            priority, item = items[0]
            actual_priority = -priority if context.get(f'_pq_{resolved_name}_max', True) else priority

            result = {'item': item, 'priority': actual_priority}
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"查看: {item}, 优先级 {actual_priority}",
                data={
                    'name': resolved_name,
                    'item': item,
                    'priority': actual_priority,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查看失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'priority_queue', 'output_var': 'peek_item'}


class PriorityQueueSizeAction(BaseAction):
    """Get priority queue size."""
    action_type = "priority_queue_size"
    display_name = "获取优先队列大小"
    description = "获取优先队列大小"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute size.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with queue size.
        """
        name = params.get('name', 'priority_queue')
        output_var = params.get('output_var', 'pq_size')

        try:
            resolved_name = context.resolve_value(name)

            items = context.get(f'_pq_{resolved_name}_items', [])

            context.set(output_var, len(items))

            return ActionResult(
                success=True,
                message=f"优先队列大小: {len(items)}",
                data={
                    'name': resolved_name,
                    'size': len(items),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取大小失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'priority_queue', 'output_var': 'pq_size'}
