"""Queue action module for RabAI AutoClick.

Provides queue operations:
- QueueCreateAction: Create queue
- QueueEnqueueAction: Enqueue item
- QueueDequeueAction: Dequeue item
- QueuePeekAction: Peek queue
- QueueSizeAction: Get queue size
"""

from typing import Any, Dict, List, Optional
from collections import deque

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class QueueCreateAction(BaseAction):
    """Create queue."""
    action_type = "queue_create"
    display_name = "创建队列"
    description = "创建队列"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create.

        Args:
            context: Execution context.
            params: Dict with name, maxsize.

        Returns:
            ActionResult indicating created.
        """
        name = params.get('name', 'queue')
        maxsize = params.get('maxsize', 0)

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_maxsize = int(context.resolve_value(maxsize))

            context.set(f'_queue_{resolved_name}_items', [])
            context.set(f'_queue_{resolved_name}_maxsize', resolved_maxsize)

            return ActionResult(
                success=True,
                message=f"队列 {resolved_name} 创建",
                data={
                    'name': resolved_name,
                    'maxsize': resolved_maxsize
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建队列失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'queue', 'maxsize': 0}


class QueueEnqueueAction(BaseAction):
    """Enqueue item."""
    action_type = "queue_enqueue"
    display_name = "入队"
    description = "将项目加入队列"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute enqueue.

        Args:
            context: Execution context.
            params: Dict with name, item.

        Returns:
            ActionResult indicating enqueued.
        """
        name = params.get('name', 'queue')
        item = params.get('item', None)

        try:
            resolved_name = context.resolve_value(name)
            resolved_item = context.resolve_value(item)

            items = context.get(f'_queue_{resolved_name}_items', [])
            maxsize = context.get(f'_queue_{resolved_name}_maxsize', 0)

            if maxsize > 0 and len(items) >= maxsize:
                return ActionResult(
                    success=False,
                    message=f"队列 {resolved_name} 已满"
                )

            items.append(resolved_item)
            context.set(f'_queue_{resolved_name}_items', items)

            return ActionResult(
                success=True,
                message=f"入队: {len(items)} 项",
                data={
                    'name': resolved_name,
                    'item': resolved_item,
                    'size': len(items)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"入队失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'queue'}


class QueueDequeueAction(BaseAction):
    """Dequeue item."""
    action_type = "queue_dequeue"
    display_name = "出队"
    description = "从队列取出项目"

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
        name = params.get('name', 'queue')
        output_var = params.get('output_var', 'dequeued_item')

        try:
            resolved_name = context.resolve_value(name)

            items = context.get(f'_queue_{resolved_name}_items', [])

            if not items:
                return ActionResult(
                    success=False,
                    message=f"队列 {resolved_name} 为空"
                )

            item = items.pop(0)
            context.set(f'_queue_{resolved_name}_items', items)
            context.set(output_var, item)

            return ActionResult(
                success=True,
                message=f"出队: {len(items)} 项剩余",
                data={
                    'name': resolved_name,
                    'item': item,
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
        return {'name': 'queue', 'output_var': 'dequeued_item'}


class QueuePeekAction(BaseAction):
    """Peek queue."""
    action_type = "queue_peek"
    display_name = "查看队列"
    description = "查看队列前端项目"

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
        name = params.get('name', 'queue')
        output_var = params.get('output_var', 'peek_item')

        try:
            resolved_name = context.resolve_value(name)

            items = context.get(f'_queue_{resolved_name}_items', [])

            if not items:
                return ActionResult(
                    success=False,
                    message=f"队列 {resolved_name} 为空"
                )

            item = items[0]
            context.set(output_var, item)

            return ActionResult(
                success=True,
                message=f"查看队列前端",
                data={
                    'name': resolved_name,
                    'item': item,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查看队列失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'queue', 'output_var': 'peek_item'}


class QueueSizeAction(BaseAction):
    """Get queue size."""
    action_type = "queue_size"
    display_name = "获取队列大小"
    description = "获取队列大小"

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
        name = params.get('name', 'queue')
        output_var = params.get('output_var', 'queue_size')

        try:
            resolved_name = context.resolve_value(name)

            items = context.get(f'_queue_{resolved_name}_items', [])
            maxsize = context.get(f'_queue_{resolved_name}_maxsize', 0)

            context.set(output_var, len(items))

            return ActionResult(
                success=True,
                message=f"队列大小: {len(items)}/{maxsize if maxsize > 0 else '无限制'}",
                data={
                    'name': resolved_name,
                    'size': len(items),
                    'maxsize': maxsize,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取队列大小失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'queue', 'output_var': 'queue_size'}
