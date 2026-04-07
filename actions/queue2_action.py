"""Queue2 action module for RabAI AutoClick.

Provides additional queue operations:
- QueueEnqueueAction: Enqueue item
- QueueDequeueAction: Dequeue item
- QueueFrontAction: Get front item
- QueueRearAction: Get rear item
- QueueIsFullAction: Check if queue is full
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class QueueEnqueueAction(BaseAction):
    """Enqueue item."""
    action_type = "queue2_enqueue"
    display_name = "队列入队"
    description = "将元素加入队列"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute enqueue.

        Args:
            context: Execution context.
            params: Dict with queue, item, max_size, output_var.

        Returns:
            ActionResult with updated queue.
        """
        queue = params.get('queue', [])
        item = params.get('item', None)
        max_size = params.get('max_size', None)
        output_var = params.get('output_var', 'updated_queue')

        try:
            resolved_queue = context.resolve_value(queue)
            resolved_item = context.resolve_value(item)
            resolved_max = int(context.resolve_value(max_size)) if max_size else None

            if not isinstance(resolved_queue, (list, tuple)):
                resolved_queue = [resolved_queue]

            if resolved_max and len(resolved_queue) >= resolved_max:
                return ActionResult(
                    success=False,
                    message="队列入队失败: 队列已满"
                )

            result = list(resolved_queue)
            result.append(resolved_item)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"队列入队: {resolved_item}",
                data={
                    'item': resolved_item,
                    'queue': result,
                    'size': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"队列入队失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'queue': [], 'max_size': None, 'output_var': 'updated_queue'}


class QueueDequeueAction(BaseAction):
    """Dequeue item."""
    action_type = "queue2_dequeue"
    display_name = "队列出队"
    description = "从队列取出元素"
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
            resolved_queue = context.resolve_value(queue)

            if not isinstance(resolved_queue, (list, tuple)):
                resolved_queue = list(resolved_queue)

            if len(resolved_queue) == 0:
                return ActionResult(
                    success=False,
                    message="队列出队失败: 队列为空"
                )

            result_queue = list(resolved_queue)
            dequeued = result_queue.pop(0)

            context.set(output_var, {
                'item': dequeued,
                'queue': result_queue
            })

            return ActionResult(
                success=True,
                message=f"队列出队: {dequeued}",
                data={
                    'dequeued_item': dequeued,
                    'queue': result_queue,
                    'size': len(result_queue),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"队列出队失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dequeue_result'}


class QueueFrontAction(BaseAction):
    """Get front item."""
    action_type = "queue2_front"
    display_name = "获取队首"
    description = "获取队列第一个元素"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute front.

        Args:
            context: Execution context.
            params: Dict with queue, output_var.

        Returns:
            ActionResult with front item.
        """
        queue = params.get('queue', [])
        output_var = params.get('output_var', 'front_result')

        try:
            resolved_queue = context.resolve_value(queue)

            if not isinstance(resolved_queue, (list, tuple)):
                resolved_queue = list(resolved_queue)

            if len(resolved_queue) == 0:
                return ActionResult(
                    success=False,
                    message="获取队首失败: 队列为空"
                )

            result = resolved_queue[0]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"队首元素: {result}",
                data={
                    'front': result,
                    'queue': resolved_queue,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取队首失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'front_result'}


class QueueRearAction(BaseAction):
    """Get rear item."""
    action_type = "queue2_rear"
    display_name = "获取队尾"
    description = "获取队列最后一个元素"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rear.

        Args:
            context: Execution context.
            params: Dict with queue, output_var.

        Returns:
            ActionResult with rear item.
        """
        queue = params.get('queue', [])
        output_var = params.get('output_var', 'rear_result')

        try:
            resolved_queue = context.resolve_value(queue)

            if not isinstance(resolved_queue, (list, tuple)):
                resolved_queue = list(resolved_queue)

            if len(resolved_queue) == 0:
                return ActionResult(
                    success=False,
                    message="获取队尾失败: 队列为空"
                )

            result = resolved_queue[-1]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"队尾元素: {result}",
                data={
                    'rear': result,
                    'queue': resolved_queue,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取队尾失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rear_result'}


class QueueIsFullAction(BaseAction):
    """Check if queue is full."""
    action_type = "queue2_is_full"
    display_name = "判断队列已满"
    description = "判断队列是否已满"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is full.

        Args:
            context: Execution context.
            params: Dict with queue, max_size, output_var.

        Returns:
            ActionResult with is full result.
        """
        queue = params.get('queue', [])
        max_size = params.get('max_size', None)
        output_var = params.get('output_var', 'is_full')

        try:
            resolved_queue = context.resolve_value(queue)
            resolved_max = int(context.resolve_value(max_size)) if max_size else None

            if not isinstance(resolved_queue, (list, tuple)):
                resolved_queue = list(resolved_queue)

            if resolved_max is None:
                return ActionResult(
                    success=True,
                    message=f"队列已满: 否 (无限制)",
                    data={
                        'is_full': False,
                        'size': len(resolved_queue),
                        'max_size': None,
                        'output_var': output_var
                    }
                )

            is_full = len(resolved_queue) >= resolved_max

            context.set(output_var, is_full)

            return ActionResult(
                success=True,
                message=f"队列已满: {'是' if is_full else '否'}",
                data={
                    'is_full': is_full,
                    'size': len(resolved_queue),
                    'max_size': resolved_max,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断队列已满失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue', 'max_size']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_full'}