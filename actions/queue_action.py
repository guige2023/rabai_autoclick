"""Queue operations action module for RabAI AutoClick.

Provides queue operations:
- QueuePushAction: Push item to queue
- QueuePopAction: Pop item from queue
- QueuePeekAction: Peek at queue item
- QueueSizeAction: Get queue size
- QueueClearAction: Clear queue
- QueueListAction: List queue contents
"""

from __future__ import annotations

import sys
import os
import json
from typing import Any, Dict, List, Optional, Union
from collections import deque

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class QueuePushAction(BaseAction):
    """Push item to queue."""
    action_type = "queue_push"
    display_name = "队列推入"
    description = "向队列推入元素"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute queue push."""
        queue_var = params.get('queue_var', 'default_queue')
        item = params.get('item', '')
        position = params.get('position', 'back')  # back or front
        output_var = params.get('output_var', 'queue_result')

        if item == '':
            return ActionResult(success=False, message="item is required")

        try:
            resolved_item = context.resolve_value(item) if context else item
            resolved_pos = context.resolve_value(position) if context else position

            if not hasattr(context, '_queues'):
                context._queues = {}

            if queue_var not in context._queues:
                context._queues[queue_var] = deque()

            queue = context._queues[queue_var]
            if resolved_pos == 'front':
                queue.appendleft(resolved_item)
            else:
                queue.append(resolved_item)

            result = {'size': len(queue), 'queue': queue_var, 'position': resolved_pos}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Pushed to {queue_var}: {len(queue)} items", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Queue push error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'queue_var': 'default_queue', 'position': 'back', 'output_var': 'queue_result'}


class QueuePopAction(BaseAction):
    """Pop item from queue."""
    action_type = "queue_pop"
    display_name = "队列弹出"
    description = "从队列弹出元素"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute queue pop."""
        queue_var = params.get('queue_var', 'default_queue')
        position = params.get('position', 'front')  # front or back
        output_var = params.get('output_var', 'queue_pop_result')

        try:
            if not hasattr(context, '_queues') or queue_var not in context._queues:
                return ActionResult(success=False, message=f"Queue '{queue_var}' not found or empty")

            queue = context._queues[queue_var]
            if not queue:
                return ActionResult(success=False, message=f"Queue '{queue_var}' is empty")

            if position == 'back':
                item = queue.pop()
            else:
                item = queue.popleft()

            result = {'item': item, 'remaining': len(queue), 'queue': queue_var}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Popped: {str(item)[:50]}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Queue pop error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'queue_var': 'default_queue', 'position': 'front', 'output_var': 'queue_pop_result'}


class QueuePeekAction(BaseAction):
    """Peek at queue item without removing."""
    action_type = "queue_peek"
    display_name = "队列查看"
    description = "查看队列元素"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute queue peek."""
        queue_var = params.get('queue_var', 'default_queue')
        index = params.get('index', 0)
        output_var = params.get('output_var', 'queue_peek_result')

        try:
            if not hasattr(context, '_queues') or queue_var not in context._queues:
                return ActionResult(success=False, message=f"Queue '{queue_var}' not found")

            queue = context._queues[queue_var]
            if not queue:
                return ActionResult(success=False, message=f"Queue '{queue_var}' is empty")

            resolved_index = context.resolve_value(index) if context else index
            if 0 <= resolved_index < len(queue):
                item = queue[resolved_index]
                result = {'item': item, 'index': resolved_index, 'queue': queue_var}
                if context:
                    context.set(output_var, result)
                return ActionResult(success=True, message=f"Peek[{resolved_index}]: {str(item)[:50]}", data=result)
            else:
                return ActionResult(success=False, message=f"Index {resolved_index} out of range")
        except Exception as e:
            return ActionResult(success=False, message=f"Queue peek error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'queue_var': 'default_queue', 'index': 0, 'output_var': 'queue_peek_result'}


class QueueSizeAction(BaseAction):
    """Get queue size."""
    action_type = "queue_size"
    display_name = "队列大小"
    description = "获取队列大小"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute queue size."""
        queue_var = params.get('queue_var', 'default_queue')
        output_var = params.get('output_var', 'queue_size')

        try:
            if not hasattr(context, '_queues') or queue_var not in context._queues:
                size = 0
            else:
                size = len(context._queues[queue_var])

            result = {'size': size, 'queue': queue_var}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Queue {queue_var}: {size} items", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Queue size error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'queue_var': 'default_queue', 'output_var': 'queue_size'}


class QueueClearAction(BaseAction):
    """Clear queue."""
    action_type = "queue_clear"
    display_name = "清空队列"
    description = "清空队列"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute queue clear."""
        queue_var = params.get('queue_var', 'default_queue')
        output_var = params.get('output_var', 'queue_clear_result')

        try:
            cleared = 0
            if hasattr(context, '_queues') and queue_var in context._queues:
                cleared = len(context._queues[queue_var])
                del context._queues[queue_var]

            result = {'cleared': cleared, 'queue': queue_var}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Cleared {cleared} items from {queue_var}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Queue clear error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'queue_var': 'default_queue', 'output_var': 'queue_clear_result'}


class QueueListAction(BaseAction):
    """List queue contents."""
    action_type = "queue_list"
    display_name = "队列列表"
    description = "列出队列内容"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute queue list."""
        queue_var = params.get('queue_var', 'default_queue')
        start = params.get('start', 0)
        end = params.get('end', None)
        output_var = params.get('output_var', 'queue_items')

        try:
            if not hasattr(context, '_queues') or queue_var not in context._queues:
                items = []
            else:
                items = list(context._queues[queue_var])

            resolved_start = context.resolve_value(start) if context else start
            resolved_end = context.resolve_value(end) if context else end

            if resolved_end is not None:
                items = items[resolved_start:resolved_end]
            elif resolved_start > 0:
                items = items[resolved_start:]

            result = {'items': items, 'count': len(items), 'queue': queue_var, 'total': len(context._queues.get(queue_var, deque()))}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Queue {queue_var}: {len(items)} items shown", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Queue list error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'queue_var': 'default_queue', 'start': 0, 'end': None, 'output_var': 'queue_items'}
