"""Priority queue action module for RabAI AutoClick.

Provides priority-based task queue management with
enqueue, dequeue, peek, and batch operations.
"""

import sys
import os
import time
import threading
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import heapq
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class QueueStrategy(Enum):
    """Queue processing strategies."""
    FIFO = "fifo"
    LIFO = "lifo"
    PRIORITY = "priority"
    ROUND_ROBIN = "round_robin"


@dataclass(order=True)
class PriorityItem:
    """A priority queue item."""
    priority: int
    sequence: int = field(compare=False)
    item_id: str = field(compare=False, default="")
    data: Any = field(compare=False, default=None, repr=False)
    metadata: Dict[str, Any] = field(compare=False, default_factory=dict, repr=False)
    enqueued_at: float = field(compare=False, default_factory=time.time, repr=False)


class PriorityQueueAction(BaseAction):
    """Manage a priority-based task queue.
    
    Supports FIFO, LIFO, priority-based, and round-robin
    queue strategies with thread-safe operations.
    """
    action_type = "priority_queue"
    display_name = "优先级队列"
    description = "优先级队列管理，支持FIFO/LIFO/优先级策略"

    _queues: Dict[str, List] = {}
    _locks: Dict[str, threading.Lock] = {}
    _sequences: Dict[str, int] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Perform queue operations.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str (enqueue/dequeue/peek/size/clear/batch_dequeue)
                - queue_name: str, name of the queue
                - items: list, items to enqueue (for enqueue/batch)
                - priority: int, priority value (lower = higher priority)
                - strategy: str (fifo/lifo/priority)
                - max_items: int, max items to dequeue (for batch)
                - timeout: float, timeout for dequeue (seconds)
                - save_to_var: str, output variable
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', '')
        queue_name = params.get('queue_name', 'default')
        strategy = params.get('strategy', 'fifo')

        self._ensure_queue(queue_name, strategy)

        if operation == 'enqueue':
            return self._enqueue(context, params, queue_name, strategy)
        elif operation == 'dequeue':
            return self._dequeue(context, params, queue_name)
        elif operation == 'peek':
            return self._peek(context, params, queue_name)
        elif operation == 'size':
            return self._size(context, params, queue_name)
        elif operation == 'clear':
            return self._clear(context, params, queue_name)
        elif operation == 'batch_dequeue':
            return self._batch_dequeue(context, params, queue_name)
        elif operation == 'list_queues':
            return self._list_queues(context, params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )

    def _ensure_queue(self, queue_name: str, strategy: str) -> None:
        """Ensure queue exists with proper type."""
        if queue_name not in self._queues:
            with threading.Lock():
                if queue_name not in self._queues:
                    if strategy == 'priority':
                        self._queues[queue_name] = []
                    else:
                        self._queues[queue_name] = []
                    self._locks[queue_name] = threading.Lock()
                    self._sequences[queue_name] = 0

    def _enqueue(
        self, context: Any, params: Dict[str, Any],
        queue_name: str, strategy: str
    ) -> ActionResult:
        """Enqueue one or more items."""
        items = params.get('items', [])
        priority = params.get('priority', 0)
        metadata = params.get('metadata', {})
        save_to_var = params.get('save_to_var', None)

        if isinstance(items, dict):
            items = [items]
        if not isinstance(items, list):
            items = [items]

        with self._locks[queue_name]:
            for item in items:
                item_id = str(uuid.uuid4())[:8]
                if strategy == 'priority':
                    self._sequences[queue_name] += 1
                    priority_item = PriorityItem(
                        priority=priority,
                        sequence=self._sequences[queue_name],
                        item_id=item_id,
                        data=item,
                        metadata=metadata
                    )
                    heapq.heappush(self._queues[queue_name], priority_item)
                else:
                    self._queues[queue_name].append({
                        'id': item_id,
                        'data': item,
                        'metadata': metadata,
                        'priority': priority,
                        'enqueued_at': time.time()
                    })

        count = len(items)
        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = item_id if count == 1 else [item_id] * count

        return ActionResult(
            success=True,
            message=f"Enqueued {count} item(s) to '{queue_name}'",
            data={'queue_name': queue_name, 'count': count, 'item_ids': item_id if count == 1 else [item_id] * count}
        )

    def _dequeue(self, context: Any, params: Dict[str, Any], queue_name: str) -> ActionResult:
        """Dequeue an item."""
        timeout = params.get('timeout', None)
        strategy = params.get('strategy', 'fifo')
        save_to_var = params.get('save_to_var', None)

        if timeout and timeout > 0:
            deadline = time.time() + timeout
            while time.time() < deadline:
                with self._locks[queue_name]:
                    if len(self._queues[queue_name]) > 0:
                        item = self._queues[queue_name].pop(0) if strategy != 'priority' else heapq.heappop(self._queues[queue_name])
                        if save_to_var and hasattr(context, 'vars'):
                            context.vars[save_to_var] = item.data
                        return ActionResult(
                            success=True,
                            message=f"Dequeued from '{queue_name}'",
                            data=item.data if hasattr(item, 'data') else item.get('data')
                        )
                time.sleep(0.05)
            return ActionResult(
                success=False,
                message=f"Dequeue timeout after {timeout}s (queue empty)",
                data=None
            )

        with self._locks[queue_name]:
            if not self._queues[queue_name]:
                return ActionResult(
                    success=False,
                    message=f"Queue '{queue_name}' is empty",
                    data=None
                )

            if strategy == 'priority':
                item = heapq.heappop(self._queues[queue_name])
            elif strategy == 'lifo':
                item = self._queues[queue_name].pop()
            else:  # fifo
                item = self._queues[queue_name].pop(0)

            if save_to_var and hasattr(context, 'vars'):
                context.vars[save_to_var] = item.data if hasattr(item, 'data') else item.get('data')

            return ActionResult(
                success=True,
                message=f"Dequeued from '{queue_name}'",
                data=item.data if hasattr(item, 'data') else item.get('data')
            )

    def _peek(self, context: Any, params: Dict[str, Any], queue_name: str) -> ActionResult:
        """Peek at the next item without removing it."""
        save_to_var = params.get('save_to_var', None)
        strategy = params.get('strategy', 'fifo')

        with self._locks[queue_name]:
            if not self._queues[queue_name]:
                return ActionResult(
                    success=False,
                    message=f"Queue '{queue_name}' is empty",
                    data=None
                )
            if strategy == 'priority':
                item = self._queues[queue_name][0]
            elif strategy == 'lifo':
                item = self._queues[queue_name][-1]
            else:
                item = self._queues[queue_name][0]

            if save_to_var and hasattr(context, 'vars'):
                context.vars[save_to_var] = item.data if hasattr(item, 'data') else item.get('data')

            return ActionResult(
                success=True,
                message=f"Peeked at '{queue_name}'",
                data=item.data if hasattr(item, 'data') else item.get('data')
            )

    def _size(self, context: Any, params: Dict[str, Any], queue_name: str) -> ActionResult:
        """Get queue size."""
        save_to_var = params.get('save_to_var', None)
        with self._locks[queue_name]:
            size = len(self._queues[queue_name])
            if save_to_var and hasattr(context, 'vars'):
                context.vars[save_to_var] = size
            return ActionResult(
                success=True,
                message=f"Queue '{queue_name}' size: {size}",
                data=size
            )

    def _clear(self, context: Any, params: Dict[str, Any], queue_name: str) -> ActionResult:
        """Clear all items from queue."""
        with self._locks[queue_name]:
            count = len(self._queues[queue_name])
            self._queues[queue_name].clear()
            return ActionResult(
                success=True,
                message=f"Cleared {count} items from '{queue_name}'",
                data={'cleared': count}
            )

    def _batch_dequeue(
        self, context: Any, params: Dict[str, Any], queue_name: str
    ) -> ActionResult:
        """Dequeue multiple items at once."""
        max_items = params.get('max_items', 10)
        strategy = params.get('strategy', 'fifo')
        save_to_var = params.get('save_to_var', None)

        items = []
        with self._locks[queue_name]:
            for _ in range(min(max_items, len(self._queues[queue_name]))):
                if strategy == 'priority':
                    item = heapq.heappop(self._queues[queue_name])
                elif strategy == 'lifo':
                    item = self._queues[queue_name].pop()
                else:
                    item = self._queues[queue_name].pop(0)
                items.append(item.data if hasattr(item, 'data') else item.get('data'))

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = items

        return ActionResult(
            success=True,
            message=f"Batch dequeued {len(items)} items from '{queue_name}'",
            data=items
        )

    def _list_queues(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """List all queues and their sizes."""
        save_to_var = params.get('save_to_var', None)
        result = {}
        for qname in self._queues:
            with self._locks.get(qname, threading.Lock()):
                result[qname] = len(self._queues[qname])

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = result

        return ActionResult(
            success=True,
            message=f"Active queues: {len(result)}",
            data=result
        )

    def get_required_params(self) -> List[str]:
        return ['operation', 'queue_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'items': [],
            'priority': 0,
            'metadata': {},
            'strategy': 'fifo',
            'max_items': 10,
            'timeout': None,
            'save_to_var': None,
        }
