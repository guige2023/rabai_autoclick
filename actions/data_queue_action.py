"""Data Queue Action Module for RabAI AutoClick.

Provides thread-safe queue operations for buffering and
processing data items in FIFO order with priority support.
"""

import time
import threading
import heapq
import uuid
import sys
import os
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import IntEnum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class Priority(IntEnum):
    """Queue priority levels. Lower number = higher priority."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


@dataclass(order=True)
class QueueItem:
    """Priority queue item with ordering."""
    priority: int
    sequence: int
    timestamp: float
    item_id: str = field(compare=False)
    data: Any = field(compare=False, default=None)
    metadata: Dict[str, Any] = field(compare=False, default_factory=dict)


class DataQueueAction(BaseAction):
    """Thread-safe priority queue for data processing.

    Implements a multi-priority queue with FIFO ordering within
    each priority level. Supports blocking get/put operations,
    timeouts, batch operations, and queue monitoring.
    """
    action_type = "data_queue"
    display_name = "数据队列"
    description = "线程安全优先级队列，支持批量操作"

    _queues: Dict[str, Dict[str, Any]] = {}
    _lock = threading.RLock()

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute queue operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'create', 'put', 'get', 'peek',
                               'size', 'clear', 'stats'
                - queue_name: str - name of the queue
                - item: Any (optional) - item to enqueue
                - priority: int (optional) - priority 0-4, default 2
                - timeout: float (optional) - get timeout in seconds
                - batch_size: int (optional) - number of items for batch get
                - metadata: dict (optional) - item metadata

        Returns:
            ActionResult with queue operation result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'put')
            queue_name = params.get('queue_name', 'default')

            if operation == 'create':
                return self._create_queue(queue_name, start_time)
            elif operation == 'put':
                return self._put_item(queue_name, params, start_time)
            elif operation == 'get':
                return self._get_item(queue_name, params, start_time)
            elif operation == 'peek':
                return self._peek_item(queue_name, start_time)
            elif operation == 'size':
                return self._get_size(queue_name, start_time)
            elif operation == 'clear':
                return self._clear_queue(queue_name, start_time)
            elif operation == 'stats':
                return self._get_stats(queue_name, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Queue operation failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _create_queue(self, queue_name: str, start_time: float) -> ActionResult:
        """Create a new priority queue."""
        with self._lock:
            if queue_name in self._queues:
                return ActionResult(
                    success=True,
                    message=f"Queue already exists: {queue_name}",
                    data={'queue_name': queue_name, 'created': False},
                    duration=time.time() - start_time
                )

            self._queues[queue_name] = {
                'heap': [],
                'lock': threading.RLock(),
                'not_empty': threading.Condition(),
                'not_full': threading.Condition(),
                'max_size': 0,
                'created_at': time.time(),
                'sequence': 0,
                'total_enqueued': 0,
                'total_dequeued': 0
            }

            return ActionResult(
                success=True,
                message=f"Queue created: {queue_name}",
                data={'queue_name': queue_name, 'created': True},
                duration=time.time() - start_time
            )

    def _put_item(self, queue_name: str, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Enqueue an item."""
        with self._lock:
            if queue_name not in self._queues:
                self._create_queue(queue_name, start_time)

            queue = self._queues[queue_name]
            item = params.get('item')
            priority = params.get('priority', Priority.NORMAL)
            metadata = params.get('metadata', {})

            if item is None:
                return ActionResult(
                    success=False,
                    message="Item is required for put operation",
                    duration=time.time() - start_time
                )

            queue['sequence'] += 1
            seq = queue['sequence']

            queue_item = QueueItem(
                priority=priority,
                sequence=seq,
                timestamp=time.time(),
                item_id=str(uuid.uuid4()),
                data=item,
                metadata=metadata
            )

            heapq.heappush(queue['heap'], queue_item)
            queue['total_enqueued'] += 1

            with queue['not_empty']:
                queue['not_empty'].notify()

            return ActionResult(
                success=True,
                message=f"Item enqueued: {queue_item.item_id}",
                data={
                    'queue_name': queue_name,
                    'item_id': queue_item.item_id,
                    'priority': priority,
                    'queue_size': len(queue['heap'])
                },
                duration=time.time() - start_time
            )

    def _get_item(self, queue_name: str, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Dequeue an item."""
        with self._lock:
            if queue_name not in self._queues:
                return ActionResult(
                    success=False,
                    message=f"Queue not found: {queue_name}",
                    duration=time.time() - start_time
                )

            queue = self._queues[queue_name]
            timeout = params.get('timeout', 0)
            batch_size = params.get('batch_size', 1)

            if not queue['heap']:
                if timeout == 0:
                    return ActionResult(
                        success=False,
                        message=f"Queue is empty: {queue_name}",
                        data={'queue_name': queue_name, 'empty': True},
                        duration=time.time() - start_time
                    )
                else:
                    with queue['not_empty']:
                        queue['not_empty'].wait(timeout=timeout)
                        if not queue['heap']:
                            return ActionResult(
                                success=False,
                                message=f"Queue empty after timeout: {queue_name}",
                                data={'queue_name': queue_name, 'empty': True},
                                duration=time.time() - start_time
                            )

            items = []
            for _ in range(min(batch_size, len(queue['heap']))):
                if queue['heap']:
                    item = heapq.heappop(queue['heap'])
                    items.append({
                        'item_id': item.item_id,
                        'data': item.data,
                        'priority': item.priority,
                        'timestamp': item.timestamp,
                        'metadata': item.metadata
                    })
                    queue['total_dequeued'] += 1

            return ActionResult(
                success=True,
                message=f"Items dequeued: {len(items)}",
                data={
                    'queue_name': queue_name,
                    'items': items,
                    'count': len(items),
                    'remaining': len(queue['heap'])
                },
                duration=time.time() - start_time
            )

    def _peek_item(self, queue_name: str, start_time: float) -> ActionResult:
        """Peek at next item without removing it."""
        with self._lock:
            if queue_name not in self._queues:
                return ActionResult(
                    success=False,
                    message=f"Queue not found: {queue_name}",
                    duration=time.time() - start_time
                )

            queue = self._queues[queue_name]
            if not queue['heap']:
                return ActionResult(
                    success=True,
                    message=f"Queue is empty: {queue_name}",
                    data={'queue_name': queue_name, 'empty': True},
                    duration=time.time() - start_time
                )

            item = queue['heap'][0]
            return ActionResult(
                success=True,
                message=f"Next item: {item.item_id}",
                data={
                    'queue_name': queue_name,
                    'item': {
                        'item_id': item.item_id,
                        'data': item.data,
                        'priority': item.priority,
                        'timestamp': item.timestamp,
                        'metadata': item.metadata
                    }
                },
                duration=time.time() - start_time
            )

    def _get_size(self, queue_name: str, start_time: float) -> ActionResult:
        """Get queue size."""
        with self._lock:
            if queue_name not in self._queues:
                return ActionResult(
                    success=False,
                    message=f"Queue not found: {queue_name}",
                    duration=time.time() - start_time
                )

            queue = self._queues[queue_name]
            return ActionResult(
                success=True,
                message=f"Queue size: {len(queue['heap'])}",
                data={'queue_name': queue_name, 'size': len(queue['heap'])},
                duration=time.time() - start_time
            )

    def _clear_queue(self, queue_name: str, start_time: float) -> ActionResult:
        """Clear all items from queue."""
        with self._lock:
            if queue_name not in self._queues:
                return ActionResult(
                    success=False,
                    message=f"Queue not found: {queue_name}",
                    duration=time.time() - start_time
                )

            queue = self._queues[queue_name]
            cleared = len(queue['heap'])
            queue['heap'] = []

            return ActionResult(
                success=True,
                message=f"Queue cleared: {cleared} items removed",
                data={'queue_name': queue_name, 'cleared_count': cleared},
                duration=time.time() - start_time
            )

    def _get_stats(self, queue_name: str, start_time: float) -> ActionResult:
        """Get queue statistics."""
        with self._lock:
            if queue_name not in self._queues:
                return ActionResult(
                    success=False,
                    message=f"Queue not found: {queue_name}",
                    duration=time.time() - start_time
                )

            queue = self._queues[queue_name]
            priority_counts = {}
            for item in queue['heap']:
                priority_counts[str(Priority(item.priority).name)] = \
                    priority_counts.get(Priority(item.priority).name, 0) + 1

            return ActionResult(
                success=True,
                message=f"Queue stats: {queue_name}",
                data={
                    'queue_name': queue_name,
                    'size': len(queue['heap']),
                    'total_enqueued': queue['total_enqueued'],
                    'total_dequeued': queue['total_dequeued'],
                    'priority_distribution': priority_counts,
                    'created_at': queue['created_at']
                },
                duration=time.time() - start_time
            )
