"""Queue action module for RabAI AutoClick.

Provides queue-based processing actions including FIFO, LIFO,
priority queues, and dead-letter queue handling.
"""

import sys
import os
import time
import threading
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
from collections import deque, PriorityQueue
import queue

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class QueueType(Enum):
    """Queue type enumeration."""
    FIFO = "fifo"
    LIFO = "lifo"
    PRIORITY = "priority"
    DEAD_LETTER = "dead_letter"


@dataclass(order=True)
class PriorityItem:
    """Priority queue item with ordering support."""
    priority: int
    item: Any = field(compare=False)
    timestamp: float = field(default_factory=time.time, compare=False)
    retry_count: int = field(default=0, compare=False)


class QueueManager:
    """Thread-safe queue manager for managing multiple queues."""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._queues = {}
                    cls._instance._dlq = deque(maxlen=1000)
                    cls._instance._stats = {}
        return cls._instance
    
    def create_queue(self, name: str, qtype: QueueType = QueueType.FIFO,
                     maxsize: int = 0) -> bool:
        """Create a named queue.
        
        Args:
            name: Queue identifier.
            qtype: Type of queue.
            maxsize: Maximum queue size (0 = unlimited).
        
        Returns:
            True if created, False if already exists.
        """
        if name in self._queues:
            return False
        
        if qtype == QueueType.PRIORITY:
            self._queues[name] = PriorityQueue(maxsize=maxsize)
        elif qtype == QueueType.DEAD_LETTER:
            self._queues[name] = deque(maxlen=maxsize if maxsize > 0 else 1000)
        else:
            self._queues[name] = queue.Queue(maxsize=maxsize)
        
        self._queues[name]._type = qtype
        self._stats[name] = {"enqueued": 0, "dequeued": 0, "failed": 0}
        return True
    
    def enqueue(self, name: str, item: Any, priority: int = 0) -> bool:
        """Add item to queue.
        
        Args:
            name: Queue name.
            item: Item to enqueue.
            priority: Priority value (lower = higher priority).
        
        Returns:
            True if successful.
        """
        if name not in self._queues:
            self.create_queue(name)
        
        q = self._queues[name]
        qtype = getattr(q, '_type', QueueType.FIFO)
        
        try:
            if qtype == QueueType.PRIORITY:
                q.put(PriorityItem(priority=priority, item=item))
            else:
                q.put(item)
            self._stats[name]["enqueued"] += 1
            return True
        except queue.Full:
            self._stats[name]["failed"] += 1
            return False
    
    def dequeue(self, name: str, timeout: float = 0.0) -> Tuple[bool, Any]:
        """Remove and return item from queue.
        
        Args:
            name: Queue name.
            timeout: Wait timeout in seconds (0 = non-blocking).
        
        Returns:
            Tuple of (success, item).
        """
        if name not in self._queues:
            return False, None
        
        q = self._queues[name]
        try:
            if timeout > 0:
                item = q.get(timeout=timeout)
            else:
                item = q.get_nowait()
            self._stats[name]["dequeued"] += 1
            return True, item
        except queue.Empty:
            return False, None
    
    def move_to_dlq(self, name: str, item: Any, reason: str = "") -> bool:
        """Move failed item to dead-letter queue.
        
        Args:
            name: Source queue name.
            item: Item to move.
            reason: Failure reason.
        
        Returns:
            True if successful.
        """
        dlq_item = {"original_queue": name, "item": item, "reason": reason,
                    "timestamp": time.time()}
        self._dlq.append(dlq_item)
        self._stats[name]["failed"] += 1
        return True
    
    def get_stats(self, name: str) -> Dict[str, Any]:
        """Get queue statistics."""
        if name not in self._queues:
            return {}
        return self._stats.get(name, {}).copy()
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all queues."""
        return {name: self._stats.get(name, {}) for name in self._queues}
    
    def clear_queue(self, name: str) -> int:
        """Clear all items from queue.
        
        Returns:
            Number of items cleared.
        """
        if name not in self._queues:
            return 0
        q = self._queues[name]
        count = 0
        while True:
            try:
                if hasattr(q, 'get_nowait'):
                    q.get_nowait()
                elif isinstance(q, deque):
                    q.pop()
                count += 1
            except (queue.Empty, IndexError):
                break
        return count
    
    def list_queues(self) -> List[str]:
        """List all queue names."""
        return list(self._queues.keys())
    
    def queue_size(self, name: str) -> int:
        """Get current queue size."""
        if name not in self._queues:
            return 0
        q = self._queues[name]
        if hasattr(q, 'qsize'):
            return q.qsize()
        elif isinstance(q, deque):
            return len(q)
        return 0


class EnqueueAction(BaseAction):
    """Add item to queue.
    
    Supports FIFO, LIFO, and priority queues.
    """
    action_type = "enqueue"
    display_name = "入队"
    description = "将数据添加到队列"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute enqueue operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: queue_name, item, priority, create_if_missing.
        
        Returns:
            ActionResult with enqueue status.
        """
        queue_name = params.get('queue_name', 'default')
        item = params.get('item')
        priority = params.get('priority', 0)
        create_if_missing = params.get('create_if_missing', True)
        qtype_str = params.get('queue_type', 'fifo')
        
        try:
            qtype = QueueType(qtype_str)
        except ValueError:
            qtype = QueueType.FIFO
        
        manager = QueueManager()
        
        if queue_name not in manager.list_queues():
            if create_if_missing:
                manager.create_queue(queue_name, qtype)
            else:
                return ActionResult(
                    success=False,
                    message=f"Queue '{queue_name}' does not exist"
                )
        
        success = manager.enqueue(queue_name, item, priority)
        
        if success:
            stats = manager.get_stats(queue_name)
            return ActionResult(
                success=True,
                message=f"Enqueued to '{queue_name}'",
                data={"queue": queue_name, "size": manager.queue_size(queue_name), "stats": stats}
            )
        else:
            return ActionResult(
                success=False,
                message=f"Queue '{queue_name}' is full"
            )


class DequeueAction(BaseAction):
    """Remove and return item from queue."""
    action_type = "dequeue"
    display_name = "出队"
    description = "从队列取出数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute dequeue operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: queue_name, timeout, save_to_var.
        
        Returns:
            ActionResult with dequeued item.
        """
        queue_name = params.get('queue_name', 'default')
        timeout = params.get('timeout', 0.0)
        save_to_var = params.get('save_to_var', 'last_dequeued')
        
        manager = QueueManager()
        
        if queue_name not in manager.list_queues():
            return ActionResult(
                success=False,
                message=f"Queue '{queue_name}' does not exist"
            )
        
        success, item = manager.dequeue(queue_name, timeout)
        
        if success:
            if context and hasattr(context, 'set_variable'):
                context.set_variable(save_to_var, item)
            
            return ActionResult(
                success=True,
                message=f"Dequeued from '{queue_name}'",
                data={"queue": queue_name, "item": item, "remaining": manager.queue_size(queue_name)}
            )
        else:
            return ActionResult(
                success=False,
                message=f"Queue '{queue_name}' is empty" if timeout == 0 else f"Dequeue timeout after {timeout}s"
            )


class QueueStatsAction(BaseAction):
    """Get queue statistics."""
    action_type = "queue_stats"
    display_name = "队列统计"
    description = "获取队列统计信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Get queue statistics.
        
        Args:
            context: Execution context.
            params: Dict with keys: queue_name (optional, omit for all).
        
        Returns:
            ActionResult with statistics.
        """
        queue_name = params.get('queue_name')
        manager = QueueManager()
        
        if queue_name:
            stats = manager.get_stats(queue_name)
            stats['size'] = manager.queue_size(queue_name)
            return ActionResult(
                success=True,
                message=f"Stats for '{queue_name}'",
                data=stats
            )
        else:
            all_stats = manager.get_all_stats()
            for name in all_stats:
                all_stats[name]['size'] = manager.queue_size(name)
            return ActionResult(
                success=True,
                message="All queue statistics",
                data=all_stats
            )


class DeadLetterQueueAction(BaseAction):
    """Handle dead-letter queue operations."""
    action_type = "dead_letter_queue"
    display_name = "死信队列"
    description = "死信队列操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute DLQ operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: action (peek/retry/clear), item_index.
        
        Returns:
            ActionResult with DLQ contents or operation result.
        """
        action = params.get('action', 'peek')
        item_index = params.get('item_index', -1)
        
        manager = QueueManager()
        
        if action == 'peek':
            if not manager._dlq:
                return ActionResult(success=True, message="DLQ is empty", data=[])
            
            if item_index == -1:
                items = list(manager._dlq)
            else:
                try:
                    items = [manager._dlq[item_index]]
                except IndexError:
                    return ActionResult(success=False, message=f"Invalid index {item_index}")
            
            return ActionResult(
                success=True,
                message=f"DLQ contains {len(items)} items",
                data=items
            )
        
        elif action == 'retry':
            if not manager._dlq:
                return ActionResult(success=True, message="DLQ is empty")
            
            try:
                dlq_item = manager._dlq.pop(item_index)
                queue_name = dlq_item['original_queue']
                item = dlq_item['item']
                manager.enqueue(queue_name, item)
                return ActionResult(
                    success=True,
                    message=f"Retried item to '{queue_name}'",
                    data=dlq_item
                )
            except (IndexError, Exception) as e:
                return ActionResult(success=False, message=f"Retry failed: {str(e)}")
        
        elif action == 'clear':
            count = len(manager._dlq)
            manager._dlq.clear()
            return ActionResult(
                success=True,
                message=f"Cleared {count} items from DLQ"
            )
        
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )
