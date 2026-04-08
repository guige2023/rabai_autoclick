"""Queue action module for RabAI AutoClick.

Provides message queue operations including enqueue, dequeue, peek,
priority queues, and dead letter queue handling.
"""

import json
import time
import sys
import os
import threading
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, asdict
from collections import deque
from datetime import datetime
import queue

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


# Thread-safe in-memory queue storage
_queue_storage: Dict[str, "MemoryQueue"] = {}
_storage_lock = threading.Lock()


@dataclass
class QueueMessage:
    """Represents a message in the queue.
    
    Attributes:
        id: Unique message identifier.
        body: Message content.
        priority: Message priority (higher = more urgent).
        created_at: Timestamp when message was created.
        metadata: Additional message metadata.
        retries: Number of times message has been retried.
    """
    id: str
    body: Any
    priority: int = 0
    created_at: float = 0.0
    metadata: Dict[str, Any] = None
    retries: int = 0

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
        if self.created_at == 0.0:
            self.created_at = time.time()


class MemoryQueue:
    """Thread-safe in-memory queue implementation.
    
    Supports priority queues, message persistence simulation,
    and dead letter queue for failed messages.
    """
    
    def __init__(self, name: str, max_size: int = 10000):
        """Initialize a memory queue.
        
        Args:
            name: Queue identifier.
            max_size: Maximum number of messages in queue.
        """
        self.name = name
        self.max_size = max_size
        self._queue: List[QueueMessage] = []
        self._dead_letter: List[QueueMessage] = []
        self._lock = threading.RLock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)
    
    def enqueue(self, message: QueueMessage, timeout: float = 5.0) -> bool:
        """Add a message to the queue.
        
        Args:
            message: QueueMessage to enqueue.
            timeout: Seconds to wait if queue is full.
        
        Returns:
            True if enqueued successfully, False if timeout.
        """
        with self._not_full:
            if len(self._queue) >= self.max_size:
                if timeout <= 0:
                    return False
                if not self._not_full.wait(timeout=timeout):
                    return False
            
            self._queue.append(message)
            self._queue.sort(key=lambda m: -m.priority)
            self._not_empty.notify()
            return True
    
    def dequeue(self, timeout: float = 5.0) -> Optional[QueueMessage]:
        """Remove and return the highest priority message.
        
        Args:
            timeout: Seconds to wait if queue is empty.
        
        Returns:
            QueueMessage if available, None if timeout.
        """
        with self._not_empty:
            if not self._queue:
                if timeout <= 0:
                    return None
                if not self._not_empty.wait(timeout=timeout):
                    return None
            
            message = self._queue.pop(0)
            self._not_full.notify()
            return message
    
    def peek(self) -> Optional[QueueMessage]:
        """View the highest priority message without removing it.
        
        Returns:
            QueueMessage if queue is not empty, None otherwise.
        """
        with self._lock:
            if self._queue:
                return self._queue[0]
            return None
    
    def size(self) -> int:
        """Get the number of messages in the queue."""
        with self._lock:
            return len(self._queue)
    
    def move_to_dead_letter(self, message: QueueMessage) -> None:
        """Move a failed message to the dead letter queue.
        
        Args:
            message: Message that failed processing.
        """
        with self._lock:
            self._dead_letter.append(message)
    
    def get_dead_letter_size(self) -> int:
        """Get the number of messages in dead letter queue."""
        with self._lock:
            return len(self._dead_letter)
    
    def clear_dead_letter(self) -> int:
        """Clear all messages from dead letter queue.
        
        Returns:
            Number of messages cleared.
        """
        with self._lock:
            count = len(self._dead_letter)
            self._dead_letter.clear()
            return count


class EnqueueAction(BaseAction):
    """Add a message to a named queue.
    
    Supports priority messages and blocking/non-blocking modes.
    """
    action_type = "queue_enqueue"
    display_name = "队列入队"
    description = "向队列添加消息"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Enqueue a message.
        
        Args:
            context: Execution context.
            params: Dict with keys: queue_name, message, priority, 
                   timeout, message_id.
        
        Returns:
            ActionResult with enqueue status and message ID.
        """
        queue_name = params.get('queue_name', 'default')
        message_body = params.get('message', '')
        priority = params.get('priority', 0)
        timeout = params.get('timeout', 5.0)
        message_id = params.get('message_id', f"msg_{int(time.time() * 1000)}")
        
        if not message_body and message_body != 0 and message_body != '':
            return ActionResult(success=False, message="Message body is required")
        
        with _storage_lock:
            if queue_name not in _queue_storage:
                _queue_storage[queue_name] = MemoryQueue(queue_name)
            queue = _queue_storage[queue_name]
        
        message = QueueMessage(
            id=message_id,
            body=message_body,
            priority=priority,
            created_at=time.time()
        )
        
        success = queue.enqueue(message, timeout=timeout)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Message enqueued to {queue_name}",
                data={"message_id": message_id, "queue": queue_name, "priority": priority}
            )
        else:
            return ActionResult(
                success=False,
                message=f"Queue {queue_name} is full (timeout after {timeout}s)"
            )


class DequeueAction(BaseAction):
    """Remove and return a message from a named queue."""
    action_type = "queue_dequeue"
    display_name = "队列出队"
    description = "从队列取出消息"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Dequeue a message.
        
        Args:
            context: Execution context.
            params: Dict with keys: queue_name, timeout, auto_ack.
        
        Returns:
            ActionResult with dequeued message or None.
        """
        queue_name = params.get('queue_name', 'default')
        timeout = params.get('timeout', 5.0)
        
        with _storage_lock:
            if queue_name not in _queue_storage:
                return ActionResult(success=True, message=f"Queue {queue_name} is empty", data=None)
            queue = _queue_storage[queue_name]
        
        message = queue.dequeue(timeout=timeout)
        
        if message:
            return ActionResult(
                success=True,
                message=f"Message dequeued from {queue_name}",
                data={
                    "id": message.id,
                    "body": message.body,
                    "priority": message.priority,
                    "created_at": message.created_at,
                    "retries": message.retries
                }
            )
        else:
            return ActionResult(
                success=True,
                message=f"Queue {queue_name} is empty (timeout after {timeout}s)",
                data=None
            )


class QueuePeekAction(BaseAction):
    """View the next message without removing it."""
    action_type = "queue_peek"
    display_name = "队列查看"
    description = "查看队列头部消息"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Peek at the next message.
        
        Args:
            context: Execution context.
            params: Dict with keys: queue_name.
        
        Returns:
            ActionResult with next message or None.
        """
        queue_name = params.get('queue_name', 'default')
        
        with _storage_lock:
            if queue_name not in _queue_storage:
                return ActionResult(success=True, message=f"Queue {queue_name} is empty", data=None)
            queue = _queue_storage[queue_name]
        
        message = queue.peek()
        
        if message:
            return ActionResult(
                success=True,
                message=f"Next message in {queue_name}",
                data={
                    "id": message.id,
                    "body": message.body,
                    "priority": message.priority
                }
            )
        else:
            return ActionResult(
                success=True,
                message=f"Queue {queue_name} is empty",
                data=None
            )


class QueueStatusAction(BaseAction):
    """Get the status and size of a queue."""
    action_type = "queue_status"
    display_name = "队列状态"
    description = "查看队列状态"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get queue status.
        
        Args:
            context: Execution context.
            params: Dict with keys: queue_name, include_dead_letter.
        
        Returns:
            ActionResult with queue statistics.
        """
        queue_name = params.get('queue_name', 'default')
        include_dead_letter = params.get('include_dead_letter', False)
        
        with _storage_lock:
            if queue_name not in _queue_storage:
                queue_obj = None
            else:
                queue_obj = _queue_storage[queue_name]
        
        if queue_obj is None:
            return ActionResult(
                success=True,
                message=f"Queue {queue_name} does not exist",
                data={"exists": False, "size": 0, "dead_letter_size": 0}
            )
        
        data = {
            "exists": True,
            "size": queue_obj.size(),
            "max_size": queue_obj.max_size,
            "name": queue_name
        }
        
        if include_dead_letter:
            data["dead_letter_size"] = queue_obj.get_dead_letter_size()
        
        return ActionResult(
            success=True,
            message=f"Queue {queue_name} status retrieved",
            data=data
        )
