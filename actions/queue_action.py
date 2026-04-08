"""Message queue action module for RabAI AutoClick.

Provides message queue operations:
- QueueEnqueueAction: Enqueue a message
- QueueDequeueAction: Dequeue a message
- QueuePeekAction: Peek at queue messages
- QueueStatsAction: Get queue statistics
"""

import threading
import uuid
import time
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from collections import deque
import queue


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class QueueMessage:
    """Represents a queue message."""
    message_id: str
    payload: Any
    priority: int = 0
    created_at: datetime = field(default_factory=datetime.utcnow)
    acknowledged: bool = False


class PriorityQueue:
    """Thread-safe priority queue."""
    def __init__(self, maxsize: int = 0):
        self._queue: List[QueueMessage] = []
        self._lock = threading.RLock()
        self._maxsize = maxsize

    def put(self, item: QueueMessage, block: bool = True, timeout: Optional[float] = None) -> bool:
        with self._lock:
            if self._maxsize > 0 and len(self._queue) >= self._maxsize:
                if not block:
                    return False
            self._queue.append(item)
            self._queue.sort(key=lambda m: m.priority, reverse=True)
            return True

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Optional[QueueMessage]:
        with self._lock:
            if not self._queue:
                return None
            return self._queue.pop(0)

    def peek(self, count: int = 1) -> List[QueueMessage]:
        with self._lock:
            return list(self._queue[:count])

    def size(self) -> int:
        with self._lock:
            return len(self._queue)

    def clear(self) -> int:
        with self._lock:
            count = len(self._queue)
            self._queue.clear()
            return count


class QueueManager:
    """Manages multiple queues."""
    def __init__(self):
        self._queues: Dict[str, PriorityQueue] = {}
        self._lock = threading.Lock()
        self._stats: Dict[str, Dict[str, int]] = {}

    def get_or_create(self, name: str, maxsize: int = 0) -> PriorityQueue:
        with self._lock:
            if name not in self._queues:
                self._queues[name] = PriorityQueue(maxsize=maxsize)
                self._stats[name] = {"enqueued": 0, "dequeued": 0, "peeked": 0}
            return self._queues[name]

    def enqueue(self, queue_name: str, payload: Any, priority: int = 0) -> str:
        q = self.get_or_create(queue_name)
        message_id = str(uuid.uuid4())
        message = QueueMessage(message_id=message_id, payload=payload, priority=priority)
        q.put(message)
        with self._lock:
            if queue_name in self._stats:
                self._stats[queue_name]["enqueued"] += 1
        return message_id

    def dequeue(self, queue_name: str) -> Optional[QueueMessage]:
        q = self.get_or_create(queue_name)
        msg = q.get()
        if msg:
            with self._lock:
                if queue_name in self._stats:
                    self._stats[queue_name]["dequeued"] += 1
        return msg

    def peek(self, queue_name: str, count: int = 1) -> List[QueueMessage]:
        q = self.get_or_create(queue_name)
        msgs = q.peek(count)
        with self._lock:
            if queue_name in self._stats:
                self._stats[queue_name]["peeked"] += len(msgs)
        return msgs

    def size(self, queue_name: str) -> int:
        q = self.get_or_create(queue_name)
        return q.size()

    def clear(self, queue_name: str) -> int:
        q = self.get_or_create(queue_name)
        return q.clear()

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "queues": {
                    name: {
                        "size": q.size(),
                        "stats": self._stats.get(name, {})
                    }
                    for name, q in self._queues.items()
                },
                "total_queues": len(self._queues)
            }


_manager = QueueManager()


class QueueEnqueueAction(BaseAction):
    """Enqueue a message."""
    action_type = "queue_enqueue"
    display_name = "入队"
    description = "将消息加入队列"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            queue_name = params.get("queue_name", "default")
            payload = params.get("payload", {})
            priority = params.get("priority", 0)

            message_id = _manager.enqueue(queue_name, payload, priority)

            return ActionResult(
                success=True,
                message=f"Enqueued message to '{queue_name}'",
                data={"message_id": message_id, "queue_name": queue_name, "priority": priority}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Queue enqueue failed: {str(e)}")


class QueueDequeueAction(BaseAction):
    """Dequeue a message."""
    action_type = "queue_dequeue"
    display_name = "出队"
    description = "从队列取出消息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            queue_name = params.get("queue_name", "default")
            block = params.get("block", False)
            timeout = params.get("timeout", 5.0)

            if block:
                start = time.time()
                while time.time() - start < timeout:
                    msg = _manager.dequeue(queue_name)
                    if msg:
                        return ActionResult(
                            success=True,
                            message=f"Dequeued message from '{queue_name}'",
                            data={"message_id": msg.message_id, "payload": msg.payload, "priority": msg.priority}
                        )
                    time.sleep(0.1)
                return ActionResult(success=False, message=f"Queue '{queue_name}' empty after {timeout}s")

            msg = _manager.dequeue(queue_name)
            if msg:
                return ActionResult(
                    success=True,
                    message=f"Dequeued message from '{queue_name}'",
                    data={"message_id": msg.message_id, "payload": msg.payload, "priority": msg.priority}
                )
            return ActionResult(success=False, message=f"Queue '{queue_name}' empty")

        except Exception as e:
            return ActionResult(success=False, message=f"Queue dequeue failed: {str(e)}")


class QueuePeekAction(BaseAction):
    """Peek at queue messages."""
    action_type = "queue_peek"
    display_name = "队列窥视"
    description = "查看队列消息不取出"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            queue_name = params.get("queue_name", "default")
            count = params.get("count", 5)

            messages = _manager.peek(queue_name, count)
            return ActionResult(
                success=True,
                message=f"Peeked {len(messages)} messages from '{queue_name}'",
                data={
                    "messages": [
                        {"message_id": m.message_id, "payload": m.payload, "priority": m.priority}
                        for m in messages
                    ],
                    "count": len(messages),
                    "queue_name": queue_name
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Queue peek failed: {str(e)}")


class QueueStatsAction(BaseAction):
    """Get queue statistics."""
    action_type = "queue_stats"
    display_name = "队列统计"
    description = "获取队列统计信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            queue_name = params.get("queue_name", None)
            stats = _manager.get_stats()

            if queue_name:
                return ActionResult(
                    success=True,
                    message=f"Stats for queue '{queue_name}'",
                    data=stats["queues"].get(queue_name, {"size": 0, "stats": {}})
                )

            return ActionResult(
                success=True,
                message=f"{stats['total_queues']} queues",
                data=stats
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Queue stats failed: {str(e)}")
