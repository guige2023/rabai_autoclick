"""Work Queue Action Module.

Provides work queue for distributing
tasks to workers.
"""

import time
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WorkItemPriority(Enum):
    """Work item priority."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class WorkItem:
    """Work item in queue."""
    item_id: str
    data: Any
    priority: WorkItemPriority = WorkItemPriority.NORMAL
    created_at: float = field(default_factory=time.time)
    worker_id: Optional[str] = None


class WorkQueue:
    """Work queue implementation."""
    def __init__(self, queue_id: str):
        self.queue_id = queue_id
        self._items: List[WorkItem] = []
        self._lock = threading.Lock()

    def enqueue(
        self,
        data: Any,
        priority: WorkItemPriority = WorkItemPriority.NORMAL
    ) -> str:
        """Enqueue work item."""
        item_id = f"work_{int(time.time() * 1000)}"

        item = WorkItem(
            item_id=item_id,
            data=data,
            priority=priority
        )

        with self._lock:
            self._items.append(item)
            self._items.sort(key=lambda x: x.priority.value, reverse=True)

        return item_id

    def dequeue(self) -> Optional[WorkItem]:
        """Dequeue work item."""
        with self._lock:
            if self._items:
                return self._items.pop(0)
        return None

    def dequeue_for_worker(self, worker_id: str) -> Optional[WorkItem]:
        """Dequeue for specific worker."""
        with self._lock:
            for i, item in enumerate(self._items):
                if item.worker_id is None or item.worker_id == worker_id:
                    item.worker_id = worker_id
                    return self._items.pop(i)
        return None

    def size(self) -> int:
        """Get queue size."""
        with self._lock:
            return len(self._items)

    def mark_complete(self, item_id: str) -> bool:
        """Mark item as complete."""
        with self._lock:
            for item in self._items:
                if item.item_id == item_id:
                    self._items.remove(item)
                    return True
        return False


class WorkQueueManager:
    """Manages work queues."""

    def __init__(self):
        self._queues: Dict[str, WorkQueue] = {}

    def create_queue(self, queue_id: str) -> str:
        """Create work queue."""
        self._queues[queue_id] = WorkQueue(queue_id)
        return queue_id

    def get_queue(self, queue_id: str) -> Optional[WorkQueue]:
        """Get queue."""
        return self._queues.get(queue_id)


class WorkQueueAction(BaseAction):
    """Action for work queue operations."""

    def __init__(self):
        super().__init__("work_queue")
        self._manager = WorkQueueManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute work queue action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "enqueue":
                return self._enqueue(params)
            elif operation == "dequeue":
                return self._dequeue(params)
            elif operation == "size":
                return self._size(params)
            elif operation == "complete":
                return self._complete(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create queue."""
        queue_id = self._manager.create_queue(params.get("queue_id", ""))
        return ActionResult(success=True, data={"queue_id": queue_id})

    def _enqueue(self, params: Dict) -> ActionResult:
        """Enqueue item."""
        queue = self._manager.get_queue(params.get("queue_id", ""))
        if not queue:
            return ActionResult(success=False, message="Queue not found")

        item_id = queue.enqueue(
            params.get("data"),
            WorkItemPriority(params.get("priority", 1))
        )
        return ActionResult(success=True, data={"item_id": item_id})

    def _dequeue(self, params: Dict) -> ActionResult:
        """Dequeue item."""
        queue = self._manager.get_queue(params.get("queue_id", ""))
        if not queue:
            return ActionResult(success=False, message="Queue not found")

        item = queue.dequeue_for_worker(params.get("worker_id", ""))
        if not item:
            return ActionResult(success=False, message="Queue empty")

        return ActionResult(success=True, data={
            "item_id": item.item_id,
            "data": item.data
        })

    def _size(self, params: Dict) -> ActionResult:
        """Get size."""
        queue = self._manager.get_queue(params.get("queue_id", ""))
        if not queue:
            return ActionResult(success=False, message="Queue not found")

        return ActionResult(success=True, data={"size": queue.size()})

    def _complete(self, params: Dict) -> ActionResult:
        """Mark complete."""
        queue = self._manager.get_queue(params.get("queue_id", ""))
        if not queue:
            return ActionResult(success=False, message="Queue not found")

        success = queue.mark_complete(params.get("item_id", ""))
        return ActionResult(success=success)
