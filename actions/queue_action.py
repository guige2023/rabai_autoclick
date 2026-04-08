"""Queue action module for RabAI AutoClick.

Provides queue utilities:
- PriorityQueue: Priority-based queue
- BoundedQueue: Size-bounded queue
- PriorityQueueAction: Queue management
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
import heapq
import threading
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PriorityQueue:
    """Thread-safe priority queue."""

    def __init__(self, max_size: int = 0):
        self.max_size = max_size
        self._heap: List[Tuple[int, Any]] = []
        self._lock = threading.RLock()
        self._counter = 0

    def put(self, item: Any, priority: int = 0) -> bool:
        """Put item in queue."""
        with self._lock:
            if self.max_size > 0 and len(self._heap) >= self.max_size:
                return False

            heapq.heappush(self._heap, (priority, self._counter, item))
            self._counter += 1
            return True

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Optional[Any]:
        """Get item from queue."""
        with self._lock:
            if not self._heap:
                if block:
                    return None
                raise IndexError("Queue is empty")

            _, _, item = heapq.heappop(self._heap)
            return item

    def peek(self) -> Optional[Any]:
        """Peek at next item."""
        with self._lock:
            if not self._heap:
                return None
            return self._heap[0][2]

    def size(self) -> int:
        """Get queue size."""
        with self._lock:
            return len(self._heap)

    def is_empty(self) -> bool:
        """Check if empty."""
        with self._lock:
            return len(self._heap) == 0

    def is_full(self) -> bool:
        """Check if full."""
        with self._lock:
            if self.max_size <= 0:
                return False
            return len(self._heap) >= self.max_size

    def clear(self) -> None:
        """Clear queue."""
        with self._lock:
            self._heap.clear()

    def get_all(self) -> List[Any]:
        """Get all items sorted by priority."""
        with self._lock:
            return [item[2] for item in sorted(self._heap)]


class BoundedQueue:
    """Thread-safe bounded queue with drop policies."""

    DROP_POLICY_BLOCK = "block"
    DROP_POLICY_DROP_FIRST = "drop_first"
    DROP_POLICY_DROP_LAST = "drop_last"

    def __init__(self, max_size: int, drop_policy: str = DROP_POLICY_BLOCK):
        self.max_size = max_size
        self.drop_policy = drop_policy
        self._queue: List[Any] = []
        self._lock = threading.RLock()
        self._not_full = threading.Condition(self._lock)

    def put(self, item: Any) -> bool:
        """Put item in queue."""
        with self._not_full:
            while len(self._queue) >= self.max_size:
                if self.drop_policy == self.DROP_POLICY_BLOCK:
                    self._not_full.wait()
                elif self.drop_policy == self.DROP_POLICY_DROP_FIRST:
                    self._queue.pop(0)
                elif self.drop_policy == self.DROP_POLICY_DROP_LAST:
                    self._queue.pop()
                else:
                    return False

            self._queue.append(item)
            self._not_full.notify()
            return True

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Optional[Any]:
        """Get item from queue."""
        with self._lock:
            if not self._queue:
                if block:
                    return None
                raise IndexError("Queue is empty")

            item = self._queue.pop(0)
            self._not_full.notify()
            return item

    def size(self) -> int:
        """Get queue size."""
        with self._lock:
            return len(self._queue)

    def is_empty(self) -> bool:
        """Check if empty."""
        with self._lock:
            return len(self._queue) == 0

    def is_full(self) -> bool:
        """Check if full."""
        with self._lock:
            return len(self._queue) >= self.max_size

    def clear(self) -> None:
        """Clear queue."""
        with self._lock:
            self._queue.clear()


class QueueAction(BaseAction):
    """Queue action."""
    action_type = "queue"
    display_name = "队列管理"
    description = "优先级队列"

    def __init__(self):
        super().__init__()
        self._queues: Dict[str, Any] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")

            if operation == "create_priority":
                return self._create_priority(params)
            elif operation == "create_bounded":
                return self._create_bounded(params)
            elif operation == "put":
                return self._put(params)
            elif operation == "get":
                return self._get(params)
            elif operation == "peek":
                return self._peek(params)
            elif operation == "size":
                return self._size(params)
            elif operation == "clear":
                return self._clear(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Queue error: {str(e)}")

    def _create_priority(self, params: Dict[str, Any]) -> ActionResult:
        """Create priority queue."""
        name = params.get("name", str(uuid.uuid4()))
        max_size = params.get("max_size", 0)

        queue = PriorityQueue(max_size=max_size)
        self._queues[name] = queue

        return ActionResult(success=True, message=f"Priority queue created: {name}", data={"name": name})

    def _create_bounded(self, params: Dict[str, Any]) -> ActionResult:
        """Create bounded queue."""
        name = params.get("name", str(uuid.uuid4()))
        max_size = params.get("max_size", 100)
        drop_policy = params.get("drop_policy", "block")

        queue = BoundedQueue(max_size=max_size, drop_policy=drop_policy)
        self._queues[name] = queue

        return ActionResult(success=True, message=f"Bounded queue created: {name}", data={"name": name})

    def _put(self, params: Dict[str, Any]) -> ActionResult:
        """Put item in queue."""
        name = params.get("name")
        item = params.get("item")
        priority = params.get("priority", 0)

        if not name:
            return ActionResult(success=False, message="name is required")

        queue = self._queues.get(name)
        if not queue:
            return ActionResult(success=False, message=f"Queue not found: {name}")

        if isinstance(queue, PriorityQueue):
            success = queue.put(item, priority)
        else:
            success = queue.put(item)

        return ActionResult(success=success, message="Put" if success else "Queue full")

    def _get(self, params: Dict[str, Any]) -> ActionResult:
        """Get item from queue."""
        name = params.get("name")

        if not name:
            return ActionResult(success=False, message="name is required")

        queue = self._queues.get(name)
        if not queue:
            return ActionResult(success=False, message=f"Queue not found: {name}")

        try:
            item = queue.get(block=False)
            return ActionResult(success=True, message="Got item", data={"item": item})
        except IndexError:
            return ActionResult(success=False, message="Queue empty")

    def _peek(self, params: Dict[str, Any]) -> ActionResult:
        """Peek at next item."""
        name = params.get("name")

        if not name:
            return ActionResult(success=False, message="name is required")

        queue = self._queues.get(name)
        if not queue:
            return ActionResult(success=False, message=f"Queue not found: {name}")

        item = queue.peek()
        if item is None and not queue.is_empty():
            return ActionResult(success=False, message="Queue empty")

        return ActionResult(success=True, message="Peeked", data={"item": item})

    def _size(self, params: Dict[str, Any]) -> ActionResult:
        """Get queue size."""
        name = params.get("name")

        if not name:
            return ActionResult(success=False, message="name is required")

        queue = self._queues.get(name)
        if not queue:
            return ActionResult(success=False, message=f"Queue not found: {name}")

        size = queue.size()

        return ActionResult(success=True, message=f"Size: {size}", data={"size": size, "empty": size == 0, "full": queue.is_full()})

    def _clear(self, params: Dict[str, Any]) -> ActionResult:
        """Clear queue."""
        name = params.get("name")

        if not name:
            return ActionResult(success=False, message="name is required")

        queue = self._queues.get(name)
        if not queue:
            return ActionResult(success=False, message=f"Queue not found: {name}")

        queue.clear()

        return ActionResult(success=True, message="Queue cleared")
