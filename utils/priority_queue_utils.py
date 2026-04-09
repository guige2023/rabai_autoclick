"""Priority queue and scheduling utilities.

Provides priority-based task scheduling with deadline
awareness and queue management for automation workflows.
"""

import heapq
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar


T = TypeVar("T")


@dataclass(order=True)
class PriorityItem(Generic[T]):
    """Item with priority in queue.

    Lower priority number = higher priority.
    """
    priority: float
    deadline: float = field(default=0.0, compare=True)
    sequence: int = field(default=0, compare=True)
    item: T = field(default=None, compare=False)


class PriorityQueue:
    """Thread-safe priority queue with deadline support.

    Example:
        pq = PriorityQueue()
        pq.push(task, priority=1.0, deadline=time.time() + 60)
        while not pq.empty():
            task = pq.pop()
            process(task)
    """

    def __init__(self) -> None:
        self._heap: List[PriorityItem] = []
        self._counter = 0
        self._lock: Any = None

    def push(
        self,
        item: T,
        priority: float = 0.0,
        deadline: float = 0.0,
    ) -> None:
        """Add item to queue.

        Args:
            item: Item to enqueue.
            priority: Lower = higher priority.
            deadline: Unix timestamp deadline.
        """
        entry = PriorityItem(
            priority=priority,
            deadline=deadline,
            sequence=self._counter,
            item=item,
        )
        self._counter += 1
        heapq.heappush(self._heap, entry)

    def pop(self) -> Optional[T]:
        """Remove and return highest priority item.

        Returns:
            Item or None if queue empty.
        """
        if not self._heap:
            return None
        entry = heapq.heappop(self._heap)
        return entry.item

    def peek(self) -> Optional[T]:
        """Return highest priority item without removing.

        Returns:
            Item or None if queue empty.
        """
        if not self._heap:
            return None
        return self._heap[0].item

    def empty(self) -> bool:
        """Check if queue is empty."""
        return len(self._heap) == 0

    def size(self) -> int:
        """Return number of items in queue."""
        return len(self._heap)

    def clear(self) -> None:
        """Remove all items from queue."""
        self._heap.clear()
        self._counter = 0

    def remove(self, predicate: Callable[[T], bool]) -> int:
        """Remove items matching predicate.

        Args:
            predicate: Function returning True for items to remove.

        Returns:
            Number of items removed.
        """
        removed = 0
        new_heap = []
        for entry in self._heap:
            if predicate(entry.item):
                removed += 1
            else:
                new_heap.append(entry)
        self._heap = new_heap
        heapq.heapify(self._heap)
        return removed

    def get_deadline_missed(self, current_time: Optional[float] = None) -> List[T]:
        """Get items with missed deadlines.

        Args:
            current_time: Current timestamp. Uses time.time() if None.

        Returns:
            List of items with missed deadlines.
        """
        if current_time is None:
            current_time = time.time()

        missed = []
        for entry in self._heap:
            if entry.deadline > 0 and entry.deadline < current_time:
                missed.append(entry.item)
        return missed


class TaskScheduler:
    """Schedule and execute tasks with priority and deadlines.

    Example:
        scheduler = TaskScheduler()
        scheduler.schedule(daily_report, priority=1.0, delay=3600)
        scheduler.schedule(backup, priority=0.5, delay=86400)
        scheduler.run_pending()
    """

    def __init__(self) -> None:
        self._queue = PriorityQueue()
        self._scheduled: Dict[str, Callable] = {}

    def schedule(
        self,
        func: Callable[..., Any],
        name: Optional[str] = None,
        priority: float = 0.0,
        delay: float = 0.0,
        deadline: float = 0.0,
    ) -> str:
        """Schedule a function for later execution.

        Args:
            func: Function to execute.
            name: Unique identifier. Auto-generated if None.
            priority: Lower = executes first.
            delay: Seconds until execution.
            deadline: Unix timestamp deadline.

        Returns:
            Task identifier.
        """
        task_id = name or f"task_{id(func)}_{self._queue.size()}"
        deadline_time = deadline if deadline > 0 else time.time() + delay

        self._queue.push(
            (task_id, func),
            priority=priority,
            deadline=deadline_time,
        )
        self._scheduled[task_id] = func
        return task_id

    def cancel(self, task_id: str) -> bool:
        """Cancel a scheduled task.

        Args:
            task_id: Task identifier.

        Returns:
            True if task was found and removed.
        """
        if task_id in self._scheduled:
            del self._scheduled[task_id]
            removed = self._queue.remove(lambda x: x[0] == task_id)
            return removed > 0
        return False

    def run_pending(self, max_count: int = 10) -> List[Any]:
        """Execute pending tasks that are due.

        Args:
            max_count: Maximum tasks to execute per run.

        Returns:
            List of task results.
        """
        results = []
        current_time = time.time()
        count = 0

        while count < max_count and not self._queue.empty():
            item = self._queue.peek()
            if item is None:
                break

            task_id, func = item
            deadline = self._get_deadline(task_id)

            if deadline > 0 and deadline > current_time:
                break

            self._queue.pop()
            if task_id in self._scheduled:
                del self._scheduled[task_id]

            try:
                result = func()
                results.append({"task_id": task_id, "result": result, "error": None})
            except Exception as e:
                results.append({"task_id": task_id, "result": None, "error": str(e)})

            count += 1

        return results

    def _get_deadline(self, task_id: str) -> float:
        """Get deadline for task."""
        for entry in self._queue._heap:
            if isinstance(entry.item, tuple) and entry.item[0] == task_id:
                return entry.deadline
        return 0.0

    def get_next_deadline(self) -> Optional[float]:
        """Get earliest deadline among pending tasks."""
        if self._queue.empty():
            return None
        return self._queue.peek()[1] if isinstance(self._queue.peek(), tuple) else None

    def pending_count(self) -> int:
        """Number of pending tasks."""
        return self._queue.size()


class MultiQueue:
    """Multiple named priority queues.

    Example:
        mq = MultiQueue()
        mq.enqueue("high", task1, priority=1.0)
        mq.enqueue("low", task2, priority=3.0)
        mq.enqueue("high", task3, priority=2.0)
        # Pops from "high" first due to higher priority
        task = mq.dequeue()
    """

    def __init__(self) -> None:
        self._queues: Dict[str, PriorityQueue] = {}
        self._queue_priorities: Dict[str, float] = {}

    def create_queue(self, name: str, priority: float = 0.0) -> PriorityQueue:
        """Create a named queue.

        Args:
            name: Queue identifier.
            priority: Queue priority (lower = higher priority).

        Returns:
            The created queue.
        """
        q = PriorityQueue()
        self._queues[name] = q
        self._queue_priorities[name] = priority
        return q

    def enqueue(self, queue_name: str, item: T, priority: float = 0.0) -> None:
        """Enqueue item into named queue.

        Args:
            queue_name: Target queue name.
            item: Item to enqueue.
            priority: Item priority.
        """
        if queue_name not in self._queues:
            self.create_queue(queue_name)
        self._queues[queue_name].push(item, priority=priority)

    def dequeue(self) -> Optional[T]:
        """Dequeue from highest priority non-empty queue.

        Returns:
            Item or None if all queues empty.
        """
        sorted_queues = sorted(
            self._queue_priorities.items(),
            key=lambda x: x[1]
        )

        for name, _ in sorted_queues:
            if name in self._queues and not self._queues[name].empty():
                return self._queues[name].pop()

        return None

    def empty(self) -> bool:
        """Check if all queues are empty."""
        return all(q.empty() for q in self._queues.values())

    def size(self) -> int:
        """Total items across all queues."""
        return sum(q.size() for q in self._queues.values())

    def queue_size(self, name: str) -> int:
        """Size of specific queue."""
        if name in self._queues:
            return self._queues[name].size()
        return 0
