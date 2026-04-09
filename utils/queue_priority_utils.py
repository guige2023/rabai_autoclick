"""Priority queue and scheduling utilities.

Provides priority queues, task schedulers, and
work queue management for automation workflows.
"""

from __future__ import annotations

from typing import (
    TypeVar, Generic, Callable, Optional, Tuple, Any,
    List, Dict, Set, Iterator, Protocol, runtime_checkable
)
from dataclasses import dataclass, field
from enum import Enum, auto
import heapq
import threading
import time
import itertools


T = TypeVar('T')
P = TypeVar('P', int, float)


class Priority(Enum):
    """Standard priority levels (lower number = higher priority)."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


@dataclass(order=True)
class PrioritizedItem(Generic[T]):
    """Item with priority for priority queue."""
    priority: int
    sequence: int = field(compare=False)
    item: T = field(compare=False)


class PriorityQueue(Generic[T]):
    """Thread-safe priority queue with unique ordering.

    Example:
        pq = PriorityQueue[int](max_priority=0)
        pq.push(42, priority=1)
        pq.push(10, priority=0)
        val = pq.pop()  # Returns 10 (higher priority)
    """

    def __init__(
        self,
        max_priority: int = 0,
        min_priority: int = 10,
    ) -> None:
        self._max_priority = max_priority
        self._min_priority = min_priority
        self._heap: List[PrioritizedItem[T]] = []
        self._lock = threading.RLock()
        self._counter = itertools.count()
        self._size = 0

    def push(self, item: T, priority: Optional[int] = None) -> None:
        """Add item to queue with priority (default NORMAL)."""
        if priority is None:
            priority = Priority.NORMAL.value
        if priority < self._max_priority:
            priority = self._max_priority
        if priority > self._min_priority:
            priority = self._min_priority
        with self._lock:
            seq = next(self._counter)
            heapq.heappush(
                self._heap,
                PrioritizedItem(priority=priority, sequence=seq, item=item)
            )
            self._size += 1

    def pop(self) -> Optional[T]:
        """Remove and return highest priority item."""
        with self._lock:
            if not self._heap:
                return None
            self._size -= 1
            return heapq.heappop(self._heap).item

    def peek(self) -> Optional[T]:
        """Get highest priority item without removing it."""
        with self._lock:
            if not self._heap:
                return None
            return self._heap[0].item

    def pop_priority(self) -> Tuple[Optional[T], Optional[int]]:
        """Remove and return (item, priority)."""
        with self._lock:
            if not self._heap:
                return None, None
            self._size -= 1
            entry = heapq.heappop(self._heap)
            return entry.item, entry.priority

    def remove(self, predicate: Callable[[T], bool]) -> List[T]:
        """Remove all items matching predicate. Returns removed items."""
        with self._lock:
            removed = []
            new_heap = []
            for item in self._heap:
                if predicate(item.item):
                    removed.append(item.item)
                    self._size -= 1
                else:
                    new_heap.append(item)
            self._heap = new_heap
            heapq.heapify(self._heap)
            return removed

    def clear(self) -> None:
        """Clear all items from queue."""
        with self._lock:
            self._heap.clear()
            self._size = 0

    def to_list(self) -> List[T]:
        """Get all items as list (not ordered)."""
        with self._lock:
            return [item.item for item in self._heap]

    @property
    def size(self) -> int:
        """Get number of items in queue."""
        with self._lock:
            return self._size

    @property
    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return self.size == 0

    def __len__(self) -> int:
        return self.size


@dataclass
class ScheduledTask(Generic[T]):
    """Task scheduled for future execution."""
    run_at: float  # Unix timestamp
    task_id: str
    func: Callable[..., T]
    args: Tuple[Any, ...] = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    priority: int = Priority.NORMAL.value


class TaskScheduler:
    """Scheduler for time-delayed and periodic tasks.

    Example:
        scheduler = TaskScheduler()
        scheduler.schedule(lambda: print("Later!"), delay_seconds=5.0)
        scheduler.schedule_periodic(lambda: print("Repeating!"), interval_seconds=10.0)
        scheduler.run()
    """

    def __init__(self) -> None:
        self._tasks: List[ScheduledTask[Any]] = []
        self._lock = threading.RLock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._task_counter = itertools.count()

    def schedule(
        self,
        func: Callable[..., T],
        delay_seconds: float = 0.0,
        at_time: Optional[float] = None,
        priority: int = Priority.NORMAL.value,
        *args: Any,
        **kwargs: Any
    ) -> str:
        """Schedule task to run after delay.

        Returns:
            Task ID for cancellation.
        """
        if delay_seconds < 0:
            raise ValueError("delay_seconds must be non-negative")
        run_at = (at_time if at_time is not None
                  else time.time() + delay_seconds)
        task_id = f"task_{next(self._task_counter)}"
        task = ScheduledTask(
            run_at=run_at,
            task_id=task_id,
            func=func,
            args=args,
            kwargs=kwargs,
            priority=priority,
        )
        with self._lock:
            self._tasks.append(task)
            heapq.heapify(self._tasks)
        return task_id

    def schedule_periodic(
        self,
        func: Callable[..., Any],
        interval_seconds: float,
        initial_delay: float = 0.0,
        priority: int = Priority.NORMAL.value,
        *args: Any,
        **kwargs: Any
    ) -> str:
        """Schedule task to run periodically.

        Returns:
            Task ID for cancellation.
        """
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        task_id = f"periodic_{next(self._task_counter)}"

        def wrapper() -> None:
            func(*args, **kwargs)
            with self._lock:
                if task_id in [t.task_id for t in self._tasks]:
                    next_run = time.time() + interval_seconds
                    new_task = ScheduledTask(
                        run_at=next_run,
                        task_id=task_id,
                        func=wrapper,
                        priority=priority,
                    )
                    self._tasks.append(new_task)
                    heapq.heapify(self._tasks)

        run_at = time.time() + initial_delay
        task = ScheduledTask(
            run_at=run_at,
            task_id=task_id,
            func=wrapper,
            priority=priority,
        )
        with self._lock:
            self._tasks.append(task)
            heapq.heapify(self._tasks)
        return task_id

    def cancel(self, task_id: str) -> bool:
        """Cancel scheduled task. Returns True if task was found."""
        with self._lock:
            for i, task in enumerate(self._tasks):
                if task.task_id == task_id:
                    self._tasks.pop(i)
                    heapq.heapify(self._tasks)
                    return True
            return False

    def get_pending(self) -> List[str]:
        """Get list of pending task IDs."""
        with self._lock:
            return [t.task_id for t in self._tasks]

    def clear(self) -> None:
        """Clear all pending tasks."""
        with self._lock:
            self._tasks.clear()

    def start(self) -> None:
        """Start scheduler in background thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop scheduler."""
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _run(self) -> None:
        """Scheduler main loop."""
        while self._running:
            self._dispatch_ready()
            with self._lock:
                if not self._tasks:
                    time.sleep(0.1)
                    continue
                next_task = min(self._tasks, key=lambda t: t.run_at)
                wait_time = max(0, next_task.run_at - time.time())
            if wait_time > 0:
                time.sleep(min(wait_time, 0.1))

    def _dispatch_ready(self) -> None:
        """Dispatch all tasks that are ready to run."""
        now = time.time()
        with self._lock:
            ready = [t for t in self._tasks if t.run_at <= now]
            for task in ready:
                self._tasks.remove(task)
                heapq.heapify(self._tasks)
        for task in ready:
            try:
                threading.Thread(
                    target=task.func,
                    args=task.args,
                    kwargs=task.kwargs,
                    daemon=True
                ).start()
            except Exception:
                pass


@runtime_checkable
class WorkItem(Protocol[T]):
    """Protocol for work queue items."""
    @property
    def work_id(self) -> str: ...
    def execute(self) -> T: ...


@dataclass
class WorkTask(Generic[T]):
    """Generic work task wrapper."""
    work_id: str
    func: Callable[..., T]
    args: Tuple[Any, ...] = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)

    def execute(self) -> T:
        """Execute the work task."""
        return self.func(*self.args, **self.kwargs)


class WorkQueue(Generic[T]):
    """Thread-safe work queue with priorities.

    Example:
        queue = WorkQueue[Dict[str, Any]](max_size=100)
        queue.submit("task1", lambda: {"result": 42})
        result = queue.get().execute()
    """

    def __init__(self, max_size: int = 0) -> None:
        if max_size < 0:
            raise ValueError("max_size must be non-negative")
        self._max_size = max_size
        self._queue: List[WorkTask[T]] = []
        self._lock = threading.RLock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)
        self._size = 0

    def submit(
        self,
        work_id: str,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any
    ) -> bool:
        """Submit work to queue. Returns True if accepted."""
        with self._not_full:
            if self._max_size > 0 and self._size >= self._max_size:
                return False
            task = WorkTask(
                work_id=work_id,
                func=func,
                args=args,
                kwargs=kwargs,
            )
            self._queue.append(task)
            self._size += 1
            self._not_empty.notify()
        return True

    def get(self, timeout: Optional[float] = None) -> Optional[WorkTask[T]]:
        """Get next work item (blocking)."""
        with self._not_empty:
            if timeout is None:
                while not self._queue:
                    self._not_empty.wait()
            else:
                if not self._queue:
                    if not self._not_empty.wait(timeout):
                        return None
            self._size -= 1
            task = self._queue.pop(0)
            self._not_full.notify()
            return task

    def get_nowait(self) -> Optional[WorkTask[T]]:
        """Get next work item (non-blocking). Returns None if empty."""
        with self._not_empty:
            if not self._queue:
                return None
            self._size -= 1
            task = self._queue.pop(0)
            self._not_full.notify()
            return task

    def cancel(self, work_id: str) -> bool:
        """Cancel work by ID. Returns True if found."""
        with self._lock:
            for i, task in enumerate(self._queue):
                if task.work_id == work_id:
                    self._queue.pop(i)
                    self._size -= 1
                    self._not_full.notify()
                    return True
            return False

    def clear(self) -> None:
        """Clear all pending work."""
        with self._lock:
            self._queue.clear()
            self._size = 0

    @property
    def size(self) -> int:
        return self._size

    @property
    def is_empty(self) -> bool:
        return self.size == 0

    @property
    def is_full(self) -> bool:
        return self._max_size > 0 and self._size >= self._max_size
