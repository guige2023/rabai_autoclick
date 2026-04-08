"""Worker pool and task distribution utilities.

Provides worker pool for distributing tasks across
multiple threads in automation workflows.
"""

import queue
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Generic, List, Optional, TypeVar


T = TypeVar("T")


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class Task(Generic[T]):
    """Represents a task in the pool."""
    id: str
    func: Callable[..., T]
    args: tuple = ()
    kwargs: dict = None
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Optional[Exception] = None

    def __post_init__(self):
        if self.kwargs is None:
            self.kwargs = {}


class WorkerPool:
    """Thread pool for executing tasks.

    Example:
        pool = WorkerPool(num_workers=4)
        task = pool.submit(do_work, arg1, arg2)
        result = task.result()
        pool.shutdown()
    """

    def __init__(self, num_workers: int = 4) -> None:
        self._num_workers = num_workers
        self._tasks: queue.Queue[Task] = queue.Queue()
        self._results: dict = {}
        self._workers: List[threading.Thread] = []
        self._running = False
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start worker threads."""
        if self._running:
            return
        self._running = True
        for i in range(self._num_workers):
            t = threading.Thread(target=self._worker_loop, daemon=True)
            t.start()
            self._workers.append(t)

    def _worker_loop(self) -> None:
        """Worker thread main loop."""
        while self._running:
            try:
                task = self._tasks.get(timeout=0.1)
            except queue.Empty:
                continue

            task.status = TaskStatus.RUNNING
            try:
                task.result = task.func(*task.args, **task.kwargs)
                task.status = TaskStatus.COMPLETED
            except Exception as e:
                task.error = e
                task.status = TaskStatus.FAILED
            finally:
                with self._lock:
                    self._results[task.id] = task

    def submit(
        self,
        func: Callable[..., T],
        *args: Any,
        task_id: Optional[str] = None,
        **kwargs: Any,
    ) -> Task[T]:
        """Submit task to pool.

        Args:
            func: Function to execute.
            *args: Positional arguments.
            task_id: Optional task identifier.
            **kwargs: Keyword arguments.

        Returns:
            Task object.
        """
        if task_id is None:
            task_id = f"task_{int(time.time() * 1000000)}"

        task = Task(
            id=task_id,
            func=func,
            args=args,
            kwargs=kwargs,
        )

        self._tasks.put(task)
        return task

    def get_result(self, task_id: str, timeout: float = None) -> Any:
        """Get result of completed task.

        Args:
            task_id: Task identifier.
            timeout: Max wait time.

        Returns:
            Task result.

        Raises:
            KeyError: If task not found.
            TimeoutError: If timeout expires.
        """
        start = time.time()
        while True:
            with self._lock:
                if task_id in self._results:
                    task = self._results[task_id]
                    if task.status == TaskStatus.FAILED:
                        raise task.error
                    return task.result

            if timeout and (time.time() - start) >= timeout:
                raise TimeoutError()

            time.sleep(0.01)

    def wait_task(self, task_id: str, timeout: float = None) -> TaskStatus:
        """Wait for task to complete.

        Args:
            task_id: Task identifier.
            timeout: Max wait time.

        Returns:
            Final task status.
        """
        start = time.time()
        while True:
            with self._lock:
                if task_id in self._results:
                    return self._results[task_id].status

            if timeout and (time.time() - start) >= timeout:
                return TaskStatus.CANCELLED

            time.sleep(0.01)

    def cancel(self, task_id: str) -> bool:
        """Attempt to cancel task.

        Args:
            task_id: Task identifier.

        Returns:
            True if cancelled.
        """
        with self._lock:
            if task_id in self._results:
                task = self._results[task_id]
                if task.status == TaskStatus.PENDING:
                    task.status = TaskStatus.CANCELLED
                    return True
        return False

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the pool.

        Args:
            wait: Wait for pending tasks.
        """
        self._running = False
        if wait:
            for t in self._workers:
                t.join(timeout=1.0)

    def __enter__(self) -> "WorkerPool":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.shutdown()


class TaskScheduler:
    """Scheduler for priority-based task execution.

    Example:
        scheduler = TaskScheduler()
        scheduler.schedule(priority=1, func=task1)
        scheduler.schedule(priority=0, func=task2)  # runs first
        scheduler.run()
    """

    def __init__(self, num_workers: int = 2) -> None:
        self._pool = WorkerPool(num_workers)
        self._pending: queue.PriorityQueue = queue.PriorityQueue()

    def schedule(
        self,
        func: Callable[..., T],
        *args: Any,
        priority: int = 5,
        **kwargs: Any,
    ) -> str:
        """Schedule a task.

        Args:
            func: Function to execute.
            *args: Positional arguments.
            priority: Lower = higher priority.
            **kwargs: Keyword arguments.

        Returns:
            Task ID.
        """
        task_id = f"sched_{int(time.time() * 1000000)}"
        self._pending.put((priority, task_id, func, args, kwargs))
        return task_id

    def run(self) -> None:
        """Run all scheduled tasks."""
        self._pool.start()
        while not self._pending.empty():
            try:
                priority, task_id, func, args, kwargs = self._pending.get_nowait()
                self._pool.submit(func, *args, task_id=task_id, **kwargs)
            except queue.Empty:
                break


class RoundRobinDispatcher:
    """Round-robin task dispatcher.

    Example:
        dispatcher = RoundRobinDispatcher(workers=["w1", "w2", "w3"])
        for task in tasks:
            worker = dispatcher.dispatch()
            submit_to_worker(worker, task)
    """

    def __init__(self, workers: List[Any]) -> None:
        self._workers = workers
        self._index = 0
        self._lock = threading.Lock()

    def dispatch(self) -> Any:
        """Get next worker in round-robin.

        Returns:
            Worker object.
        """
        with self._lock:
            worker = self._workers[self._index]
            self._index = (self._index + 1) % len(self._workers)
            return worker

    def add_worker(self, worker: Any) -> None:
        """Add new worker."""
        with self._lock:
            self._workers.append(worker)

    def remove_worker(self, worker: Any) -> bool:
        """Remove worker."""
        with self._lock:
            try:
                self._workers.remove(worker)
                if self._index >= len(self._workers):
                    self._index = 0
                return True
            except ValueError:
                return False
