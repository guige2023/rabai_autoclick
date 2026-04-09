"""Async/await and concurrency utilities.

Provides async wrappers, Future implementations,
concurrency primitives, and parallel execution utilities.
"""

from __future__ import annotations

from typing import (
    TypeVar, Generic, Callable, Optional, Any, List, Tuple,
    Dict, Set, Awaitable, Future as StdFuture, Coroutine
)
from dataclasses import dataclass, field
from enum import Enum, auto
import threading
import asyncio
import concurrent.futures
import time
from functools import wraps


T = TypeVar('T')
T_co = TypeVar('T_co', covariant=True)


class FutureState(Enum):
    """States of a Future."""
    PENDING = auto()
    RUNNING = auto()
    DONE = auto()
    CANCELLED = auto()
    FAILED = auto()


@dataclass
class FutureResult(Generic[T]):
    """Result holder for Future operations."""
    success: bool
    value: Optional[T] = None
    error: Optional[Exception] = None


class Future(Generic[T]):
    """Simple Future implementation for async operations.

    Example:
        future = Future[str]()
        def setter():
            future.set_result("Done!")
        Thread(target=setter).start()
        result = future.get()  # Blocks until result is set
    """

    def __init__(self) -> None:
        self._state = FutureState.PENDING
        self._result: Optional[T] = None
        self._error: Optional[Exception] = None
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._callbacks: List[Callable[[Future[T]], None]] = []

    def set_result(self, value: T) -> None:
        """Set the result and notify waiters."""
        with self._lock:
            if self._state not in (FutureState.PENDING, FutureState.RUNNING):
                raise ValueError(f"Future already {self._state}")
            self._result = value
            self._state = FutureState.DONE
            self._condition.notify_all()
        self._notify_callbacks()

    def set_error(self, error: Exception) -> None:
        """Set an error and notify waiters."""
        with self._lock:
            if self._state not in (FutureState.PENDING, FutureState.RUNNING):
                raise ValueError(f"Future already {self._state}")
            self._error = error
            self._state = FutureState.FAILED
            self._condition.notify_all()
        self._notify_callbacks()

    def set_running(self) -> None:
        """Mark future as running."""
        with self._lock:
            if self._state == FutureState.PENDING:
                self._state = FutureState.RUNNING

    def cancel(self) -> bool:
        """Attempt to cancel the future."""
        with self._lock:
            if self._state in (FutureState.DONE, FutureState.FAILED, FutureState.CANCELLED):
                return False
            self._state = FutureState.CANCELLED
            self._condition.notify_all()
        self._notify_callbacks()
        return True

    def get(self, timeout: Optional[float] = None) -> T:
        """Get result, blocking until available."""
        with self._condition:
            while self._state in (FutureState.PENDING, FutureState.RUNNING):
                if not self._condition.wait(timeout):
                    raise TimeoutError("Future get timed out")
            if self._state == FutureState.CANCELLED:
                raise concurrent.futures.CancelledError()
            if self._state == FutureState.FAILED:
                raise self._error  # type: ignore
            return self._result  # type: ignore

    def result(self, timeout: Optional[float] = None) -> T:
        """Alias for get()."""
        return self.get(timeout)

    @property
    def state(self) -> FutureState:
        """Get current state."""
        return self._state

    @property
    def is_done(self) -> bool:
        """Check if future is complete."""
        return self._state in (
            FutureState.DONE, FutureState.FAILED, FutureState.CANCELLED
        )

    @property
    def is_cancelled(self) -> bool:
        """Check if future was cancelled."""
        return self._state == FutureState.CANCELLED

    def add_done_callback(
        self, callback: Callable[[Future[T]], None]
    ) -> None:
        """Add callback to be called when future completes."""
        with self._lock:
            if self._state in (FutureState.DONE, FutureState.FAILED, FutureState.CANCELLED):
                callback(self)
            else:
                self._callbacks.append(callback)

    def _notify_callbacks(self) -> None:
        """Notify all registered callbacks."""
        with self._lock:
            callbacks = self._callbacks[:]
        for callback in callbacks:
            try:
                callback(self)
            except Exception:
                pass


def make_future(
    func: Callable[..., T],
    *args: Any,
    **kwargs: Any
) -> Future[T]:
    """Execute function in thread and return Future for result.

    Example:
        future = make_future(expensive_computation, arg1, arg2)
        result = future.get()
    """
    future: Future[T] = Future[T]()

    def runner() -> None:
        try:
            future.set_running()
            result = func(*args, **kwargs)
            future.set_result(result)
        except Exception as e:
            future.set_error(e)

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    return future


@dataclass
class AsyncTask(Generic[T]):
    """Wrapper for async task management."""
    task_id: str
    future: Future[T]
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None


class TaskPool:
    """Pool for managing multiple async tasks.

    Example:
        pool = TaskPool(max_workers=4)
        task = pool.submit(lambda: compute_value(42))
        results = pool.wait_all(timeout=10.0)
    """

    def __init__(self, max_workers: Optional[int] = None) -> None:
        self._max_workers = max_workers
        self._tasks: Dict[str, AsyncTask[Any]] = {}
        self._lock = threading.RLock()
        self._counter = itertools.count()

    def submit(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any
    ) -> AsyncTask[T]:
        """Submit task to pool."""
        task_id = f"task_{next(self._counter)}"
        future = make_future(func, *args, **kwargs)
        task = AsyncTask(task_id=task_id, future=future)
        with self._lock:
            self._tasks[task_id] = task
        return task

    def get_result(
        self, task_id: str, timeout: Optional[float] = None
    ) -> Any:
        """Get result of specific task."""
        with self._lock:
            task = self._tasks.get(task_id)
        if task is None:
            raise KeyError(f"Task {task_id} not found")
        return task.future.get(timeout)

    def wait_task(
        self, task_id: str, timeout: Optional[float] = None
    ) -> bool:
        """Wait for specific task. Returns True if completed."""
        with self._lock:
            task = self._tasks.get(task_id)
        if task is None:
            return False
        try:
            task.future.get(timeout)
            return True
        except TimeoutError:
            return False

    def wait_all(
        self, timeout: Optional[float] = None
    ) -> Dict[str, Any]:
        """Wait for all tasks and return results."""
        start = time.time()
        remaining = timeout
        results: Dict[str, Any] = {}
        with self._lock:
            task_ids = list(self._tasks.keys())
        for task_id in task_ids:
            task_timeout = None
            if remaining is not None:
                elapsed = time.time() - start
                task_timeout = max(0.1, remaining - elapsed)
                if task_timeout <= 0:
                    break
            try:
                results[task_id] = self.get_result(task_id, task_timeout)
            except TimeoutError:
                break
            if remaining is not None:
                elapsed = time.time() - start
                remaining = timeout - elapsed
        return results

    def cancel_task(self, task_id: str) -> bool:
        """Cancel specific task."""
        with self._lock:
            task = self._tasks.get(task_id)
        if task is None:
            return False
        return task.future.cancel()

    def cancel_all(self) -> int:
        """Cancel all tasks. Returns count cancelled."""
        with self._lock:
            count = 0
            for task in self._tasks.values():
                if task.future.cancel():
                    count += 1
            return count

    def get_pending(self) -> List[str]:
        """Get list of pending task IDs."""
        with self._lock:
            return [
                tid for tid, task in self._tasks.items()
                if not task.future.is_done
            ]

    def get_completed(self) -> List[str]:
        """Get list of completed task IDs."""
        with self._lock:
            return [
                tid for tid, task in self._tasks.items()
                if task.future.is_done
            ]

    def remove_completed(self) -> int:
        """Remove completed tasks from pool."""
        with self._lock:
            completed = [
                tid for tid, task in self._tasks.items()
                if task.future.is_done
            ]
            for tid in completed:
                del self._tasks[tid]
            return len(completed)

    @property
    def pending_count(self) -> int:
        return len(self.get_pending())

    @property
    def total_count(self) -> int:
        with self._lock:
            return len(self._tasks)


import itertools


def async_to_sync(awaitable: Awaitable[T]) -> T:
    """Convert awaitable to synchronous result.

    Note: This creates a new event loop, suitable for use outside of async context.
    """
    try:
        loop = asyncio.get_running_loop()
        future: StdFuture[T] = asyncio.ensure_future(
            awaitable  # type: ignore
        )
        return future.result()
    except RuntimeError:
        return asyncio.run(awaitable)  # type: ignore


def run_in_executor(
    executor: Optional[concurrent.futures.Executor],
    func: Callable[..., T],
    *args: Any
) -> Future[T]:
    """Run blocking function in executor, return Future."""
    return make_future(lambda: executor.submit(func, *args).result())


class SynchronizationBarrier:
    """Barrier for synchronizing multiple threads/tasks.

    Example:
        barrier = SynchronizationBarrier(parties=3)
        for i in range(3):
            Thread(target=worker, args=(barrier,)).start()
    """

    def __init__(self, parties: int) -> None:
        if parties <= 0:
            raise ValueError("parties must be positive")
        self._parties = parties
        self._count = 0
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._generation = 0

    def wait(self, timeout: Optional[float] = None) -> bool:
        """Wait for all parties to reach barrier."""
        with self._condition:
            gen = self._generation
            self._count += 1
            if self._count == self._parties:
                self._count = 0
                self._generation += 1
                self._condition.notify_all()
                return True
            end_time = None
            if timeout is not None:
                end_time = time.time() + timeout
            while self._generation == gen:
                if timeout is not None:
                    remaining = end_time - time.time()
                    if remaining <= 0:
                        return False
                    self._condition.wait(remaining)
                else:
                    self._condition.wait()
            return True

    @property
    def parties(self) -> int:
        return self._parties

    @property
    def waiting(self) -> int:
        return self._count
