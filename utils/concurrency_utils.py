"""Concurrency utilities for RabAI AutoClick.

Provides:
- Worker pools and thread executors
- Producer-consumer patterns
- Read-write locks
- Thread-safe data structures
"""

import asyncio
import atexit
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Future
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Deque,
    Generic,
    Iterator,
    List,
    Optional,
    TypeVar,
    Union,
)
import queue
import multiprocessing as mp

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


class ThreadPool:
    """Thread pool with context manager support."""

    def __init__(
        self,
        max_workers: Optional[int] = None,
        thread_name_prefix: str = "rabai-worker",
    ) -> None:
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=thread_name_prefix,
        )
        self._shutdown = False

    def submit(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> Future[T]:
        """Submit a callable to be executed.

        Args:
            fn: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            A Future representing the execution.
        """
        return self._executor.submit(fn, *args, **kwargs)

    def map(self, fn: Callable[..., T], *iterables: Any, timeout: Optional[float] = None) -> Iterator[T]:
        """Map a function over iterables.

        Args:
            fn: Function to apply.
            *iterables: Iterables to map over.
            timeout: Optional timeout.

        Returns:
            Iterator of results.
        """
        return self._executor.map(fn, *iterables, timeout=timeout)

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the thread pool.

        Args:
            wait: If True, wait for all tasks to complete.
        """
        if not self._shutdown:
            self._executor.shutdown(wait=wait)
            self._shutdown = True

    def __enter__(self) -> "ThreadPool":
        return self

    def __exit__(self, *args: Any) -> None:
        self.shutdown(wait=True)


class ProcessPool:
    """Process pool for CPU-bound work."""

    def __init__(
        self,
        max_workers: Optional[int] = None,
    ) -> None:
        self._executor = ProcessPoolExecutor(max_workers=max_workers)
        self._shutdown = False

    def submit(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> Future[T]:
        """Submit a callable to be executed in a process."""
        return self._executor.submit(fn, *args, **kwargs)

    def map(self, fn: Callable[..., T], *iterables: Any, timeout: Optional[float] = None) -> Iterator[T]:
        """Map a function over iterables using processes."""
        return self._executor.map(fn, *iterables, timeout=timeout)

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the process pool."""
        if not self._shutdown:
            self._executor.shutdown(wait=wait)
            self._shutdown = True

    def __enter__(self) -> "ProcessPool":
        return self

    def __exit__(self, *args: Any) -> None:
        self.shutdown(wait=True)


class ReadWriteLock:
    """Read-write lock implementation.

    Allows multiple readers or a single writer.
    Writers have priority to prevent starvation.
    """

    def __init__(self) -> None:
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0
        self._writers_waiting = 0
        self._writer_active = False

    @contextmanager
    def read_lock(self) -> Iterator[None]:
        """Acquire a read lock."""
        with self._read_ready:
            while self._writer_active or self._writers_waiting > 0:
                self._read_ready.wait()
            self._readers += 1
        try:
            yield
        finally:
            with self._read_ready:
                self._readers -= 1
                if self._readers == 0:
                    self._read_ready.notify_all()

    @contextmanager
    def write_lock(self) -> Iterator[None]:
        """Acquire a write lock."""
        with self._read_ready:
            self._writers_waiting += 1
            while self._readers > 0 or self._writer_active:
                self._read_ready.wait()
            self._writers_waiting -= 1
            self._writer_active = True
        try:
            yield
        finally:
            with self._read_ready:
                self._writer_active = False
                self._read_ready.notify_all()


class ProducerConsumer(Generic[T]):
    """Producer-consumer pattern with bounded queue.

    Args:
        maxsize: Maximum queue size (0 for unlimited).
        num_consumers: Number of consumer threads.
    """

    def __init__(
        self,
        maxsize: int = 0,
        num_consumers: int = 1,
    ) -> None:
        self._queue: queue.Queue[Optional[T]] = queue.Queue(maxsize=maxsize)
        self._num_consumers = num_consumers
        self._consumers: List[threading.Thread] = []
        self._running = False
        self._sentinel = None  # type: ignore

    def start(
        self,
        consumer_fn: Callable[[T], None],
    ) -> None:
        """Start consumer threads.

        Args:
            consumer_fn: Function to call for each item.
        """
        self._running = True
        for i in range(self._num_consumers):
            t = threading.Thread(
                target=self._consume,
                args=(consumer_fn,),
                name=f"consumer-{i}",
                daemon=True,
            )
            t.start()
            self._consumers.append(t)

    def _consume(self, consumer_fn: Callable[[T], None]) -> None:
        """Consumer worker loop."""
        while self._running:
            try:
                item = self._queue.get(timeout=0.5)
                if item is None:
                    break
                consumer_fn(item)
                self._queue.task_done()
            except queue.Empty:
                continue

    def put(self, item: T, block: bool = True, timeout: Optional[float] = None) -> None:
        """Put an item into the queue.

        Args:
            item: Item to produce.
            block: Whether to block if queue is full.
            timeout: Timeout in seconds.
        """
        self._queue.put(item, block=block, timeout=timeout)

    def stop(self, timeout: Optional[float] = None) -> None:
        """Stop consumers and wait for queue to drain.

        Args:
            timeout: Timeout for joining threads.
        """
        self._running = False
        for _ in range(self._num_consumers):
            self._queue.put(self._sentinel)
        for t in self._consumers:
            t.join(timeout=timeout)
        self._queue.join()


class AtomicCounter:
    """Thread-safe atomic counter."""

    def __init__(self, initial: int = 0) -> None:
        self._value = initial
        self._lock = threading.Lock()

    def increment(self, delta: int = 1) -> int:
        """Increment the counter.

        Args:
            delta: Amount to increment by.

        Returns:
            New counter value.
        """
        with self._lock:
            self._value += delta
            return self._value

    def decrement(self, delta: int = 1) -> int:
        """Decrement the counter.

        Args:
            delta: Amount to decrement by.

        Returns:
            New counter value.
        """
        with self._lock:
            self._value -= delta
            return self._value

    def get(self) -> int:
        """Get current value."""
        with self._lock:
            return self._value

    def set(self, value: int) -> None:
        """Set the counter value."""
        with self._lock:
            self._value = value


class ThreadSafeDeque(Generic[T]):
    """Thread-safe deque wrapper."""

    def __init__(self, maxlen: Optional[int] = None) -> None:
        self._deque: Deque[T] = deque(maxlen=maxlen)
        self._lock = threading.Lock()

    def append(self, item: T) -> None:
        """Append an item."""
        with self._lock:
            self._deque.append(item)

    def appendleft(self, item: T) -> None:
        """Append an item to the left."""
        with self._lock:
            self._deque.appendleft(item)

    def pop(self) -> Optional[T]:
        """Pop an item from the right."""
        with self._lock:
            return self._deque.pop() if self._deque else None

    def popleft(self) -> Optional[T]:
        """Pop an item from the left."""
        with self._lock:
            return self._deque.popleft() if self._deque else None

    def extend(self, items: List[T]) -> None:
        """Extend with items."""
        with self._lock:
            self._deque.extend(items)

    def clear(self) -> None:
        """Clear all items."""
        with self._lock:
            self._deque.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._deque)

    def __contains__(self, item: T) -> bool:
        with self._lock:
            return item in self._deque


@dataclass
class WorkerStats:
    """Statistics for a worker pool."""

    tasks_submitted: int = 0
    tasks_completed: int = 0
    tasks_failed: int = 0
    total_duration: float = 0.0
    avg_duration: float = 0.0


class WorkerPool:
    """Worker pool with task tracking and statistics."""

    def __init__(
        self,
        num_workers: int = 4,
        queue_size: int = 0,
    ) -> None:
        self._num_workers = num_workers
        self._task_queue: queue.Queue[Optional[tuple]] = queue.Queue(maxsize=queue_size)
        self._result_queue: queue.Queue[Any] = queue.Queue()
        self._workers: List[threading.Thread] = []
        self._running = False
        self._stats = WorkerStats()
        self._stats_lock = threading.Lock()
        atexit.register(self.shutdown)

    def start(self) -> None:
        """Start worker threads."""
        if self._running:
            return
        self._running = True
        for i in range(self._num_workers):
            t = threading.Thread(
                target=self._worker_loop,
                name=f"worker-{i}",
                daemon=True,
            )
            t.start()
            self._workers.append(t)

    def _worker_loop(self) -> None:
        """Worker thread main loop."""
        while self._running:
            try:
                task = self._task_queue.get(timeout=0.5)
                if task is None:
                    break
                task_id, fn, args, kwargs = task
                try:
                    import time
                    start = time.perf_counter()
                    result = fn(*args, **kwargs)
                    duration = time.perf_counter() - start
                    self._result_queue.put((task_id, "success", result, duration))
                    with self._stats_lock:
                        self._stats.tasks_completed += 1
                        self._stats.total_duration += duration
                        if self._stats.tasks_completed > 0:
                            self._stats.avg_duration = (
                                self._stats.total_duration / self._stats.tasks_completed
                            )
                except Exception as e:
                    self._result_queue.put((task_id, "error", e, 0.0))
                    with self._stats_lock:
                        self._stats.tasks_failed += 1
                finally:
                    self._task_queue.task_done()
            except queue.Empty:
                continue

    def submit(
        self,
        fn: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> tuple[int, queue.Queue]:
        """Submit a task.

        Args:
            fn: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Tuple of (task_id, result_queue).
        """
        task_id = self._stats.tasks_submitted
        self._task_queue.put((task_id, fn, args, kwargs))
        with self._stats_lock:
            self._stats.tasks_submitted += 1
        return task_id, self._result_queue

    def get_stats(self) -> WorkerStats:
        """Get worker statistics."""
        with self._stats_lock:
            return WorkerStats(
                tasks_submitted=self._stats.tasks_submitted,
                tasks_completed=self._stats.tasks_completed,
                tasks_failed=self._stats.tasks_failed,
                total_duration=self._stats.total_duration,
                avg_duration=self._stats.avg_duration,
            )

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the worker pool."""
        if not self._running:
            return
        self._running = False
        for _ in range(self._num_workers):
            self._task_queue.put(None)
        if wait:
            for t in self._workers:
                t.join()
        self._workers.clear()

    def __enter__(self) -> "WorkerPool":
        self.start()
        return self

    def __exit__(self, *args: Any) -> None:
        self.shutdown()
