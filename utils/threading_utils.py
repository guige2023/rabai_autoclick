"""Threading utilities for RabAI AutoClick.

Provides:
- Thread pool executor
- Async helpers
- Lock utilities
- Thread-safe collections
"""

import asyncio
import threading
import weakref
from concurrent.futures import Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar, Union


T = TypeVar("T")


class ThreadPool:
    """Thread pool for concurrent task execution.

    Usage:
        pool = ThreadPool(max_workers=4)
        future = pool.submit(my_func, arg1, arg2)
        result = future.result()
        pool.shutdown()
    """

    def __init__(
        self,
        max_workers: Optional[int] = None,
        thread_name_prefix: str = "rabai_pool",
    ) -> None:
        """Initialize thread pool.

        Args:
            max_workers: Maximum number of worker threads.
            thread_name_prefix: Prefix for thread names.
        """
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=thread_name_prefix,
        )

    def submit(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> Future[T]:
        """Submit a task to the pool.

        Args:
            func: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Future representing the task.
        """
        return self._executor.submit(func, *args, **kwargs)

    def map(
        self,
        func: Callable[..., T],
        *iterables: Any,
        timeout: Optional[float] = None,
    ) -> List[T]:
        """Map function over iterables.

        Args:
            func: Function to apply.
            *iterables: Iterables of arguments.
            timeout: Optional timeout.

        Returns:
            List of results.
        """
        return list(self._executor.map(func, *iterables, timeout=timeout))

    def shutdown(
        self,
        wait: bool = True,
        cancel_futures: bool = False,
    ) -> None:
        """Shutdown the thread pool.

        Args:
            wait: If True, wait for tasks to complete.
            cancel_futures: If True, cancel pending futures.
        """
        self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)

    def __enter__(self) -> 'ThreadPool':
        """Context manager entry."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit."""
        self.shutdown()


class AsyncExecutor:
    """Execute functions asynchronously with thread pool.

    Provides both sync and async interfaces.
    """

    def __init__(
        self,
        max_workers: Optional[int] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        """Initialize async executor.

        Args:
            max_workers: Maximum worker threads.
            loop: Optional event loop to use.
        """
        self._pool = ThreadPoolExecutor(max_workers=max_workers)
        self._loop = loop

    def run_in_executor(
        self,
        func: Callable[..., T],
        *args: Any,
    ) -> asyncio.Future[T]:
        """Run function in thread pool.

        Args:
            func: Function to execute.
            *args: Arguments for function.

        Returns:
            asyncio.Future representing the task.
        """
        loop = self._loop or asyncio.get_event_loop()
        return loop.run_in_executor(self._pool, func, *args)

    async def run_sync(
        self,
        func: Callable[..., T],
        *args: Any,
    ) -> T:
        """Run sync function asynchronously.

        Args:
            func: Synchronous function to run.
            *args: Arguments for function.

        Returns:
            Result of function.
        """
        future = self.run_in_executor(func, *args)
        return await future

    def shutdown(self) -> None:
        """Shutdown executor."""
        self._pool.shutdown()


@dataclass
class ThreadLocal:
    """Thread-local storage.

    Provides thread-local data storage.
    """
    _data: Dict[int, Any] = None

    def __post_init__(self) -> None:
        self._data = {}

    def set(self, value: T) -> None:
        """Set value for current thread."""
        self._data[threading.current_thread().ident] = value

    def get(self, default: Optional[T] = None) -> Optional[T]:
        """Get value for current thread."""
        return self._data.get(threading.current_thread().ident, default)

    def clear(self) -> None:
        """Clear value for current thread."""
        self._data.pop(threading.current_thread().ident, None)


class RWLock:
    """Read-Write lock implementation.

    Allows multiple readers or a single writer.
    """

    def __init__(self) -> None:
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0
        self._writers_waiting = 0
        self._writer_active = False

    def acquire_read(self) -> None:
        """Acquire read lock."""
        with self._read_ready:
            while self._writer_active or self._writers_waiting > 0:
                self._read_ready.wait()
            self._readers += 1

    def release_read(self) -> None:
        """Release read lock."""
        with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notify_all()

    def acquire_write(self) -> None:
        """Acquire write lock."""
        with self._read_ready:
            self._writers_waiting += 1
            while self._readers > 0 or self._writer_active:
                self._read_ready.wait()
            self._writers_waiting -= 1
            self._writer_active = True

    def release_write(self) -> None:
        """Release write lock."""
        with self._read_ready:
            self._writer_active = False
            self._read_ready.notify_all()

    @contextmanager
    def read_lock(self) -> None:
        """Context manager for read lock."""
        self.acquire_read()
        try:
            yield
        finally:
            self.release_read()

    @contextmanager
    def write_lock(self) -> None:
        """Context manager for write lock."""
        self.acquire_write()
        try:
            yield
        finally:
            self.release_write()


class ThreadSafeDict(Dict[T, Any]):
    """Thread-safe dictionary.

    Provides atomic operations on dictionary.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._lock = threading.RLock()

    def atomic_update(
        self,
        key: T,
        update_func: Callable[[Any], Any],
        default: Optional[Any] = None,
    ) -> Any:
        """Atomically update a value.

        Args:
            key: Key to update.
            update_func: Function to apply to current value.
            default: Default value if key not present.

        Returns:
            New value.
        """
        with self._lock:
            current = self.get(key, default)
            new_value = update_func(current)
            self[key] = new_value
            return new_value

    def get_or_create(
        self,
        key: T,
        factory: Callable[[], Any],
    ) -> Any:
        """Get value or create if not exists.

        Args:
            key: Key to get/create.
            factory: Function to create default value.

        Returns:
            Existing or new value.
        """
        with self._lock:
            if key not in self:
                self[key] = factory()
            return self[key]

    def update_if_exists(
        self,
        key: T,
        value: Any,
    ) -> bool:
        """Update value only if key exists.

        Args:
            key: Key to update.
            value: New value.

        Returns:
            True if key existed and was updated.
        """
        with self._lock:
            if key in self:
                self[key] = value
                return True
            return False


class ThreadSafeCounter:
    """Thread-safe counter with atomic increment/decrement."""

    def __init__(self, initial: int = 0) -> None:
        self._value = initial
        self._lock = threading.Lock()

    @property
    def value(self) -> int:
        """Get current value."""
        with self._lock:
            return self._value

    def inc(self, amount: int = 1) -> int:
        """Increment and return new value."""
        with self._lock:
            self._value += amount
            return self._value

    def dec(self, amount: int = 1) -> int:
        """Decrement and return new value."""
        with self._lock:
            self._value -= amount
            return self._value


class FutureGroup:
    """Manage a group of futures.

    Provides waiting and cancellation for multiple futures.
    """

    def __init__(self) -> None:
        self._futures: List[Future] = []
        self._lock = threading.Lock()

    def add(self, future: Future) -> None:
        """Add a future to the group."""
        with self._lock:
            self._futures.append(future)

    def wait(
        self,
        timeout: Optional[float] = None,
        return_when: str = "ALL_COMPLETED",
    ) -> List[Future]:
        """Wait for futures to complete.

        Args:
            timeout: Optional timeout in seconds.
            return_when: When to return (ALL_COMPLETED, FIRST_EXCEPTION, FIRST_COMPLETED).

        Returns:
            List of completed futures.
        """
        with self._lock:
            futures = self._futures.copy()

        if not futures:
            return []

        done, _ = wait(futures, timeout=timeout, return_when=return_when)
        return list(done)

    def cancel_all(self) -> int:
        """Cancel all futures.

        Returns:
            Number of futures cancelled.
        """
        with self._lock:
            futures = self._futures.copy()

        cancelled = 0
        for future in futures:
            if future.cancel():
                cancelled += 1

        return cancelled


def run_in_thread(func: Callable[..., T]) -> Callable[..., asyncio.Future]:
    """Decorator to run function in a thread pool.

    Usage:
        @run_in_thread
        def blocking_io():
            return do_blocking_io()
    """
    def wrapper(*args: Any, **kwargs: Any) -> asyncio.Future:
        loop = asyncio.get_event_loop()
        return loop.run_in_executor(None, lambda: func(*args, **kwargs))
    return wrapper


class Once:
    """Run code exactly once across all threads.

    Usage:
        def setup():
            print("Setup called")

        once = Once()
        once(setup)  # Only this call runs setup
        once(setup)  # This call is no-op
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._done = False

    def __call__(self, func: Callable[[], None]) -> None:
        """Execute func exactly once."""
        if not self._done:
            with self._lock:
                if not self._done:
                    func()
                    self._done = True