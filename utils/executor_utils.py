"""Executor and thread pool utilities.

Provides thread pool and executor implementations for
parallel task execution in automation workflows.
"""

import concurrent.futures
import threading
import time
from typing import Any, Callable, Dict, List, Optional, TypeVar


T = TypeVar("T")


class ThreadPoolExecutor:
    """Thread pool executor for parallel task execution.

    Example:
        executor = ThreadPoolExecutor(max_workers=4)
        futures = [executor.submit(task, arg) for arg in args]
        results = [f.result() for f in futures]
    """

    def __init__(self, max_workers: int = 4) -> None:
        self._max_workers = max_workers
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._running = True

    def submit(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> concurrent.futures.Future:
        """Submit task to thread pool.

        Args:
            func: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Future object.
        """
        return self._executor.submit(func, *args, **kwargs)

    def map(self, func: Callable[..., T], *iterables: Any, timeout: float = None) -> List[T]:
        """Map function over iterables.

        Args:
            func: Function to apply.
            *iterables: Input iterables.
            timeout: Optional timeout.

        Returns:
            List of results.
        """
        return list(self._executor.map(func, *iterables))

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the executor.

        Args:
            wait: Wait for pending tasks.
        """
        self._running = False
        self._executor.shutdown(wait=wait)

    def __enter__(self) -> "ThreadPoolExecutor":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.shutdown()


class WorkQueue:
    """Thread-safe work queue with worker threads.

    Example:
        queue = WorkQueue(max_workers=3)
        queue.add_task(task_func, arg1, arg2)
        queue.wait_all()
    """

    def __init__(self, max_workers: int = 2) -> None:
        self._max_workers = max_workers
        self._tasks: List[tuple] = []
        self._results: List[Any] = []
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._workers: List[threading.Thread] = []
        self._running = True

    def add_task(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> None:
        """Add task to queue.

        Args:
            func: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        """
        with self._condition:
            self._tasks.append((func, args, kwargs))
            if len(self._workers) < self._max_workers:
                self._start_worker()
            self._condition.notify()

    def _start_worker(self) -> None:
        thread = threading.Thread(target=self._worker_loop, daemon=True)
        thread.start()
        self._workers.append(thread)

    def _worker_loop(self) -> None:
        while self._running:
            task = None
            with self._condition:
                while not self._tasks and self._running:
                    self._condition.wait()
                if self._tasks:
                    task = self._tasks.pop(0)
            if task:
                func, args, kwargs = task
                try:
                    result = func(*args, **kwargs)
                    with self._results:
                        pass
                except Exception as e:
                    result = e

    def wait_all(self, timeout: float = None) -> List[Any]:
        """Wait for all tasks to complete.

        Args:
            timeout: Maximum wait time.

        Returns:
            List of results.
        """
        start = time.time()
        with self._condition:
            while self._tasks and (timeout is None or time.time() - start < timeout):
                self._condition.wait(timeout)
        return self._results

    def shutdown(self) -> None:
        """Shutdown the work queue."""
        self._running = False
        with self._condition:
            self._condition.notify_all()


class FutureResult:
    """Wrapper for async result with callbacks.

    Example:
        future = FutureResult()
        def on_done(result):
            print(f"Done: {result}")
        future.then(on_done)
        future.set_result(42)
    """

    def __init__(self) -> None:
        self._result = None
        self._error: Optional[Exception] = None
        self._done = False
        self._callbacks: List[Callable[[Any], None]] = []
        self._lock = threading.Lock()

    def set_result(self, result: T) -> None:
        """Set the result.

        Args:
            result: Result value.
        """
        with self._lock:
            self._result = result
            self._done = True
            self._run_callbacks()

    def set_error(self, error: Exception) -> None:
        """Set an error.

        Args:
            error: Exception instance.
        """
        with self._lock:
            self._error = error
            self._done = True
            self._run_callbacks()

    def result(self, timeout: float = None) -> T:
        """Get result, blocking until available.

        Args:
            timeout: Maximum wait time.

        Returns:
            Result value.

        Raises:
            Exception: If error was set.
            TimeoutError: If timeout expires.
        """
        start = time.time()
        with self._lock:
            while not self._done:
                remaining = None if timeout is None else timeout - (time.time() - start)
                if remaining is not None and remaining <= 0:
                    raise TimeoutError()
            if self._error:
                raise self._error
            return self._result

    def then(self, callback: Callable[[T], None]) -> None:
        """Register callback to run when complete.

        Args:
            callback: Callback function.
        """
        with self._lock:
            if self._done:
                callback(self._result if self._error is None else self._error)
            else:
                self._callbacks.append(callback)

    def _run_callbacks(self) -> None:
        for callback in self._callbacks:
            try:
                callback(self._result if self._error is None else self._error)
            except Exception:
                pass
        self._callbacks.clear()

    @property
    def done(self) -> bool:
        """Check if result is available."""
        with self._lock:
            return self._done


def run_parallel(
    *funcs: Callable[..., T],
    timeout: float = None,
) -> List[T]:
    """Run multiple functions in parallel.

    Args:
        *funcs: Functions to execute.
        timeout: Maximum wait time.

    Returns:
        List of results.
    """
    with ThreadPoolExecutor(max_workers=len(funcs)) as executor:
        futures = [executor.submit(f) for f in funcs]
        return [f.result(timeout=timeout) for f in futures]
