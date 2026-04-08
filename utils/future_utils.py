"""
Future Utilities

Higher-level async result wrappers, timeout handling,
and composition helpers for concurrent operations.

License: MIT
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import (
    Any,
    Callable,
    Generic,
    TypeVar,
    Optional,
    Union,
    overload,
    Awaitable,
)
from concurrent.futures import Future as StdFuture, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, field
from enum import Enum, auto
import queue

T = TypeVar("T")
U = TypeVar("U")


class AsyncResult(Generic[T]):
    """Async result holder with blocking get and optional timeout.
    
    Example:
        result = AsyncResult()
        Thread(target=compute, args=(result,)).start()
        value = result.get(timeout=5.0)
    """
    
    __slots__ = ("_value", "_error", "_done", "_condition", "_traceback")
    
    def __init__(self) -> None:
        self._value: T | None = None
        self._error: Exception | None = None
        self._done = False
        self._condition = threading.Condition()
        self._traceback: str | None = None
    
    def set(self, value: T) -> None:
        with self._condition:
            self._value = value
            self._done = True
            self._condition.notify_all()
    
    def set_exception(self, error: Exception, tb: str | None = None) -> None:
        with self._condition:
            self._error = error
            self._traceback = tb
            self._done = True
            self._condition.notify_all()
    
    def get(self, timeout: float | None = None) -> T:
        with self._condition:
            if not self._done:
                if not self._condition.wait(timeout):
                    raise FuturesTimeoutError("Result not available within timeout")
        if self._error:
            raise self._error
        return self._value  # type: ignore
    
    def ready(self) -> bool:
        with self._condition:
            return self._done
    
    @property
    def value(self) -> T | None:
        return self._value
    
    @property
    def error(self) -> Exception | None:
        return self._error


@dataclass
class TimedResult(Generic[T]):
    """Result with timing information."""
    value: T
    elapsed_ms: float
    success: bool
    error: Exception | None = None


def with_timeout(
    seconds: float,
    default: T | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T | None]]:
    """Decorator to apply timeout to a function.
    
    Returns default if timeout is exceeded.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T | None]:
        def wrapper(*args: Any, **kwargs: Any) -> T | None:
            result: list[T | None] = [default]
            error: list[Exception] = [None]
            done = threading.Event()
            
            def target() -> None:
                try:
                    result[0] = func(*args, **kwargs)
                except Exception as e:
                    error[0] = e
                finally:
                    done.set()
            
            t = threading.Thread(target=target, daemon=True)
            t.start()
            if not done.wait(seconds):
                return default
            if error[0]:
                raise error[0]
            return result[0]
        return wrapper
    return decorator


class CancellationToken:
    """Token to signal cancellation for long-running operations."""
    
    def __init__(self) -> None:
        self._cancelled = False
        self._lock = threading.Lock()
    
    def cancel(self) -> None:
        with self._lock:
            self._cancelled = True
    
    @property
    def is_cancelled(self) -> bool:
        with self._lock:
            return self._cancelled
    
    def check(self) -> None:
        if self.is_cancelled:
            raise OperationCancelledError()


class OperationCancelledError(Exception):
    """Raised when an operation is cancelled via CancellationToken."""
    pass


class ResourcePool(Generic[T]):
    """Pool of reusable resources with acquisition/release.
    
    Example:
        pool = ResourcePool(factory=lambda: DatabaseConnection(), max_size=5)
        conn = pool.acquire()
        try:
            conn.query("SELECT 1")
        finally:
            pool.release(conn)
    """
    
    def __init__(
        self,
        factory: Callable[[], T],
        max_size: int = 10,
        timeout: float = 30.0,
    ) -> None:
        self._factory = factory
        self._max_size = max_size
        self._timeout = timeout
        self._pool: queue.Queue[T] = queue.Queue(maxsize=max_size)
        self._created = 0
        self._lock = threading.Lock()
    
    def acquire(self) -> T:
        try:
            return self._pool.get_nowait()
        except queue.Empty:
            with self._lock:
                if self._created < self._max_size:
                    self._created += 1
                    return self._factory()
            return self._pool.get(timeout=self._timeout)
    
    def release(self, resource: T) -> None:
        try:
            self._pool.put_nowait(resource)
        except queue.Full:
            pass  # discard if pool is full


def parallel_map(
    func: Callable[[T], U],
    items: list[T],
    max_workers: int = 4,
    timeout: float = 60.0,
) -> list[U]:
    """Map function over items in parallel threads."""
    results: list[U] = []
    lock = threading.Lock()
    
    def worker(item: T) -> None:
        try:
            result = func(item)
            with lock:
                results.append(result)
        except Exception as e:
            with lock:
                results.append(e)
    
    threads = [threading.Thread(target=worker, args=(item,), daemon=True) for item in items]
    for t in threads[:max_workers]:
        t.start()
    
    for t in threads:
        t.join(timeout=timeout / len(items) if items else timeout)
    
    return results


@dataclass
class BatchFuture(Generic[T]):
    """Future that collects multiple results over time."""
    _results: list[T] = field(default_factory=list)
    _errors: list[Exception] = field(default_factory=list)
    _done: bool = False
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _condition: threading.Condition = field(default_factory=threading.Condition)
    _expected: int = 0
    
    def add_result(self, value: T) -> None:
        with self._condition:
            self._results.append(value)
            self._condition.notify_all()
    
    def add_error(self, error: Exception) -> None:
        with self._condition:
            self._errors.append(error)
            self._condition.notify_all()
    
    def set_complete(self) -> None:
        with self._condition:
            self._done = True
            self._condition.notify_all()
    
    def wait(self, timeout: float | None = None) -> tuple[list[T], list[Exception]]:
        with self._condition:
            while not self._done:
                self._condition.wait(timeout)
            return list(self._results), list(self._errors)
    
    @property
    def results(self) -> list[T]:
        with self._lock:
            return list(self._results)
    
    @property
    def errors(self) -> list[Exception]:
        with self._lock:
            return list(self._errors)


__all__ = [
    "AsyncResult",
    "TimedResult",
    "with_timeout",
    "CancellationToken",
    "OperationCancelledError",
    "ResourcePool",
    "parallel_map",
    "BatchFuture",
]
