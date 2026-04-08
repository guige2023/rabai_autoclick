"""
Context Manager Utilities

Decorator-based and factory context managers for common
resource management patterns: timeout, retry, rolling window.

License: MIT
"""

from __future__ import annotations

import threading
import time
import functools
from typing import (
    Any,
    Callable,
    TypeVar,
    Generic,
    Optional,
    Union,
    Iterator,
    ContextManager,
)
from contextlib import contextmanager, ExitStack
from dataclasses import dataclass, field
import queue

T = TypeVar("T")


class TimeoutError(Exception):
    """Raised when a context block exceeds its time limit."""
    pass


@contextmanager
def timeout_context(seconds: float, error_msg: str | None = None):
    """Context manager that raises TimeoutError if block exceeds time.
    
    Example:
        with timeout_context(5.0):
            data = slow_operation()
    """
    start = time.monotonic()
    yield
    elapsed = time.monotonic() - start
    if elapsed > seconds:
        raise TimeoutError(error_msg or f"Block exceeded {seconds}s limit")


@contextmanager
def retry_context(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
):
    """Context manager that retries the block on exception.
    
    Example:
        with retry_context(max_attempts=5, delay=1.0, backoff=2.0):
            unstable_operation()
    """
    attempt = 0
    current_delay = delay
    while True:
        try:
            attempt += 1
            yield
            return
        except exceptions as e:
            if attempt >= max_attempts:
                raise
            time.sleep(current_delay)
            current_delay *= backoff


@contextmanager
def sliding_window_context(
    max_size: int = 100,
    ttl_seconds: float = 60.0,
):
    """Context manager that maintains a sliding window of items.
    
    Removes items older than ttl_seconds and enforces max_size.
    """
    window: list[tuple[float, Any]] = []
    try:
        yield window
    finally:
        cutoff = time.monotonic() - ttl_seconds
        window[:] = [(t, v) for t, v in window if t > cutoff]
        if len(window) > max_size:
            window[:] = window[-max_size:]


@contextmanager
def bounded_queue_context(maxsize: int = 100, timeout: float = 1.0):
    """Context manager for a bounded queue with backpressure."""
    q: queue.Queue[Any] = queue.Queue(maxsize=maxsize)
    try:
        yield q
    finally:
        while not q.empty():
            try:
                q.get_nowait()
            except queue.Empty:
                pass


@dataclass
class RateLimiter(ContextManager):
    """Token bucket rate limiter as context manager.
    
    Example:
        with RateLimiter(rate=10, capacity=20) as limiter:
            if limiter.acquire():
                do_ratelimited_operation()
    """
    rate: float  # tokens per second
    capacity: float  # max tokens
    _tokens: float = field(default=0.0, init=False)
    _last_update: float = field(default=0.0, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    
    def __post_init__(self) -> None:
        self._tokens = self._capacity
        self._last_update = time.monotonic()
    
    @property
    def _max_tokens(self) -> float:
        return self._capacity
    
    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_update
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
        self._last_update = now
    
    def acquire(self, tokens: float = 1.0, blocking: bool = True, timeout: float | None = None) -> bool:
        """Attempt to acquire tokens, returns True if successful."""
        deadline = time.monotonic() + timeout if timeout else None
        with self._lock:
            while True:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return True
                if not blocking:
                    return False
                wait_time = (tokens - self._tokens) / self._rate
                if deadline and time.monotonic() + wait_time > deadline:
                    return False
                time.sleep(min(wait_time, 0.1))
    
    def __enter__(self) -> RateLimiter:
        return self
    
    def __exit__(self, *args: Any) -> bool:
        return False


@contextmanager
def conditional_context(condition: bool, *contexts):
    """Conditionally enter one or more context managers.
    
    Example:
        with conditional_context(debug_mode, open_log(), track_timing()):
            do_something()
    """
    with ExitStack() as stack:
        if condition:
            for ctx in contexts:
                stack.enter_context(ctx)
        yield


@contextmanager  
def resource_monitor(threshold_mb: float = 1000.0):
    """Monitor memory usage within a context."""
    import resource
    start_rusage = resource.getrusage(resource.RUSAGE_SELF)
    start_mb = start_rusage.ru_maxrss / 1024
    yield start_mb
    end_rusage = resource.getrusage(resource.RUSAGE_SELF)
    end_mb = end_rusage.ru_maxrss / 1024
    if end_mb - start_mb > threshold_mb:
        import warnings
        warnings.warn(f"Memory increased by {end_mb - start_mb:.1f}MB, threshold: {threshold_mb}MB")


@contextmanager
def lock_context(lock: threading.Lock, timeout: float | None = None):
    """Acquire a lock as a context manager with optional timeout."""
    acquired = lock.acquire(timeout=timeout)
    if not acquired:
        raise TimeoutError(f"Could not acquire lock within {timeout}s")
    try:
        yield
    finally:
        lock.release()


def context_manager(func: Callable[..., Iterator[T]]) -> Callable[..., ContextManager[T]]:
    """Decorator to convert a generator function to a context manager."""
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> ContextManager[T]:
        return func(*args, **kwargs)
    return wrapper


@dataclass
class Timer(ContextManager):
    """Timer context manager that tracks elapsed time."""
    _start: float = field(default=0.0, init=False)
    _end: float = field(default=0.0, init=False)
    _elapsed: float = field(default=0.0, init=False)
    
    def __enter__(self) -> Timer:
        self._start = time.perf_counter()
        return self
    
    def __exit__(self, *args: Any) -> bool:
        self._end = time.perf_counter()
        self._elapsed = self._end - self._start
        return False
    
    @property
    def elapsed_seconds(self) -> float:
        return self._elapsed


__all__ = [
    "TimeoutError",
    "timeout_context",
    "retry_context",
    "sliding_window_context",
    "bounded_queue_context",
    "RateLimiter",
    "conditional_context",
    "resource_monitor",
    "lock_context",
    "context_manager",
    "Timer",
]
