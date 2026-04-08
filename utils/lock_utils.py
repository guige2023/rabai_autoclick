"""Lock utilities for RabAI AutoClick.

Provides:
- Reentrant lock wrapper
- Read-write lock
- Semaphore helpers
- Deadlock prevention utilities
"""

from __future__ import annotations

import threading
import time
from typing import (
    Callable,
    Optional,
    TypeVar,
)


T = TypeVar("T")


class ReadWriteLock:
    """A lock that allows multiple readers or a single writer."""

    def __init__(self) -> None:
        self._readers = 0
        self._writers_waiting = 0
        self._writer_active = False
        self._lock = threading.Lock()
        self._readers_ok = threading.Condition(self._lock)
        self._writers_ok = threading.Condition(self._lock)

    def acquire_read(self) -> None:
        """Acquire a read lock."""
        with self._lock:
            while self._writer_active or self._writers_waiting > 0:
                self._readers_ok.wait()
            self._readers += 1

    def release_read(self) -> None:
        """Release a read lock."""
        with self._lock:
            self._readers -= 1
            if self._readers == 0:
                self._writers_ok.notify()

    def acquire_write(self) -> None:
        """Acquire a write lock."""
        with self._lock:
            self._writers_waiting += 1
            while self._readers > 0 or self._writer_active:
                self._writers_ok.wait()
            self._writers_waiting -= 1
            self._writer_active = True

    def release_write(self) -> None:
        """Release a write lock."""
        with self._lock:
            self._writer_active = False
            self._readers_ok.notify_all()
            self._writers_ok.notify()

    def reader(self) -> Callable[[Callable[..., T]], Callable[..., T]]:
        """Decorator for read-critical sections."""
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            def wrapper(*args: Any, **kwargs: Any) -> T:
                self.acquire_read()
                try:
                    return func(*args, **kwargs)
                finally:
                    self.release_read()
            return wrapper
        return decorator

    def writer(self) -> Callable[[Callable[..., T]], Callable[..., T]]:
        """Decorator for write-critical sections."""
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            def wrapper(*args: Any, **kwargs: Any) -> T:
                self.acquire_write()
                try:
                    return func(*args, **kwargs)
                finally:
                    self.release_write()
            return wrapper
        return decorator


class TimedLock:
    """A lock that can be acquired with a timeout."""

    def __init__(self, timeout: float = 10.0) -> None:
        self._lock = threading.Lock()
        self._timeout = timeout

    def acquire(self, timeout: Optional[float] = None) -> bool:
        """Try to acquire the lock within timeout.

        Args:
            timeout: Seconds to wait. None uses default.

        Returns:
            True if acquired, False if timed out.
        """
        return self._lock.acquire(timeout=timeout or self._timeout)

    def release(self) -> None:
        """Release the lock."""
        self._lock.release()

    def __enter__(self) -> None:
        self.acquire()

    def __exit__(self, *args: Any) -> None:
        self.release()


class CounterLock:
    """A lock that tracks acquisition count (for reentrant-like behavior)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._count = 0
        self._owner: Optional[int] = None

    def acquire(self) -> bool:
        """Acquire the lock (reentrant on same thread)."""
        me = threading.get_ident()
        with self._lock:
            if self._owner == me:
                self._count += 1
                return True
            while self._owner is not None:
                self._lock.wait()
            self._owner = me
            self._count = 1
            return True

    def release(self) -> None:
        """Release the lock."""
        with self._lock:
            if self._owner != threading.get_ident():
                raise RuntimeError("Not the lock owner")
            self._count -= 1
            if self._count == 0:
                self._owner = None
                self._lock.notify_all()

    def __enter__(self) -> None:
        self.acquire()

    def __exit__(self, *args: Any) -> None:
        self.release()


class SemaphorePool:
    """A pool of resources managed by a semaphore."""

    def __init__(self, size: int) -> None:
        if size <= 0:
            raise ValueError("Pool size must be positive")
        self._semaphore = threading.Semaphore(size)
        self._size = size

    def acquire(self, timeout: Optional[float] = None) -> bool:
        """Acquire a resource from the pool.

        Args:
            timeout: Seconds to wait. None means infinite.

        Returns:
            True if acquired.
        """
        return self._semaphore.acquire(timeout=timeout)

    def release(self) -> None:
        """Release a resource back to the pool."""
        self._semaphore.release()

    def __enter__(self) -> None:
        self.acquire()

    def __exit__(self, *args: Any) -> None:
        self.release()


__all__ = [
    "ReadWriteLock",
    "TimedLock",
    "CounterLock",
    "SemaphorePool",
]
