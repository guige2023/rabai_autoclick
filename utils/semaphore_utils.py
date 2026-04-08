"""
Semaphore and lock primitives utilities.

Provides specialized semaphore implementations including
weighted semaphores, read-write locks, and timed locks
for advanced concurrency control.

Example:
    >>> from utils.semaphore_utils import ReadWriteLock
    >>> lock = ReadWriteLock()
    >>> with lock.read_lock():
    ...     data = read_data()
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any, Callable, Optional


class WeightedSemaphore:
    """
    Weighted semaphore for resource management.

    Allows acquiring and releasing weighted permits,
    useful for managing limited resources like connection pools.

    Attributes:
        value: Current number of available permits.
    """

    def __init__(
        self,
        value: int,
        max_value: Optional[int] = None,
    ) -> None:
        """
        Initialize the weighted semaphore.

        Args:
            value: Initial number of permits.
            max_value: Maximum number of permits.
        """
        self._value = value
        self._max_value = max_value or value
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._waiters = 0

    def acquire(self, weight: int = 1, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Acquire permits from the semaphore.

        Args:
            weight: Number of permits to acquire.
            blocking: If True, wait until permits are available.
            timeout: Maximum wait time in seconds.

        Returns:
            True if permits were acquired, False otherwise.
        """
        with self._condition:
            if not blocking:
                if self._value >= weight:
                    self._value -= weight
                    return True
                return False

            if timeout is not None:
                end_time = time.monotonic() + timeout
            else:
                end_time = None

            while self._value < weight:
                if timeout is not None:
                    remaining = end_time - time.monotonic()
                    if remaining <= 0:
                        return False
                    self._condition.wait(remaining)
                else:
                    self._condition.wait()

            self._value -= weight
            return True

    def release(self, weight: int = 1) -> None:
        """
        Release permits back to the semaphore.

        Args:
            weight: Number of permits to release.
        """
        with self._condition:
            self._value = min(self._max_value, self._value + weight)
            self._condition.notify_all()

    @property
    def available(self) -> int:
        """Get the number of available permits."""
        with self._lock:
            return self._value

    def __enter__(self) -> "WeightedSemaphore":
        """Context manager entry."""
        self.acquire()
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit."""
        self.release()


class AsyncWeightedSemaphore:
    """
    Async-weighted semaphore for resource management.

    Async-compatible version of WeightedSemaphore.
    """

    def __init__(
        self,
        value: int,
        max_value: Optional[int] = None,
    ) -> None:
        """
        Initialize the async weighted semaphore.

        Args:
            value: Initial number of permits.
            max_value: Maximum number of permits.
        """
        self._value = value
        self._max_value = max_value or value
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)

    async def acquire(self, weight: int = 1, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Acquire permits asynchronously."""
        async with self._condition:
            if not blocking:
                if self._value >= weight:
                    self._value -= weight
                    return True
                return False

            try:
                if timeout is not None:
                    await asyncio.wait_for(self._condition.wait(), timeout)
                else:
                    await self._condition.wait()

                self._value -= weight
                return True
            except asyncio.TimeoutError:
                return False

    async def release(self, weight: int = 1) -> None:
        """Release permits asynchronously."""
        async with self._condition:
            self._value = min(self._max_value, self._value + weight)
            self._condition.notify_all()

    @property
    async def available(self) -> int:
        """Get the number of available permits."""
        async with self._lock:
            return self._value


class ReadWriteLock:
    """
    Read-write lock for concurrent read, exclusive write access.

    Multiple readers can hold the lock simultaneously, but
    writers get exclusive access.
    """

    def __init__(self) -> None:
        """Initialize the read-write lock."""
        self._readers = 0
        self._writers = 0
        self._writer_waiting = 0
        self._lock = threading.Lock()
        self._read_ready = threading.Condition(self._lock)
        self._write_ready = threading.Condition(self._lock)

    def read_lock(self) -> "_ReadLockContext":
        """
        Get a context manager for read access.

        Returns:
            Read lock context manager.
        """
        return _ReadLockContext(self)

    def write_lock(self) -> "_WriteLockContext":
        """
        Get a context manager for write access.

        Returns:
            Write lock context manager.
        """
        return _WriteLockContext(self)

    def acquire_read(self) -> None:
        """Acquire a read lock."""
        with self._lock:
            while self._writers > 0 or self._writer_waiting > 0:
                self._read_ready.wait()
            self._readers += 1

    def release_read(self) -> None:
        """Release a read lock."""
        with self._lock:
            self._readers -= 1
            if self._readers == 0:
                self._write_ready.notifyAll()

    def acquire_write(self) -> None:
        """Acquire a write lock."""
        with self._lock:
            self._writer_waiting += 1
            while self._readers > 0 or self._writers > 0:
                self._write_ready.wait()
            self._writer_waiting -= 1
            self._writers += 1

    def release_write(self) -> None:
        """Release a write lock."""
        with self._lock:
            self._writers -= 1
            self._read_ready.notifyAll()
            self._write_ready.notifyAll()


class _ReadLockContext:
    """Context manager for read lock."""

    def __init__(self, lock: ReadWriteLock) -> None:
        self._lock = lock

    def __enter__(self) -> "_ReadLockContext":
        self._lock.acquire_read()
        return self

    def __exit__(self, *args: Any) -> None:
        self._lock.release_read()


class _WriteLockContext:
    """Context manager for write lock."""

    def __init__(self, lock: ReadWriteLock) -> None:
        self._lock = lock

    def __enter__(self) -> "_WriteLockContext":
        self._lock.acquire_write()
        return self

    def __exit__(self, *args: Any) -> None:
        self._lock.release_write()


class TimedLock:
    """
    Lock with acquisition timeout.

    Provides a regular mutex that can be acquired with
    a timeout to prevent deadlocks.
    """

    def __init__(self) -> None:
        """Initialize the timed lock."""
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._owner: Optional[int] = None
        self._acquire_count = 0

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Acquire the lock.

        Args:
            blocking: If True, wait for lock.
            timeout: Maximum wait time in seconds.

        Returns:
            True if lock was acquired, False otherwise.
        """
        thread_id = threading.get_ident()

        if timeout is None:
            if self._lock.acquire(blocking):
                self._owner = thread_id
                self._acquire_count += 1
                return True
            return False

        end_time = time.monotonic() + timeout
        while True:
            remaining = end_time - time.monotonic()
            if remaining <= 0:
                return False

            acquired = self._lock.acquire(timeout=remaining)
            if acquired:
                self._owner = thread_id
                self._acquire_count += 1
                return True

    def release(self) -> None:
        """Release the lock."""
        with self._lock:
            if self._owner == threading.get_ident():
                self._acquire_count -= 1
                if self._acquire_count == 0:
                    self._owner = None
                    self._lock.release()

    @property
    def is_locked(self) -> bool:
        """Check if lock is held."""
        with self._lock:
            return self._owner is not None

    def __enter__(self) -> "TimedLock":
        """Context manager entry."""
        self.acquire()
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit."""
        self.release()


class SpinLock:
    """
    Simple spin lock for fine-grained concurrency.

    Uses busy-waiting instead of kernel synchronization,
    suitable for very short critical sections.
    """

    def __init__(self) -> None:
        """Initialize the spin lock."""
        self._locked = False
        self._lock = threading.Lock()

    def acquire(self, blocking: bool = True) -> bool:
        """
        Acquire the spin lock.

        Args:
            blocking: If True, spin until acquired.

        Returns:
            True if lock was acquired.
        """
        if not blocking:
            if self._lock.acquire(False):
                self._locked = True
                return True
            return False

        while True:
            if self._lock.acquire(False):
                self._locked = True
                return True

    def release(self) -> None:
        """Release the spin lock."""
        self._locked = False
        self._lock.release()

    def __enter__(self) -> "SpinLock":
        """Context manager entry."""
        self.acquire()
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit."""
        self.release()


def create_semaphore(
    value: int,
    weighted: bool = False,
    **kwargs
) -> Any:
    """
    Factory function to create a semaphore.

    Args:
        value: Initial permit count.
        weighted: Use weighted semaphore.
        **kwargs: Additional arguments.

    Returns:
        Semaphore instance.
    """
    if weighted:
        return WeightedSemaphore(value, **kwargs)
    return threading.Semaphore(value)
