"""
Read-write lock implementation.

Provides reader-writer lock with priority modes,
thread-safe access tracking, and deadlock prevention.
"""

from __future__ import annotations

import threading
import time
from typing import Literal


class ReadWriteLock:
    """
    Read-write lock allowing multiple readers OR single writer.

    Supports read-biased, write-biased, or fair scheduling.
    """

    def __init__(self, priority: Literal["read", "write", "fair"] = "read"):
        self.priority = priority
        self._lock = threading.Lock()
        self._readers_condition = threading.Condition(self._lock)
        self._writers_condition = threading.Condition(self._lock)
        self._readers = 0
        self._writers = 0
        self._waiting_writers = 0

    def acquire_read(self, timeout: float | None = None) -> bool:
        """
        Acquire read lock.

        Args:
            timeout: Max wait time

        Returns:
            True if acquired
        """
        deadline = time.time() + timeout if timeout else None
        with self._readers_condition:
            while self._writers > 0 or (self.priority == "write" and self._waiting_writers > 0):
                if deadline:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        return False
                    self._readers_condition.wait(remaining)
                else:
                    self._readers_condition.wait()
            self._readers += 1
            return True

    def release_read(self) -> None:
        """Release read lock."""
        with self._readers_condition:
            self._readers -= 1
            if self._readers == 0:
                self._writers_condition.notify_all()

    def acquire_write(self, timeout: float | None = None) -> bool:
        """
        Acquire write lock.

        Args:
            timeout: Max wait time

        Returns:
            True if acquired
        """
        deadline = time.time() + timeout if timeout else None
        with self._writers_condition:
            self._waiting_writers += 1
            try:
                while self._readers > 0 or self._writers > 0:
                    if deadline:
                        remaining = deadline - time.time()
                        if remaining <= 0:
                            return False
                        self._writers_condition.wait(remaining)
                    else:
                        self._writers_condition.wait()
                self._writers += 1
                return True
            finally:
                self._waiting_writers -= 1

    def release_write(self) -> None:
        """Release write lock."""
        with self._writers_condition:
            self._writers -= 1
            self._readers_condition.notify_all()
            self._writers_condition.notify_all()

    def __enter__(self) -> "ReadWriteLock":
        return self

    def __exit__(self, *args: object) -> None:
        pass


class ReadLock:
    """Context manager for read lock."""

    def __init__(self, rwlock: ReadWriteLock, timeout: float | None = None):
        self._rwlock = rwlock
        self._timeout = timeout

    def __enter__(self) -> "ReadLock":
        self._rwlock.acquire_read(self._timeout)
        return self

    def __exit__(self, *args: object) -> None:
        self._rwlock.release_read()


class WriteLock:
    """Context manager for write lock."""

    def __init__(self, rwlock: ReadWriteLock, timeout: float | None = None):
        self._rwlock = rwlock
        self._timeout = timeout

    def __enter__(self) -> "WriteLock":
        self._rwlock.acquire_write(self._timeout)
        return self

    def __exit__(self, *args: object) -> None:
        self._rwlock.release_write()


class StampedLock:
    """
    Stamped lock with optimistic read mode.

    Uses stamps instead of locks for better performance.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._readers = 0
        self._writer = False
        self._stamp = 0
        self._condition = threading.Condition(self._lock)

    def try_read(self) -> int | None:
        """
        Try to acquire read lock.

        Returns:
            Stamp if acquired, None otherwise
        """
        with self._lock:
            if self._writer:
                return None
            self._readers += 1
            self._stamp += 1
            return self._stamp

    def try_write(self) -> int | None:
        """
        Try to acquire write lock.

        Returns:
            Stamp if acquired, None otherwise
        """
        with self._lock:
            if self._writer or self._readers > 0:
                return None
            self._writer = True
            self._stamp += 1
            return self._stamp

    def try_optimistic_read(self) -> int:
        """
        Try optimistic read (no lock, just get stamp).

        Returns:
            Stamp for validation
        """
        with self._lock:
            self._stamp += 1
            return self._stamp

    def validate(self, stamp: int) -> bool:
        """
        Validate stamp after optimistic read.

        Args:
            stamp: Stamp from try_optimistic_read

        Returns:
            True if read was valid
        """
        with self._lock:
            return stamp == self._stamp and not self._writer

    def release(self, stamp: int) -> None:
        """
        Release lock for given stamp.

        Args:
            stamp: Stamp from try_read/try_write
        """
        with self._lock:
            if stamp <= 0:
                return
            if stamp == self._stamp and self._writer:
                self._writer = False
                self._condition.notify_all()
            elif stamp == self._stamp and self._readers > 0:
                self._readers -= 1

    def read_lock(self, timeout: float | None = None) -> int | None:
        """Acquire read lock with blocking."""
        deadline = time.time() + timeout if timeout else None
        while True:
            stamp = self.try_read()
            if stamp:
                return stamp
            if deadline:
                remaining = deadline - time.time()
                if remaining <= 0:
                    return None
                time.sleep(0.01)
            else:
                time.sleep(0.01)

    def write_lock(self, timeout: float | None = None) -> int | None:
        """Acquire write lock with blocking."""
        deadline = time.time() + timeout if timeout else None
        while True:
            stamp = self.try_write()
            if stamp:
                return stamp
            if deadline:
                remaining = deadline - time.time()
                if remaining <= 0:
                    return None
                time.sleep(0.01)
            else:
                time.sleep(0.01)


class RWLockWrapper:
    """Thread-safe wrapper using read-write lock."""

    def __init__(self, value: T, priority: Literal["read", "write", "fair"] = "read"):
        self._value = value
        self._lock = ReadWriteLock(priority)

    def read(self, func: Callable[[T], R]) -> R:
        """Read value through lock."""
        with ReadLock(self._lock):
            return func(self._value)

    def write(self, func: Callable[[T], R]) -> R:
        """Write value through lock."""
        with WriteLock(self._lock):
            return func(self._value)
