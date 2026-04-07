"""Locking utilities for RabAI AutoClick.

Provides:
- Reentrant lock
- Read-write lock
- Semaphore
- Critical section
"""

import threading
import time
from typing import Optional


class ReentrantLock:
    """Reentrant lock that allows same thread to acquire multiple times."""

    def __init__(self) -> None:
        """Initialize lock."""
        self._lock = threading.Lock()
        self._owner: Optional[int] = None
        self._count = 0

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquire lock.

        Args:
            blocking: Wait for lock if True.
            timeout: Timeout in seconds.

        Returns:
            True if acquired.
        """
        thread_id = threading.current_thread().ident

        if self._owner == thread_id:
            self._count += 1
            return True

        if self._lock.acquire(blocking=blocking, timeout=timeout if timeout > 0 else -1):
            self._owner = thread_id
            self._count = 1
            return True

        return False

    def release(self) -> None:
        """Release lock."""
        if self._owner == threading.current_thread().ident:
            self._count -= 1
            if self._count == 0:
                self._owner = None
                self._lock.release()

    def __enter__(self) -> "ReentrantLock":
        self.acquire()
        return self

    def __exit__(self, *args: any) -> None:
        self.release()


class ReadWriteLock:
    """Read-write lock allowing multiple readers or single writer."""

    def __init__(self) -> None:
        """Initialize lock."""
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0
        self._writers_waiting = 0
        self._writer_active = False

    def acquire_read(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquire read lock.

        Args:
            blocking: Wait for lock if True.
            timeout: Timeout in seconds.

        Returns:
            True if acquired.
        """
        deadline = time.time() + timeout if timeout > 0 else None

        with self._read_ready:
            while self._writer_active or self._writers_waiting > 0:
                if not blocking:
                    return False
                if timeout > 0 and time.time() >= deadline:
                    return False
                self._read_ready.wait(timeout=timeout if timeout > 0 else 1.0)

            self._readers += 1
            return True

    def release_read(self) -> None:
        """Release read lock."""
        with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notify_all()

    def acquire_write(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquire write lock.

        Args:
            blocking: Wait for lock if True.
            timeout: Timeout in seconds.

        Returns:
            True if acquired.
        """
        deadline = time.time() + timeout if timeout > 0 else None

        with self._read_ready:
            self._writers_waiting += 1
            try:
                while self._readers > 0 or self._writer_active:
                    if not blocking:
                        return False
                    if timeout > 0 and time.time() >= deadline:
                        return False
                    self._read_ready.wait(timeout=timeout if timeout > 0 else 1.0)

                self._writer_active = True
                return True
            finally:
                self._writers_waiting -= 1

    def release_write(self) -> None:
        """Release write lock."""
        with self._read_ready:
            self._writer_active = False
            self._read_ready.notify_all()

    def __enter__(self) -> "ReadWriteLock":
        self.acquire_write()
        return self

    def __exit__(self, *args: any) -> None:
        self.release_write()


class Semaphore:
    """Counting semaphore."""

    def __init__(self, value: int = 1) -> None:
        """Initialize semaphore.

        Args:
            value: Initial count.
        """
        self._semaphore = threading.Semaphore(value)

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquire semaphore.

        Args:
            blocking: Wait if True.
            timeout: Timeout in seconds.

        Returns:
            True if acquired.
        """
        return self._semaphore.acquire(blocking=blocking, timeout=timeout)

    def release(self) -> None:
        """Release semaphore."""
        self._semaphore.release()

    def __enter__(self) -> "Semaphore":
        self.acquire()
        return self

    def __exit__(self, *args: any) -> None:
        self.release()


class CriticalSection:
    """Critical section with automatic lock management."""

    def __init__(self, lock: Optional[threading.Lock] = None) -> None:
        """Initialize critical section.

        Args:
            lock: Optional lock to use.
        """
        self._lock = lock or threading.Lock()

    def enter(self) -> None:
        """Enter critical section."""
        self._lock.acquire()

    def leave(self) -> None:
        """Leave critical section."""
        self._lock.release()

    def __enter__(self) -> "CriticalSection":
        self.enter()
        return self

    def __exit__(self, *args: any) -> None:
        self.leave()


class LockManager:
    """Manage multiple named locks."""

    def __init__(self) -> None:
        """Initialize manager."""
        self._locks: dict = {}
        self._lock = threading.Lock()

    def get_lock(self, name: str) -> threading.Lock:
        """Get or create named lock.

        Args:
            name: Lock name.

        Returns:
            Lock object.
        """
        with self._lock:
            if name not in self._locks:
                self._locks[name] = threading.Lock()
            return self._locks[name]

    def remove_lock(self, name: str) -> bool:
        """Remove named lock.

        Args:
            name: Lock name.

        Returns:
            True if lock existed.
        """
        with self._lock:
            if name in self._locks:
                del self._locks[name]
                return True
            return False

    def clear(self) -> None:
        """Clear all locks."""
        with self._lock:
            self._locks.clear()


class InterProcessLock:
    """File-based lock for cross-process synchronization."""

    def __init__(self, name: str) -> None:
        """Initialize lock.

        Args:
            name: Lock name.
        """
        self._name = name
        self._lock_file = f"/tmp/rabai_lock_{name}.lock"
        self._fd = None

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquire lock.

        Args:
            blocking: Wait for lock.
            timeout: Timeout in seconds.

        Returns:
            True if acquired.
        """
        import os
        import fcntl

        try:
            self._fd = open(self._lock_file, "w")
            fcntl.flock(self._fd.fileno(), fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))

            if timeout > 0:
                # Would need threading for actual timeout
                pass

            return True
        except Exception:
            if self._fd:
                self._fd.close()
                self._fd = None
            return False

    def release(self) -> None:
        """Release lock."""
        import fcntl

        if self._fd:
            try:
                fcntl.flock(self._fd.fileno(), fcntl.LOCK_UN)
                self._fd.close()
            except Exception:
                pass
            finally:
                self._fd = None

    def __enter__(self) -> "InterProcessLock":
        self.acquire()
        return self

    def __exit__(self, *args: any) -> None:
        self.release()
