"""Read-Write lock utilities.

Provides read-write lock for managing concurrent
read and write access to shared resources.
"""

import threading
from typing import Optional


class ReadWriteLock:
    """Read-Write lock implementation.

    Multiple readers can hold the lock simultaneously,
    but writers get exclusive access.

    Example:
        rwlock = ReadWriteLock()
        with rwlock.read_lock():
            # read shared data
            data = self._data
        with rwlock.write_lock():
            # write exclusive access
            self._data = new_data
    """

    def __init__(self) -> None:
        self._readers = 0
        self._writers_waiting = 0
        self._writer_active = False
        self._lock = threading.Lock()
        self._readers_condition = threading.Condition(self._lock)
        self._writers_condition = threading.Condition(self._lock)

    class ReadLock:
        """Read lock context manager."""

        def __init__(self, rwlock: "ReadWriteLock") -> None:
            self._rwlock = rwlock

        def __enter__(self) -> None:
            self._rwlock.acquire_read()

        def __exit__(self, exc_type, exc_val, exc_tb) -> None:
            self._rwlock.release_read()

    class WriteLock:
        """Write lock context manager."""

        def __init__(self, rwlock: "ReadWriteLock") -> None:
            self._rwlock = rwlock

        def __enter__(self) -> None:
            self._rwlock.acquire_write()

        def __exit__(self, exc_type, exc_val, exc_tb) -> None:
            self._rwlock.release_write()

    def read_lock(self) -> ReadLock:
        """Acquire read lock.

        Returns:
            ReadLock context manager.
        """
        return self.ReadLock(self)

    def write_lock(self) -> WriteLock:
        """Acquire write lock.

        Returns:
            WriteLock context manager.
        """
        return self.WriteLock(self)

    def acquire_read(self) -> None:
        """Acquire read lock.

        Multiple readers can hold simultaneously.
        Writers wait for all readers to finish.
        """
        with self._lock:
            while self._writer_active or self._writers_waiting > 0:
                self._readers_condition.wait()
            self._readers += 1

    def release_read(self) -> None:
        """Release read lock."""
        with self._lock:
            self._readers -= 1
            if self._readers == 0:
                self._writers_condition.notify()

    def acquire_write(self) -> None:
        """Acquire write lock.

        Exclusive access - waits for all readers and
        other writers to finish.
        """
        with self._lock:
            self._writers_waiting += 1
            while self._readers > 0 or self._writer_active:
                self._writers_condition.wait()
            self._writers_waiting -= 1
            self._writer_active = True

    def release_write(self) -> None:
        """Release write lock."""
        with self._lock:
            self._writer_active = False
            self._readers_condition.notify_all()
            self._writers_condition.notify()

    @property
    def readers(self) -> int:
        """Get number of active readers."""
        with self._lock:
            return self._readers

    @property
    def writers_waiting(self) -> int:
        """Get number of waiting writers."""
        with self._lock:
            return self._writers_waiting

    @property
    def writer_active(self) -> bool:
        """Check if writer is active."""
        with self._lock:
            return self._writer_active


class ReadLock:
    """Read lock context manager."""

    def __init__(self, lock: ReadWriteLock) -> None:
        self._lock = lock

    def __enter__(self) -> "ReadLock":
        self._lock.acquire_read()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._lock.release_read()


class WriteLock:
    """Write lock context manager."""

    def __init__(self, lock: ReadWriteLock) -> None:
        self._lock = lock

    def __enter__(self) -> "WriteLock":
        self._lock.acquire_write()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._lock.release_write()


class RWLock:
    """Alias for ReadWriteLock for convenience."""

    def __init__(self) -> None:
        self._lock = ReadWriteLock()

    def read_lock(self) -> ReadLock:
        return self._lock.read_lock()

    def write_lock(self) -> WriteLock:
        return self._lock.write_lock()


class TryReadLock:
    """Try-read lock (non-blocking)."""

    def __init__(self, lock: ReadWriteLock) -> None:
        self._lock = lock
        self._acquired = False

    def __enter__(self) -> "TryReadLock":
        with self._lock._lock:
            if not self._lock._writer_active and self._lock._writers_waiting == 0:
                self._lock._readers += 1
                self._acquired = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._acquired:
            self._lock.release_read()
            self._acquired = False

    @property
    def acquired(self) -> bool:
        return self._acquired


class TryWriteLock:
    """Try-write lock (non-blocking)."""

    def __init__(self, lock: ReadWriteLock) -> None:
        self._lock = lock
        self._acquired = False

    def __enter__(self) -> "TryWriteLock":
        with self._lock._lock:
            if self._lock._readers == 0 and not self._lock._writer_active:
                self._lock._writers_waiting += 1
                self._lock._writer_active = True
                self._acquired = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._acquired:
            self._lock.release_write()
            self._acquired = False

    @property
    def acquired(self) -> bool:
        return self._acquired
