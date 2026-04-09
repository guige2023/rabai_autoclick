"""
Read-write lock utilities for concurrent access control.

Provides multiple RW lock implementations optimized for different scenarios:
- Basic RW lock for general use
- Priority RW lock for read or write preference
- Upgradeable RW lock allowing read-to-write transitions

Example:
    >>> from rw_lock_utils import ReadWriteLock, PriorityRWLock
    >>> rwlock = ReadWriteLock()
    >>> with rwlock.read_lock():
    ...     # multiple readers allowed
    ...     pass
    >>> with rwlock.write_lock():
    ...     # exclusive access
    ...     pass
"""

from __future__ import annotations

import asyncio
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Callable, Deque, List, Optional


# =============================================================================
# Exceptions
# =============================================================================


class RWLockError(Exception):
    """Base exception for RW lock errors."""
    pass


class RWLockTimeoutError(RWLockError):
    """Raised when lock acquisition times out."""
    pass


class LockUpgradeError(RWLockError):
    """Raised when upgrading a read lock to write lock fails."""
    pass


# =============================================================================
# Read-Write Lock (Basic)
# =============================================================================


@dataclass
class ReadWriteLock:
    """
    A basic read-write lock implementation.

    Allows multiple concurrent readers OR a single exclusive writer.
    Uses condition variables for efficient waiting.

    Attributes:
        _lock: Main lock protecting internal state.
        _condition: Condition for writer waiting.
        _readers: Current number of active readers.
        _writers_waiting: Number of writers waiting.
        _writer_active: Whether a writer holds the lock.
    """

    _lock: threading.Lock = field(default_factory=threading.Lock)
    _condition: threading.Condition = field(
        default_factory=lambda: threading.Condition(threading.Lock())
    )
    _readers: int = field(default=0)
    _writers_waiting: int = field(default=0)
    _writer_active: bool = field(default=False)

    def acquire_read(self, timeout: Optional[float] = None) -> bool:
        """
        Acquire a read lock.

        Args:
            timeout: Maximum seconds to wait.

        Returns:
            True if acquisition succeeded.
        """
        deadline = None if timeout is None else time.monotonic() + timeout

        with self._lock:
            while self._writer_active or self._writers_waiting > 0:
                remaining = deadline - time.monotonic() if deadline else None
                if remaining is not None and remaining <= 0:
                    return False
                notified = self._condition.wait(timeout=remaining)
                if not notified and deadline and time.monotonic() >= deadline:
                    return False
            self._readers += 1
            return True

    def acquire_write(self, timeout: Optional[float] = None) -> bool:
        """
        Acquire a write lock.

        Args:
            timeout: Maximum seconds to wait.

        Returns:
            True if acquisition succeeded.
        """
        deadline = None if timeout is None else time.monotonic() + timeout

        with self._lock:
            self._writers_waiting += 1
            try:
                while self._readers > 0 or self._writer_active:
                    remaining = deadline - time.monotonic() if deadline else None
                    if remaining is not None and remaining <= 0:
                        return False
                    notified = self._condition.wait(timeout=remaining)
                    if not notified and deadline and time.monotonic() >= deadline:
                        return False
                self._writer_active = True
                return True
            finally:
                self._writers_waiting -= 1

    def release_read(self) -> None:
        """Release a read lock."""
        with self._lock:
            self._readers = max(0, self._readers - 1)
            if self._readers == 0:
                self._condition.notify_all()

    def release_write(self) -> None:
        """Release a write lock."""
        with self._lock:
            self._writer_active = False
            self._condition.notify_all()

    @contextmanager
    def read_lock(self, timeout: Optional[float] = None):
        """
        Context manager for read locking.

        Example:
            >>> rwlock = ReadWriteLock()
            >>> with rwlock.read_lock():
            ...     value = self.data
        """
        if not self.acquire_read(timeout=timeout):
            raise RWLockTimeoutError(
                f"Failed to acquire read lock within {timeout}s"
            )
        try:
            yield
        finally:
            self.release_read()

    @contextmanager
    def write_lock(self, timeout: Optional[float] = None):
        """
        Context manager for write locking.

        Example:
            >>> rwlock = ReadWriteLock()
            >>> with rwlock.write_lock():
            ...     self.data = new_value
        """
        if not self.acquire_write(timeout=timeout):
            raise RWLockTimeoutError(
                f"Failed to acquire write lock within {timeout}s"
            )
        try:
            yield
        finally:
            self.release_write()


# =============================================================================
# Priority Read-Write Lock
# =============================================================================


@dataclass
class PriorityRWLock:
    """
    A read-write lock with configurable priority.

    Supports reader priority, writer priority, or fair (alternating) mode.

    Attributes:
        mode: 'readers' (reader priority), 'writers' (writer priority),
              or 'fair' (alternating).
    """

    mode: str = "writers"
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _condition: threading.Condition = field(
        default_factory=lambda: threading.Condition(threading.Lock())
    )
    _readers: int = field(default=0)
    _writers_waiting: int = field(default=0)
    _writer_active: bool = field(default=False)
    _fair_read_turn: bool = field(default=True)

    def _can_read(self) -> bool:
        """Check if a reader can acquire."""
        if self._writer_active:
            return False
        if self.mode == "writers" and self._writers_waiting > 0:
            return False
        if self.mode == "fair":
            return self._fair_read_turn or self._writers_waiting == 0
        return True

    def _can_write(self) -> bool:
        """Check if a writer can acquire."""
        if self._writer_active:
            return False
        if self._readers > 0:
            return False
        if self.mode == "readers" and self._writers_waiting > 0:
            return False
        if self.mode == "fair":
            return not self._fair_read_turn or self._readers == 0
        return True

    def acquire_read(self, timeout: Optional[float] = None) -> bool:
        """
        Acquire a read lock with priority semantics.

        Args:
            timeout: Maximum seconds to wait.

        Returns:
            True if acquisition succeeded.
        """
        deadline = None if timeout is None else time.monotonic() + timeout

        with self._lock:
            while not self._can_read():
                remaining = deadline - time.monotonic() if deadline else None
                if remaining is not None and remaining <= 0:
                    return False
                self._condition.wait(timeout=remaining if remaining else None)
            self._readers += 1
            return True

    def acquire_write(self, timeout: Optional[float] = None) -> bool:
        """
        Acquire a write lock with priority semantics.

        Args:
            timeout: Maximum seconds to wait.

        Returns:
            True if acquisition succeeded.
        """
        deadline = None if timeout is None else time.monotonic() + timeout

        with self._lock:
            self._writers_waiting += 1
            try:
                while not self._can_write():
                    remaining = deadline - time.monotonic() if deadline else None
                    if remaining is not None and remaining <= 0:
                        return False
                    self._condition.wait(timeout=remaining if remaining else None)
                self._writer_active = True
                self._fair_read_turn = False
                return True
            finally:
                self._writers_waiting -= 1

    def release_read(self) -> None:
        """Release a read lock."""
        with self._lock:
            self._readers = max(0, self._readers - 1)
            if self._readers == 0:
                self._condition.notify_all()

    def release_write(self) -> None:
        """Release a write lock."""
        with self._lock:
            self._writer_active = False
            if self.mode == "fair":
                self._fair_read_turn = True
            self._condition.notify_all()

    @contextmanager
    def read_lock(self, timeout: Optional[float] = None):
        """Context manager for read locking."""
        if not self.acquire_read(timeout=timeout):
            raise RWLockTimeoutError(f"Read lock timeout after {timeout}s")
        try:
            yield
        finally:
            self.release_read()

    @contextmanager
    def write_lock(self, timeout: Optional[float] = None):
        """Context manager for write locking."""
        if not self.acquire_write(timeout=timeout):
            raise RWLockTimeoutError(f"Write lock timeout after {timeout}s")
        try:
            yield
        finally:
            self.release_write()


# =============================================================================
# Upgradeable Read-Write Lock
# =============================================================================


@dataclass
class UpgradeableRWLock:
    """
    A read-write lock that supports upgrading a read lock to write.

    A thread holding a read lock can upgrade to a write lock, merging
    the two locks atomically without releasing and re-acquiring.

    Attributes:
        _rwlock: Underlying read-write lock.
        _upgrade_lock: Thread-local lock for upgrade tracking.
        _upgrade_owner: ID of the thread that upgraded.
        _read_count_at_upgrade: Reader count when upgrade occurred.
    """

    _rwlock: ReadWriteLock = field(default_factory=ReadWriteLock)
    _thread_local: threading.local = field(default_factory=threading.local)
    _upgrade_owner: int = field(default=0)
    _read_count_at_upgrade: int = field(default=0)

    def acquire_read(self, timeout: Optional[float] = None) -> bool:
        """Acquire a read lock."""
        return self._rwlock.acquire_read(timeout=timeout)

    def acquire_upgrade(self, timeout: Optional[float] = None) -> bool:
        """
        Upgrade the current read lock to a write lock.

        Must be called while holding a read lock. Atomically releases
        the read lock and acquires a write lock.

        Args:
            timeout: Maximum seconds to wait for the write lock.

        Returns:
            True if upgrade succeeded.

        Raises:
            LockUpgradeError: If not holding a read lock or upgrade fails.
        """
        tid = threading.get_ident()
        with self._rwlock._lock:
            if self._upgrade_owner == tid:
                return True
            if self._upgrade_owner != 0:
                raise LockUpgradeError(
                    "Another thread has already upgraded"
                )

        self._rwlock.release_read()

        deadline = None if timeout is None else time.monotonic() + timeout
        with self._rwlock._lock:
            while self._rwlock._readers > 1 or self._rwlock._writer_active:
                remaining = deadline - time.monotonic() if deadline else None
                if remaining is not None and remaining <= 0:
                    raise LockUpgradeError("Upgrade timed out")
                self._rwlock._condition.wait(
                    timeout=remaining if remaining else None
                )

        with self._rwlock._lock:
            self._upgrade_owner = tid
            self._read_count_at_upgrade = self._rwlock._readers
            self._rwlock._writer_active = True
            self._rwlock._readers = 0
            self._rwlock._condition.notify_all()
            return True

    def release_upgrade(self) -> None:
        """Downgrade from write lock back to read lock."""
        tid = threading.get_ident()
        with self._rwlock._lock:
            if self._upgrade_owner != tid:
                return
            self._rwlock._writer_active = False
            self._rwlock._readers = self._read_count_at_upgrade
            self._upgrade_owner = 0
            self._rwlock._condition.notify_all()

    def release_read(self) -> None:
        """Release a read lock."""
        if self._upgrade_owner == threading.get_ident():
            self.release_upgrade()
        else:
            self._rwlock.release_read()

    def release_write(self) -> None:
        """Release a write lock."""
        self._rwlock.release_write()

    @contextmanager
    def read_lock(self, timeout: Optional[float] = None):
        """Context manager for read locking."""
        if not self.acquire_read(timeout=timeout):
            raise RWLockTimeoutError(f"Read lock timeout after {timeout}s")
        try:
            yield
        finally:
            self.release_read()

    @contextmanager
    def write_lock(self, timeout: Optional[float] = None):
        """Context manager for write locking (direct write, no upgrade)."""
        if not self._rwlock.acquire_write(timeout=timeout):
            raise RWLockTimeoutError(f"Write lock timeout after {timeout}s")
        try:
            yield
        finally:
            self._rwlock.release_write()
