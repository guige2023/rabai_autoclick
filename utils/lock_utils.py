"""Lock utilities for RabAI AutoClick.

Provides:
- Distributed-style locking primitives
- Lock timeout and retry helpers
- Read-write lock implementations
"""

import threading
import time
from contextlib import contextmanager
from typing import (
    Callable,
    Generator,
    Optional,
)


class TimeoutLock:
    """Lock with timeout support."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._owner: Optional[int] = None
        self._count = 0

    def acquire(self, timeout: Optional[float] = None) -> bool:
        """Acquire lock with timeout.

        Args:
            timeout: Max time to wait (None = wait forever).

        Returns:
            True if acquired.
        """
        if timeout is None:
            self._lock.acquire()
            self._owner = threading.get_ident()
            self._count += 1
            return True

        start = time.monotonic()
        while True:
            if self._lock.acquire(blocking=False):
                self._owner = threading.get_ident()
                self._count += 1
                return True
            if time.monotonic() - start >= timeout:
                return False
            time.sleep(0.01)

    def release(self) -> None:
        """Release lock."""
        self._lock.release()
        self._count -= 1
        if self._count == 0:
            self._owner = None

    def locked(self) -> bool:
        """Check if lock is held."""
        return self._lock.locked()

    def __enter__(self) -> "TimeoutLock":
        self.acquire()
        return self

    def __exit__(self, *args: Any) -> None:
        self.release()


class RLock:
    """Reentrant lock with timeout."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._owner: Optional[int] = None
        self._count = 0

    def acquire(self, timeout: Optional[float] = None) -> bool:
        """Acquire lock with timeout."""
        ident = threading.get_ident()

        if self._owner == ident:
            self._count += 1
            return True

        if timeout is None:
            self._lock.acquire()
            self._owner = ident
            self._count = 1
            return True

        start = time.monotonic()
        while True:
            if self._lock.acquire(blocking=False):
                self._owner = ident
                self._count = 1
                return True
            if time.monotonic() - start >= timeout:
                return False
            time.sleep(0.01)

    def release(self) -> None:
        """Release lock."""
        if self._owner == threading.get_ident():
            self._count -= 1
            if self._count == 0:
                self._owner = None
                self._lock.release()

    def locked(self) -> bool:
        """Check if lock is held by current thread."""
        return self._owner == threading.get_ident()

    def __enter__(self) -> "RLock":
        self.acquire()
        return self

    def __exit__(self, *args: Any) -> None:
        self.release()


class Semaphore:
    """Counting semaphore."""

    def __init__(self, value: int = 1) -> None:
        self._sem = threading.Semaphore(value)

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Acquire semaphore."""
        return self._sem.acquire(blocking=blocking, timeout=timeout)

    def release(self) -> None:
        """Release semaphore."""
        self._sem.release()

    def __enter__(self) -> "Semaphore":
        self._sem.acquire()
        return self

    def __exit__(self, *args: Any) -> None:
        self._sem.release()


class Condition:
    """Condition variable with timeout."""

    def __init__(self, lock: Optional[threading.Lock] = None) -> None:
        self._cond = threading.Condition(lock)

    def wait(self, timeout: Optional[float] = None) -> bool:
        """Wait for condition."""
        return self._cond.wait(timeout=timeout)

    def notify(self) -> None:
        """Notify one waiter."""
        self._cond.notify()

    def notify_all(self) -> None:
        """Notify all waiters."""
        self._cond.notify_all()

    def acquire(self) -> None:
        """Acquire underlying lock."""
        self._cond.acquire()

    def release(self) -> None:
        """Release underlying lock."""
        self._cond.release()

    def __enter__(self) -> "Condition":
        self._cond.acquire()
        return self

    def __exit__(self, *args: Any) -> None:
        self._cond.release()


class Barrier:
    """Threading barrier for synchronization."""

    def __init__(self, parties: int) -> None:
        self._barrier = threading.Barrier(parties)

    def wait(self, timeout: Optional[float] = None) -> int:
        """Wait for all parties."""
        return self._barrier.wait(timeout=timeout)

    def reset(self) -> None:
        """Reset the barrier."""
        self._barrier.reset()

    def abort(self) -> None:
        """Abort the barrier."""
        self._barrier.abort()


@contextmanager
def lock_with_timeout(
    lock: threading.Lock,
    timeout: float,
) -> Generator[bool, None, None]:
    """Context manager for acquiring lock with timeout.

    Args:
        lock: Lock to acquire.
        timeout: Max time to wait.

    Yields:
        True if acquired, False if timed out.
    """
    timeout_lock = TimeoutLock()
    acquired = timeout_lock.acquire(timeout=timeout)
    try:
        yield acquired
    finally:
        if acquired:
            timeout_lock.release()


def retry_with_lock(
    lock: threading.Lock,
    func: Callable[[], T],
    max_attempts: int = 3,
    delay: float = 0.1,
) -> T:
    """Retry a function while holding a lock.

    Args:
        lock: Lock to hold.
        func: Function to execute.
        max_attempts: Max retry attempts.
        delay: Delay between retries.

    Returns:
        Function result.

    Raises:
        Last exception if all attempts fail.
    """
    last_exc: Optional[Exception] = None
    for _ in range(max_attempts):
        try:
            with lock:
                return func()
        except Exception as e:
            last_exc = e
            time.sleep(delay)
    if last_exc:
        raise last_exc
    raise RuntimeError("Unexpected retry failure")


class LockPool:
    """Pool of locks for different resources."""

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._lock = threading.Lock()

    def get_lock(self, resource: str) -> threading.Lock:
        """Get or create a lock for a resource.

        Args:
            resource: Resource identifier.

        Returns:
            Lock for the resource.
        """
        with self._lock:
            if resource not in self._locks:
                self._locks[resource] = threading.Lock()
            return self._locks[resource]

    def acquire(self, resource: str) -> threading.Lock:
        """Acquire lock for a resource.

        Args:
            resource: Resource identifier.

        Returns:
            Acquired lock.
        """
        lock = self.get_lock(resource)
        lock.acquire()
        return lock

    def release(self, resource: str) -> None:
        """Release lock for a resource.

        Args:
            resource: Resource identifier.
        """
        with self._lock:
            lock = self._locks.get(resource)
        if lock:
            lock.release()


class TimedSemaphore:
    """Semaphore that resets after a time period."""

    def __init__(
        self,
        value: int,
        period: float = 1.0,
    ) -> None:
        """Initialize timed semaphore.

        Args:
            value: Max count.
            period: Reset period in seconds.
        """
        self._sem = threading.Semaphore(value)
        self._value = value
        self._period = period
        self._last_reset = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Acquire with automatic reset."""
        self._check_reset()
        return self._sem.acquire(blocking=blocking, timeout=timeout)

    def _check_reset(self) -> None:
        """Check if semaphore should reset."""
        with self._lock:
            if time.monotonic() - self._last_reset >= self._period:
                # Release all permits and reset
                while self._sem.acquire(blocking=False):
                    pass
                for _ in range(self._value):
                    self._sem.release()
                self._last_reset = time.monotonic()

    def release(self) -> None:
        """Release semaphore."""
        self._sem.release()
