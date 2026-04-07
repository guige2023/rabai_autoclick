"""semaphore action module for rabai_autoclick.

Provides semaphore-based concurrency primitives: counting semaphores,
bounded semaphores, binary semaphores, read-write locks, and
rate limiters with token bucket algorithm.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional

__all__ = [
    "Semaphore",
    "BoundedSemaphore",
    "BinarySemaphore",
    "ReadWriteLock",
    "ReadLock",
    "WriteLock",
    "TokenBucket",
    "TokenBucketRateLimiter",
    "TicketLock",
    "Turnstile",
    "Multiplex",
    "Switches",
]


class Semaphore:
    """Classic counting semaphore.

    Manages a counter that can be incremented and decremented.
    Acquire blocks when counter is zero.
    """

    def __init__(self, value: int = 1) -> None:
        if value < 0:
            raise ValueError("Semaphore initial value must be non-negative")
        self._value = value
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._waiters = 0

    def acquire(self, timeout: Optional[float] = None) -> bool:
        """Acquire one unit from the semaphore.

        Args:
            timeout: Max seconds to wait (None = infinite).

        Returns:
            True if acquired, False on timeout.
        """
        with self._cond:
            while self._value == 0:
                self._waiters += 1
                if not self._cond.wait(timeout=timeout):
                    self._waiters -= 1
                    return False
                self._waiters -= 1
            self._value -= 1
            return True

    def release(self) -> None:
        """Release one unit back to the semaphore."""
        with self._cond:
            self._value += 1
            if self._waiters > 0:
                self._cond.notify()

    def __enter__(self) -> "Semaphore":
        self.acquire()
        return self

    def __exit__(self, *args: Any) -> None:
        self.release()

    @property
    def value(self) -> int:
        return self._value

    @property
    def waiters(self) -> int:
        return self._waiters


class BoundedSemaphore(Semaphore):
    """Semaphore with an upper bound on the counter.

    Unlike unbounded Semaphore, release() raises if called more
    times than acquire() without matching releases.
    """

    def __init__(self, value: int = 1) -> None:
        super().__init__(value)
        self._max_value = value

    def release(self) -> None:
        with self._cond:
            if self._value >= self._max_value:
                raise ValueError("BoundedSemaphore release() overflow")
            self._value += 1
            if self._waiters > 0:
                self._cond.notify()


class BinarySemaphore:
    """Binary semaphore (mutex) implementation.

    Has only values 0 (locked) and 1 (unlocked).
    Guarantees fair ordering.
    """

    def __init__(self, value: int = 1) -> None:
        if value not in (0, 1):
            raise ValueError("BinarySemaphore value must be 0 or 1")
        self._value = value
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._queue: deque = deque()

    def acquire(self, timeout: Optional[float] = None) -> bool:
        """Acquire the lock (set to 0)."""
        with self._cond:
            while self._value == 0:
                if not self._cond.wait(timeout=timeout):
                    return False
            self._value = 0
            return True

    def release(self) -> None:
        """Release the lock (set to 1) and notify one waiter."""
        with self._cond:
            self._value = 1
            self._cond.notify()

    def __enter__(self) -> "BinarySemaphore":
        self.acquire()
        return self

    def __exit__(self, *args: Any) -> None:
        self.release()


class ReadWriteLock:
    """Reader-writer lock implementation.

    Allows multiple readers OR a single writer at a time.
    Writers are prioritized by default.
    """

    def __init__(self, writer_priority: bool = True) -> None:
        self._reader_count = 0
        self._writer_waiting = 0
        self._writer_active = False
        self._lock = threading.Lock()
        self._readers_ok = threading.Condition(self._lock)
        self._writers_ok = threading.Condition(self._lock)
        self._writer_priority = writer_priority

    def acquire_read(self, timeout: Optional[float] = None) -> bool:
        """Acquire read lock."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._lock:
            if self._writer_priority:
                while self._writer_active or self._writer_waiting > 0:
                    if timeout is not None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            return False
                        if not self._readers_ok.wait(timeout=remaining):
                            return False
                    else:
                        self._readers_ok.wait()
            else:
                while self._writer_active:
                    if timeout is not None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            return False
                        if not self._readers_ok.wait(timeout=remaining):
                            return False
                    else:
                        self._readers_ok.wait()
            self._reader_count += 1
            return True

    def release_read(self) -> None:
        """Release read lock."""
        with self._lock:
            self._reader_count -= 1
            if self._reader_count == 0 and self._writer_waiting > 0:
                self._writers_ok.notify()

    def acquire_write(self, timeout: Optional[float] = None) -> bool:
        """Acquire write lock."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._lock:
            self._writer_waiting += 1
            try:
                while self._reader_count > 0 or self._writer_active:
                    if timeout is not None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            return False
                        if not self._writers_ok.wait(timeout=remaining):
                            return False
                    else:
                        self._writers_ok.wait()
                self._writer_active = True
                return True
            finally:
                self._writer_waiting -= 1

    def release_write(self) -> None:
        """Release write lock."""
        with self._lock:
            self._writer_active = False
            self._readers_ok.notify_all()
            self._writers_ok.notify()

    @contextmanager
    def read_lock(self, timeout: Optional[float] = None):
        """Context manager for read lock."""
        self.acquire_read(timeout=timeout)
        try:
            yield
        finally:
            self.release_read()

    @contextmanager
    def write_lock(self, timeout: Optional[float] = None):
        """Context manager for write lock."""
        self.acquire_write(timeout=timeout)
        try:
            yield
        finally:
            self.release_write()


class ReadLock:
    """RAII read lock helper."""

    def __init__(self, rwlock: ReadWriteLock, timeout: Optional[float] = None) -> None:
        self._rwlock = rwlock
        self._timeout = timeout
        self._acquired = False

    def __enter__(self) -> "ReadLock":
        self._rwlock.acquire_read(timeout=self._timeout)
        self._acquired = True
        return self

    def __exit__(self, *args: Any) -> None:
        if self._acquired:
            self._rwlock.release_read()


class WriteLock:
    """RAII write lock helper."""

    def __init__(self, rwlock: ReadWriteLock, timeout: Optional[float] = None) -> None:
        self._rwlock = rwlock
        self._timeout = timeout
        self._acquired = False

    def __enter__(self) -> "WriteLock":
        self._rwlock.acquire_write(timeout=self._timeout)
        self._acquired = True
        return self

    def __exit__(self, *args: Any) -> None:
        if self._acquired:
            self._rwlock.release_write()


class TokenBucket:
    """Token bucket algorithm for rate limiting.

    Tokens are added at a constant rate up to capacity.
    Each acquire() consumes one token.
    """

    def __init__(self, capacity: int, refill_rate: float) -> None:
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        """Add tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._last_refill = now

    def acquire(self, tokens: int = 1, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Try to acquire tokens.

        Args:
            tokens: Number of tokens to acquire.
            blocking: Wait if not enough tokens available.
            timeout: Max wait time in seconds.

        Returns:
            True if tokens acquired, False otherwise.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._lock:
            while True:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return True
                if not blocking:
                    return False
                wait_time = (tokens - self._tokens) / self.refill_rate
                if timeout is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    wait_time = min(wait_time, remaining)
                time.sleep(wait_time)

    @property
    def available(self) -> float:
        """Current number of available tokens."""
        with self._lock:
            self._refill()
            return self._tokens


class TokenBucketRateLimiter:
    """Rate limiter using token bucket."""

    def __init__(self, rate: float, burst: int = 1) -> None:
        self.rate = rate
        self.burst = burst
        self._bucket = TokenBucket(capacity=burst, refill_rate=rate)

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Acquire permission to proceed."""
        return self._bucket.acquire(tokens=1, blocking=blocking, timeout=timeout)

    def __enter__(self) -> "TokenBucketRateLimiter":
        self.acquire()
        return self

    def __exit__(self, *args: Any) -> None:
        pass


class TicketLock:
    """Ticket lock implementation for fair ordering.

    Each thread gets a ticket number and waits for its turn.
    """

    def __init__(self) -> None:
        self._ticket = 0
        self._serving = 0
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)

    def acquire(self, timeout: Optional[float] = None) -> bool:
        """Get a ticket and wait until served."""
        my_ticket: int
        with self._lock:
            my_ticket = self._ticket
            self._ticket += 1
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            while self._serving != my_ticket:
                if not self._cond.wait(timeout=timeout):
                    return False
            return True

    def release(self) -> None:
        """Signal next ticket holder."""
        with self._cond:
            self._serving += 1
            self._cond.notify_all()


class Turnstile:
    """Turnstile primitive for controlling access to a resource.

    Threads pass through one at a time when unlocked.
    """

    def __init__(self) -> None:
        self._semaphore = Semaphore(1)
        self._count = 0
        self._lock = threading.Lock()

    def enter(self, timeout: Optional[float] = None) -> bool:
        """Enter the turnstile."""
        with self._lock:
            self._count += 1
            if self._count == 1:
                return self._semaphore.acquire(timeout=timeout)
            return True

    def exit(self) -> None:
        """Exit the turnstile."""
        with self._lock:
            self._count -= 1
            if self._count == 0:
                self._semaphore.release()


class Multiplex:
    """Multiplexer that combines N semaphores into one.

    Allows up to N concurrent holders.
    """

    def __init__(self, n: int) -> None:
        self._semaphore = Semaphore(n)

    def acquire(self, timeout: Optional[float] = None) -> bool:
        return self._semaphore.acquire(timeout=timeout)

    def release(self) -> None:
        self._semaphore.release()

    def __enter__(self) -> "Multiplex":
        self.acquire()
        return self

    def __exit__(self, *args: Any) -> None:
        self.release()


class Switches:
    """On/off switches for controlling flow.

    Each switch can be opened or closed independently.
    """

    def __init__(self, count: int = 1) -> None:
        self._switches = [threading.Event() for _ in range(count)]

    def open(self, index: int = 0) -> None:
        """Open (set) a switch."""
        self._switches[index].set()

    def close(self, index: int = 0) -> None:
        """Close (clear) a switch."""
        self._switches[index].clear()

    def is_open(self, index: int = 0) -> bool:
        """Check if switch is open."""
        return self._switches[index].is_set()

    def wait(self, index: int = 0, timeout: Optional[float] = None) -> bool:
        """Block until switch is open."""
        return self._switches[index].wait(timeout=timeout)


from contextlib import contextmanager
