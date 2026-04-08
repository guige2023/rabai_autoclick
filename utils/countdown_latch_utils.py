"""Countdown latch and barrier utilities.

Provides synchronization primitives for coordinating
concurrent tasks in automation workflows.
"""

import threading
from typing import List


class CountDownLatch:
    """Countdown latch for thread synchronization.

    Example:
        latch = CountDownLatch(3)
        for _ in range(3):
            Thread(target=lambda: latch.count_down()).start()
        latch.await()  # blocks until count reaches 0
    """

    def __init__(self, count: int) -> None:
        if count < 0:
            raise ValueError("Count cannot be negative")
        self._count = count
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)

    def count_down(self) -> None:
        """Decrement the count."""
        with self._condition:
            if self._count > 0:
                self._count -= 1
                if self._count == 0:
                    self._condition.notify_all()

    def await(self, timeout: float = None) -> bool:
        """Wait for count to reach zero.

        Args:
            timeout: Maximum wait time in seconds.

        Returns:
            True if count reached zero, False on timeout.
        """
        with self._condition:
            while self._count > 0:
                if not self._condition.wait(timeout):
                    return False
            return True

    @property
    def count(self) -> int:
        """Get current count."""
        with self._lock:
            return self._count


class CyclicBarrier:
    """Cyclic barrier for thread synchronization.

    Example:
        barrier = CyclicBarrier(3)
        for _ in range(3):
            Thread(target=lambda: barrier.wait()).start()
        # all 3 threads block until all arrive, then all proceed
    """

    def __init__(self, parties: int) -> None:
        if parties <= 0:
            raise ValueError("Parties must be positive")
        self._parties = parties
        self._count = parties
        self._generation = 0
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)

    def wait(self, timeout: float = None) -> int:
        """Wait until all parties arrive.

        Args:
            timeout: Maximum wait time in seconds.

        Returns:
            Arrival index (0 for first, self._parties-1 for last).

        Raises:
            TimeoutError: If timeout expires.
        """
        with self._condition:
            index = self._count - 1
            my_generation = self._generation

            while self._count > 0 and my_generation == self._generation:
                if not self._condition.wait(timeout):
                    raise TimeoutError()

            return index

    def reset(self) -> None:
        """Reset barrier to initial state."""
        with self._lock:
            self._count = self._parties
            self._generation += 1
            self._condition.notify_all()

    @property
    def parties(self) -> int:
        """Get number of parties."""
        return self._parties

    @property
    def waiting(self) -> int:
        """Get number of threads currently waiting."""
        with self._lock:
            return self._parties - self._count


class ReadWriteLock:
    """Read-Write lock for thread synchronization.

    Example:
        rwlock = ReadWriteLock()
        with rwlock.read_lock():
            # multiple readers allowed
            data = self._data
        with rwlock.write_lock():
            # exclusive access for writer
            self._data = new_data
    """

    def __init__(self) -> None:
        self._readers = 0
        self._writers_waiting = 0
        self._writer_active = False
        self._lock = threading.Lock()
        self._readers_condition = threading.Condition(self._lock)
        self._writers_condition = threading.Condition(self._lock)

    class _ReadLock:
        def __init__(self, rwlock: "ReadWriteLock") -> None:
            self._rwlock = rwlock

        def __enter__(self) -> None:
            self._rwlock.acquire_read()

        def __exit__(self, *args) -> None:
            self._rwlock.release_read()

    class _WriteLock:
        def __init__(self, rwlock: "ReadWriteLock") -> None:
            self._rwlock = rwlock

        def __enter__(self) -> None:
            self._rwlock.acquire_write()

        def __exit__(self, *args) -> None:
            self._rwlock.release_write()

    def read_lock(self) -> _ReadLock:
        """Acquire read lock."""
        return self._ReadLock(self)

    def write_lock(self) -> _WriteLock:
        """Acquire write lock."""
        return self._WriteLock(self)

    def acquire_read(self) -> None:
        """Acquire read lock."""
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
        """Acquire write lock."""
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


class Semaphore:
    """Semaphore for controlling resource access.

    Example:
        sem = Semaphore(2)  # allow 2 concurrent accesses
        with sem:
            # do work
            pass
    """

    def __init__(self, value: int = 1) -> None:
        if value < 0:
            raise ValueError("Semaphore value must be non-negative")
        self._value = value
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)

    def acquire(self, blocking: bool = True, timeout: float = None) -> bool:
        """Acquire semaphore.

        Args:
            blocking: If False, return immediately.
            timeout: Maximum wait time.

        Returns:
            True if acquired.
        """
        with self._condition:
            if self._value > 0:
                self._value -= 1
                return True
            if not blocking:
                return False
            if not self._condition.wait(timeout):
                return False
            self._value -= 1
            return True

    def release(self) -> None:
        """Release semaphore."""
        with self._condition:
            self._value += 1
            self._condition.notify()

    @property
    def value(self) -> int:
        """Get current semaphore value."""
        with self._lock:
            return self._value

    def __enter__(self) -> None:
        self.acquire()

    def __exit__(self, *args) -> None:
        self.release()
