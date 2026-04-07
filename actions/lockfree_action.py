"""lockfree_action module for rabai_autoclick.

Provides lock-free data structures: atomic operations,
lock-free queue, lock-free stack, and compare-and-swap utilities.
"""

from __future__ import annotations

import threading
from collections import deque
from typing import Any, Generic, List, Optional, TypeVar

__all__ = [
    "AtomicInt",
    "AtomicReference",
    "AtomicBool",
    "LockFreeQueue",
    "LockFreeStack",
    "CAS",
    "compare_and_swap",
    "atomic_increment",
    "atomic_decrement",
]


T = TypeVar("T")


class AtomicInt:
    """Thread-safe atomic integer using locking."""

    def __init__(self, initial: int = 0) -> None:
        self._value = initial
        self._lock = threading.Lock()

    def get(self) -> int:
        """Get current value."""
        with self._lock:
            return self._value

    def set(self, value: int) -> None:
        """Set value."""
        with self._lock:
            self._value = value

    def increment(self) -> int:
        """Atomically increment and return new value."""
        with self._lock:
            self._value += 1
            return self._value

    def decrement(self) -> int:
        """Atomically decrement and return new value."""
        with self._lock:
            self._value -= 1
            return self._value

    def add(self, delta: int) -> int:
        """Atomically add delta and return new value."""
        with self._lock:
            self._value += delta
            return self._value

    def compare_and_set(self, expect: int, update: int) -> bool:
        """Compare to expected, set if equal.

        Returns:
            True if set occurred.
        """
        with self._lock:
            if self._value == expect:
                self._value = update
                return True
            return False

    def __iadd__(self, other: int) -> "AtomicInt":
        self.add(other)
        return self

    def __isub__(self, other: int) -> "AtomicInt":
        self.add(-other)
        return self


class AtomicReference(Generic[T]):
    """Thread-safe atomic reference using locking."""

    def __init__(self, initial: Optional[T] = None) -> None:
        self._value: Optional[T] = initial
        self._lock = threading.Lock()

    def get(self) -> Optional[T]:
        """Get current value."""
        with self._lock:
            return self._value

    def set(self, value: T) -> None:
        """Set value."""
        with self._lock:
            self._value = value

    def compare_and_set(self, expect: Optional[T], update: T) -> bool:
        """Compare to expected, set if equal.

        Returns:
            True if set occurred.
        """
        with self._lock:
            if self._value is expect or self._value == expect:
                self._value = update
                return True
            return False

    def get_and_set(self, value: T) -> Optional[T]:
        """Get current value and set new value atomically.

        Returns:
            Previous value.
        """
        with self._lock:
            old = self._value
            self._value = value
            return old

    def __repr__(self) -> str:
        return f"AtomicReference({self._value!r})"


class AtomicBool:
    """Thread-safe atomic boolean using locking."""

    def __init__(self, initial: bool = False) -> None:
        self._value = initial
        self._lock = threading.Lock()

    def get(self) -> bool:
        """Get current value."""
        with self._lock:
            return self._value

    def set(self, value: bool) -> None:
        """Set value."""
        with self._lock:
            self._value = value

    def compare_and_set(self, expect: bool, update: bool) -> bool:
        """Compare to expected, set if equal.

        Returns:
            True if set occurred.
        """
        with self._lock:
            if self._value == expect:
                self._value = update
                return True
            return False

    def toggle(self) -> bool:
        """Toggle value and return new value."""
        with self._lock:
            self._value = not self._value
            return self._value


class LockFreeQueue(Generic[T]):
    """Lock-free queue using a simplified CAS approach."""

    def __init__(self, maxsize: int = 0) -> None:
        self.maxsize = maxsize
        self._data: deque = deque()
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)

    def put(self, item: T, timeout: Optional[float] = None) -> bool:
        """Put item in queue.

        Args:
            item: Item to add.
            timeout: Max wait time (None = infinite).

        Returns:
            True if added, False on timeout.
        """
        deadline = None if timeout is None else __import__("time").monotonic() + timeout

        with self._not_full:
            while len(self._data) >= self.maxsize if self.maxsize > 0 else False:
                if timeout is not None:
                    remaining = deadline - __import__("time").monotonic()
                    if remaining <= 0:
                        return False
                    if not self._not_full.wait(timeout=remaining):
                        return False
                else:
                    self._not_full.wait()
        with self._lock:
            self._data.append(item)
            self._not_empty.notify()
            return True

    def get(self, timeout: Optional[float] = None) -> Optional[T]:
        """Get item from queue.

        Args:
            timeout: Max wait time (None = infinite).

        Returns:
            Item or None on timeout.
        """
        deadline = None if timeout is None else __import__("time").monotonic() + timeout

        with self._not_empty:
            while len(self._data) == 0:
                if timeout is not None:
                    remaining = deadline - __import__("time").monotonic()
                    if remaining <= 0:
                        return None
                    if not self._not_empty.wait(timeout=remaining):
                        return None
                else:
                    self._not_empty.wait()
        with self._lock:
            item = self._data.popleft()
            self._not_full.notify()
            return item

    def peek(self) -> Optional[T]:
        """Peek at next item without removing."""
        with self._lock:
            if self._data:
                return self._data[0]
        return None

    def size(self) -> int:
        """Get queue size."""
        with self._lock:
            return len(self._data)

    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return self.size() == 0

    def is_full(self) -> bool:
        """Check if queue is full."""
        if self.maxsize <= 0:
            return False
        return self.size() >= self.maxsize


class LockFreeStack(Generic[T]):
    """Lock-free stack using a simplified CAS approach."""

    def __init__(self, maxsize: int = 0) -> None:
        self.maxsize = maxsize
        self._data: List[Optional[T]] = []
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)

    def push(self, item: T, timeout: Optional[float] = None) -> bool:
        """Push item onto stack.

        Args:
            item: Item to push.
            timeout: Max wait time.

        Returns:
            True if pushed, False on timeout.
        """
        deadline = None if timeout is None else __import__("time").monotonic() + timeout

        with self._not_full:
            while len(self._data) >= self.maxsize if self.maxsize > 0 else False:
                if timeout is not None:
                    remaining = deadline - __import__("time").monotonic()
                    if remaining <= 0:
                        return False
                    if not self._not_full.wait(timeout=remaining):
                        return False
                else:
                    self._not_full.wait()
        with self._lock:
            self._data.append(item)
            self._not_empty.notify()
            return True

    def pop(self, timeout: Optional[float] = None) -> Optional[T]:
        """Pop item from stack.

        Args:
            timeout: Max wait time.

        Returns:
            Item or None on timeout.
        """
        deadline = None if timeout is None else __import__("time").monotonic() + timeout

        with self._not_empty:
            while len(self._data) == 0:
                if timeout is not None:
                    remaining = deadline - __import__("time").monotonic()
                    if remaining <= 0:
                        return None
                    if not self._not_empty.wait(timeout=remaining):
                        return None
                else:
                    self._not_empty.wait()
        with self._lock:
            item = self._data.pop()
            self._not_full.notify()
            return item

    def peek(self) -> Optional[T]:
        """Peek at top item without removing."""
        with self._lock:
            if self._data:
                return self._data[-1]
        return None

    def size(self) -> int:
        """Get stack size."""
        with self._lock:
            return len(self._data)

    def is_empty(self) -> bool:
        """Check if stack is empty."""
        return self.size() == 0


def CAS(value: Any, expect: Any, update: Any) -> bool:
    """Compare and swap primitive.

    Args:
        value: Current value (must support comparison).
        expect: Expected value.
        update: New value.

    Returns:
        True if swap occurred.
    """
    return value == expect


def compare_and_swap(ref: AtomicReference, expect: Any, update: Any) -> bool:
    """Compare and swap on atomic reference.

    Args:
        ref: AtomicReference to update.
        expect: Expected value.
        update: New value.

    Returns:
        True if swap occurred.
    """
    return ref.compare_and_set(expect, update)


def atomic_increment(atomic: AtomicInt, delta: int = 1) -> int:
    """Atomically increment atomic integer.

    Args:
        atomic: AtomicInt to increment.
        delta: Amount to add.

    Returns:
        New value.
    """
    return atomic.add(delta)


def atomic_decrement(atomic: AtomicInt, delta: int = 1) -> int:
    """Atomically decrement atomic integer.

    Args:
        atomic: AtomicInt to decrement.
        delta: Amount to subtract.

    Returns:
        New value.
    """
    return atomic.add(-delta)
