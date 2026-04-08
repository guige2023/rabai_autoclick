"""
Lock-free data structure utilities.

Provides lock-free stack, queue, and counter
implementations using atomic operations.
"""

from __future__ import annotations

import threading
import weakref
from typing import Generic, TypeVar


T = TypeVar("T")


class AtomicInteger:
    """Thread-safe atomic integer using locks."""

    def __init__(self, initial: int = 0):
        self._value = initial
        self._lock = threading.Lock()

    def get(self) -> int:
        return self._value

    def set(self, value: int) -> None:
        with self._lock:
            self._value = value

    def increment(self) -> int:
        with self._lock:
            self._value += 1
            return self._value

    def decrement(self) -> int:
        with self._lock:
            self._value -= 1
            return self._value

    def add(self, delta: int) -> int:
        with self._lock:
            self._value += delta
            return self._value

    def compare_and_set(self, expect: int, update: int) -> bool:
        """Compare and set if equal."""
        with self._lock:
            if self._value == expect:
                self._value = update
                return True
            return False


class AtomicReference(Generic[T]):
    """Thread-safe atomic reference using locks."""

    def __init__(self, initial: T | None = None):
        self._value = initial
        self._lock = threading.Lock()

    def get(self) -> T | None:
        return self._value

    def set(self, value: T | None) -> None:
        with self._lock:
            self._value = value

    def compare_and_set(self, expect: T | None, update: T | None) -> bool:
        """Compare and set if equal."""
        with self._lock:
            if self._value == expect:
                self._value = update
                return True
            return False

    def get_and_set(self, value: T | None) -> T | None:
        """Atomically get and set new value."""
        with self._lock:
            old = self._value
            self._value = value
            return old


class LockFreeStack(Generic[T]):
    """
    Lock-free stack using compare-and-swap.

    Based on Treiber's algorithm.
    """

    class _Node:
        __slots__ = ["value", "next"]
        def __init__(self, value: T):
            self.value = value
            self.next: "LockFreeStack._Node | None" = None

    def __init__(self):
        self._top = AtomicReference[LockFreeStack._Node | None](None)
        self._size = AtomicInteger(0)

    def push(self, item: T) -> None:
        """Push item onto stack."""
        node = self._Node(item)
        while True:
            old_top = self._top.get()
            node.next = old_top
            if self._top.compare_and_set(old_top, node):
                self._size.increment()
                return

    def pop(self) -> T | None:
        """Pop item from stack."""
        while True:
            old_top = self._top.get()
            if old_top is None:
                self._size.decrement()
                return None
            new_top = old_top.next
            if self._top.compare_and_set(old_top, new_top):
                self._size.decrement()
                return old_top.value

    @property
    def size(self) -> int:
        return self._size.get()

    def is_empty(self) -> bool:
        return self._top.get() is None


class LockFreeQueue(Generic[T]):
    """
    Lock-free queue using linked nodes.

    Simple non-blocking queue implementation.
    """

    class _Node:
        __slots__ = ["value", "next"]
        def __init__(self, value: T):
            self.value = value
            self.next: "LockFreeQueue._Node | None" = None

    def __init__(self):
        self._head = AtomicReference[LockFreeQueue._Node | None](None)
        self._tail = AtomicReference[LockFreeQueue._Node | None](None)
        self._size = AtomicInteger(0)
        dummy = self._Node(None)
        self._head.set(dummy)
        self._tail.set(dummy)

    def enqueue(self, item: T) -> None:
        """Add item to queue."""
        node = self._Node(item)
        while True:
            tail = self._tail.get()
            next_node = tail.next
            if next_node is None:
                node.next = None
                if tail.compare_and_set(next_node, node):
                    self._tail.compare_and_set(tail, node)
                    self._size.increment()
                    return

    def dequeue(self) -> T | None:
        """Remove and return item from queue."""
        while True:
            head = self._head.get()
            tail = self._tail.get()
            first = head.next
            if first is None:
                if head == tail:
                    self._size.decrement()
                    return None
                continue
            if head.compare_and_set(head, first):
                self._size.decrement()
                return first.value

    @property
    def size(self) -> int:
        return self._size.get()

    def is_empty(self) -> bool:
        return self._size.get() == 0


class ReadWriteLock:
    """Simple read-write lock using conditions."""

    def __init__(self):
        self._readers = AtomicInteger(0)
        self._writers_waiting = AtomicInteger(0)
        self._writer_active = AtomicInteger(0)
        self._read_cond = threading.Condition()
        self._write_cond = threading.Condition()

    def acquire_read(self) -> None:
        while self._writer_active.get() > 0 or self._writers_waiting.get() > 0:
            with self._read_cond:
                self._read_cond.wait()
        self._readers.increment()

    def release_read(self) -> None:
        self._readers.decrement()
        if self._readers.get() == 0:
            self._write_cond.notify()

    def acquire_write(self) -> None:
        self._writers_waiting.increment()
        while self._readers.get() > 0 or self._writer_active.get() > 0:
            with self._write_cond:
                self._write_cond.wait()
        self._writers_waiting.decrement()
        self._writer_active.increment()

    def release_write(self) -> None:
        self._writer_active.decrement()
        self._read_cond.notify_all()
        self._write_cond.notify()

    def __enter__(self) -> "ReadWriteLock":
        return self

    def __exit__(self, *args: object) -> None:
        pass
