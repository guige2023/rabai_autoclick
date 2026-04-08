"""
Buffer utilities for managing bounded collections with overflow handling.

Provides thread-safe buffers with configurable capacity,
overflow strategies, and blocking/non-blocking operations.

Example:
    >>> from utils.buffer_utils_v2 import BoundedBuffer, RingBuffer
    >>> buf = BoundedBuffer(capacity=100, overflow="drop")
    >>> buf.put(item)
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Deque, Generic, List, Optional, TypeVar

T = TypeVar("T")


class BufferError(Exception):
    """Raised when buffer operations fail."""
    pass


class BufferOverflow(BufferError):
    """Raised when buffer overflows with strict mode."""
    pass


@dataclass
class BufferStats:
    """Statistics for a buffer."""
    capacity: int
    size: int
    puts: int
    gets: int
    overflows: int
    underflows: int


class BoundedBuffer(Generic[T]):
    """
    Thread-safe bounded buffer with overflow handling.

    Supports multiple overflow strategies:
    - drop: Drop oldest item
    - drop_new: Drop new item
    - block: Block until space available
    - error: Raise BufferOverflow

    Attributes:
        capacity: Maximum buffer size.
        overflow: Overflow strategy.
    """

    def __init__(
        self,
        capacity: int,
        overflow: str = "drop",
        blocking: bool = False,
    ) -> None:
        """
        Initialize the bounded buffer.

        Args:
            capacity: Maximum number of items.
            overflow: Overflow strategy ('drop', 'drop_new', 'error').
            blocking: If True, put blocks when full.
        """
        self.capacity = capacity
        self.overflow = overflow
        self.blocking = blocking
        self._buffer: Deque[T] = deque(maxlen=capacity if overflow == "drop" else None)
        self._lock = threading.RLock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)

        self._stats = BufferStats(
            capacity=capacity,
            size=0,
            puts=0,
            gets=0,
            overflows=0,
            underflows=0,
        )

    def put(
        self,
        item: T,
        block: Optional[bool] = None,
        timeout: Optional[float] = None,
    ) -> bool:
        """
        Put an item into the buffer.

        Args:
            item: Item to add.
            block: Override blocking behavior.
            timeout: Maximum wait time.

        Returns:
            True if item was added, False if dropped.

        Raises:
            BufferOverflow: If strict mode and buffer is full.
        """
        block = block if block is not None else self.blocking

        with self._not_full:
            self._stats.puts += 1

            while len(self._buffer) >= self.capacity:
                if self.overflow == "error":
                    self._stats.overflows += 1
                    raise BufferOverflow("Buffer is full")
                elif self.overflow == "drop_new":
                    self._stats.overflows += 1
                    return False
                elif self.overflow == "drop" and self._buffer:
                    self._buffer.popleft()

                if not block:
                    if len(self._buffer) >= self.capacity:
                        self._stats.overflows += 1
                        return False
                else:
                    if timeout is not None:
                        end_time = time.monotonic() + timeout
                        remaining = timeout
                        while len(self._buffer) >= self.capacity and remaining > 0:
                            if not self._not_full.wait(remaining):
                                remaining = end_time - time.monotonic()
                        else:
                            if len(self._buffer) >= self.capacity:
                                self._stats.overflows += 1
                                return False
                    else:
                        self._not_full.wait()

            self._buffer.append(item)
            self._stats.size = len(self._buffer)
            self._not_empty.notify()
            return True

    def get(
        self,
        block: bool = True,
        timeout: Optional[float] = None,
    ) -> Optional[T]:
        """
        Get an item from the buffer.

        Args:
            block: If True, wait for item if empty.
            timeout: Maximum wait time.

        Returns:
            Item from buffer or None.

        Raises:
            BufferError: If timeout and buffer is empty.
        """
        with self._not_empty:
            while len(self._buffer) == 0:
                self._stats.underflows += 1
                if not block:
                    return None

                if timeout is not None:
                    if not self._not_empty.wait(timeout):
                        self._stats.underflows += 1
                        return None
                else:
                    self._not_empty.wait()

            item = self._buffer.popleft()
            self._stats.size = len(self._buffer)
            self._stats.gets += 1
            self._not_full.notify()
            return item

    def peek(self) -> Optional[T]:
        """
        Get the next item without removing it.

        Returns:
            Next item or None.
        """
        with self._lock:
            if self._buffer:
                return self._buffer[0]
            return None

    def clear(self) -> None:
        """Clear all items from the buffer."""
        with self._lock:
            self._buffer.clear()
            self._stats.size = 0
            self._not_full.notify_all()

    @property
    def size(self) -> int:
        """Get current buffer size."""
        with self._lock:
            return len(self._buffer)

    @property
    def available(self) -> int:
        """Get available space in buffer."""
        with self._lock:
            return self.capacity - len(self._buffer)

    def is_empty(self) -> bool:
        """Check if buffer is empty."""
        with self._lock:
            return len(self._buffer) == 0

    def is_full(self) -> bool:
        """Check if buffer is full."""
        with self._lock:
            return len(self._buffer) >= self.capacity

    def get_stats(self) -> BufferStats:
        """Get buffer statistics."""
        with self._lock:
            return BufferStats(
                capacity=self.capacity,
                size=len(self._buffer),
                puts=self._stats.puts,
                gets=self._stats.gets,
                overflows=self._stats.overflows,
                underflows=self._stats.underflows,
            )


class RingBuffer(Generic[T]):
    """
    Ring buffer (circular buffer) implementation.

    Fixed-size buffer that overwrites oldest elements
    when full.
    """

    def __init__(self, capacity: int) -> None:
        """
        Initialize the ring buffer.

        Args:
            capacity: Maximum number of items.
        """
        self.capacity = capacity
        self._buffer: List[Optional[T]] = [None] * capacity
        self._head = 0
        self._tail = 0
        self._size = 0
        self._lock = threading.RLock()

    def push(self, item: T) -> None:
        """
        Push an item to the buffer.

        Args:
            item: Item to add.
        """
        with self._lock:
            self._buffer[self._tail] = item
            self._tail = (self._tail + 1) % self.capacity

            if self._size < self.capacity:
                self._size += 1
            else:
                self._head = (self._head + 1) % self.capacity

    def pop(self) -> Optional[T]:
        """
        Pop the oldest item from the buffer.

        Returns:
            Oldest item or None if empty.
        """
        with self._lock:
            if self._size == 0:
                return None

            item = self._buffer[self._head]
            self._buffer[self._head] = None
            self._head = (self._head + 1) % self.capacity
            self._size -= 1
            return item

    def peek(self) -> Optional[T]:
        """
        Peek at the oldest item.

        Returns:
            Oldest item or None.
        """
        with self._lock:
            if self._size == 0:
                return None
            return self._buffer[self._head]

    def __len__(self) -> int:
        """Get current size."""
        with self._lock:
            return self._size

    def is_empty(self) -> bool:
        """Check if empty."""
        return len(self) == 0

    def is_full(self) -> bool:
        """Check if full."""
        with self._lock:
            return self._size == self.capacity


class AsyncBoundedBuffer(Generic[T]):
    """
    Async bounded buffer with await support.
    """

    def __init__(
        self,
        capacity: int,
        overflow: str = "drop",
    ) -> None:
        """
        Initialize the async bounded buffer.

        Args:
            capacity: Maximum buffer size.
            overflow: Overflow strategy.
        """
        self.capacity = capacity
        self.overflow = overflow
        self._buffer: asyncio.Queue[T] = asyncio.Queue(maxsize=capacity)

    async def put(self, item: T) -> bool:
        """
        Put an item into the buffer.

        Args:
            item: Item to add.

        Returns:
            True if added, False if dropped.
        """
        try:
            self._buffer.put_nowait(item)
            return True
        except asyncio.QueueFull:
            if self.overflow == "drop":
                try:
                    self._buffer.get_nowait()
                    self._buffer.put_nowait(item)
                    return True
                except asyncio.QueueEmpty:
                    pass
            elif self.overflow == "drop_new":
                return False
            return False

    async def get(self) -> T:
        """Get an item from the buffer."""
        return await self._buffer.get()

    async def get_nowait(self) -> Optional[T]:
        """Try to get an item without waiting."""
        try:
            return self._buffer.get_nowait()
        except asyncio.QueueEmpty:
            return None

    @property
    def size(self) -> int:
        """Get current size."""
        return self._buffer.qsize()

    @property
    def available(self) -> int:
        """Get available space."""
        return self._buffer.maxsize - self._buffer.qsize()


def create_buffer(
    capacity: int,
    buffer_type: str = "bounded",
    **kwargs
) -> Any:
    """
    Factory to create a buffer.

    Args:
        capacity: Buffer capacity.
        buffer_type: Type of buffer ('bounded', 'ring', 'async').
        **kwargs: Additional arguments.

    Returns:
        Buffer instance.
    """
    if buffer_type == "bounded":
        return BoundedBuffer(capacity=capacity, **kwargs)
    elif buffer_type == "ring":
        return RingBuffer(capacity=capacity)
    elif buffer_type == "async":
        return AsyncBoundedBuffer(capacity=capacity, **kwargs)
    else:
        raise ValueError(f"Unknown buffer type: {buffer_type}")
