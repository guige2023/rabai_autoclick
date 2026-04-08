"""Cyclic buffer utilities for streaming data.

Provides fixed-capacity cyclic buffers for efficient
FIFO/filo data streaming in automation workflows.
"""

from typing import Any, Generic, Iterator, List, Optional, TypeVar


T = TypeVar("T")


class CyclicBuffer(Generic[T]):
    """Fixed-capacity cyclic buffer.

    Overwrites oldest elements when full.

    Example:
        buf = CyclicBuffer(capacity=5)
        for i in range(10):
            buf.push(i)  # [5, 6, 7, 8, 9]
    """

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("Capacity must be positive")
        self._capacity = capacity
        self._buffer: List[Optional[T]] = [None] * capacity
        self._head = 0
        self._tail = 0
        self._size = 0

    def push(self, item: T) -> None:
        """Push item to buffer (overwrites if full)."""
        self._buffer[self._tail] = item
        self._tail = (self._tail + 1) % self._capacity
        if self._size < self._capacity:
            self._size += 1
        else:
            self._head = (self._head + 1) % self._capacity

    def pop(self) -> Optional[T]:
        """Pop oldest item from buffer.

        Returns:
            Oldest item or None if empty.
        """
        if self._size == 0:
            return None
        item = self._buffer[self._head]
        self._buffer[self._head] = None
        self._head = (self._head + 1) % self._capacity
        self._size -= 1
        return item

    def peek(self) -> Optional[T]:
        """Peek at oldest item without removing.

        Returns:
            Oldest item or None if empty.
        """
        if self._size == 0:
            return None
        return self._buffer[self._head]

    def peek_last(self) -> Optional[T]:
        """Peek at newest item without removing.

        Returns:
            Newest item or None if empty.
        """
        if self._size == 0:
            return None
        idx = (self._tail - 1) % self._capacity
        return self._buffer[idx]

    def is_full(self) -> bool:
        """Check if buffer is full."""
        return self._size == self._capacity

    def is_empty(self) -> bool:
        """Check if buffer is empty."""
        return self._size == 0

    @property
    def size(self) -> int:
        """Get current size."""
        return self._size

    @property
    def capacity(self) -> int:
        """Get buffer capacity."""
        return self._capacity

    def clear(self) -> None:
        """Clear all items."""
        self._buffer = [None] * self._capacity
        self._head = 0
        self._tail = 0
        self._size = 0

    def to_list(self) -> List[T]:
        """Convert to list (oldest first)."""
        if self._size == 0:
            return []
        if self._head < self._tail:
            return [self._buffer[i] for i in range(self._head, self._tail)]  # type: ignore
        return [self._buffer[i] for i in range(self._head, self._capacity)] + \
               [self._buffer[i] for i in range(0, self._tail)]  # type: ignore

    def __len__(self) -> int:
        return self._size

    def __bool__(self) -> bool:
        return self._size > 0

    def __iter__(self) -> Iterator[T]:
        if self._size == 0:
            return iter([])
        if self._head < self._tail:
            return iter(self._buffer[self._head:self._tail])  # type: ignore
        return iter([self._buffer[i] for i in range(self._head, self._capacity)] +
                    [self._buffer[i] for i in range(0, self._tail)])  # type: ignore


class CyclicByteBuffer:
    """Efficient byte buffer for streaming bytes."""

    def __init__(self, capacity: int = 4096) -> None:
        self._capacity = capacity
        self._buffer = bytearray(capacity)
        self._head = 0
        self._tail = 0
        self._size = 0

    def write(self, data: bytes) -> int:
        """Write bytes to buffer.

        Returns:
            Number of bytes written.
        """
        written = 0
        for byte in data:
            self._buffer[self._tail] = byte
            self._tail = (self._tail + 1) % self._capacity
            if self._size < self._capacity:
                self._size += 1
            else:
                self._head = (self._head + 1) % self._capacity
            written += 1
        return written

    def read(self, count: int = -1) -> bytes:
        """Read bytes from buffer.

        Args:
            count: Number of bytes to read. -1 for all.

        Returns:
            Bytes read.
        """
        if count == -1 or count > self._size:
            count = self._size
        result = bytearray(count)
        for i in range(count):
            result[i] = self._buffer[self._head]
            self._head = (self._head + 1) % self._capacity
        self._size -= count
        return bytes(result)

    def peek(self, count: int = -1) -> bytes:
        """Peek at bytes without consuming.

        Returns:
            Bytes peeked.
        """
        if count == -1 or count > self._size:
            count = self._size
        result = bytearray(count)
        idx = self._head
        for i in range(count):
            result[i] = self._buffer[idx]
            idx = (idx + 1) % self._capacity
        return bytes(result)

    @property
    def size(self) -> int:
        return self._size

    @property
    def available(self) -> int:
        return self._capacity - self._size

    def is_empty(self) -> bool:
        return self._size == 0

    def is_full(self) -> bool:
        return self._size == self._capacity

    def clear(self) -> None:
        self._head = 0
        self._tail = 0
        self._size = 0


class MovingWindow(Generic[T]):
    """Sliding window over data stream.

    Example:
        window = MovingWindow(size=5)
        for value in data_stream:
            window.add(value)
            if window.is_full:
                print(window.average)
    """

    def __init__(self, size: int) -> None:
        self._size = size
        self._buffer = CyclicBuffer[T](size)
        self._sum = 0
        self._count = 0

    def add(self, item: T) -> None:
        """Add item to window."""
        if self._buffer.is_full:
            old = self._buffer.peek()
            self._sum -= float(old)  # type: ignore
        else:
            self._count += 1
        self._buffer.push(item)
        self._sum += float(item)  # type: ignore

    @property
    def is_full(self) -> bool:
        return self._buffer.is_full

    @property
    def is_empty(self) -> bool:
        return self._buffer.is_empty

    @property
    def average(self) -> float:
        if self._count == 0:
            return 0.0
        return self._sum / self._count

    @property
    def sum(self) -> float:
        return self._sum

    def to_list(self) -> List[T]:
        return self._buffer.to_list()

    def __len__(self) -> int:
        return len(self._buffer)
