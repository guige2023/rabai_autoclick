"""Buffer utilities for RabAI AutoClick.

Provides:
- Circular buffer
- Ring buffer
- Stream buffer
"""

import threading
from collections import deque
from typing import Any, Generic, List, Optional, TypeVar


T = TypeVar("T")


class CircularBuffer(Generic[T]):
    """Fixed-size circular buffer.

    Overwrites oldest elements when full.
    """

    def __init__(self, capacity: int) -> None:
        """Initialize circular buffer.

        Args:
            capacity: Maximum buffer size.
        """
        self._capacity = capacity
        self._buffer: List[Optional[T]] = [None] * capacity
        self._head = 0
        self._tail = 0
        self._size = 0
        self._lock = threading.Lock()

    def append(self, item: T) -> None:
        """Append item to buffer.

        Args:
            item: Item to add.
        """
        with self._lock:
            self._buffer[self._tail] = item
            self._tail = (self._tail + 1) % self._capacity

            if self._size < self._capacity:
                self._size += 1
            else:
                # Buffer full, overwrite oldest
                self._head = (self._head + 1) % self._capacity

    def get(self, index: int) -> Optional[T]:
        """Get item at index.

        Args:
            index: Position (0 = oldest).

        Returns:
            Item or None if out of bounds.
        """
        with self._lock:
            if index < 0 or index >= self._size:
                return None

            actual_index = (self._head + index) % self._capacity
            return self._buffer[actual_index]

    def to_list(self) -> List[T]:
        """Convert buffer to list.

        Returns:
            List of items in order (oldest first).
        """
        with self._lock:
            result = []
            for i in range(self._size):
                actual_index = (self._head + i) % self._capacity
                result.append(self._buffer[actual_index])
            return result

    def clear(self) -> None:
        """Clear buffer."""
        with self._lock:
            self._buffer = [None] * self._capacity
            self._head = 0
            self._tail = 0
            self._size = 0

    @property
    def size(self) -> int:
        """Get current size."""
        with self._lock:
            return self._size

    @property
    def capacity(self) -> int:
        """Get capacity."""
        return self._capacity

    def __len__(self) -> int:
        return self.size

    def __iter__(self):
        return iter(self.to_list())


class RingBuffer(Generic[T]):
    """Simple ring buffer.

    Similar to CircularBuffer but with different interface.
    """

    def __init__(self, size: int) -> None:
        """Initialize ring buffer.

        Args:
            size: Buffer size.
        """
        self._size = size
        self._buffer: deque = deque(maxlen=size)

    def push(self, item: T) -> None:
        """Push item to buffer."""
        self._buffer.append(item)

    def pop(self) -> Optional[T]:
        """Pop oldest item."""
        if self._buffer:
            return self._buffer.popleft()
        return None

    def peek(self) -> Optional[T]:
        """Peek at oldest item."""
        if self._buffer:
            return self._buffer[0]
        return None

    @property
    def is_full(self) -> bool:
        """Check if buffer is full."""
        return len(self._buffer) >= self._size

    @property
    def is_empty(self) -> bool:
        """Check if buffer is empty."""
        return len(self._buffer) == 0

    def __len__(self) -> int:
        return len(self._buffer)


class StreamBuffer(Generic[T]):
    """Buffered stream for async-like processing.

    Supports blocking read/write.
    """

    def __init__(self, maxsize: int = 0) -> None:
        """Initialize stream buffer.

        Args:
            maxsize: Maximum buffer size (0 = unlimited).
        """
        self._buffer: deque = deque()
        self._maxsize = maxsize
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)

    def write(self, item: T, timeout: Optional[float] = None) -> bool:
        """Write item to buffer.

        Args:
            item: Item to write.
            timeout: Optional timeout.

        Returns:
            True if written.
        """
        with self._not_full:
            if self._maxsize > 0:
                if not self._not_full.wait_for(
                    lambda: len(self._buffer) < self._maxsize,
                    timeout=timeout
                ):
                    return False

            self._buffer.append(item)
            self._not_empty.notify()
            return True

    def read(self, timeout: Optional[float] = None) -> Optional[T]:
        """Read item from buffer.

        Args:
            timeout: Optional timeout.

        Returns:
            Item or None on timeout.
        """
        with self._not_empty:
            if not self._not_empty.wait_for(lambda: len(self._buffer) > 0, timeout=timeout):
                return None

            item = self._buffer.popleft()
            self._not_full.notify()
            return item

    def peek(self, timeout: Optional[float] = None) -> Optional[T]:
        """Peek at next item without removing.

        Args:
            timeout: Optional timeout.

        Returns:
            Next item or None.
        """
        with self._not_empty:
            if not self._not_empty.wait_for(lambda: len(self._buffer) > 0, timeout=timeout):
                return None
            return self._buffer[0]

    def size(self) -> int:
        """Get buffer size."""
        return len(self._buffer)

    def is_empty(self) -> bool:
        """Check if buffer is empty."""
        return len(self._buffer) == 0


class BufferedWriter:
    """Buffer writes and flush periodically or on threshold."""

    def __init__(
        self,
        writer: callable,
        buffer_size: int = 1000,
        flush_interval: float = 5.0,
    ) -> None:
        """Initialize buffered writer.

        Args:
            writer: Function to write data.
            buffer_size: Max buffer size before auto-flush.
            flush_interval: Auto-flush interval in seconds.
        """
        self._writer = writer
        self._buffer: List[Any] = []
        self._buffer_size = buffer_size
        self._flush_interval = flush_interval
        self._last_flush = time.time()
        self._lock = threading.Lock()

    def write(self, item: Any) -> None:
        """Buffer an item.

        Args:
            item: Item to buffer.
        """
        with self._lock:
            self._buffer.append(item)

            if len(self._buffer) >= self._buffer_size:
                self._flush()

    def _flush(self) -> None:
        """Flush buffer to writer."""
        if not self._buffer:
            return

        try:
            self._writer(self._buffer)
            self._buffer.clear()
            self._last_flush = time.time()
        except Exception:
            pass

    def flush(self) -> None:
        """Manually flush buffer."""
        with self._lock:
            self._flush()


import time