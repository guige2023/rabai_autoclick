"""Ring buffer (circular buffer) utilities.

Provides fixed-size FIFO buffer implementation for
streaming data and sliding window operations.
"""

from typing import Any, Generic, List, Optional, TypeVar


T = TypeVar("T")


class RingBuffer(Generic[T]):
    """Fixed-size circular buffer.

    Example:
        buffer = RingBuffer(max_size=5)
        buffer.append(1)
        buffer.append(2)
        print(buffer.to_list())  # [1, 2]
    """

    def __init__(self, max_size: int) -> None:
        if max_size <= 0:
            raise ValueError("max_size must be positive")
        self._max_size = max_size
        self._buffer: List[Optional[T]] = [None] * max_size
        self._head = 0
        self._tail = 0
        self._size = 0

    def append(self, item: T) -> None:
        """Add item to the buffer.

        Args:
            item: Item to add.
        """
        self._buffer[self._tail] = item
        self._tail = (self._tail + 1) % self._max_size
        if self._size < self._max_size:
            self._size += 1
        else:
            self._head = (self._head + 1) % self._max_size

    def get(self, index: int) -> T:
        """Get item at index (0 = oldest).

        Args:
            index: Index to get.

        Returns:
            Item at index.

        Raises:
            IndexError: If index out of range.
        """
        if index < 0 or index >= self._size:
            raise IndexError(f"Index {index} out of range for size {self._size}")
        actual_index = (self._head + index) % self._max_size
        return self._buffer[actual_index]  # type: ignore

    def __getitem__(self, index: int) -> T:
        """Get item at index."""
        return self.get(index)

    def __len__(self) -> int:
        """Get current size."""
        return self._size

    def __bool__(self) -> bool:
        """Check if buffer is non-empty."""
        return self._size > 0

    def is_full(self) -> bool:
        """Check if buffer is full."""
        return self._size == self._max_size

    def is_empty(self) -> bool:
        """Check if buffer is empty."""
        return self._size == 0

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
        last_index = (self._tail - 1) % self._max_size
        return self._buffer[last_index]

    def to_list(self) -> List[T]:
        """Convert buffer to list (oldest first).

        Returns:
            List of items.
        """
        if self._size == 0:
            return []
        if self._head < self._tail:
            return [self._buffer[i] for i in range(self._head, self._tail)]  # type: ignore
        return [self._buffer[i] for i in range(self._head, self._max_size)] + \
               [self._buffer[i] for i in range(0, self._tail)]  # type: ignore

    def clear(self) -> None:
        """Clear all items from buffer."""
        self._head = 0
        self._tail = 0
        self._size = 0

    @property
    def max_size(self) -> int:
        """Get maximum buffer size."""
        return self._max_size


class SlidingWindow(Generic[T]):
    """Sliding window over a sequence.

    Example:
        window = SlidingWindow(size=3)
        for item in [1, 2, 3, 4, 5]:
            window.append(item)
            if len(window) == 3:
                print(window.to_list())  # [1,2,3], [2,3,4], [3,4,5]
    """

    def __init__(self, size: int) -> None:
        self._buffer = RingBuffer[T](size)

    def append(self, item: T) -> None:
        """Add item and evict oldest if full."""
        self._buffer.append(item)

    def to_list(self) -> List[T]:
        """Get window contents (oldest first)."""
        return self._buffer.to_list()

    def __len__(self) -> int:
        """Get current window size."""
        return len(self._buffer)

    def is_full(self) -> bool:
        """Check if window is at max size."""
        return self._buffer.is_full()

    def is_empty(self) -> bool:
        """Check if window is empty."""
        return self._buffer.is_empty()

    def get(self, index: int) -> T:
        """Get item at index."""
        return self._buffer.get(index)

    def clear(self) -> None:
        """Clear window."""
        self._buffer.clear()

    @property
    def size(self) -> int:
        """Get window size."""
        return self._buffer.max_size
