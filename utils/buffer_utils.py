"""Buffer management utilities for RabAI AutoClick.

Provides:
- Ring buffer
- Sliding buffer
- Double buffer
- Buffered iterator
"""

from __future__ import annotations

from collections import deque
from typing import (
    Any,
    Callable,
    Deque,
    Generic,
    Iterator,
    List,
    Optional,
    TypeVar,
)


T = TypeVar("T")


class RingBuffer(Generic[T]):
    """Fixed-size ring buffer.

    When full, new items overwrite oldest items.

    Example:
        buffer = RingBuffer[int](3)
        buffer.push(1)
        buffer.push(2)
        buffer.push(3)
        buffer.push(4)  # Overwrites 1
        list(buffer)     # [2, 3, 4]
    """

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError(f"Capacity must be positive, got {capacity}")
        self._capacity = capacity
        self._buffer: List[Optional[T]] = [None] * capacity
        self._head = 0
        self._tail = 0
        self._size = 0

    def push(self, item: T) -> None:
        """Add item to buffer (overwrites oldest if full)."""
        self._buffer[self._tail] = item
        self._tail = (self._tail + 1) % self._capacity
        if self._size < self._capacity:
            self._size += 1
        else:
            self._head = (self._head + 1) % self._capacity

    def pop(self) -> T:
        """Remove and return oldest item.

        Raises:
            IndexError: If buffer is empty.
        """
        if self._size == 0:
            raise IndexError("pop from empty ring buffer")
        item = self._buffer[self._head]
        self._buffer[self._head] = None
        self._head = (self._head + 1) % self._capacity
        self._size -= 1
        return item  # type: ignore

    def peek(self) -> T:
        """Return oldest item without removing."""
        if self._size == 0:
            raise IndexError("peek from empty ring buffer")
        return self._buffer[self._head]  # type: ignore

    def get_all(self) -> List[T]:
        """Get all items in order (oldest to newest)."""
        result = []
        idx = self._head
        for _ in range(self._size):
            result.append(self._buffer[idx])  # type: ignore
            idx = (idx + 1) % self._capacity
        return result

    def __len__(self) -> int:
        return self._size

    def __bool__(self) -> bool:
        return self._size > 0

    def is_full(self) -> bool:
        return self._size == self._capacity

    def is_empty(self) -> bool:
        return self._size == 0

    def clear(self) -> None:
        self._buffer = [None] * self._capacity
        self._head = 0
        self._tail = 0
        self._size = 0

    def __iter__(self) -> Iterator[T]:
        return iter(self.get_all())

    def __repr__(self) -> str:
        return f"RingBuffer({self.get_all()})"


class SlidingBuffer(Generic[T]):
    """Sliding window buffer that holds last N items.

    Example:
        buffer = SlidingBuffer[int](3)
        buffer.push(1)
        buffer.push(2)
        buffer.push(3)
        buffer.push(4)
        list(buffer)  # [2, 3, 4]
    """

    def __init__(self, size: int) -> None:
        if size <= 0:
            raise ValueError(f"Size must be positive, got {size}")
        self._size = size
        self._buffer: Deque[T] = deque(maxlen=size)

    def push(self, item: T) -> None:
        self._buffer.append(item)

    def push_many(self, items: List[T]) -> None:
        for item in items:
            self.push(item)

    def get_all(self) -> List[T]:
        return list(self._buffer)

    def last(self, n: int = 1) -> List[T]:
        """Get last n items."""
        return list(self._buffer)[-n:]

    def __len__(self) -> int:
        return len(self._buffer)

    def __bool__(self) -> bool:
        return len(self._buffer) > 0

    def __iter__(self) -> Iterator[T]:
        return iter(self._buffer)

    def __repr__(self) -> str:
        return f"SlidingBuffer({list(self._buffer)})"


class DoubleBuffer(Generic[T]):
    """Double buffer for smooth switching between read and write.

    Provides two buffers - one for reading, one for writing.
    Swap to make writes visible to readers.

    Example:
        db = DoubleBuffer[int]()
        db.write_buffer.extend([1, 2, 3])
        db.flip()  # Switch to reading from [1, 2, 3]
        print(list(db.read_buffer))  # [1, 2, 3]
        db.write_buffer.append(4)  # Still writing to new write buffer
    """

    def __init__(self) -> None:
        self._read_buffer: Deque[T] = deque()
        self._write_buffer: Deque[T] = deque()
        self._is_flipped = False

    @property
    def read_buffer(self) -> Deque[T]:
        return self._read_buffer if not self._is_flipped else self._write_buffer

    @property
    def write_buffer(self) -> Deque[T]:
        return self._write_buffer if not self._is_flipped else self._read_buffer

    def flip(self) -> None:
        """Switch read and write buffers."""
        self._is_flipped = not self._is_flipped
        if self._is_flipped:
            self._read_buffer = self._write_buffer
            self._write_buffer = deque()
        else:
            self._write_buffer = self._read_buffer
            self._read_buffer = deque()

    def push(self, item: T) -> None:
        self.write_buffer.append(item)

    def extend(self, items: List[T]) -> None:
        self.write_buffer.extend(items)

    def clear_write(self) -> None:
        self.write_buffer.clear()

    def __len__(self) -> int:
        return len(self.read_buffer)

    def __iter__(self) -> Iterator[T]:
        return iter(self.read_buffer)


class BufferedIterator(Generic[T]):
    """Iterator with buffering.

    Pre-fetches items in background.

    Example:
        buffered = BufferedIterator(source_iter, buffer_size=10)
        for item in buffered:
            process(item)
    """

    def __init__(
        self,
        source: Iterator[T],
        buffer_size: int = 100,
    ) -> None:
        self._source = source
        self._buffer: Deque[T] = deque(maxlen=buffer_size)
        self._exhausted = False

    def fill(self) -> int:
        """Fill buffer from source.

        Returns:
            Number of items added.
        """
        added = 0
        while len(self._buffer) < self._buffer.maxlen:  # type: ignore
            try:
                item = next(self._source)
                self._buffer.append(item)
                added += 1
            except StopIteration:
                self._exhausted = True
                break
        return added

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        if not self._buffer:
            if self._exhausted:
                raise StopIteration
            if self.fill() == 0:
                raise StopIteration
        return self._buffer.popleft()  # type: ignore
