"""
Data Ring Buffer Action Module.

Provides a ring buffer implementation for streaming data processing,
offering efficient fixed-capacity storage with overwrite semantics.
"""

from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class RingBufferStats:
    """Statistics for a ring buffer."""
    capacity: int
    size: int
    available: int
    overwrite_count: int
    read_count: int
    write_count: int
    oldest_timestamp: Optional[datetime] = None
    newest_timestamp: Optional[datetime] = None

    def utilization(self) -> float:
        """Calculate buffer utilization percentage."""
        return (self.size / self.capacity * 100) if self.capacity > 0 else 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "capacity": self.capacity,
            "size": self.size,
            "available": self.available,
            "overwrite_count": self.overwrite_count,
            "read_count": self.read_count,
            "write_count": self.write_count,
            "utilization_percent": self.utilization(),
            "oldest_timestamp": self.oldest_timestamp.isoformat() if self.oldest_timestamp else None,
            "newest_timestamp": self.newest_timestamp.isoformat() if self.newest_timestamp else None,
        }


class DataRingBufferAction(Generic[T]):
    """
    Implements a ring buffer for efficient fixed-capacity storage.

    A ring buffer (circular buffer) stores elements in a fixed-size
    circular array, overwriting oldest elements when full. Ideal for
    streaming data and bounded queues.

    Example:
        >>> buffer = DataRingBufferAction(capacity=100)
        >>> buffer.push(1)
        >>> buffer.push(2)
        >>> buffer.to_list()
        [1, 2]
    """

    def __init__(
        self,
        capacity: int,
        overwrite: bool = True,
        on_overwrite: Optional[Callable[[T], None]] = None,
    ):
        """
        Initialize the Ring Buffer.

        Args:
            capacity: Maximum number of elements.
            overwrite: Whether to overwrite oldest elements when full.
            on_overwrite: Optional callback when element is overwritten.
        """
        if capacity <= 0:
            raise ValueError("Capacity must be positive")

        self.capacity = capacity
        self.overwrite = overwrite
        self.on_overwrite = on_overwrite

        self._buffer: List[Optional[T]] = [None] * capacity
        self._head = 0
        self._tail = 0
        self._size = 0
        self._write_count = 0
        self._read_count = 0
        self._overwrite_count = 0
        self._oldest_timestamp: Optional[datetime] = None
        self._newest_timestamp: Optional[datetime] = None

    def push(self, item: T, timestamp: Optional[datetime] = None) -> bool:
        """
        Push an item onto the buffer.

        Args:
            item: Item to add.
            timestamp: Optional timestamp for the item.

        Returns:
            True if added successfully.
        """
        if self._size == self.capacity:
            if not self.overwrite:
                return False

            evicted = self._buffer[self._tail]
            self._tail = (self._tail + 1) % self.capacity
            self._size -= 1
            self._overwrite_count += 1

            if self.on_overwrite and evicted is not None:
                self.on_overwrite(evicted)

        self._buffer[self._head] = item
        self._head = (self._head + 1) % self.capacity
        self._size += 1
        self._write_count += 1

        now = timestamp or datetime.now(timezone.utc)
        if self._oldest_timestamp is None:
            self._oldest_timestamp = now
        self._newest_timestamp = now

        return True

    def pop(self) -> Optional[T]:
        """
        Pop the oldest item from the buffer.

        Returns:
            Oldest item or None if empty.
        """
        if self._size == 0:
            return None

        item = self._buffer[self._tail]
        self._buffer[self._tail] = None
        self._tail = (self._tail + 1) % self.capacity
        self._size -= 1
        self._read_count += 1

        if self._size == 0:
            self._oldest_timestamp = None

        return item

    def peek(self) -> Optional[T]:
        """
        Peek at the oldest item without removing it.

        Returns:
            Oldest item or None if empty.
        """
        if self._size == 0:
            return None
        return self._buffer[self._tail]

    def peek_many(self, count: int) -> List[T]:
        """
        Peek at multiple oldest items.

        Args:
            count: Number of items to peek.

        Returns:
            List of items.
        """
        result = []
        remaining = min(count, self._size)

        for i in range(remaining):
            idx = (self._tail + i) % self.capacity
            result.append(self._buffer[idx])

        return result

    def get(self, index: int) -> Optional[T]:
        """
        Get item at logical index.

        Args:
            index: Index from 0 (oldest) to size-1 (newest).

        Returns:
            Item at index or None.
        """
        if index < 0 or index >= self._size:
            return None

        actual_index = (self._tail + index) % self.capacity
        return self._buffer[actual_index]

    def to_list(self) -> List[T]:
        """
        Convert buffer to list (oldest to newest).

        Returns:
            List of all items.
        """
        return self.peek_many(self._size)

    def clear(self) -> None:
        """Clear all items from the buffer."""
        self._buffer = [None] * self.capacity
        self._head = 0
        self._tail = 0
        self._size = 0
        self._oldest_timestamp = None
        self._newest_timestamp = None

    def is_empty(self) -> bool:
        """Check if buffer is empty."""
        return self._size == 0

    def is_full(self) -> bool:
        """Check if buffer is full."""
        return self._size == self.capacity

    def __len__(self) -> int:
        """Get current size."""
        return self._size

    def __iter__(self) -> Iterator[T]:
        """Iterate over items (oldest to newest)."""
        for i in range(self._size):
            idx = (self._tail + i) % self.capacity
            yield self._buffer[idx]  # type: ignore

    def __reversed__(self) -> Iterator[T]:
        """Iterate over items (newest to oldest)."""
        for i in range(self._size - 1, -1, -1):
            idx = (self._tail + i) % self.capacity
            yield self._buffer[idx]  # type: ignore

    def get_stats(self) -> RingBufferStats:
        """Get buffer statistics."""
        return RingBufferStats(
            capacity=self.capacity,
            size=self._size,
            available=self.capacity - self._size,
            overwrite_count=self._overwrite_count,
            read_count=self._read_count,
            write_count=self._write_count,
            oldest_timestamp=self._oldest_timestamp,
            newest_timestamp=self._newest_timestamp,
        )


class BlockingRingBuffer(Generic[T]):
    """Thread-safe ring buffer with blocking operations."""

    def __init__(
        self,
        capacity: int,
        overwrite: bool = False,
        on_overwrite: Optional[Callable[[T], None]] = None,
    ):
        """
        Initialize blocking ring buffer.

        Args:
            capacity: Maximum capacity.
            overwrite: Whether to overwrite when full.
            on_overwrite: Callback for overwritten items.
        """
        import threading

        self._buffer = DataRingBufferAction(capacity, overwrite, on_overwrite)
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)

    def push(
        self,
        item: T,
        timeout: Optional[float] = None,
        timestamp: Optional[datetime] = None,
    ) -> bool:
        """
        Push item with blocking if full.

        Args:
            item: Item to push.
            timeout: Optional timeout.
            timestamp: Optional timestamp.

        Returns:
            True if pushed.
        """
        with self._not_full:
            if self._buffer.is_full():
                if not self._buffer.overwrite:
                    if timeout is None:
                        self._not_full.wait()
                    elif timeout > 0:
                        self._not_full.wait(timeout=timeout)

            result = self._buffer.push(item, timestamp)

            self._not_empty.notify()
            return result

    def pop(self, timeout: Optional[float] = None) -> Optional[T]:
        """
        Pop item with blocking if empty.

        Args:
            timeout: Optional timeout.

        Returns:
            Item or None.
        """
        with self._not_empty:
            if self._buffer.is_empty():
                if timeout is None:
                    self._not_empty.wait()
                elif timeout > 0:
                    if not self._not_empty.wait(timeout=timeout):
                        return None

            item = self._buffer.pop()
            self._not_full.notify()
            return item

    def peek(self) -> Optional[T]:
        """Peek at oldest item."""
        return self._buffer.peek()

    def is_empty(self) -> bool:
        """Check if empty."""
        return self._buffer.is_empty()

    def is_full(self) -> bool:
        """Check if full."""
        return self._buffer.is_full()

    def __len__(self) -> int:
        """Get size."""
        return len(self._buffer)


class StreamingRingBuffer(DataRingBufferAction[T]):
    """Ring buffer optimized for streaming data processing."""

    def __init__(
        self,
        capacity: int,
        transform: Optional[Callable[[T], Any]] = None,
        filter_func: Optional[Callable[[T], bool]] = None,
    ):
        """
        Initialize streaming ring buffer.

        Args:
            capacity: Buffer capacity.
            transform: Optional transform for each item.
            filter_func: Optional filter for items.
        """
        super().__init__(capacity)
        self.transform = transform
        self.filter_func = filter_func

    def push(self, item: T, timestamp: Optional[datetime] = None) -> bool:
        """Push with optional transformation and filtering."""
        if self.filter_func and not self.filter_func(item):
            return False

        processed = self.transform(item) if self.transform else item
        return super().push(processed, timestamp)

    def get_window(self, start: int, end: int) -> List[T]:
        """Get a window of items."""
        result = []
        for i in range(start, min(end, self._size)):
            idx = (self._tail + i) % self.capacity
            result.append(self._buffer[idx])
        return result

    def aggregate(
        self,
        func: Callable[[List[T]], Any],
        window_size: Optional[int] = None,
    ) -> Any:
        """
        Aggregate items using a function.

        Args:
            func: Aggregation function.
            window_size: Optional window size.

        Returns:
            Aggregated result.
        """
        items = self.to_list() if window_size is None else self.peek_many(window_size)
        return func(items)


def create_ring_buffer(
    capacity: int,
    overwrite: bool = True,
    **kwargs,
) -> DataRingBufferAction:
    """Factory function to create a DataRingBufferAction."""
    return DataRingBufferAction(capacity=capacity, overwrite=overwrite, **kwargs)
