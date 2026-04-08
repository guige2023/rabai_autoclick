"""
Circular Buffer Utilities

Provides circular buffer implementations for
efficient fixed-size buffer management.

Author: Agent3
"""
from __future__ import annotations

from typing import Generic, TypeVar
from collections import deque

T = TypeVar("T")


class CircularBuffer(Generic[T]):
    """
    A fixed-size circular buffer implementation.
    
    Automatically overwrites oldest elements when
    the buffer is full.
    """

    def __init__(self, capacity: int) -> None:
        """
        Initialize circular buffer.
        
        Args:
            capacity: Maximum number of elements.
        """
        if capacity <= 0:
            raise ValueError("Capacity must be positive")
        self._capacity = capacity
        self._buffer: deque[T] = deque(maxlen=capacity)
        self._head = 0
        self._size = 0

    def append(self, item: T) -> None:
        """Append an item to the buffer."""
        if self._size < self._capacity:
            self._buffer.append(item)
            self._size += 1
        else:
            self._buffer[self._head] = item
        self._head = (self._head + 1) % self._capacity

    def get(self, index: int) -> T | None:
        """Get item at index (0-based, oldest first)."""
        if index < 0 or index >= self._size:
            return None
        actual_index = (self._head - self._size + index) % self._capacity
        return self._buffer[actual_index]

    def to_list(self) -> list[T]:
        """Convert buffer to list (oldest first)."""
        result = []
        for i in range(self._size):
            result.append(self.get(i))  # type: ignore
        return result

    def __len__(self) -> int:
        """Get current number of elements."""
        return self._size

    def __bool__(self) -> bool:
        """Check if buffer has elements."""
        return self._size > 0

    def clear(self) -> None:
        """Clear all elements."""
        self._buffer.clear()
        self._head = 0
        self._size = 0

    def is_full(self) -> bool:
        """Check if buffer is at capacity."""
        return self._size >= self._capacity

    def is_empty(self) -> bool:
        """Check if buffer is empty."""
        return self._size == 0

    @property
    def capacity(self) -> int:
        """Get buffer capacity."""
        return self._capacity


def create_circular_buffer(
    capacity: int,
    items: list[T] | None = None
) -> CircularBuffer[T]:
    """
    Factory function to create a circular buffer.
    
    Args:
        capacity: Maximum buffer size.
        items: Optional initial items.
        
    Returns:
        Configured CircularBuffer instance.
    """
    buf = CircularBuffer[T](capacity)
    if items:
        for item in items:
            buf.append(item)
    return buf
