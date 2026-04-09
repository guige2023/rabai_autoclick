"""
Data Min Heap Action Module.

Provides a min heap implementation for priority-based data processing,
offering O(log n) insertion and O(1) min element retrieval.
"""

from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)


@dataclass
class HeapNode(Generic[T]):
    """A node in the heap."""
    key: float
    value: T
    index: int = 0

    def __lt__(self, other: "HeapNode") -> bool:
        """Compare nodes by key."""
        return self.key < other.key

    def __repr__(self) -> str:
        """String representation."""
        return f"HeapNode(key={self.key}, value={self.value})"


class DataMinHeapAction(Generic[T]):
    """
    Implements a Min Heap for priority-based processing.

    A min heap is a binary tree where each node's key is less than
    or equal to its children's keys. This allows O(1) retrieval of
    the minimum element and O(log n) insertion/deletion.

    Example:
        >>> heap = DataMinHeapAction()
        >>> heap.insert(3, "three")
        >>> heap.insert(1, "one")
        >>> heap.insert(2, "two")
        >>> heap.extract_min()
        ('one', 1)
    """

    def __init__(self):
        """Initialize the Min Heap."""
        self._heap: List[HeapNode[T]] = []
        self._index_counter = 0

    def insert(self, key: float, value: T) -> int:
        """
        Insert an element into the heap.

        Args:
            key: Priority key.
            value: Value to store.

        Returns:
            Node index.
        """
        node = HeapNode(key=key, value=value, index=self._index_counter)
        self._index_counter += 1

        self._heap.append(node)
        self._sift_up(len(self._heap) - 1)

        return node.index

    def extract_min(self) -> Optional[Tuple[T, float]]:
        """
        Extract and return the minimum element.

        Returns:
            Tuple of (value, key) or None if empty.
        """
        if not self._heap:
            return None

        min_node = self._heap[0]
        last = self._heap.pop()

        if self._heap:
            self._heap[0] = last
            self._sift_down(0)

        return (min_node.value, min_node.key)

    def peek_min(self) -> Optional[Tuple[T, float]]:
        """
        Peek at the minimum element without removing it.

        Returns:
            Tuple of (value, key) or None if empty.
        """
        if not self._heap:
            return None

        min_node = self._heap[0]
        return (min_node.value, min_node.key)

    def peek(self) -> Optional[HeapNode[T]]:
        """Peek at the minimum node."""
        if not self._heap:
            return None
        return self._heap[0]

    def find(self, value: T) -> Optional[int]:
        """
        Find a value in the heap.

        Args:
            value: Value to find.

        Returns:
            Node index or None.
        """
        for node in self._heap:
            if node.value == value:
                return node.index
        return None

    def delete(self, index: int) -> bool:
        """
        Delete a node by index.

        Args:
            index: Index of node to delete.

        Returns:
            True if deleted.
        """
        for i, node in enumerate(self._heap):
            if node.index == index:
                self._heap[i] = self._heap[-1]
                self._heap.pop()

                if i < len(self._heap):
                    self._sift_down(i)
                    self._sift_up(i)

                return True

        return False

    def delete_value(self, value: T) -> bool:
        """
        Delete a value from the heap.

        Args:
            value: Value to delete.

        Returns:
            True if deleted.
        """
        index = self.find(value)
        if index is not None:
            return self.delete(index)
        return False

    def replace(
        self,
        index: int,
        key: float,
        value: Optional[T] = None,
    ) -> bool:
        """
        Replace a node's key and optionally value.

        Args:
            index: Node index.
            key: New key.
            value: Optional new value.

        Returns:
            True if replaced.
        """
        for i, node in enumerate(self._heap):
            if node.index == index:
                old_key = node.key
                node.key = key
                if value is not None:
                    node.value = value

                if key < old_key:
                    self._sift_up(i)
                else:
                    self._sift_down(i)

                return True

        return False

    def heapify(self, items: List[Tuple[float, T]]) -> None:
        """
        Build heap from list of (key, value) tuples.

        Args:
            items: List of (key, value) tuples.
        """
        self._heap = [
            HeapNode(key=k, value=v, index=self._index_counter + i)
            for i, (k, v) in enumerate(items)
        ]
        self._index_counter += len(items)

        for i in range(len(self._heap) // 2 - 1, -1, -1):
            self._sift_down(i)

    def merge(self, other: "DataMinHeapAction[T]") -> None:
        """
        Merge another heap into this one.

        Args:
            other: Another min heap.
        """
        for node in other._heap:
            new_node = HeapNode(
                key=node.key,
                value=node.value,
                index=self._index_counter,
            )
            self._index_counter += 1
            self._heap.append(new_node)

        for i in range(len(self._heap) // 2 - 1, -1, -1):
            self._sift_down(i)

    def _sift_up(self, index: int) -> None:
        """Move node up to maintain heap property."""
        while index > 0:
            parent = (index - 1) // 2

            if self._heap[index].key < self._heap[parent].key:
                self._heap[index], self._heap[parent] = (
                    self._heap[parent],
                    self._heap[index],
                )
                index = parent
            else:
                break

    def _sift_down(self, index: int) -> None:
        """Move node down to maintain heap property."""
        size = len(self._heap)

        while True:
            smallest = index
            left = 2 * index + 1
            right = 2 * index + 2

            if left < size and self._heap[left].key < self._heap[smallest].key:
                smallest = left

            if right < size and self._heap[right].key < self._heap[smallest].key:
                smallest = right

            if smallest != index:
                self._heap[index], self._heap[smallest] = (
                    self._heap[smallest],
                    self._heap[index],
                )
                index = smallest
            else:
                break

    def __len__(self) -> int:
        """Get number of elements."""
        return len(self._heap)

    def __bool__(self) -> bool:
        """Check if heap is non-empty."""
        return len(self._heap) > 0

    def __iter__(self) -> Iterator[Tuple[float, T]]:
        """Iterate over elements in heap order (not sorted)."""
        for node in self._heap:
            yield (node.key, node.value)

    def is_empty(self) -> bool:
        """Check if heap is empty."""
        return len(self._heap) == 0

    def clear(self) -> None:
        """Clear all elements."""
        self._heap.clear()

    def get_sorted(self) -> List[Tuple[float, T]]:
        """Get all elements sorted by key."""
        result = []
        temp_heap = DataMinHeapAction[T]()
        temp_heap._heap = self._heap.copy()
        temp_heap._index_counter = self._index_counter

        while temp_heap:
            item = temp_heap.extract_min()
            if item:
                result.append(item)

        return result


class MaxHeap(Generic[T]):
    """A max heap implementation."""

    def __init__(self):
        """Initialize the Max Heap."""
        self._min_heap = DataMinHeapAction[T]()

    def insert(self, key: float, value: T) -> int:
        """Insert with negated key for max behavior."""
        return self._min_heap.insert(-key, value)

    def extract_max(self) -> Optional[Tuple[T, float]]:
        """Extract maximum element."""
        result = self._min_heap.extract_min()
        if result:
            return (result[0], -result[1])
        return None

    def peek_max(self) -> Optional[Tuple[T, float]]:
        """Peek at maximum element."""
        result = self._min_heap.peek_min()
        if result:
            return (result[0], -result[1])
        return None

    def __len__(self) -> int:
        """Get number of elements."""
        return len(self._min_heap)

    def is_empty(self) -> bool:
        """Check if empty."""
        return self._min_heap.is_empty()


class PriorityQueue(Generic[T]):
    """A priority queue based on min heap."""

    def __init__(self):
        """Initialize the priority queue."""
        self._heap = DataMinHeapAction[T]()

    def enqueue(self, priority: float, item: T) -> int:
        """Add an item with priority."""
        return self._heap.insert(priority, item)

    def dequeue(self) -> Optional[Tuple[T, float]]:
        """Remove and return highest priority item."""
        return self._heap.extract_min()

    def peek(self) -> Optional[Tuple[T, float]]:
        """Return highest priority item without removing."""
        return self._heap.peek_min()

    def __len__(self) -> int:
        """Get queue size."""
        return len(self._heap)

    def is_empty(self) -> bool:
        """Check if empty."""
        return self._heap.is_empty()

    def clear(self) -> None:
        """Clear the queue."""
        self._heap.clear()


def create_min_heap() -> DataMinHeapAction:
    """Factory function to create a DataMinHeapAction."""
    return DataMinHeapAction()


def create_priority_queue() -> PriorityQueue:
    """Factory function to create a PriorityQueue."""
    return PriorityQueue()
