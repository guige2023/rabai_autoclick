"""Data structure utilities for RabAI AutoClick.

Provides:
- Additional data structures
- Collection helpers
"""

from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Generator, Generic, List, Optional, Set, Tuple, TypeVar


T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


class LinkedListNode(Generic[T]):
    """Node in a linked list."""

    def __init__(self, value: T) -> None:
        """Initialize node.

        Args:
            value: Node value.
        """
        self.value = value
        self.next: Optional['LinkedListNode[T]'] = None
        self.prev: Optional['LinkedListNode[T]'] = None


class LinkedList(Generic[T]):
    """Doubly linked list."""

    def __init__(self) -> None:
        self._head: Optional[LinkedListNode[T]] = None
        self._tail: Optional[LinkedListNode[T]] = None
        self._size = 0

    def append(self, value: T) -> None:
        """Append value to end.

        Args:
            value: Value to append.
        """
        node = LinkedListNode(value)

        if self._tail:
            self._tail.next = node
            node.prev = self._tail
            self._tail = node
        else:
            self._head = self._tail = node

        self._size += 1

    def prepend(self, value: T) -> None:
        """Prepend value to start.

        Args:
            value: Value to prepend.
        """
        node = LinkedListNode(value)

        if self._head:
            self._head.prev = node
            node.next = self._head
            self._head = node
        else:
            self._head = self._tail = node

        self._size += 1

    def remove(self, value: T) -> bool:
        """Remove first occurrence of value.

        Args:
            value: Value to remove.

        Returns:
            True if removed.
        """
        current = self._head
        while current:
            if current.value == value:
                if current.prev:
                    current.prev.next = current.next
                else:
                    self._head = current.next

                if current.next:
                    current.next.prev = current.prev
                else:
                    self._tail = current.prev

                self._size -= 1
                return True

            current = current.next

        return False

    def __iter__(self) -> Generator[T, None, None]:
        """Iterate over values."""
        current = self._head
        while current:
            yield current.value
            current = current.next

    def __len__(self) -> int:
        """Get size."""
        return self._size


class Stack(Generic[T]):
    """Simple stack implementation."""

    def __init__(self) -> None:
        self._items: List[T] = []

    def push(self, item: T) -> None:
        """Push item onto stack."""
        self._items.append(item)

    def pop(self) -> Optional[T]:
        """Pop item from stack."""
        if self._items:
            return self._items.pop()
        return None

    def peek(self) -> Optional[T]:
        """Get top item without removing."""
        if self._items:
            return self._items[-1]
        return None

    def is_empty(self) -> bool:
        """Check if stack is empty."""
        return len(self._items) == 0

    def __len__(self) -> int:
        return len(self._items)


class Queue(Generic[T]):
    """Simple queue implementation."""

    def __init__(self) -> None:
        self._items: deque = deque()

    def enqueue(self, item: T) -> None:
        """Add item to queue."""
        self._items.append(item)

    def dequeue(self) -> Optional[T]:
        """Remove item from queue."""
        if self._items:
            return self._items.popleft()
        return None

    def peek(self) -> Optional[T]:
        """Get front item without removing."""
        if self._items:
            return self._items[0]
        return None

    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return len(self._items) == 0

    def __len__(self) -> int:
        return len(self._items)


class PriorityQueue(Generic[T]):
    """Priority queue using heap."""

    def __init__(self) -> None:
        self._heap: List[Tuple[int, T]] = []
        self._counter = 0

    def enqueue(self, item: T, priority: int = 0) -> None:
        """Add item with priority.

        Args:
            item: Item to add.
            priority: Priority (lower = higher priority).
        """
        import heapq
        heapq.heappush(self._heap, (priority, self._counter, item))
        self._counter += 1

    def dequeue(self) -> Optional[T]:
        """Remove and return highest priority item."""
        import heapq
        if self._heap:
            _, _, item = heapq.heappop(self._heap)
            return item
        return None

    def peek(self) -> Optional[T]:
        """Get highest priority item without removing."""
        if self._heap:
            return self._heap[0][2]
        return None

    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return len(self._heap) == 0

    def __len__(self) -> int:
        return len(self._heap)


class BiMap(Generic[K, V]):
    """Bidirectional map.

    Provides O(1) lookup in both directions.
    """

    def __init__(self) -> None:
        self._forward: Dict[K, V] = {}
        self._reverse: Dict[V, K] = {}

    def put(self, key: K, value: V) -> None:
        """Add key-value pair.

        Args:
            key: Key.
            value: Value.
        """
        # Remove old mappings
        if key in self._forward:
            old_value = self._forward[key]
            del self._reverse[old_value]

        if value in self._reverse:
            old_key = self._reverse[value]
            del self._forward[old_key]

        self._forward[key] = value
        self._reverse[value] = key

    def get_by_key(self, key: K) -> Optional[V]:
        """Get value by key."""
        return self._forward.get(key)

    def get_by_value(self, value: V) -> Optional[K]:
        """Get key by value."""
        return self._reverse.get(value)

    def __contains__(self, item: Any) -> bool:
        """Check if key or value exists."""
        return item in self._forward or item in self._reverse

    def __len__(self) -> int:
        return len(self._forward)


class MultiDict(Generic[K, V]):
    """Dictionary with multiple values per key."""

    def __init__(self) -> None:
        self._data: Dict[K, List[V]] = defaultdict(list)

    def add(self, key: K, value: V) -> None:
        """Add value for key."""
        self._data[key].append(value)

    def get(self, key: K) -> List[V]:
        """Get all values for key."""
        return self._data.get(key, [])

    def get_first(self, key: K, default: V = None) -> V:
        """Get first value for key."""
        values = self._data.get(key, [])
        return values[0] if values else default

    def __contains__(self, key: K) -> bool:
        """Check if key exists."""
        return key in self._data

    def __len__(self) -> int:
        return len(self._data)


class FrozenDict(Dict[K, V]):
    """Immutable dictionary."""

    def __hash__(self) -> int:  # type: ignore
        """Make hashable."""
        return hash(tuple(sorted(self.items())))

    def _immutable(self, *args: Any, **kwargs: Any) -> Any:
        raise TypeError("FrozenDict is immutable")


def group_by_seq(items: List[T], key_func: Callable[[T], K]) -> Dict[K, List[T]]:
    """Group items by key function (stable order).

    Args:
        items: Items to group.
        key_func: Function to extract key.

    Returns:
        Dictionary mapping keys to lists of items.
    """
    result: Dict[K, List[T]] = {}
    for item in items:
        key = key_func(item)
        if key not in result:
            result[key] = []
        result[key].append(item)
    return result


def chunk_list(items: List[T], size: int) -> Generator[List[T], None, None]:
    """Split list into chunks.

    Args:
        items: Items to chunk.
        size: Chunk size.

    Yields:
        Chunks of items.
    """
    for i in range(0, len(items), size):
        yield items[i:i + size]


def flatten_dict(nested: Dict[str, Any], separator: str = ".") -> Dict[str, Any]:
    """Flatten nested dictionary.

    Args:
        nested: Nested dictionary.
        separator: Key separator.

    Returns:
        Flattened dictionary.
    """
    result: Dict[str, Any] = {}

    def _flatten(obj: Any, prefix: str = "") -> None:
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_key = f"{prefix}{separator}{key}" if prefix else key
                _flatten(value, new_key)
        else:
            result[prefix] = obj

    _flatten(nested)
    return result