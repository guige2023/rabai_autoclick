"""
Data Skip List Action Module.

Provides a skip list implementation for sorted data storage
with O(log n) average-case operations.
"""

from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone
import random
import logging

logger = logging.getLogger(__name__)


@dataclass
class SkipListNode:
    """A node in the skip list."""
    key: Any
    value: Any
    forward: List["SkipListNode"] = field(default_factory=list)
    level: int = 0

    def __repr__(self) -> str:
        """String representation."""
        return f"SkipListNode(key={self.key}, level={self.level})"


@dataclass
class SkipListStats:
    """Statistics for a skip list."""
    size: int
    max_level: int
    current_level: int
    random_seed: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "size": self.size,
            "max_level": self.max_level,
            "current_level": self.current_level,
            "random_seed": self.random_seed,
        }


class DataSkipListAction:
    """
    Implements a Skip List for sorted key-value storage.

    A skip list is a probabilistic data structure that provides
    O(log n) average-case complexity for insertion, deletion,
    and search operations.

    Example:
        >>> skip_list = DataSkipListAction()
        >>> skip_list.insert(3, "three")
        >>> skip_list.insert(1, "one")
        >>> skip_list.search(3)
        'three'
        >>> skip_list.range_query(1, 5)
        [(1, 'one'), (3, 'three')]
    """

    P = 0.25
    MAX_LEVEL = 16

    def __init__(
        self,
        max_level: int = MAX_LEVEL,
        p: float = P,
        random_seed: Optional[int] = None,
    ):
        """
        Initialize the Skip List.

        Args:
            max_level: Maximum level for nodes.
            p: Probability factor for level generation.
            random_seed: Optional random seed for reproducibility.
        """
        self.max_level = max_level
        self.p = p
        self.random_seed = random_seed

        if random_seed is not None:
            random.seed(random_seed)

        self._header = SkipListNode(key=None, value=None, level=max_level)
        self._level = 0
        self._size = 0

    def _random_level(self) -> int:
        """Generate random level for a new node."""
        level = 0
        while random.random() < self.p and level < self.max_level - 1:
            level += 1
        return level

    def insert(self, key: Any, value: Any) -> bool:
        """
        Insert a key-value pair.

        Args:
            key: Key to insert.
            value: Value to associate.

        Returns:
            True if inserted, False if key already exists.
        """
        update = [None] * self.max_level
        current = self._header

        for i in range(self._level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
            update[i] = current

        current = current.forward[0]

        if current and current.key == key:
            current.value = value
            return False

        new_level = self._random_level()

        if new_level > self._level:
            for i in range(self._level + 1, new_level + 1):
                update[i] = self._header
            self._level = new_level

        new_node = SkipListNode(key=key, value=value, level=new_level)

        for i in range(new_level + 1):
            new_node.forward.append(update[i].forward[i])
            update[i].forward[i] = new_node

        self._size += 1
        return True

    def search(self, key: Any) -> Optional[Any]:
        """
        Search for a key.

        Args:
            key: Key to search for.

        Returns:
            Value if found, None otherwise.
        """
        current = self._header

        for i in range(self._level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
            current = current.forward[i]

            if current and current.key == key:
                return current.value

        return None

    def delete(self, key: Any) -> bool:
        """
        Delete a key.

        Args:
            key: Key to delete.

        Returns:
            True if deleted, False if not found.
        """
        update = [None] * self.max_level
        current = self._header

        for i in range(self._level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
            update[i] = current

        current = current.forward[0]

        if not current or current.key != key:
            return False

        for i in range(current.level + 1):
            update[i].forward[i] = current.forward[i]

        while self._level > 0 and not self._header.forward[self._level]:
            self._level -= 1

        self._size -= 1
        return True

    def range_query(
        self,
        start_key: Any,
        end_key: Any,
        inclusive: bool = True,
    ) -> List[Tuple[Any, Any]]:
        """
        Query a range of keys.

        Args:
            start_key: Start of range.
            end_key: End of range.
            inclusive: Whether to include endpoint keys.

        Returns:
            List of (key, value) tuples.
        """
        results = []
        current = self._header

        for i in range(self._level, -1, -1):
            while current.forward[i] and current.forward[i].key < start_key:
                current = current.forward[i]

        current = current.forward[0]

        while current:
            if inclusive:
                if current.key > end_key:
                    break
                results.append((current.key, current.value))
            else:
                if current.key >= end_key:
                    break
                results.append((current.key, current.value))

            current = current.forward[0]

        return results

    def contains(self, key: Any) -> bool:
        """Check if key exists."""
        return self.search(key) is not None

    def __len__(self) -> int:
        """Get size of skip list."""
        return self._size

    def __contains__(self, key: Any) -> bool:
        """Check if key exists (in operator)."""
        return self.contains(key)

    def __iter__(self) -> Iterator[Any]:
        """Iterate over keys in order."""
        current = self._header.forward[0]
        while current:
            yield current.key
            current = current.forward[0]

    def __reversed__(self) -> Iterator[Any]:
        """Iterate over keys in reverse order."""
        keys = []
        current = self._header.forward[0]

        while current:
            keys.append(current.key)
            current = current.forward[0]

        for key in reversed(keys):
            yield key

    def get_all(self) -> List[Tuple[Any, Any]]:
        """Get all key-value pairs in order."""
        results = []
        current = self._header.forward[0]

        while current:
            results.append((current.key, current.value))
            current = current.forward[0]

        return results

    def get_stats(self) -> SkipListStats:
        """Get skip list statistics."""
        return SkipListStats(
            size=self._size,
            max_level=self.max_level,
            current_level=self._level,
            random_seed=self.random_seed,
        )

    def clear(self) -> None:
        """Clear all entries."""
        self._header.forward = [None] * self.max_level
        self._level = 0
        self._size = 0

    def floor_key(self, key: Any) -> Optional[Any]:
        """Get largest key <= given key."""
        current = self._header

        for i in range(self._level, -1, -1):
            while current.forward[i] and current.forward[i].key <= key:
                current = current.forward[i]

        if current != self._header:
            return current.key

        return None

    def ceiling_key(self, key: Any) -> Optional[Any]:
        """Get smallest key >= given key."""
        current = self._header

        for i in range(self._level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]

        if current.forward[0]:
            return current.forward[0].key

        return None

    def rank(self, key: Any) -> Optional[int]:
        """Get rank (0-based index) of key."""
        current = self._header.forward[0]
        rank = 0

        while current and current.key < key:
            rank += 1
            current = current.forward[0]

        if current and current.key == key:
            return rank

        return None

    def select(self, index: int) -> Optional[Tuple[Any, Any]]:
        """Get key-value at given index."""
        if index < 0 or index >= self._size:
            return None

        current = self._header.forward[0]

        for _ in range(index):
            if current:
                current = current.forward[0]

        if current:
            return (current.key, current.value)

        return None


class ConcurrentSkipList(DataSkipListAction):
    """Thread-safe skip list implementation."""

    def __init__(self, **kwargs):
        """Initialize concurrent skip list."""
        import threading
        super().__init__(**kwargs)
        self._lock = threading.RLock()

    def insert(self, key: Any, value: Any) -> bool:
        """Insert with thread safety."""
        with self._lock:
            return super().insert(key, value)

    def search(self, key: Any) -> Optional[Any]:
        """Search with thread safety."""
        with self._lock:
            return super().search(key)

    def delete(self, key: Any) -> bool:
        """Delete with thread safety."""
        with self._lock:
            return super().delete(key)

    def range_query(
        self,
        start_key: Any,
        end_key: Any,
        inclusive: bool = True,
    ) -> List[Tuple[Any, Any]]:
        """Range query with thread safety."""
        with self._lock:
            return super().range_query(start_key, end_key, inclusive)


def create_skiplist(random_seed: Optional[int] = None, **kwargs) -> DataSkipListAction:
    """Factory function to create a DataSkipListAction."""
    return DataSkipListAction(random_seed=random_seed, **kwargs)
