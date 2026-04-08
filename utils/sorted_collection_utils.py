"""Sorted collection utilities.

Provides sorted list, set, and multiset implementations
with efficient search and insertion operations.
"""

import bisect
from typing import Any, Callable, Generic, Iterator, List, Optional, TypeVar


T = TypeVar("T")


class SortedList(Generic[T]):
    """Sorted list with binary search.

    Example:
        slist = SortedList()
        slist.add(3)
        slist.add(1)
        slist.add(2)
        print(slist.to_list())  # [1, 2, 3]
        print(slist[0])  # 1
    """

    def __init__(
        self,
        data: Optional[List[T]] = None,
        key: Optional[Callable[[T], Any]] = None,
    ) -> None:
        self._key = key or (lambda x: x)
        self._items: List[T] = []
        if data:
            for item in data:
                self.add(item)

    def add(self, item: T) -> None:
        """Add item maintaining sort order."""
        key = self._key(item)
        idx = bisect.bisect_left(
            [self._key(x) for x in self._items],
            key
        )
        self._items.insert(idx, item)

    def remove(self, item: T) -> bool:
        """Remove first occurrence of item."""
        try:
            self._items.remove(item)
            return True
        except ValueError:
            return False

    def pop(self, index: int = -1) -> T:
        """Remove and return item at index."""
        return self._items.pop(index)

    def __getitem__(self, index: int) -> T:
        return self._items[index]

    def __len__(self) -> int:
        return len(self._items)

    def __bool__(self) -> bool:
        return len(self._items) > 0

    def __contains__(self, item: object) -> bool:
        return item in self._items

    def __iter__(self) -> Iterator[T]:
        return iter(self._items)

    def to_list(self) -> List[T]:
        """Convert to list."""
        return list(self._items)

    def clear(self) -> None:
        """Clear all items."""
        self._items.clear()

    def index(self, item: T) -> int:
        """Get index of item."""
        return self._items.index(item)

    def bisect_left(self, value: Any) -> int:
        """Get index to insert value (maintaining sort)."""
        return bisect.bisect_left(
            [self._key(x) for x in self._items],
            value
        )

    def bisect_right(self, value: Any) -> int:
        """Get index to insert value (after existing)."""
        return bisect.bisect_right(
            [self._key(x) for x in self._items],
            value
        )

    def find_le(self, value: Any) -> Optional[T]:
        """Find largest item <= value."""
        idx = self.bisect_right(value) - 1
        if idx >= 0:
            return self._items[idx]
        return None

    def find_lt(self, value: Any) -> Optional[T]:
        """Find largest item < value."""
        idx = self.bisect_left(value) - 1
        if idx >= 0:
            return self._items[idx]
        return None

    def find_ge(self, value: Any) -> Optional[T]:
        """Find smallest item >= value."""
        idx = self.bisect_left(value)
        if idx < len(self._items):
            return self._items[idx]
        return None

    def find_gt(self, value: Any) -> Optional[T]:
        """Find smallest item > value."""
        idx = self.bisect_right(value)
        if idx < len(self._items):
            return self._items[idx]
        return None


class SortedSet(Generic[T]):
    """Sorted set with unique elements.

    Example:
        sset = SortedSet([3, 1, 2])
        print(sset.to_list())  # [1, 2, 3]
        sset.add(2)  # no-op, already exists
    """

    def __init__(
        self,
        data: Optional[List[T]] = None,
        key: Optional[Callable[[T], Any]] = None,
    ) -> None:
        self._key = key or (lambda x: x)
        self._items: List[T] = []
        if data:
            for item in data:
                self.add(item)

    def add(self, item: T) -> bool:
        """Add item if not present.

        Returns:
            True if added, False if already exists.
        """
        key = self._key(item)
        idx = bisect.bisect_left(
            [self._key(x) for x in self._items],
            key
        )
        if idx < len(self._items) and self._key(self._items[idx]) == key:
            return False
        self._items.insert(idx, item)
        return True

    def remove(self, item: T) -> bool:
        """Remove item.

        Returns:
            True if removed, False if not found.
        """
        try:
            self._items.remove(item)
            return True
        except ValueError:
            return False

    def __contains__(self, item: object) -> bool:
        key = self._key(item)
        idx = bisect.bisect_left(
            [self._key(x) for x in self._items],
            key
        )
        return idx < len(self._items) and self._key(self._items[idx]) == key

    def __len__(self) -> int:
        return len(self._items)

    def __bool__(self) -> bool:
        return len(self._items) > 0

    def __iter__(self) -> Iterator[T]:
        return iter(self._items)

    def to_list(self) -> List[T]:
        return list(self._items)


class SortedMultiSet(Generic[T]):
    """Sorted multiset allowing duplicate elements.

    Example:
        mset = SortedMultiSet()
        mset.add(1)
        mset.add(1)
        mset.add(2)
        print(mset.count(1))  # 2
    """

    def __init__(
        self,
        data: Optional[List[T]] = None,
        key: Optional[Callable[[T], Any]] = None,
    ) -> None:
        self._key = key or (lambda x: x)
        self._items: List[T] = []
        if data:
            for item in data:
                self.add(item)

    def add(self, item: T) -> None:
        """Add item (duplicates allowed)."""
        key = self._key(item)
        idx = bisect.bisect_right(
            [self._key(x) for x in self._items],
            key
        )
        self._items.insert(idx, item)

    def remove_one(self, item: T) -> bool:
        """Remove one occurrence of item.

        Returns:
            True if removed.
        """
        key = self._key(item)
        idx = bisect.bisect_left(
            [self._key(x) for x in self._items],
            key
        )
        while idx < len(self._items) and self._key(self._items[idx]) == key:
            if self._items[idx] == item:
                self._items.pop(idx)
                return True
            idx += 1
        return False

    def remove_all(self, item: T) -> int:
        """Remove all occurrences of item.

        Returns:
            Number removed.
        """
        key = self._key(item)
        removed = 0
        self._items = [x for x in self._items if self._key(x) != key]
        return removed

    def count(self, item: T) -> int:
        """Count occurrences of item."""
        key = self._key(item)
        return sum(1 for x in self._items if self._key(x) == key)

    def __len__(self) -> int:
        return len(self._items)

    def __bool__(self) -> bool:
        return len(self._items) > 0

    def __iter__(self) -> Iterator[T]:
        return iter(self._items)

    def to_list(self) -> List[T]:
        return list(self._items)
