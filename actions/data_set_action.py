"""
Data Set Action Module.

Provides set data structure operations including union, intersection,
difference, Cartesian product, and specialized set queries.

Author: RabAi Team
"""

from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, Hashable, Iterator, Optional


@dataclass
class SetConfig:
    """Set configuration."""
    thread_safe: bool = False
    immutable: bool = False


@dataclass
class SetOperation:
    """Record of a set operation."""
    operation: str
    operands: list[str]
    result_size: int
    timestamp: float


class DataSet(Generic[Hashable]):
    """Generic set data structure with extended operations."""

    def __init__(
        self,
        items: Optional[list[Hashable]] = None,
        config: Optional[SetConfig] = None,
    ):
        self._items: set[Hashable] = set(items) if items else set()
        self._config = config or SetConfig()
        self._lock = threading.RLock() if self._config.thread_safe else None
        self._operations: list[SetOperation] = []
        self._index: dict[Hashable, int] = {}

    def add(self, item: Hashable) -> bool:
        """Add item to set, return True if added."""
        with self._lock_guard():
            if item not in self._items:
                self._items.add(item)
                self._reindex()
                return True
            return False

    def remove(self, item: Hashable) -> bool:
        """Remove item from set, return True if removed."""
        with self._lock_guard():
            if item in self._items:
                self._items.remove(item)
                self._reindex()
                return True
            return False

    def discard(self, item: Hashable) -> None:
        """Remove item without raising if missing."""
        with self._lock_guard():
            self._items.discard(item)
            self._reindex()

    def contains(self, item: Hashable) -> bool:
        """Check if item is in set."""
        with self._lock_guard():
            return item in self._items

    def union(self, other: DataSet[Hashable]) -> DataSet[Hashable]:
        """Return union of two sets."""
        result = DataSet[Hashable](config=SetConfig(thread_safe=self._config.thread_safe))
        with self._lock_guard():
            result._items = self._items.copy()
        with other._lock_guard():
            result._items.update(other._items)
        self._record_op("union", [id(self), id(other)], len(result))
        return result

    def intersection(self, other: DataSet[Hashable]) -> DataSet[Hashable]:
        """Return intersection of two sets."""
        result = DataSet[Hashable](config=SetConfig(thread_safe=self._config.thread_safe))
        with self._lock_guard():
            result._items = self._items & other._items
        self._record_op("intersection", [id(self), id(other)], len(result))
        return result

    def difference(self, other: DataSet[Hashable]) -> DataSet[Hashable]:
        """Return difference of two sets."""
        result = DataSet[Hashable](config=SetConfig(thread_safe=self._config.thread_safe))
        with self._lock_guard():
            result._items = self._items - other._items
        self._record_op("difference", [id(self), id(other)], len(result))
        return result

    def symmetric_difference(self, other: DataSet[Hashable]) -> DataSet[Hashable]:
        """Return symmetric difference."""
        result = DataSet[Hashable](config=SetConfig(thread_safe=self._config.thread_safe))
        with self._lock_guard():
            result._items = self._items ^ other._items
        self._record_op("symmetric_difference", [id(self), id(other)], len(result))
        return result

    def is_subset(self, other: DataSet[Hashable]) -> bool:
        """Check if this set is subset of another."""
        with self._lock_guard():
            return self._items <= other._items

    def is_superset(self, other: DataSet[Hashable]) -> bool:
        """Check if this set is superset of another."""
        with self._lock_guard():
            return self._items >= other._items

    def is_disjoint(self, other: DataSet[Hashable]) -> bool:
        """Check if sets have no common elements."""
        with self._lock_guard():
            return self._items.isdisjoint(other._items)

    def cartesian_product(self, other: DataSet[Hashable]) -> list[tuple[Hashable, Hashable]]:
        """Return Cartesian product of two sets."""
        with self._lock_guard():
            self_items = list(self._items)
        with other._lock_guard():
            other_items = list(other._items)
        return [(a, b) for a in self_items for b in other_items]

    def power_set(self) -> list[DataSet[Hashable]]:
        """Return power set of this set."""
        with self._lock_guard():
            items = list(self._items)
        n = len(items)
        result = []
        for mask in range(1 << n):
            subset = DataSet[Hashable]()
            for i in range(n):
                if mask & (1 << i):
                    subset.add(items[i])
            result.append(subset)
        return result

    def filter(self, predicate: Callable[[Hashable], bool]) -> DataSet[Hashable]:
        """Return set with elements matching predicate."""
        result = DataSet[Hashable](config=SetConfig(thread_safe=self._config.thread_safe))
        with self._lock_guard():
            result._items = {x for x in self._items if predicate(x)}
        return result

    def map(self, func: Callable[[Hashable], Hashable]) -> DataSet[Hashable]:
        """Return set with function applied to each element."""
        result = DataSet[Hashable](config=SetConfig(thread_safe=self._config.thread_safe))
        with self._lock_guard():
            result._items = {func(x) for x in self._items}
        return result

    def reduce(self, func: Callable[[Hashable, Hashable], Hashable], initial: Optional[Hashable] = None) -> Hashable:
        """Reduce set to single value."""
        with self._lock_guard():
            items = list(self._items)
        if not items:
            if initial is not None:
                return initial
            raise ValueError("Cannot reduce empty set without initial value")
        result = initial if initial is not None else items[0]
        for item in (items[1:] if initial is None else items):
            result = func(result, item)
        return result

    def __len__(self) -> int:
        with self._lock_guard():
            return len(self._items)

    def __contains__(self, item: Hashable) -> bool:
        return self.contains(item)

    def __iter__(self) -> Iterator[Hashable]:
        with self._lock_guard():
            return iter(list(self._items))

    def to_list(self) -> list[Hashable]:
        """Convert to sorted list."""
        with self._lock_guard():
            return sorted(self._items, key=lambda x: str(x))

    def _reindex(self) -> None:
        """Rebuild internal index."""
        self._index = {item: i for i, item in enumerate(self._items)}

    def _record_op(self, op: str, operands: list[int], result_size: int) -> None:
        """Record operation for debugging/analytics."""
        import time
        self._operations.append(SetOperation(
            operation=op,
            operands=[str(o) for o in operands],
            result_size=result_size,
            timestamp=time.time(),
        ))

    def _lock_guard(self):
        if self._lock is None:
            return NoLock()
        return self._lock


class NoLock:
    """No-op context manager for non-thread-safe mode."""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class UnionFind:
    """Union-Find (Disjoint Set Union) data structure."""

    def __init__(self):
        self._parent: dict[Hashable, Hashable] = {}
        self._rank: dict[Hashable, int] = {}

    def make_set(self, x: Hashable) -> None:
        """Create a new set containing only x."""
        if x not in self._parent:
            self._parent[x] = x
            self._rank[x] = 0

    def find(self, x: Hashable) -> Hashable:
        """Find root with path compression."""
        if x not in self._parent:
            self.make_set(x)
        if self._parent[x] != x:
            self._parent[x] = self.find(self._parent[x])
        return self._parent[x]

    def union(self, x: Hashable, y: Hashable) -> None:
        """Union two sets by rank."""
        root_x = self.find(x)
        root_y = self.find(y)
        if root_x != root_y:
            if self._rank[root_x] < self._rank[root_y]:
                self._parent[root_x] = root_y
            elif self._rank[root_x] > self._rank[root_y]:
                self._parent[root_y] = root_x
            else:
                self._parent[root_y] = root_x
                self._rank[root_x] += 1

    def connected(self, x: Hashable, y: Hashable) -> bool:
        """Check if two elements are in same set."""
        return self.find(x) == self.find(y)


class BloomFilter:
    """Probabilistic Bloom filter for set membership."""

    def __init__(self, size: int = 1000, num_hashes: int = 7):
        self._size = size
        self._num_hashes = num_hashes
        self._bits = [False] * size
        self._count = 0

    def _hashes(self, item: Hashable) -> list[int]:
        """Generate hash values for item."""
        item_bytes = str(item).encode()
        results = []
        for i in range(self._num_hashes):
            h = hashlib.md5(item_bytes + str(i).encode()).hexdigest()
            results.append(int(h, 16) % self._size)
        return results

    def add(self, item: Hashable) -> None:
        """Add item to filter."""
        for idx in self._hashes(item):
            self._bits[idx] = True
        self._count += 1

    def contains(self, item: Hashable) -> bool:
        """Check if item might be in filter."""
        return all(self._bits[idx] for idx in self._hashes(item))

    def __len__(self) -> int:
        return self._count


def from_dict_list(data: list[dict], key_field: str) -> DataSet:
    """Create DataSet from list of dicts using a key field."""
    items = [str(d.get(key_field, "")) for d in data]
    return DataSet(items=items)


async def demo():
    """Demo set operations."""
    a = DataSet(items=[1, 2, 3, 4, 5])
    b = DataSet(items=[4, 5, 6, 7, 8])

    print(f"Union: {a.union(b).to_list()}")
    print(f"Intersection: {a.intersection(b).to_list()}")
    print(f"Difference: {a.difference(b).to_list()}")

    uf = UnionFind()
    for x in [1, 2, 3, 4, 5]:
        uf.make_set(x)
    uf.union(1, 2)
    uf.union(3, 4)
    print(f"1 connected to 2: {uf.connected(1, 2)}")
    print(f"1 connected to 3: {uf.connected(1, 3)}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(demo())
