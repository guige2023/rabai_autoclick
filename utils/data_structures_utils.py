"""Data structures utilities.

Provides common data structure implementations and
operations for automation workflows.
"""

from collections import defaultdict
from typing import Any, Dict, Generic, Iterator, List, Optional, Set, Tuple, TypeVar


T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


class BiMap(Generic[K, V]):
    """Bidirectional map for key-value and value-key lookup.

    Example:
        bimap = BiMap()
        bimap["key1"] = "value1"
        print(bimap["key1"])  # "value1"
        print(bimap.inverse["value1"])  # "key1"
    """

    def __init__(self) -> None:
        self._forward: Dict[K, V] = {}
        self._inverse: Dict[V, K] = {}

    def __setitem__(self, key: K, value: V) -> None:
        if key in self._forward:
            del self._inverse[self._forward[key]]
        if value in self._inverse:
            del self._forward[self._inverse[value]]
        self._forward[key] = value
        self._inverse[value] = key

    def __getitem__(self, key: K) -> V:
        return self._forward[key]

    def get(self, key: K, default: Optional[V] = None) -> Optional[V]:
        return self._forward.get(key, default)

    def __contains__(self, key: object) -> bool:
        return key in self._forward

    @property
    def inverse(self) -> Dict[V, K]:
        """Get inverse view."""
        return self._inverse

    def keys(self) -> List[K]:
        return list(self._forward.keys())

    def values(self) -> List[V]:
        return list(self._forward.values())

    def items(self) -> List[Tuple[K, V]]:
        return list(self._forward.items())

    def __len__(self) -> int:
        return len(self._forward)

    def clear(self) -> None:
        self._forward.clear()
        self._inverse.clear()


class MultiMap(Generic[K, V]):
    """Map that supports multiple values per key.

    Example:
        mmap = MultiMap()
        mmap.add("key1", "value1")
        mmap.add("key1", "value2")
        print(mmap["key1"])  # ["value1", "value2"]
    """

    def __init__(self) -> None:
        self._data: Dict[K, List[V]] = defaultdict(list)

    def add(self, key: K, value: V) -> None:
        """Add value for key."""
        self._data[key].append(value)

    def get(self, key: K) -> List[V]:
        """Get all values for key."""
        return list(self._data[key])

    def get_first(self, key: K, default: Optional[V] = None) -> Optional[V]:
        """Get first value for key."""
        if key in self._data and self._data[key]:
            return self._data[key][0]
        return default

    def remove(self, key: K, value: V) -> bool:
        """Remove specific value from key."""
        if key in self._data:
            try:
                self._data[key].remove(value)
                if not self._data[key]:
                    del self._data[key]
                return True
            except ValueError:
                pass
        return False

    def remove_all(self, key: K) -> None:
        """Remove all values for key."""
        if key in self._data:
            del self._data[key]

    def __getitem__(self, key: K) -> List[V]:
        return self.get(key)

    def __contains__(self, key: object) -> bool:
        return key in self._data

    def keys(self) -> List[K]:
        return list(self._data.keys())

    def items(self) -> List[Tuple[K, List[V]]]:
        return [(k, list(v)) for k, v in self._data.items()]

    def __len__(self) -> int:
        return len(self._data)


class UnionFind:
    """Union-Find (Disjoint Set Union) data structure.

    Example:
        uf = UnionFind(5)  # 5 elements: 0, 1, 2, 3, 4
        uf.union(0, 1)
        uf.union(1, 2)
        print(uf.find(0) == uf.find(2))  # True
    """

    def __init__(self, n: int) -> None:
        self._parent = list(range(n))
        self._rank = [0] * n

    def find(self, x: int) -> int:
        """Find root with path compression."""
        if self._parent[x] != x:
            self._parent[x] = self.find(self._parent[x])
        return self._parent[x]

    def union(self, x: int, y: int) -> None:
        """Union two sets by rank."""
        px, py = self.find(x), self.find(y)
        if px == py:
            return
        if self._rank[px] < self._rank[py]:
            px, py = py, px
        self._parent[py] = px
        if self._rank[px] == self._rank[py]:
            self._rank[px] += 1

    def connected(self, x: int, y: int) -> bool:
        """Check if two elements are in same set."""
        return self.find(x) == self.find(y)


class PriorityQueue(Generic[T]):
    """Priority queue implementation using heap.

    Example:
        pq = PriorityQueue()
        pq.push(1, "low")
        pq.push(3, "high")
        print(pq.pop())  # ("high", 3)
    """

    def __init__(self) -> None:
        self._heap: List[Tuple[int, T]] = []

    def push(self, priority: int, item: T) -> None:
        """Push item with priority."""
        import heapq
        heapq.heappush(self._heap, (priority, item))

    def pop(self) -> Optional[T]:
        """Pop highest priority item."""
        import heapq
        if not self._heap:
            return None
        return heapq.heappop(self._heap)[1]

    def peek(self) -> Optional[T]:
        """Peek at highest priority without removing."""
        if not self._heap:
            return None
        return self._heap[0][1]

    def __len__(self) -> int:
        return len(self._heap)

    def __bool__(self) -> bool:
        return len(self._heap) > 0


class SortedList(Generic[T]):
    """Sorted list with binary search.

    Example:
        slist = SortedList()
        slist.add(3)
        slist.add(1)
        slist.add(2)
        print(slist.to_list())  # [1, 2, 3]
    """

    def __init__(self, items: Optional[List[T]] = None) -> None:
        self._items = sorted(items) if items else []

    def add(self, item: T) -> None:
        """Add item maintaining sort order."""
        import bisect
        bisect.insort(self._items, item)

    def remove(self, item: T) -> bool:
        """Remove item from list."""
        try:
            self._items.remove(item)
            return True
        except ValueError:
            return False

    def __len__(self) -> int:
        return len(self._items)

    def __bool__(self) -> bool:
        return len(self._items) > 0

    def __contains__(self, item: object) -> bool:
        return item in self._items

    def __iter__(self) -> Iterator[T]:
        return iter(self._items)

    def to_list(self) -> List[T]:
        return list(self._items)

    def clear(self) -> None:
        self._items.clear()
