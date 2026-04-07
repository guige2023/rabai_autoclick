"""collections action extensions for rabai_autoclick.

Provides utilities for working with Python collections including
counters, ordered dicts, default dicts, named tuples, and more.
"""

from __future__ import annotations

import collections
from collections import (
    Counter,
    OrderedDict,
    defaultdict,
    deque,
    namedtuple,
)
from typing import (
    Any,
    Callable,
    Generic,
    Iterator,
    TypeVar,
)

__all__ = [
    "Counter",
    "OrderedDict",
    "defaultdict",
    "deque",
    "namedtuple",
    "count_items",
    "most_common",
    "least_common",
    "count_by",
    "group_by",
    "partition",
    "flatten",
    "unflatten",
    "merge_dicts",
    "chain_iterables",
    "chunk_iterable",
    "window_iterable",
    "unique_iterable",
    "unique_everseen",
    "partition_by",
    "frequency",
    "count_values",
    "invert_dict",
    "safe_get",
    "safe_set",
    "deep_get",
    "deep_set",
    "deep_defaultdict",
    "FrozenCounter",
    "LRUCache",
    "FIFOCache",
    "SortedDict",
    "OrderedDefaultDict",
    "ExpiringDict",
]


T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


def count_items(iterable: Iterable, /) -> Counter:
    """Count items in iterable.

    Args:
        iterable: Items to count.

    Returns:
        Counter with counts.
    """
    return collections.Counter(iterable)


def most_common(
    counter: Counter,
    n: int | None = None,
    reverse: bool = True,
) -> list[tuple[Any, int]]:
    """Get most common elements.

    Args:
        counter: Counter object.
        n: Return top n elements (None for all).
        reverse: Sort descending (most common first).

    Returns:
        List of (item, count) tuples.
    """
    if reverse:
        return counter.most_common(n)
    items = counter.most_common()
    if n is not None:
        items = items[-n:]
    return list(reversed(items))


def least_common(
    counter: Counter,
    n: int | None = None,
) -> list[tuple[Any, int]]:
    """Get least common elements.

    Args:
        counter: Counter object.
        n: Return bottom n elements (None for all).

    Returns:
        List of (item, count) tuples.
    """
    items = counter.most_common()
    if n is not None:
        items = items[:-n-1:-1]
    else:
        items = list(reversed(items))
    return items


def count_by(
    iterable: Iterable,
    key: Callable[[Any], Any] | None = None,
) -> dict[Any, int]:
    """Count items by key function.

    Args:
        iterable: Items to count.
        key: Function to group by.

    Returns:
        Dict of key to count.
    """
    counter: Counter = collections.Counter()
    for item in iterable:
        k = key(item) if key else item
        counter[k] += 1
    return dict(counter)


def group_by(
    iterable: Iterable,
    key: Callable[[Any], K],
) -> dict[K, list]:
    """Group items by key function.

    Args:
        iterable: Items to group.
        key: Function to group by.

    Returns:
        Dict of key to list of items.
    """
    result: dict[K, list] = {}
    for item in iterable:
        k = key(item)
        if k not in result:
            result[k] = []
        result[k].append(item)
    return result


def partition(
    iterable: Iterable,
    predicate: Callable[[Any], bool],
) -> tuple[list, list]:
    """Partition iterable by predicate.

    Args:
        iterable: Items to partition.
        predicate: Function returning True for first group.

    Returns:
        Tuple of (matching, non-matching).
    """
    matching = []
    non_matching = []
    for item in iterable:
        if predicate(item):
            matching.append(item)
        else:
            non_matching.append(item)
    return matching, non_matching


def flatten(nested: Iterable[Iterable[T]]) -> list[T]:
    """Flatten nested iterables.

    Args:
        nested: Nested iterable structure.

    Returns:
        Flattened list.
    """
    result = []
    for item in nested:
        if isinstance(item, (list, tuple, set)):
            result.extend(flatten(item))  # type: ignore
        else:
            result.append(item)
    return result


def unflatten(flat: list[T], structure: list[int]) -> list[list[T]]:
    """Unflatten list into nested structure.

    Args:
        flat: Flattened list.
        structure: Sizes of sublists.

    Returns:
        Nested list.
    """
    result = []
    idx = 0
    for size in structure:
        result.append(flat[idx:idx+size])
        idx += size
    return result


def merge_dicts(*dicts: dict[K, V], strategy: str = "last") -> dict[K, V]:
    """Merge multiple dicts.

    Args:
        *dicts: Dicts to merge.
        strategy: "last" keeps last value, "first" keeps first.

    Returns:
        Merged dict.
    """
    result: dict[K, V] = {}
    for d in dicts:
        for k, v in d.items():
            if strategy == "last" or k not in result:
                result[k] = v
    return result


def chain_iterables(*iterables: Iterable) -> Iterator:
    """Chain multiple iterables.

    Args:
        *iterables: Iterables to chain.

    Returns:
        Iterator over all items.
    """
    return (item for iterable in iterables for item in iterable)


def chunk_iterable(
    iterable: Iterable,
    size: int,
) -> Iterator[list]:
    """Split iterable into chunks.

    Args:
        iterable: Items to chunk.
        size: Chunk size.

    Yields:
        Lists of chunk size.
    """
    items = list(iterable)
    for i in range(0, len(items), size):
        yield items[i:i+size]


def window_iterable(
    iterable: Iterable,
    size: int,
    step: int = 1,
) -> Iterator[list]:
    """Create sliding window over iterable.

    Args:
        iterable: Items to window.
        size: Window size.
        step: Step between windows.

    Yields:
        Lists of window size.
    """
    items = list(iterable)
    for i in range(0, len(items) - size + 1, step):
        yield items[i:i+size]


def unique_iterable(iterable: Iterable, /, key: Callable | None = None) -> list:
    """Get unique items preserving order.

    Args:
        iterable: Items to dedupe.
        key: Optional key function.

    Returns:
        List of unique items.
    """
    if key is None:
        seen = set()
        result = []
        for item in iterable:
            if item not in seen:
                seen.add(item)
                result.append(item)
        return result
    else:
        seen = set()
        result = []
        for item in iterable:
            k = key(item)
            if k not in seen:
                seen.add(k)
                result.append(item)
        return result


def unique_everseen(iterable: Iterable, /, key: Callable | None = None) -> Iterator:
    """Yield unique items in order, unseen.

    Args:
        iterable: Items to dedupe.
        key: Optional key function.

    Yields:
        Unique items.
    """
    seen = set()
    for item in iterable:
        k = key(item) if key else item
        if k not in seen:
            seen.add(k)
            yield item


def partition_by(
    iterable: Iterable,
    key: Callable[[Any], Any] | None = None,
) -> Iterator[list]:
    """Partition by consecutive same keys.

    Args:
        iterable: Items to partition.
        key: Key function.

    Yields:
        Lists of consecutive items.
    """
    current_key = None
    current_group = []

    for item in iterable:
        k = key(item) if key else item
        if k != current_key:
            if current_group:
                yield current_group
            current_key = k
            current_group = [item]
        else:
            current_group.append(item)

    if current_group:
        yield current_group


def frequency(iterable: Iterable) -> dict[Any, int]:
    """Count frequency of items.

    Args:
        iterable: Items to count.

    Returns:
        Dict of item to frequency.
    """
    return dict(collections.Counter(iterable))


def count_values(iterable: Iterable) -> int:
    """Count total number of values.

    Args:
        iterable: Items to count.

    Returns:
        Total count.
    """
    return sum(collections.Counter(iterable).values())


def invert_dict(d: dict[K, V]) -> dict[V, K]:
    """Invert dict (swap keys and values).

    Args:
        d: Dict to invert.

    Returns:
        Inverted dict.

    Raises:
        ValueError: If values are not hashable.
    """
    return {v: k for k, v in d.items()}


def safe_get(d: dict, *keys: Any, default: Any = None) -> Any:
    """Safely get nested dict value.

    Args:
        d: Dict to search.
        *keys: Sequence of keys.
        default: Default if not found.

    Returns:
        Value or default.
    """
    result = d
    for key in keys:
        if isinstance(result, dict) and key in result:
            result = result[key]
        else:
            return default
    return result


def safe_set(d: dict, *keys: Any, value: Any = None) -> None:
    """Safely set nested dict value.

    Args:
        d: Dict to modify.
        *keys: Sequence of keys (last is value).
        value: Value to set (if keys empty).
    """
    if not keys:
        return

    *key_path, final_key = keys
    target = d

    for key in key_path:
        if key not in target:
            target[key] = {}
        target = target[key]

    target[final_key] = value


def deep_get(d: dict, path: list[str], default: Any = None) -> Any:
    """Get value from nested dict using path.

    Args:
        d: Dict to search.
        path: List of keys.
        default: Default if not found.

    Returns:
        Value or default.
    """
    return safe_get(d, *path, default=default)


def deep_set(d: dict, path: list[str], value: Any) -> None:
    """Set value in nested dict using path.

    Args:
        d: Dict to modify.
        path: List of keys.
        value: Value to set.
    """
    if not path:
        return
    *key_path, final_key = path
    safe_set(d, *key_path, **{final_key: value})


def deep_defaultdict(default_factory: Callable) -> defaultdict:
    """Create infinitely nested defaultdict.

    Args:
        default_factory: Factory for missing values.

    Returns:
        Nested defaultdict.
    """
    return collections.defaultdict(lambda: deep_defaultdict(default_factory))


class FrozenCounter(collections.Counter):
    """Immutable Counter with hash support."""

    def __hash__(self) -> int:
        return hash(tuple(sorted(self.elements())))

    def __setitem__(self, key: Any, value: int) -> None:
        raise TypeError("FrozenCounter is immutable")

    def __delitem__(self, key: Any) -> None:
        raise TypeError("FrozenCounter is immutable")

    def clear(self) -> None:
        raise TypeError("FrozenCounter is immutable")


class LRUCache(Generic[K, V]):
    """Least Recently Used cache."""

    def __init__(self, maxsize: int = 128) -> None:
        self._cache: OrderedDict[K, V] = OrderedDict()
        self._maxsize = maxsize

    def get(self, key: K) -> V | None:
        """Get value, moving to end (most recent)."""
        if key in self._cache:
            self._cache.move_to_end(key)
            return self._cache[key]
        return None

    def set(self, key: K, value: V) -> None:
        """Set value, evicting LRU if full."""
        if key in self._cache:
            self._cache.move_to_end(key)
        else:
            if len(self._cache) >= self._maxsize:
                self._cache.popitem(last=False)
        self._cache[key] = value

    def __contains__(self, key: object) -> bool:
        return key in self._cache

    def __len__(self) -> int:
        return len(self._cache)

    def clear(self) -> None:
        self._cache.clear()


class FIFOCache(Generic[K, V]):
    """First In First Out cache."""

    def __init__(self, maxsize: int = 128) -> None:
        self._cache: dict[K, V] = {}
        self._order: deque[K] = deque()
        self._maxsize = maxsize

    def get(self, key: K) -> V | None:
        """Get value."""
        return self._cache.get(key)

    def set(self, key: K, value: V) -> None:
        """Set value, evicting oldest if full."""
        if key in self._cache:
            return
        if len(self._cache) >= self._maxsize:
            oldest = self._order.popleft()
            del self._cache[oldest]
        self._cache[key] = value
        self._order.append(key)

    def __contains__(self, key: object) -> bool:
        return key in self._cache

    def __len__(self) -> int:
        return len(self._cache)


class SortedDict(Generic[K, V]):
    """Dict with sorted keys."""

    def __init__(self, data: dict[K, V] | None = None) -> None:
        self._data: dict[K, V] = data or {}
        self._keys: list[K] = sorted(self._data.keys())

    def __getitem__(self, key: K) -> V:
        return self._data[key]

    def __setitem__(self, key: K, value: V) -> None:
        if key not in self._data:
            self._keys.append(key)
            self._keys.sort()
        self._data[key] = value

    def __delitem__(self, key: K) -> None:
        del self._data[key]
        self._keys.remove(key)

    def __iter__(self) -> Iterator[K]:
        return iter(self._keys)

    def __len__(self) -> int:
        return len(self._keys)

    def keys(self) -> list[K]:
        return self._keys.copy()

    def values(self) -> list[V]:
        return [self._data[k] for k in self._keys]

    def items(self) -> list[tuple[K, V]]:
        return [(k, self._data[k]) for k in self._keys]


class OrderedDefaultDict(collections.defaultdict):
    """DefaultDict that maintains insertion order."""

    def __init__(self, default_factory: Callable | None = None) -> None:
        super().__init__(default_factory)
        self._order: list = []

    def __setitem__(self, key: Any, value: Any) -> None:
        if key not in self:
            self._order.append(key)
        super().__setitem__(key, value)

    def __delitem__(self, key: Any) -> None:
        super().__delitem__(key)
        self._order.remove(key)

    def __iter__(self) -> Iterator:
        return iter(self._order)

    def keys(self) -> list:
        return self._order.copy()

    def values(self) -> list:
        return [self[k] for k in self._order]

    def items(self) -> list[tuple]:
        return [(k, self[k]) for k in self._order]


class ExpiringDict(dict):
    """Dict with TTL for each entry."""

    def __init__(self, ttl: float = 60.0) -> None:
        import time
        super().__init__()
        self._ttl = ttl
        self._times: dict = {}

    def __setitem__(self, key: Any, value: Any) -> None:
        import time
        super().__setitem__(key, value)
        self._times[key] = time.time()

    def __getitem__(self, key: Any) -> Any:
        import time
        if super().__contains__(key):
            if time.time() - self._times[key] > self._ttl:
                del self[key]
                del self._times[key]
                raise KeyError(key)
        return super().__getitem__(key)

    def cleanup(self) -> None:
        """Remove expired entries."""
        import time
        expired = [
            k for k, t in self._times.items()
            if time.time() - t > self._ttl
        ]
        for k in expired:
            del self[k]
            del self._times[k]


from collections.abc import Iterable
