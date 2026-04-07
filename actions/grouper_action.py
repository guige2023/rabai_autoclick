"""grouper action module for rabai_autoclick.

Provides data grouping operations: groupby, categorize, cluster,
histogram binning, and bucketing utilities for organizing data.
"""

from __future__ import annotations

import bisect
import heapq
from collections import defaultdict, OrderedDict
from dataclasses import dataclass, field
from functools import reduce
from itertools import groupby as itertools_groupby
from operator import itemgetter
from typing import Any, Callable, Generic, Iterable, Iterator, List, Optional, Sequence, TypeVar, Tuple, Union

__all__ = [
    "groupby",
    "categorize",
    "bucket",
    "histogram",
    "cluster_by_range",
    "group_by_size",
    "group_consecutive",
    "group_adjacent_by",
    "CountMinSketch",
    "HyperLogLog",
    "GroupedData",
    "Aggregation",
    "sum_agg",
    "avg_agg",
    "min_agg",
    "max_agg",
    "count_agg",
    "first_agg",
    "last_agg",
    "collect_agg",
    "distinct_agg",
]


T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")
U = TypeVar("U")


class Aggregation(Generic[T, U]):
    """Aggregation function wrapper."""

    def __init__(self, name: str, func: Callable[[List[T]], U]) -> None:
        self.name = name
        self.func = func

    def __call__(self, items: List[T]) -> U:
        return self.func(items)


def sum_agg(items: List[T]) -> U:
    """Sum aggregation."""
    return reduce(lambda a, b: a + b, items)  # type: ignore


def avg_agg(items: List[Union[int, float]]) -> float:
    """Average aggregation."""
    if not items:
        return 0.0
    return sum(items) / len(items)


def min_agg(items: List[T]) -> T:
    """Min aggregation."""
    return min(items)


def max_agg(items: List[T]) -> T:
    """Max aggregation."""
    return max(items)


def count_agg(items: List[T]) -> int:
    """Count aggregation."""
    return len(items)


def first_agg(items: List[T]) -> Optional[T]:
    """First item aggregation."""
    return items[0] if items else None


def last_agg(items: List[T]) -> Optional[T]:
    """Last item aggregation."""
    return items[-1] if items else None


def collect_agg(items: List[T]) -> List[T]:
    """Collect all items into a list."""
    return list(items)


def distinct_agg(items: List[T]) -> List[T]:
    """Return distinct items preserving order."""
    seen: set = set()
    result: List[T] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


class GroupedData(Generic[K, V]):
    """Container for grouped data with aggregation support."""

    def __init__(self, data: dict[K, List[V]]) -> None:
        self._data = data

    def get(self, key: K) -> List[V]:
        return self._data.get(key, [])

    def keys(self) -> Iterator[K]:
        return iter(self._data.keys())

    def items(self) -> Iterator[Tuple[K, List[V]]]:
        return iter(self._data.items())

    def groups(self) -> dict[K, List[V]]:
        return dict(self._data)

    def aggregate(self, key: K, agg: Aggregation[V, U]) -> Optional[U]:
        """Aggregate a specific group."""
        items = self.get(key)
        if not items:
            return None
        return agg.func(items)

    def aggregate_all(self, agg: Aggregation[V, U]) -> dict[K, Optional[U]]:
        """Aggregate all groups with same function."""
        return {k: self.aggregate(k, agg) for k in self._data.keys()}

    def merge(self, other: "GroupedData[K, V]") -> "GroupedData[K, V]":
        """Merge another GroupedData into this one."""
        result = defaultdict(list)
        for k, v in self._data.items():
            result[k].extend(v)
        for k, v in other._data.items():
            result[k].extend(v)
        return GroupedData(dict(result))

    def filter_groups(self, predicate: Callable[[K, List[V]], bool]) -> "GroupedData[K, V]":
        """Filter groups by predicate."""
        filtered = {k: v for k, v in self._data.items() if predicate(k, v)}
        return GroupedData(filtered)

    def transform_groups(self, transform: Callable[[List[V]], List[V]]) -> "GroupedData[K, V]":
        """Transform each group's values."""
        transformed = {k: transform(v) for k, v in self._data.items()}
        return GroupedData(transformed)


def groupby(
    data: Iterable[T],
    key: Callable[[T], K],
    value: Optional[Callable[[T], V]] = None,
) -> GroupedData[K, V]:
    """Group items by key function.

    Args:
        data: Input items.
        key: Function to extract group key from item.
        value: Optional function to extract value (default: whole item).

    Returns:
        GroupedData container.

    Example:
        >>> data = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 1, "b": "z"}]
        >>> grouped = groupby(data, key=lambda x: x["a"])
        >>> grouped.get(1)
        [{'a': 1, 'b': 'x'}, {'a': 1, 'b': 'z'}]
    """
    groups: dict[K, List[V]] = defaultdict(list)
    for item in data:
        k = key(item)
        v = item if value is None else value(item)
        groups[k].append(v)
    return GroupedData(dict(groups))


def categorize(
    items: Iterable[T],
    categories: dict[K, Callable[[T], bool]],
) -> dict[K, List[T]]:
    """Categorize items into named buckets.

    Args:
        items: Input items.
        categories: Mapping of category name to predicate function.

    Returns:
        Dict mapping category name to list of matching items.

    Example:
        >>> categorize([1, 2, 3, 4, 5], {"even": lambda x: x%2==0, "odd": lambda x: x%2==1})
        {"even": [2, 4], "odd": [1, 3, 5]}
    """
    result: dict[K, List[T]] = defaultdict(list)
    for item in items:
        for cat_name, predicate in categories.items():
            if predicate(item):
                result[cat_name].append(item)
                break
    return dict(result)


def bucket(
    data: Sequence[Union[int, float]],
    boundaries: Sequence[Union[int, float]],
    labels: Optional[Sequence[str]] = None,
    right_inclusive: bool = True,
) -> List[str]:
    """Assign items to buckets based on boundaries.

    Args:
        data: Input numeric values.
        boundaries: Sorted bucket boundaries.
        labels: Optional labels for each bucket.
        right_inclusive: If True, boundaries are right-inclusive.

    Returns:
        List of bucket labels for each item.

    Example:
        >>> bucket([1, 5, 10, 15, 20], [0, 10, 20])
        ["[0,10)", "[0,10)", "[10,20)", "[10,20)", "[10,20]"]
    """
    if not labels:
        labels = [f"bucket_{i}" for i in range(len(boundaries))]
    if len(labels) != len(boundaries) + 1:
        raise ValueError("labels must have len(boundaries)+1 elements")

    result: List[str] = []
    sorted_boundaries = sorted(boundaries)

    for value in data:
        idx = bisect.bisect_right(sorted_boundaries, value) if right_inclusive else bisect.bisect_left(sorted_boundaries, value)
        idx = min(idx, len(labels) - 1)
        result.append(labels[idx])

    return result


def histogram(
    data: Sequence[Union[int, float]],
    bins: int = 10,
    range_min: Optional[float] = None,
    range_max: Optional[float] = None,
) -> Tuple[List[int], List[float]]:
    """Compute histogram of values.

    Args:
        data: Input numeric values.
        bins: Number of bins.
        range_min: Optional minimum value for range.
        range_max: Optional maximum value for range.

    Returns:
        Tuple of (counts, bin_edges).
    """
    if not data:
        return [], []

    mn = range_min if range_min is not None else min(data)
    mx = range_max if range_max is not None else max(data)
    if mn == mx:
        mn -= 0.5
        mx += 0.5

    bin_width = (mx - mn) / bins
    counts = [0] * bins

    for value in data:
        if value == mx:
            counts[-1] += 1
        else:
            idx = int((value - mn) / bin_width)
            if 0 <= idx < bins:
                counts[idx] += 1

    edges = [mn + i * bin_width for i in range(bins + 1)]
    return counts, edges


def cluster_by_range(
    data: Sequence[T],
    ranges: Sequence[Tuple[T, T]],
    get_value: Callable[[T], Union[int, float]],
    labels: Optional[Sequence[str]] = None,
) -> dict[str, List[T]]:
    """Cluster items into ranges.

    Args:
        data: Input items.
        ranges: List of (min, max) range tuples.
        get_value: Function to get comparable value from item.
        labels: Optional labels for each range.

    Returns:
        Dict mapping range label to list of items.
    """
    if not labels:
        labels = [f"range_{i}" for i in range(len(ranges))]
    if len(labels) != len(ranges):
        raise ValueError("labels must have same length as ranges")

    result: dict[str, List[T]] = defaultdict(list)
    sorted_ranges = sorted(zip(ranges, labels), key=lambda x: x[0][0])

    for item in data:
        val = get_value(item)
        for (rmin, rmax), label in sorted_ranges:
            if rmin <= val <= rmax:
                result[label].append(item)
                break
    return dict(result)


def group_by_size(
    data: Sequence[T],
    size: int,
) -> List[List[T]]:
    """Group items into groups of specified size.

    Args:
        data: Input sequence.
        size: Items per group.

    Returns:
        List of groups.
    """
    result: List[List[T]] = []
    for i in range(0, len(data), size):
        result.append(list(data[i:i + size]))
    return result


def group_consecutive(
    data: Sequence[T],
    key: Optional[Callable[[T], K]] = None,
) -> List[List[T]]:
    """Group consecutive items with same key.

    Args:
        data: Input sequence.
        key: Optional key function (default: item itself).

    Returns:
        List of consecutive groups.
    """
    if key is None:
        key = lambda x: x  # type: ignore

    result: List[List[T]] = []
    current_group: List[T] = []
    current_key: Optional[K] = None

    for item in data:
        item_key = key(item)
        if current_key is None:
            current_key = item_key
            current_group.append(item)
        elif item_key == current_key:
            current_group.append(item)
        else:
            result.append(current_group)
            current_group = [item]
            current_key = item_key

    if current_group:
        result.append(current_group)

    return result


def group_adjacent_by(
    data: Sequence[T],
    predicate: Callable[[T, T], bool],
) -> List[List[T]]:
    """Group adjacent items where predicate(item[i], item[i+1]) is True.

    Args:
        data: Input sequence.
        predicate: Returns True if items should be in same group.

    Returns:
        List of groups.
    """
    if not data:
        return []

    result: List[List[T]] = [[data[0]]]
    for item in data[1:]:
        if predicate(result[-1][-1], item):
            result[-1].append(item)
        else:
            result.append([item])
    return result


class CountMinSketch:
    """Count-Min Sketch probabilistic data structure for frequency estimation."""

    def __init__(self, width: int = 1000, depth: int = 5) -> None:
        self.width = width
        self.depth = depth
        self._table: List[List[int]] = [[0] * width for _ in range(depth)]
        self._hash_funcs = self._generate_hash_funcs(depth, width)
        self._total: int = 0

    def _generate_hash_funcs(self, depth: int, width: int) -> List[Callable[[Any], int]]:
        import random
        funcs: List[Callable[[Any], int]] = []
        for i in range(depth):
            a = random.randint(1, width - 1)
            b = random.randint(0, width - 1)
            funcs.append(lambda x, a=a, b=b: (hash(x) * a + b) % width)
        return funcs

    def add(self, item: Any) -> None:
        """Add an item to the sketch."""
        self._total += 1
        for i, hf in enumerate(self._hash_funcs):
            self._table[i][hf(item)] += 1

    def estimate(self, item: Any) -> int:
        """Estimate count for an item (upper bound)."""
        return min(self._table[i][hf(item)] for i, hf in enumerate(self._hash_funcs))

    def total(self) -> int:
        """Return total items added."""
        return self._total


class HyperLogLog:
    """HyperLogLog probabilistic cardinality estimator."""

    def __init__(self, p: int = 10) -> None:
        if not (4 <= p <= 16):
            raise ValueError("p must be between 4 and 16")
        self.p = p
        self.m = 1 << p
        self._registers = [0] * self.m

    def add(self, item: Any) -> None:
        """Add an item to the estimator."""
        import math
        x = hash(item)
        idx = x & (self.m - 1)
        bits = x >> self.p
        if bits == 0:
            rho = 1
        else:
            rho = (bits.bit_length() - bits.bit_length() % 1) + 1 if bits > 0 else 1
            rho = self.p - bits.bit_length() + 1
        self._registers[idx] = max(self._registers[idx], int(rho))

    def count(self) -> int:
        """Estimate cardinality."""
        import math
        alpha = 0.7213 / (1 + 1.079 / self.m) if self.m > 128 else 0.673
        z_inv = sum(2 ** -r for r in self._registers)
        z = 1 / z_inv
        return int(alpha * self.m * self.m * z)
