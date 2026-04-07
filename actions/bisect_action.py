"""Bisect action for rabai_autoclick.

Provides binary search utilities including left/right bisect,
search sorted, key functions, and interval finding.
"""

from __future__ import annotations

import bisect
from bisect import bisect_left, bisect_right, insort_left, insort_right
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
    Iterator,
    Sequence,
    TypeVar,
)

__all__ = [
    "bisect_left",
    "bisect_right",
    "insort_left",
    "insort_right",
    "bisect_find",
    "bisect_find_all",
    "bisect_range",
    "bisect_left_by",
    "bisect_right_by",
    "bisect_insert",
    "bisect_insert_by",
    "bisect_count",
    "bisect_contains",
    "bisect_first",
    "bisect_last",
    "bisect_intervals",
    "bisect_overlapping",
    "sorted_merge",
    "sorted_intersection",
    "sorted_union",
    "bisect_rank",
    "bisect_percentile",
    "bisect_search_insert",
    "SortedList",
    "IntervalTree",
    "RankedList",
]


T = TypeVar("T")
K = TypeVar("K")


def bisect_find(haystack: Sequence[T], needle: T) -> int | None:
    """Find index of needle in sorted haystack, or None if not found.

    Args:
        haystack: Sorted sequence to search.
        needle: Value to find.

    Returns:
        Index of needle, or None if not found.
    """
    idx = bisect.bisect_left(haystack, needle)
    if idx < len(haystack) and haystack[idx] == needle:
        return idx
    return None


def bisect_find_all(haystack: Sequence[T], needle: T) -> list[int]:
    """Find all indices where needle appears in sorted haystack.

    Args:
        haystack: Sorted sequence.
        needle: Value to find.

    Returns:
        List of all matching indices.
    """
    if not haystack:
        return []
    left = bisect.bisect_left(haystack, needle)
    right = bisect.bisect_right(haystack, needle)
    return list(range(left, right))


def bisect_range(haystack: Sequence[T], lo: T, hi: T) -> tuple[int, int]:
    """Find the range of indices where values fall within [lo, hi].

    Args:
        haystack: Sorted sequence.
        lo: Lower bound.
        hi: Upper bound.

    Returns:
        Tuple of (left_index, right_index) for the range.
    """
    left = bisect.bisect_left(haystack, lo)
    right = bisect.bisect_right(haystack, hi)
    return (left, right)


def bisect_left_by(haystack: Sequence[Any], needle: Any, key: Callable[[Any], Any]) -> int:
    """Bisect left using a key function.

    Args:
        haystack: Sorted sequence of items.
        needle: Value to insert/search.
        key: Function to extract comparison key from items.

    Returns:
        Index where needle should be inserted.
    """
    return bisect.bisect_left([key(x) for x in haystack], key(needle))


def bisect_right_by(haystack: Sequence[Any], needle: Any, key: Callable[[Any], Any]) -> int:
    """Bisect right using a key function.

    Args:
        haystack: Sorted sequence of items.
        needle: Value to insert/search.
        key: Function to extract comparison key from items.

    Returns:
        Index where needle should be inserted.
    """
    return bisect.bisect_right([key(x) for x in haystack], key(needle))


def bisect_insert(haystack: list[T], item: T) -> int:
    """Insert item into sorted list and return insertion index.

    Args:
        haystack: Sorted list to insert into.
        item: Item to insert.

    Returns:
        Index where item was inserted.
    """
    idx = bisect.bisect_left(haystack, item)
    bisect.insort_left(haystack, item)
    return idx


def bisect_insert_by(haystack: list[Any], item: Any, key: Callable[[Any], Any]) -> int:
    """Insert item into sorted list using key function.

    Args:
        haystack: Sorted list to insert into.
        item: Item to insert.
        key: Function to extract sort key.

    Returns:
        Index where item was inserted.
    """
    keys = [key(x) for x in haystack]
    item_key = key(item)
    idx = bisect.bisect_left(keys, item_key)
    haystack.insert(idx, item)
    return idx


def bisect_count(haystack: Sequence[T], needle: T) -> int:
    """Count occurrences of needle in sorted haystack.

    Args:
        haystack: Sorted sequence.
        needle: Value to count.

    Returns:
        Number of occurrences.
    """
    left = bisect.bisect_left(haystack, needle)
    right = bisect.bisect_right(haystack, needle)
    return right - left


def bisect_contains(haystack: Sequence[T], needle: T) -> bool:
    """Check if needle exists in sorted haystack.

    Args:
        haystack: Sorted sequence.
        needle: Value to check.

    Returns:
        True if needle is in haystack.
    """
    idx = bisect.bisect_left(haystack, needle)
    return idx < len(haystack) and haystack[idx] == needle


def bisect_first(haystack: Sequence[T], needle: T) -> int | None:
    """Find first occurrence of needle in sorted haystack.

    Args:
        haystack: Sorted sequence.
        needle: Value to find.

    Returns:
        Index of first occurrence, or None if not found.
    """
    idx = bisect.bisect_left(haystack, needle)
    if idx < len(haystack) and haystack[idx] == needle:
        return idx
    return None


def bisect_last(haystack: Sequence[T], needle: T) -> int | None:
    """Find last occurrence of needle in sorted haystack.

    Args:
        haystack: Sorted sequence.
        needle: Value to find.

    Returns:
        Index of last occurrence, or None if not found.
    """
    idx = bisect.bisect_right(haystack, needle) - 1
    if idx >= 0 and haystack[idx] == needle:
        return idx
    return None


def bisect_intervals(haystack: Sequence[tuple[T, T]], point: T) -> list[int]:
    """Find all intervals that contain the given point.

    Args:
        haystack: Sorted sequence of (start, end) tuples.
        point: Point to check.

    Returns:
        List of indices of intervals containing point.
    """
    result = []
    for i, (lo, hi) in enumerate(haystack):
        if lo <= point <= hi:
            result.append(i)
    return result


def bisect_overlapping(a: Sequence[tuple[T, T]], b: Sequence[tuple[T, T]]) -> list[tuple[int, int]]:
    """Find all pairs of overlapping intervals between two sorted interval lists.

    Args:
        a: First sorted interval list.
        b: Second sorted interval list.

    Returns:
        List of (index_in_a, index_in_b) tuples for overlapping pairs.
    """
    result = []
    i = 0
    j = 0
    while i < len(a) and j < len(b):
        a_start, a_end = a[i]
        b_start, b_end = b[j]
        if a_start <= b_end and b_start <= a_end:
            result.append((i, j))
        if a_end < b_end:
            i += 1
        else:
            j += 1
    return result


def sorted_merge(*lists: Sequence[T]) -> list[T]:
    """Merge multiple sorted lists into one sorted list.

    Args:
        *lists: Two or more sorted sequences.

    Returns:
        Merged sorted list.
    """
    result: list[T] = []
    import heapq
    for lst in lists:
        result.extend(lst)
    result.sort()
    return result


def sorted_intersection(*lists: Sequence[T]) -> list[T]:
    """Find intersection of multiple sorted lists.

    Args:
        *lists: Two or more sorted sequences.

    Returns:
        List of elements appearing in all lists.
    """
    if not lists:
        return []
    result = list(lists[0])
    for lst in lists[1:]:
        result = sorted(set(result) & set(lst))
    return result


def sorted_union(*lists: Sequence[T]) -> list[T]:
    """Find union of multiple sorted lists.

    Args:
        *lists: Two or more sorted sequences.

    Returns:
        Sorted list of unique elements from all lists.
    """
    result = set()
    for lst in lists:
        result.update(lst)
    return sorted(result)


def bisect_rank(haystack: Sequence[T], item: T) -> int:
    """Get rank of item in sorted haystack (1-based).

    Args:
        haystack: Sorted sequence.
        item: Item to rank.

    Returns:
        1-based rank of item.
    """
    return bisect.bisect_right(haystack, item)


def bisect_percentile(haystack: Sequence[float], percentile: float) -> float:
    """Find value at the given percentile.

    Args:
        haystack: Sorted sequence of numeric values.
        percentile: Percentile between 0 and 100.

    Returns:
        Value at the given percentile.

    Raises:
        ValueError: If percentile is out of range.
    """
    if not (0 <= percentile <= 100):
        raise ValueError(f"Percentile must be between 0 and 100, got {percentile}")
    if not haystack:
        raise ValueError("Cannot find percentile of empty sequence")
    n = len(haystack)
    k = (n - 1) * percentile / 100
    f = int(k)
    c = f + 1 if f + 1 < n else f
    return haystack[f] + (haystack[c] - haystack[f]) * (k - f)


def bisect_search_insert(sorted_list: list[T], item: T) -> int:
    """Insert item into sorted list maintaining order.

    Args:
        sorted_list: Sorted list to modify in place.
        item: Item to insert.

    Returns:
        Index where item was inserted.
    """
    bisect.insort(sorted_list, item)
    return bisect.bisect_left(sorted_list, item)


class SortedList(Generic[T]):
    """Sorted list with binary search operations."""

    def __init__(self, data: Iterable[T] | None = None) -> None:
        self._data: list[T] = sorted(data) if data else []

    def add(self, item: T) -> int:
        """Add item and return insertion index."""
        idx = bisect.bisect_left(self._data, item)
        bisect.insort_left(self._data, item)
        return idx

    def remove(self, item: T) -> bool:
        """Remove first occurrence of item."""
        idx = bisect.bisect_left(self._data, item)
        if idx < len(self._data) and self._data[idx] == item:
            del self._data[idx]
            return True
        return False

    def find(self, item: T) -> int | None:
        """Find item index or None."""
        idx = bisect.bisect_left(self._data, item)
        if idx < len(self._data) and self._data[idx] == item:
            return idx
        return None

    def find_all(self, item: T) -> list[int]:
        """Find all indices of item."""
        return bisect_find_all(self._data, item)

    def count(self, item: T) -> int:
        """Count occurrences of item."""
        return bisect_count(self._data, item)

    def range(self, lo: T, hi: T) -> list[T]:
        """Get all items in [lo, hi] range."""
        left, right = bisect_range(self._data, lo, hi)
        return self._data[left:right]

    def __len__(self) -> int:
        return len(self._data)

    def __contains__(self, item: T) -> bool:
        return bisect_contains(self._data, item)

    def __getitem__(self, index: int) -> T:
        return self._data[index]

    def __iter__(self) -> Iterator[T]:
        return iter(self._data)

    def __repr__(self) -> str:
        return f"SortedList({self._data!r})"


class IntervalTree:
    """Interval tree for efficient overlap queries."""

    def __init__(self, intervals: Iterable[tuple[Any, Any]] | None = None) -> None:
        self._intervals: list[tuple[Any, Any]] = []
        if intervals:
            for interval in intervals:
                self.add(interval)

    def add(self, interval: tuple[Any, Any]) -> None:
        """Add an interval (start, end)."""
        self._intervals.append(interval)
        self._intervals.sort()

    def query_point(self, point: Any) -> list[int]:
        """Find all intervals containing point."""
        return bisect_intervals(self._intervals, point)

    def query_overlap(self, other: tuple[Any, Any]) -> list[int]:
        """Find intervals overlapping with given interval."""
        start, end = other
        result = []
        for i, (s, e) in enumerate(self._intervals):
            if s <= end and start <= e:
                result.append(i)
        return result

    def all_overlaps(self) -> list[tuple[int, int]]:
        """Find all overlapping pairs within the tree."""
        return bisect_overlapping(self._intervals, self._intervals)

    def __len__(self) -> int:
        return len(self._intervals)


class RankedList(Generic[T]):
    """Sorted list with rank/order statistics."""

    def __init__(self, data: Iterable[T] | None = None) -> None:
        self._data: list[T] = sorted(data) if data else []

    def add(self, item: T) -> None:
        """Add an item."""
        bisect.insort(self._data, item)

    def rank_of(self, item: T) -> int:
        """Get 1-based rank of item."""
        return bisect_rank(self._data, item)

    def percentile_of(self, item: T) -> float:
        """Get percentile of item (0-100)."""
        rank = bisect_rank(self._data, item)
        return (rank / len(self._data)) * 100

    def at_percentile(self, percentile: float) -> T:
        """Get item at given percentile."""
        return bisect_percentile(self._data, percentile)

    def median(self) -> T:
        """Get median item."""
        return bisect_percentile(self._data, 50)

    def __len__(self) -> int:
        return len(self._data)
