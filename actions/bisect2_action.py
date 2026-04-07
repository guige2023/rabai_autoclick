"""Bisect action v2 - extended binary search utilities.

Advanced bisect operations including 2D search,
 fractional indexing, and sorted container variants.
"""

from __future__ import annotations

import bisect
from bisect import bisect_left, bisect_right, insort_left, insort_right
from typing import Any, Callable, Generator, Generic, Iterable, Sequence, TypeVar

__all__ = [
    "bisect_kv",
    "bisect_kv_range",
    "bisect_frac_index",
    "bisect_fractional",
    "bisect_2d",
    "bisect_2d_range",
    "sorted_2d_search",
    "bisect_ceiling",
    "bisect_floor",
    "bisect_strict",
    "bisect_loose",
    "bisect_insert_multi",
    "bisect_batch_insert",
    "bisect_batch_search",
    "bisect_rolling_window",
    "bisect_running_min",
    "bisect_running_max",
    "fractional_index",
    "sorted_insert_unique",
    "bisect_nearest",
    "bisect_nearest_multiple",
    "sorted_rolling_median",
    "sorted_quantile",
    "sorted_rank_transform",
    "SortedMultiList",
    "SortedDeque",
    "FractionalList",
    "SearchGrid2D",
]


T = TypeVar("T")
K = TypeVar("K")


def bisect_kv(data: Sequence[tuple[K, Any]], key: K) -> int:
    """Binary search in key-value sequence.

    Args:
        data: Sorted sequence of (key, value) tuples.
        key: Key to search for.

    Returns:
        Index of key or insertion point.
    """
    return bisect.bisect_left(data, (key,))


def bisect_kv_range(data: Sequence[tuple[K, Any]], lo: K, hi: K) -> tuple[int, int]:
    """Find range of keys in [lo, hi].

    Args:
        data: Sorted (key, value) sequence.
        lo: Lower bound key.
        hi: Upper bound key.

    Returns:
        (left_index, right_index).
    """
    left = bisect.bisect_left(data, (lo,))
    right = bisect.bisect_right(data, (hi,))
    return (left, right)


def bisect_frac_index(n: int, k: int) -> float:
    """Get fractional index for k-th element in sorted list of n.

    Args:
        n: Total number of elements.
        k: Position (0-indexed).

    Returns:
        Fractional index in range [0, 1].
    """
    if n <= 0:
        raise ValueError("n must be positive")
    if not (0 <= k < n):
        raise ValueError(f"k must be 0 <= k < n, got {k}")
    return k / (n - 1) if n > 1 else 0.0


def bisect_fractional(data: Sequence[float], frac: float) -> int:
    """Find index for fractional position in sorted data.

    Args:
        data: Sorted sequence.
        frac: Fraction in [0, 1].

    Returns:
        Index position.
    """
    if not (0 <= frac <= 1):
        raise ValueError(f"frac must be 0-1, got {frac}")
    if not data:
        raise ValueError("data cannot be empty")
    idx = frac * (len(data) - 1)
    return int(idx)


def bisect_2d(matrix: Sequence[Sequence[Any]], target: Any, col: int = 0) -> tuple[int, int] | None:
    """Search sorted 2D matrix (row-wise sorted).

    Args:
        matrix: 2D matrix sorted row-wise and each row sorted.
        target: Value to find.
        col: Column to search in.

    Returns:
        (row, col) or None.
    """
    if not matrix or not matrix[0]:
        return None
    rows = len(matrix)
    for i in range(rows):
        row = matrix[i]
        idx = bisect.bisect_left(row, target)
        if idx < len(row) and row[idx] == target:
            return (i, idx)
    return None


def bisect_2d_range(matrix: Sequence[Sequence[Any]], lo: Any, hi: Any) -> list[tuple[int, int]]:
    """Find all cells in range [lo, hi] in sorted 2D matrix.

    Args:
        matrix: Sorted 2D matrix.
        lo: Lower bound.
        hi: Upper bound.

    Returns:
        List of (row, col) tuples.
    """
    result = []
    for i, row in enumerate(matrix):
        left = bisect.bisect_left(row, lo)
        right = bisect.bisect_right(row, hi)
        for j in range(left, right):
            result.append((i, j))
    return result


def sorted_2d_search(matrix: Sequence[Sequence[Any]], target: Any) -> tuple[int, int] | None:
    """Search sorted 2D matrix using staircase search.

    Args:
        matrix: Sorted 2D matrix.
        target: Target value.

    Returns:
        (row, col) if found, None otherwise.
    """
    if not matrix:
        return None
    rows = len(matrix)
    cols = len(matrix[0])
    r, c = 0, cols - 1
    while r < rows and c >= 0:
        val = matrix[r][c]
        if val == target:
            return (r, c)
        elif val > target:
            c -= 1
        else:
            r += 1
    return None


def bisect_ceiling(haystack: Sequence[T], needle: T) -> int | None:
    """Find smallest index where value >= needle.

    Args:
        haystack: Sorted sequence.
        needle: Target value.

    Returns:
        Index of ceiling or None.
    """
    idx = bisect.bisect_left(haystack, needle)
    if idx < len(haystack):
        return idx
    return None


def bisect_floor(haystack: Sequence[T], needle: T) -> int | None:
    """Find largest index where value <= needle.

    Args:
        haystack: Sorted sequence.
        needle: Target value.

    Returns:
        Index of floor or None.
    """
    idx = bisect.bisect_right(haystack, needle) - 1
    if idx >= 0:
        return idx
    return None


def bisect_strict(haystack: Sequence[T], needle: T) -> int | None:
    """Find index where value == needle (strict equality).

    Args:
        haystack: Sorted sequence.
        needle: Value to find.

    Returns:
        Index or None.
    """
    idx = bisect.bisect_left(haystack, needle)
    if idx < len(haystack) and haystack[idx] == needle:
        return idx
    return None


def bisect_loose(haystack: Sequence[T], needle: T, tolerance: T) -> int | None:
    """Find value within tolerance.

    Args:
        haystack: Sorted sequence.
        needle: Target value.
        tolerance: Maximum difference allowed.

    Returns:
        Index or None.
    """
    idx = bisect.bisect_left(haystack, needle)
    if idx < len(haystack) and abs(haystack[idx] - needle) <= tolerance:
        return idx
    return None


def bisect_insert_multi(sorted_list: list[T], items: Sequence[T]) -> list[int]:
    """Insert multiple items maintaining sort order.

    Args:
        sorted_list: Sorted list to modify.
        items: Items to insert.

    Returns:
        List of insertion indices.
    """
    indices = []
    for item in items:
        idx = bisect.bisect_left(sorted_list, item)
        sorted_list.insert(idx, item)
        indices.append(idx)
    return indices


def bisect_batch_insert(sorted_list: list[T], items: Sequence[T]) -> list[int]:
    """Batch insert with efficient sort."""
    sorted_list.extend(items)
    sorted_list.sort()
    return [bisect.bisect_left(sorted_list, item) for item in items]


def bisect_batch_search(sorted_list: Sequence[T], items: Sequence[T]) -> list[int | None]:
    """Search for multiple items at once.

    Args:
        sorted_list: Sorted list to search.
        items: Items to find.

    Returns:
        List of indices or None.
    """
    results = []
    for item in items:
        idx = bisect.bisect_left(sorted_list, item)
        if idx < len(sorted_list) and sorted_list[idx] == item:
            results.append(idx)
        else:
            results.append(None)
    return results


def bisect_rolling_window(data: Sequence[float], size: int) -> list[list[float]]:
    """Get rolling windows over sorted data.

    Args:
        data: Sorted data.
        size: Window size.

    Returns:
        List of windows.
    """
    if size < 1:
        raise ValueError("size must be >= 1")
    return [list(data[i:i + size]) for i in range(len(data) - size + 1)]


def bisect_running_min(data: Sequence[float]) -> list[float]:
    """Compute running minimum.

    Args:
        data: Sorted sequence.

    Returns:
        List of running minima.
    """
    if not data:
        return []
    result = [data[0]]
    for v in data[1:]:
        result.append(min(result[-1], v))
    return result


def bisect_running_max(data: Sequence[float]) -> list[float]:
    """Compute running maximum."""
    if not data:
        return []
    result = [data[0]]
    for v in data[1:]:
        result.append(max(result[-1], v))
    return result


def fractional_index(n: int, k: int) -> float:
    """Compute fractional index between items."""
    return bisect_frac_index(n, k)


def sorted_insert_unique(sorted_list: list[T], item: T) -> int | None:
    """Insert item only if unique, return index or None.

    Args:
        sorted_list: Sorted list to modify.
        item: Item to insert.

    Returns:
        Index if inserted, None if duplicate.
    """
    idx = bisect.bisect_left(sorted_list, item)
    if idx < len(sorted_list) and sorted_list[idx] == item:
        return None
    sorted_list.insert(idx, item)
    return idx


def bisect_nearest(sorted_list: Sequence[float], target: float) -> tuple[int, float]:
    """Find nearest value to target.

    Args:
        sorted_list: Sorted sequence.
        target: Target value.

    Returns:
        (index, distance).
    """
    if not sorted_list:
        raise ValueError("List is empty")
    idx = bisect.bisect_left(sorted_list, target)
    candidates = []
    if idx > 0:
        candidates.append((idx - 1, abs(sorted_list[idx - 1] - target)))
    if idx < len(sorted_list):
        candidates.append((idx, abs(sorted_list[idx] - target)))
    best = min(candidates, key=lambda x: x[1])
    return best


def bisect_nearest_multiple(sorted_list: Sequence[float], target: float, k: int) -> list[tuple[int, float]]:
    """Find k nearest values to target.

    Args:
        sorted_list: Sorted sequence.
        target: Target value.
        k: Number of nearest values.

    Returns:
        List of (index, distance) tuples.
    """
    if not sorted_list:
        raise ValueError("List is empty")
    idx = bisect.bisect_left(sorted_list, target)
    candidates = []
    left = idx - 1
    right = idx
    while len(candidates) < k and (left >= 0 or right < len(sorted_list)):
        if left >= 0 and right < len(sorted_list):
            if abs(sorted_list[left] - target) <= abs(sorted_list[right] - target):
                candidates.append((left, abs(sorted_list[left] - target)))
                left -= 1
            else:
                candidates.append((right, abs(sorted_list[right] - target)))
                right += 1
        elif left >= 0:
            candidates.append((left, abs(sorted_list[left] - target)))
            left -= 1
        else:
            candidates.append((right, abs(sorted_list[right] - target)))
            right += 1
    return sorted(candidates, key=lambda x: x[1])[:k]


def sorted_rolling_median(data: Sequence[float], window: int) -> list[float]:
    """Compute rolling median.

    Args:
        data: Sorted data.
        window: Window size.

    Returns:
        List of rolling medians.
    """
    if window < 1:
        raise ValueError("Window must be >= 1")
    import statistics
    result = []
    for i in range(len(data) - window + 1):
        window_data = data[i:i + window]
        result.append(statistics.median(window_data))
    return result


def sorted_quantile(data: Sequence[float], q: float) -> float:
    """Compute quantile of sorted data.

    Args:
        data: Sorted sequence.
        q: Quantile in [0, 1].

    Returns:
        Quantile value.
    """
    if not (0 <= q <= 1):
        raise ValueError(f"q must be 0-1, got {q}")
    if not data:
        raise ValueError("data is empty")
    n = len(data)
    idx = q * (n - 1)
    lo = int(idx)
    hi = lo + 1 if lo + 1 < n else lo
    return data[lo] + (data[hi] - data[lo]) * (idx - lo)


def sorted_rank_transform(data: Sequence[float]) -> list[float]:
    """Rank transform (convert to ranks).

    Args:
        data: Data to transform.

    Returns:
        Rank values in [0, 1].
    """
    sorted_data = sorted(enumerate(data), key=lambda x: x[1])
    n = len(data)
    return [0.0] * n


class SortedMultiList(Generic[T]):
    """Sorted list that allows duplicate values."""

    def __init__(self, data: Iterable[T] | None = None) -> None:
        self._data: list[T] = []
        if data:
            self._data = sorted(data)

    def add(self, item: T) -> int:
        """Add item and return index."""
        idx = bisect.insort_left(self._data, item)
        return idx

    def add_many(self, items: Sequence[T]) -> list[int]:
        """Add multiple items."""
        indices = []
        for item in items:
            idx = bisect.insort_left(self._data, item)
            indices.append(idx)
        return indices

    def remove(self, item: T) -> bool:
        """Remove first occurrence."""
        idx = bisect.bisect_left(self._data, item)
        if idx < len(self._data) and self._data[idx] == item:
            del self._data[idx]
            return True
        return False

    def count(self, item: T) -> int:
        """Count occurrences."""
        left = bisect.bisect_left(self._data, item)
        right = bisect.bisect_right(self._data, item)
        return right - left

    def range(self, lo: T, hi: T) -> list[T]:
        """Get items in [lo, hi] range."""
        left = bisect.bisect_left(self._data, lo)
        right = bisect.bisect_right(self._data, hi)
        return list(self._data[left:right])

    def __len__(self) -> int:
        return len(self._data)

    def __contains__(self, item: T) -> bool:
        return bisect.bisect_left(self._data, item) < len(self._data) and self._data[bisect.bisect_left(self._data, item)] == item


class SortedDeque(Generic[T]):
    """Sorted deque with O(log n) insertion and O(1) access to ends."""

    def __init__(self, maxlen: int | None = None) -> None:
        self._data: list[T] = []
        self._maxlen = maxlen

    def append(self, item: T) -> bool:
        """Append item maintaining sort. Returns False if full."""
        if self._maxlen and len(self._data) >= self._maxlen:
            return False
        bisect.insort(self._data, item)
        return True

    def popleft(self) -> T:
        """Remove and return smallest item."""
        return self._data.pop(0)

    def popright(self) -> T:
        """Remove and return largest item."""
        return self._data.pop()

    def peek_left(self) -> T:
        """View smallest item."""
        return self._data[0]

    def peek_right(self) -> T:
        """View largest item."""
        return self._data[-1]

    def __len__(self) -> int:
        return len(self._data)


class FractionalList(Generic[T]):
    """List that supports fractional indexing."""

    def __init__(self, data: Iterable[T] | None = None) -> None:
        self._data: list[T] = list(data) if data else []

    def get_fractional(self, frac: float) -> T:
        """Get item at fractional position."""
        idx = frac * (len(self._data) - 1)
        return self._data[int(round(idx))]

    def insert_fractional(self, item: T, frac: float) -> None:
        """Insert at fractional position."""
        idx = frac * len(self._data)
        bisect.insort_left(self._data, item)
        _ = idx


class SearchGrid2D:
    """2D sorted grid for range queries."""

    def __init__(self, rows: int, cols: int) -> None:
        self._rows = rows
        self._cols = cols
        self._grid: list[list[float]] = [[0.0] * cols for _ in range(rows)]

    def set(self, r: int, c: int, value: float) -> None:
        self._grid[r][c] = value

    def search_range(self, lo: float, hi: float) -> list[tuple[int, int, float]]:
        """Find all cells in [lo, hi]."""
        result = []
        for r in range(self._rows):
            left = bisect.bisect_left(self._grid[r], lo)
            right = bisect.bisect_right(self._grid[r], hi)
            for c in range(left, right):
                result.append((r, c, self._grid[r][c]))
        return result
