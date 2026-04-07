"""chunker action module for rabai_autoclick.

Provides data chunking, batching, windowing, and sliding window
operations for stream processing and batch transformations.
"""

from __future__ import annotations

import itertools
import math
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Generic, Iterable, Iterator, List, Optional, Sequence, TypeVar, Union

__all__ = [
    "chunk",
    "batches",
    "window",
    "sliding_window",
    "tumbling_window",
    "windowed",
    "partition",
    "group_adjacent",
    "split_at",
    "split_by_size",
    "split_by_count",
    "split_by_predicate",
    "flatten",
    "flatten_once",
    "chunk_by_size",
    "rolling",
    "pairwise",
    "triplets",
    "sliding_aggregates",
    "Cohort",
    "CohortTracker",
]


T = TypeVar("T")
U = TypeVar("U")


def chunk(data: Sequence[T], size: int) -> Iterator[Sequence[T]]:
    """Split sequence into chunks of specified size.

    Args:
        data: Input sequence.
        size: Maximum chunk size.

    Yields:
        Successive chunks from the input.

    Example:
        >>> list(chunk([1,2,3,4,5], 2))
        [[1, 2], [3, 4], [5]]
    """
    for i in range(0, len(data), size):
        yield data[i:i + size]


def batches(data: Iterable[T], batch_size: int, incomplete: bool = True) -> Iterator[List[T]]:
    """Yield batches from an iterable.

    Args:
        data: Input iterable.
        batch_size: Items per batch.
        incomplete: Include final incomplete batch if True.

    Yields:
        Successive batches.
    """
    batch: List[T] = []
    for item in data:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if incomplete and batch:
        yield batch


def window(data: Iterable[T], size: int) -> Iterator[List[T]]:
    """Fixed-size window over iterable (buffered).

    Args:
        data: Input iterable.
        size: Window size.

    Yields:
        Windows of size elements (last window may be smaller).

    Example:
        >>> list(window([1,2,3,4,5], 3))
        [[1, 2, 3], [2, 3, 4], [3, 4, 5]]
    """
    d = deque(maxlen=size)
    for item in data:
        d.append(item)
        if len(d) == size:
            yield list(d)


def sliding_window(data: Iterable[T], size: int, step: int = 1) -> Iterator[List[T]]:
    """Sliding window with configurable step.

    Args:
        data: Input iterable.
        size: Window size.
        step: Step between windows (1 = every element).

    Yields:
        Windows sliding by step.
    """
    d = deque(maxlen=size)
    buffer: List[T] = []
    count = 0
    for item in data:
        d.append(item)
        buffer.append(item)
        if len(d) == size:
            yield list(d)
            count += 1
            # Advance buffer by step
            for _ in range(step):
                if buffer:
                    buffer.pop(0)
            d = deque(buffer, maxlen=size)


def tumbling_window(data: Iterable[T], size: int) -> Iterator[List[T]]:
    """Tumbling (non-overlapping) window.

    Args:
        data: Input iterable.
        size: Window size.

    Yields:
        Non-overlapping windows.
    """
    return chunk(list(data), size)


def windowed(data: Sequence[T], n: int) -> Iterator[List[T]]:
    """Alias for window with sequence input."""
    return window(data, n)


def partition(
    data: Sequence[T],
    predicate: Callable[[T], bool],
) -> tuple[List[T], List[T]]:
    """Partition sequence into two lists by predicate.

    Args:
        data: Input sequence.
        predicate: Function that returns True for items in first partition.

    Returns:
        Tuple of (matching, non-matching).
    """
    matching: List[T] = []
    non_matching: List[T] = []
    for item in data:
        if predicate(item):
            matching.append(item)
        else:
            non_matching.append(item)
    return matching, non_matching


def group_adjacent(
    data: Iterable[T],
    key: Optional[Callable[[T], Any]] = None,
) -> Iterator[List[T]]:
    """Group adjacent equal elements.

    Args:
        data: Input iterable.
        key: Optional key function for comparison.

    Yields:
        Groups of adjacent equal elements.
    """
    def get_key(x: T) -> Any:
        return key(x) if key else x

    iterator = iter(data)
    try:
        first = next(iterator)
    except StopIteration:
        return

    current_group = [first]
    current_key = get_key(first)

    for item in iterator:
        item_key = get_key(item)
        if item_key == current_key:
            current_group.append(item)
        else:
            yield current_group
            current_group = [item]
            current_key = item_key

    yield current_group


def split_at(data: Sequence[T], index: int) -> tuple[Sequence[T], Sequence[T]]:
    """Split sequence at index.

    Args:
        data: Input sequence.
        index: Index at which to split.

    Returns:
        (before, after) tuple.
    """
    return data[:index], data[index:]


def split_by_size(data: Sequence[T], max_bytes: int) -> Iterator[Sequence[T]]:
    """Split sequence into parts not exceeding max byte size.

    Args:
        data: Input sequence.
        max_bytes: Maximum approximate bytes per chunk.

    Yields:
        Chunks of estimated size.
    """
    current: List[T] = []
    current_size = 0
    for item in data:
        item_size = len(str(item)) if not isinstance(item, (bytes, str)) else len(item) if isinstance(item, (bytes, str)) else len(repr(item))
        if current_size + item_size > max_bytes and current:
            yield current
            current = []
            current_size = 0
        current.append(item)
        current_size += item_size
    if current:
        yield current


def split_by_count(data: Sequence[T], count: int) -> Iterator[Sequence[T]]:
    """Split sequence into approximately equal parts.

    Args:
        data: Input sequence.
        count: Desired number of parts.

    Yields:
        Approximately equal chunks.
    """
    length = len(data)
    if count <= 0:
        yield data
        return
    part_size = math.ceil(length / count)
    for i in range(0, length, part_size):
        yield data[i:i + part_size]


def split_by_predicate(
    data: Sequence[T],
    predicate: Callable[[T], bool],
    include_separator: bool = False,
) -> Iterator[Sequence[T]]:
    """Split sequence wherever predicate is True.

    Args:
        data: Input sequence.
        predicate: Returns True at split points.
        include_separator: Include separator element in results.

    Yields:
        Consecutive segments between split points.
    """
    current: List[T] = []
    for item in data:
        if predicate(item):
            if current:
                yield current
                current = []
            if include_separator:
                yield [item]
        else:
            current.append(item)
    if current:
        yield current


def flatten(nested: Iterable[Iterable[T]]) -> Iterator[T]:
    """Recursively flatten nested iterables.

    Args:
        nested: Nested iterable structure.

    Yields:
        Individual elements in order.
    """
    for item in nested:
        if isinstance(item, (list, tuple, set, frozenset)):
            yield from flatten(item)
        else:
            yield item


def flatten_once(nested: Iterable[Iterable[T]]) -> Iterator[T]:
    """Flatten one level of nesting.

    Args:
        nested: Iterable of iterables.

    Yields:
        Individual elements.
    """
    return itertools.chain.from_iterable(nested)


def chunk_by_size(
    data: Sequence[T],
    max_size: int,
    key: Optional[Callable[[T], Union[int, float]]] = None,
) -> Iterator[List[T]]:
    """Chunk data by cumulative size rather than item count.

    Args:
        data: Input sequence.
        max_size: Maximum cumulative size per chunk.
        key: Function to get item size (default: 1 per item).

    Yields:
        Chunks where sum of sizes <= max_size.
    """
    get_size = key if key else (lambda x: 1)
    current: List[T] = []
    current_size = 0
    for item in data:
        item_size = get_size(item)
        if current_size + item_size > max_size and current:
            yield current
            current = []
            current_size = 0
        current.append(item)
        current_size += item_size
    if current:
        yield current


def rolling(
    data: Iterable[T],
    size: int,
    fill: Optional[T] = None,
) -> Iterator[List[T]]:
    """Rolling window with optional fill value.

    Args:
        data: Input iterable.
        size: Window size.
        fill: Value to use for incomplete windows.

    Yields:
        Windows of size elements.
    """
    d = deque(maxlen=size)
    sentinel_added = False
    if fill is not None and size > 0:
        d.extend([fill] * (size - 1))
    for item in data:
        d.append(item)
        yield list(d)
    if fill is not None:
        sentinel_added = True


def pairwise(data: Iterable[T]) -> Iterator[tuple[T, T]]:
    """Return successive overlapping pairs.

    Args:
        data: Input iterable.

    Yields:
        (item[i], item[i+1]) tuples.
    """
    a, b = itertools.tee(data)
    next(b, None)
    return zip(a, b)


def triplets(data: Iterable[T]) -> Iterator[tuple[T, T, T]]:
    """Return successive overlapping triplets.

    Args:
        data: Input iterable.

    Yields:
        (item[i], item[i+1], item[i+2]) tuples.
    """
    a, b, c = itertools.tee(data, 3)
    next(b, None)
    next(c, None)
    next(c, None)
    return zip(a, b, c)


def sliding_aggregates(
    data: Iterable[T],
    size: int,
    agg: Callable[[Sequence[T]], U] = sum,
    step: int = 1,
) -> Iterator[U]:
    """Compute sliding window aggregates.

    Args:
        data: Input iterable.
        size: Window size.
        agg: Aggregation function (default: sum).
        step: Step between windows.

    Yields:
        Aggregate values for each window.
    """
    for window in window(list(data), size):
        yield agg(window)


class Cohort(Generic[T]):
    """Represents a cohort (group) of items tracked over time."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._items: List[T] = []
        self._timestamps: List[float] = []
        self._data: dict[str, Any] = {}

    def add(self, item: T, timestamp: Optional[float] = None) -> None:
        """Add an item to the cohort."""
        import time as _time
        self._items.append(item)
        self._timestamps.append(timestamp or _time.time())

    def items(self) -> List[T]:
        return list(self._items)

    def count(self) -> int:
        return len(self._items)

    def is_empty(self) -> bool:
        return len(self._items) == 0

    def get_data(self, key: str) -> Any:
        return self._data.get(key)

    def set_data(self, key: str, value: Any) -> None:
        self._data[key] = value


class CohortTracker(Generic[T]):
    """Track items across sequential cohorts/windows."""

    def __init__(self, name: str = "cohort") -> None:
        self.name = name
        self._cohorts: deque[Cohort[T]] = deque()
        self._max_cohorts: int = 100

    def new_cohort(self, name: Optional[str] = None) -> Cohort[T]:
        """Create and register a new cohort."""
        import time
        cohort_name = name or f"cohort_{time.time()}"
        cohort = Cohort[T](cohort_name)
        self._cohorts.append(cohort)
        if len(self._cohorts) > self._max_cohorts:
            self._cohorts.popleft()
        return cohort

    def current_cohort(self) -> Optional[Cohort[T]]:
        """Get the most recent cohort."""
        return self._cohorts[-1] if self._cohorts else None

    def add_to_current(self, item: T) -> None:
        """Add item to current cohort."""
        cohort = self.current_cohort()
        if cohort is None:
            cohort = self.new_cohort()
        cohort.add(item)

    def all_cohorts(self) -> List[Cohort[T]]:
        return list(self._cohorts)

    def cohort_count(self) -> int:
        return len(self._cohorts)
