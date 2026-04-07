"""sorter_action module for rabai_autoclick.

Provides sorting operations: multi-key sorting, stable sorting,
top-k selection, sorting by function, and sorted merge operations.
"""

from __future__ import annotations

import heapq
from collections import defaultdict
from dataclasses import dataclass, field
from functools import cmp_to_key
from typing import Any, Callable, Iterable, Iterator, List, Optional, Sequence, Tuple, TypeVar

__all__ = [
    "sort",
    "sort_by",
    "sort_desc",
    "stable_sort",
    "top_k",
    "bottom_k",
    "sorted_merge",
    "partition_sort",
    "multisort",
    "group_sort",
    "sorted_interleave",
    "is_sorted",
    "SortedResult",
    "SortOrder",
    "SortKey",
]


T = TypeVar("T")


class SortOrder(Enum):
    """Sort order direction."""
    ASC = "asc"
    DESC = "desc"


@dataclass
class SortKey:
    """A sort key with optional comparator."""
    key_fn: Callable[[T], Any]
    order: SortOrder = SortOrder.ASC
    comparator: Optional[Callable[[Any, Any], int]] = None

    def get_value(self, item: T) -> Any:
        """Get sort value for item."""
        return self.key_fn(item)

    def reverse(self) -> bool:
        """Return True if sort should be reversed."""
        return self.order == SortOrder.DESC


@dataclass
class SortedResult(Generic[T]):
    """Result of a sorting operation."""
    items: List[T]
    comparisons: int = 0
    swaps: int = 0

    @property
    def sorted_items(self) -> List[T]:
        return self.items


def sort(
    items: Sequence[T],
    key: Optional[Callable[[T], Any]] = None,
    reverse: bool = False,
) -> List[T]:
    """Sort items using built-in sorted (Timsort, stable).

    Args:
        items: Items to sort.
        key: Optional key function.
        reverse: Sort in descending order.

    Returns:
        Sorted list.
    """
    return sorted(items, key=key, reverse=reverse)


def sort_by(
    items: Sequence[T],
    key_fn: Callable[[T], Any],
    order: SortOrder = SortOrder.ASC,
) -> List[T]:
    """Sort items by key function.

    Args:
        items: Items to sort.
        key_fn: Function to extract sort key.
        order: Sort order (ASC or DESC).

    Returns:
        Sorted list.
    """
    return sorted(items, key=key_fn, reverse=(order == SortOrder.DESC))


def sort_desc(
    items: Sequence[T],
    key: Optional[Callable[[T], Any]] = None,
) -> List[T]:
    """Sort items in descending order.

    Args:
        items: Items to sort.
        key: Optional key function.

    Returns:
        Sorted list (descending).
    """
    return sorted(items, key=key, reverse=True)


def stable_sort(
    items: Sequence[T],
    key: Optional[Callable[[T], Any]] = None,
) -> List[T]:
    """Stable sort - preserves relative order of equal elements.

    Uses Python's built-in sorted which is stable.

    Args:
        items: Items to sort.
        key: Optional key function.

    Returns:
        Stablely sorted list.
    """
    return sorted(items, key=key)


def top_k(
    items: Sequence[T],
    k: int,
    key: Optional[Callable[[T], Any]] = None,
) -> List[T]:
    """Get top k items by sort key (efficient O(n*log(k))).

    Args:
        items: Items to consider.
        k: Number of top items to return.
        key: Optional key function.

    Returns:
        List of top k items.
    """
    if k <= 0:
        return []
    if k >= len(items):
        return sorted(items, key=key, reverse=True)
    return heapq.nlargest(k, items, key=key)


def bottom_k(
    items: Sequence[T],
    k: int,
    key: Optional[Callable[[T], Any]] = None,
) -> List[T]:
    """Get bottom k items by sort key (efficient O(n*log(k))).

    Args:
        items: Items to consider.
        k: Number of bottom items to return.
        key: Optional key function.

    Returns:
        List of bottom k items.
    """
    if k <= 0:
        return []
    if k >= len(items):
        return sorted(items, key=key)
    return heapq.nsmallest(k, items, key=key)


def sorted_merge(
    *sequences: Sequence[T],
    key: Optional[Callable[[T], Any]] = None,
) -> List[T]:
    """Merge multiple sorted sequences into one sorted sequence.

    Args:
        *sequences: Sorted sequences to merge.
        key: Optional key function.

    Returns:
        Merged sorted list.
    """
    result = []
    iterators = [iter(seq) for seq in sequences]
    sentinel = object()
    heap: List[Tuple] = []

    for i, it in enumerate(iterators):
        val = next(it, sentinel)
        if val is not sentinel:
            sort_key = key(val) if key else val
            heapq.heappush(heap, (sort_key, i, val))

    while heap:
        sort_key, i, val = heapq.heappop(heap)
        result.append(val)
        next_val = next(iterators[i], sentinel)
        if next_val is not sentinel:
            next_key = key(next_val) if key else next_val
            heapq.heappush(heap, (next_key, i, next_val))

    return result


def partition_sort(
    items: List[T],
    pivot_fn: Optional[Callable[[List[T]], int]] = None,
) -> List[T]:
    """Quicksort-style partition sort (in-place).

    Args:
        items: Items to sort (modified in place).
        pivot_fn: Optional function to select pivot index.

    Returns:
        Sorted list.
    """
    if len(items) <= 1:
        return items

    if pivot_fn is None:
        pivot_fn = lambda arr: len(arr) // 2

    pivot_idx = pivot_fn(items)
    pivot_val = items[pivot_idx]
    items[pivot_idx], items[-1] = items[-1], items[pivot_idx]

    store_idx = 0
    for i in range(len(items) - 1):
        if items[i] < pivot_val:
            items[store_idx], items[i] = items[i], items[store_idx]
            store_idx += 1

    items[store_idx], items[-1] = items[-1], items[store_idx]

    partition_sort(items[:store_idx])
    partition_sort(items[store_idx + 1:])

    return items


def multisort(
    items: Sequence[T],
    sort_keys: List[SortKey],
) -> List[T]:
    """Sort by multiple keys in sequence (like SQL ORDER BY).

    Args:
        items: Items to sort.
        sort_keys: List of SortKey objects (applied in order).

    Returns:
        Multi-key sorted list.
    """
    def make_key(item: T) -> Tuple:
        return tuple(sk.get_value(item) for sk in sort_keys)

    def make_comparator(item1: T, item2: T) -> int:
        for sk in sort_keys:
            v1 = sk.get_value(item1)
            v2 = sk.get_value(item2)
            if v1 < v2:
                return -1 if not sk.reverse() else 1
            if v1 > v2:
                return 1 if not sk.reverse() else -1
        return 0

    return sorted(items, key=cmp_to_key(make_comparator))


def group_sort(
    items: Sequence[T],
    group_fn: Callable[[T], K],
    sort_key: Optional[Callable[[T], Any]] = None,
) -> dict:
    """Group items by key, sort each group.

    Args:
        items: Items to sort and group.
        group_fn: Function to extract group key.
        sort_key: Optional sort key within each group.

    Returns:
        Dict mapping group key to sorted list of items.
    """
    groups: dict = defaultdict(list)
    for item in items:
        groups[group_fn(item)].append(item)
    for key in groups:
        groups[key] = sorted(groups[key], key=sort_key)
    return dict(groups)


def sorted_interleave(
    sorted_lists: List[List[T]],
    key: Optional[Callable[[T], Any]] = None,
) -> List[T]:
    """Interleave multiple sorted lists maintaining sort order.

    Args:
        sorted_lists: List of sorted lists.
        key: Optional key function.

    Returns:
        Interleaved list that maintains global sorted order.
    """
    result: List[T] = []
    heap: List[Tuple] = []

    for lst in sorted_lists:
        if lst:
            sort_key = key(lst[0]) if key else lst[0]
            heapq.heappush(heap, (sort_key, id(lst), 0, lst))

    while heap:
        sort_key, list_id, idx, lst = heapq.heappop(heap)
        result.append(lst[idx])
        if idx + 1 < len(lst):
            next_key = key(lst[idx + 1]) if key else lst[idx + 1]
            heapq.heappush(heap, (next_key, list_id, idx + 1, lst))

    return result


def is_sorted(
    items: Sequence[T],
    key: Optional[Callable[[T], Any]] = None,
    strict: bool = False,
) -> bool:
    """Check if items are sorted.

    Args:
        items: Items to check.
        key: Optional key function.
        strict: If True, requires strictly increasing (no equals).

    Returns:
        True if items are sorted.
    """
    if len(items) <= 1:
        return True

    get_val = key if key else lambda x: x

    for i in range(1, len(items)):
        a = get_val(items[i - 1])
        b = get_val(items[i])
        if strict:
            if b <= a:
                return False
        else:
            if b < a:
                return False

    return True


def sort_and_dedupe(
    items: Sequence[T],
    key: Optional[Callable[[T], Any]] = None,
) -> List[T]:
    """Sort items and remove duplicates (stable, first occurrence kept).

    Args:
        items: Items to sort and dedupe.
        key: Optional key function for deduplication.

    Returns:
        Sorted list with duplicates removed.
    """
    sorted_items = sorted(items, key=key)
    if key is None:
        seen: set = set()
        result = []
        for item in sorted_items:
            if item not in seen:
                seen.add(item)
                result.append(item)
        return result
    else:
        seen: set = set()
        result = []
        for item in sorted_items:
            k = key(item)
            if k not in seen:
                seen.add(k)
                result.append(item)
        return result
