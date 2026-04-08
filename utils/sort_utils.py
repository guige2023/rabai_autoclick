"""Sort utilities for RabAI AutoClick.

Provides:
- Stable sorting helpers
- Multi-key sorting
- Natural sort for strings with numbers
- Sorting decorators
"""

from __future__ import annotations

import re
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
)


T = TypeVar("T")


def sort_by(
    items: List[T],
    key: Callable[[T], Any],
    reverse: bool = False,
) -> List[T]:
    """Sort items by a key function.

    Args:
        items: Items to sort.
        key: Key function.
        reverse: Sort descending if True.

    Returns:
        Sorted list.
    """
    return sorted(items, key=key, reverse=reverse)


def sort_by_keys(
    items: List[Dict[str, Any]],
    keys: List[str],
    reverse: bool = False,
) -> List[Dict[str, Any]]:
    """Sort dicts by multiple keys in order.

    Args:
        items: List of dicts to sort.
        keys: List of dict keys to sort by.
        reverse: Sort descending if True.

    Returns:
        Sorted list.
    """
    def multi_key(item: Dict[str, Any]) -> Tuple[Any, ...]:
        return tuple(item.get(k) for k in keys)
    return sorted(items, key=multi_key, reverse=reverse)


def stable_sort(
    items: List[T],
    key: Optional[Callable[[T], Any]] = None,
) -> List[T]:
    """Sort with guaranteed stability (equal items keep original order).

    Args:
        items: Items to sort.
        key: Optional key function.

    Returns:
        Sorted list (stable).
    """
    return sorted(items, key=key)


def natural_sort_key(text: str) -> List[Any]:
    """Generate a sort key for natural sorting (handles embedded numbers).

    Example: 'file10' sorts after 'file2'.

    Args:
        text: Text to generate key for.

    Returns:
        Sort key tuple.
    """
    parts = re.split(r"(\d+)", text)
    result: List[Any] = []
    for i, part in enumerate(parts):
        if i % 2 == 0:
            result.append(part.lower())
        else:
            result.append(int(part))
    return result


def natural_sort(items: List[str]) -> List[str]:
    """Sort strings with embedded numbers naturally.

    Args:
        items: Strings to sort.

    Returns:
        Naturally sorted list.
    """
    return sorted(items, key=natural_sort_key)


def sort_with_indices(
    items: List[T],
    key: Optional[Callable[[T], Any]] = None,
    reverse: bool = False,
) -> Tuple[List[T], List[int]]:
    """Sort items and return original indices.

    Args:
        items: Items to sort.
        key: Optional key function.
        reverse: Sort descending if True.

    Returns:
        Tuple of (sorted_items, original_indices).
    """
    indexed = list(enumerate(items))
    sorted_indexed = sorted(indexed, key=lambda x: key(x[1]) if key else x[1], reverse=reverse)
    sorted_items = [item for _, item in sorted_indexed]
    indices = [idx for idx, _ in sorted_indexed]
    return sorted_items, indices


def ranked_order(
    items: List[T],
    key: Callable[[T], Any],
    ties_first: bool = False,
) -> List[int]:
    """Assign ranks to items based on sorted order.

    Args:
        items: Items to rank.
        key: Key function for sorting.
        ties_first: If True, tied items get same rank then skip.

    Returns:
        List of rank values (0-based).
    """
    sorted_items = sorted(items, key=key)
    if ties_first:
        ranks = []
        current_rank = 0
        i = 0
        while i < len(sorted_items):
            j = i
            while j < len(sorted_items) and key(sorted_items[j]) == key(sorted_items[i]):
                j += 1
            for _ in range(i, j):
                ranks.append(current_rank)
            current_rank = j
            i = j
        index_map = {id(item): pos for pos, item in enumerate(items)}
        return [ranks[index_map[id(item)]] for item in items]
    else:
        return [sorted_items.index(item) for item in items]


__all__ = [
    "sort_by",
    "sort_by_keys",
    "stable_sort",
    "natural_sort_key",
    "natural_sort",
    "sort_with_indices",
    "ranked_order",
]
