"""Filter utilities for RabAI AutoClick.

Provides:
- Predicate-based filtering helpers
- Multi-condition filters
- Partition utilities
- Key-function based filtering
"""

from __future__ import annotations

from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import re


T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


def filter_by(
    items: List[T],
    predicate: Callable[[T], bool],
) -> List[T]:
    """Filter items using a predicate function.

    Args:
        items: List of items to filter.
        predicate: Function that returns True to keep item.

    Returns:
        Filtered list.
    """
    return [item for item in items if predicate(item)]


def filter_by_key(
    items: List[Dict[str, Any]],
    key: str,
    value: Any,
) -> List[Dict[str, Any]]:
    """Filter dict items by a key=value pair.

    Args:
        items: List of dicts.
        key: Dict key to match.
        value: Required value.

    Returns:
        Filtered list of dicts.
    """
    return [item for item in items if item.get(key) == value]


def filter_by_keys(
    items: List[Dict[str, Any]],
    criteria: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Filter dict items by multiple key=value criteria.

    Args:
        items: List of dicts.
        criteria: Dict of {key: value} pairs to match.

    Returns:
        Filtered list of dicts.
    """
    def matches(item: Dict[str, Any]) -> bool:
        return all(item.get(k) == v for k, v in criteria.items())
    return [item for item in items if matches(item)]


def partition(
    items: List[T],
    predicate: Callable[[T], bool],
) -> Tuple[List[T], List[T]]:
    """Partition items into two lists based on predicate.

    Args:
        items: Items to partition.
        predicate: Function that returns True for first group.

    Returns:
        Tuple of (matching, non_matching).
    """
    yes: List[T] = []
    no: List[T] = []
    for item in items:
        if predicate(item):
            yes.append(item)
        else:
            no.append(item)
    return yes, no


def partition_by_key(
    items: List[Dict[str, Any]],
    key: str,
) -> Dict[Any, List[Dict[str, Any]]]:
    """Group items by a dict key.

    Args:
        items: List of dicts.
        key: Key to group by.

    Returns:
        Dict mapping key values to lists of items.
    """
    result: Dict[Any, List[Dict[str, Any]]] = {}
    for item in items:
        k = item.get(key)
        if k not in result:
            result[k] = []
        result[k].append(item)
    return result


def filter_regex(
    items: List[str],
    pattern: str,
    flags: int = 0,
) -> List[str]:
    """Filter strings by a regex pattern.

    Args:
        items: List of strings.
        pattern: Regex pattern (items that match are kept).
        flags: Regex flags.

    Returns:
        Filtered list of matching strings.
    """
    compiled = re.compile(pattern, flags)
    return [item for item in items if compiled.search(item)]


def reject(
    items: List[T],
    predicate: Callable[[T], bool],
) -> List[T]:
    """Reject items that match the predicate (inverse filter).

    Args:
        items: Items to filter.
        predicate: Function that returns True to exclude.

    Returns:
        List with matching items removed.
    """
    return [item for item in items if not predicate(item)]


def unique_by(
    items: List[T],
    key: Callable[[T], K],
) -> List[T]:
    """Return items with duplicate keys removed (first occurrence kept).

    Args:
        items: Items to dedupe.
        key: Function to extract comparison key.

    Returns:
        Deduplicated list preserving order.
    """
    seen: set = set()
    result: List[T] = []
    for item in items:
        k = key(item)
        if k not in seen:
            seen.add(k)
            result.append(item)
    return result


def deduplicate(
    items: List[T],
) -> List[T]:
    """Remove duplicate items preserving order.

    Args:
        items: Items to dedupe.

    Returns:
        Deduplicated list.
    """
    seen: set = set()
    result: List[T] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def chunk_filter(
    items: List[T],
    chunk_size: int,
    predicate: Callable[[List[T]], bool],
) -> List[List[T]]:
    """Partition items into chunks and filter chunks by predicate.

    Args:
        items: Items to chunk.
        chunk_size: Size of each chunk.
        predicate: Returns True to keep chunk.

    Returns:
        List of kept chunks.
    """
    result: List[List[T]] = []
    for i in range(0, len(items), chunk_size):
        chunk = items[i : i + chunk_size]
        if predicate(chunk):
            result.append(chunk)
    return result


def filter_by_range(
    items: List[T],
    key: Callable[[T], float],
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
) -> List[T]:
    """Filter items by numeric range on a key function.

    Args:
        items: Items to filter.
        key: Numeric extraction function.
        min_val: Minimum value (inclusive).
        max_val: Maximum value (inclusive).

    Returns:
        Filtered list.
    """
    result: List[T] = []
    for item in items:
        val = key(item)
        if min_val is not None and val < min_val:
            continue
        if max_val is not None and val > max_val:
            continue
        result.append(item)
    return result


__all__ = [
    "filter_by",
    "filter_by_key",
    "filter_by_keys",
    "partition",
    "partition_by_key",
    "filter_regex",
    "reject",
    "unique_by",
    "deduplicate",
    "chunk_filter",
    "filter_by_range",
]
