"""Data joining and relational operations.

This module provides join operations:
- Inner, left, right, full joins
- Lookup operations
- Union and intersection
- Key-based merging

Example:
    >>> from actions.data_joiner_action import join, merge
    >>> result = join(orders, customers, on="customer_id", how="left")
"""

from __future__ import annotations

import logging
from typing import Any, Optional, Callable
from collections import defaultdict

logger = logging.getLogger(__name__)


class JoinType:
    """Join type constants."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"


def join(
    left: list[dict[str, Any]],
    right: list[dict[str, Any]],
    on: Optional[str] = None,
    left_on: Optional[str] = None,
    right_on: Optional[str] = None,
    how: str = JoinType.INNER,
    suffix_left: str = "_x",
    suffix_right: str = "_y",
) -> list[dict[str, Any]]:
    """Join two collections.

    Args:
        left: Left collection.
        right: Right collection.
        on: Field to join on (same name in both).
        left_on: Field in left collection.
        right_on: Field in right collection.
        how: Join type (inner, left, right, full, cross).
        suffix_left: Suffix for conflicting left fields.
        suffix_right: Suffix for conflicting right fields.

    Returns:
        Joined collection.
    """
    left_key = left_on or on
    right_key = right_on or on
    if how == JoinType.CROSS:
        return _cross_join(left, right)
    if not left_key or not right_key:
        raise ValueError("Must specify join key(s)")
    if how == JoinType.INNER:
        return _inner_join(left, right, left_key, right_key, suffix_left, suffix_right)
    elif how == JoinType.LEFT:
        return _left_join(left, right, left_key, right_key, suffix_left, suffix_right)
    elif how == JoinType.RIGHT:
        return _right_join(left, right, left_key, right_key, suffix_left, suffix_right)
    elif how == JoinType.FULL:
        return _full_join(left, right, left_key, right_key, suffix_left, suffix_right)
    return []


def _inner_join(
    left: list[dict[str, Any]],
    right: list[dict[str, Any]],
    left_key: str,
    right_key: str,
    suffix_left: str,
    suffix_right: str,
) -> list[dict[str, Any]]:
    """Inner join implementation."""
    right_index = defaultdict(list)
    for item in right:
        right_index[item.get(right_key)].append(item)
    result = []
    for l_item in left:
        for r_item in right_index.get(l_item.get(left_key), []):
            result.append(_merge_items(l_item, r_item, suffix_left, suffix_right))
    return result


def _left_join(
    left: list[dict[str, Any]],
    right: list[dict[str, Any]],
    left_key: str,
    right_key: str,
    suffix_left: str,
    suffix_right: str,
) -> list[dict[str, Any]]:
    """Left join implementation."""
    right_index = defaultdict(list)
    for item in right:
        right_index[item.get(right_key)].append(item)
    result = []
    for l_item in left:
        matches = right_index.get(l_item.get(left_key), [])
        if matches:
            for r_item in matches:
                result.append(_merge_items(l_item, r_item, suffix_left, suffix_right))
        else:
            result.append(l_item.copy())
    return result


def _right_join(
    left: list[dict[str, Any]],
    right: list[dict[str, Any]],
    left_key: str,
    right_key: str,
    suffix_left: str,
    suffix_right: str,
) -> list[dict[str, Any]]:
    """Right join implementation."""
    left_index = defaultdict(list)
    for item in left:
        left_index[item.get(left_key)].append(item)
    result = []
    for r_item in right:
        matches = left_index.get(r_item.get(right_key), [])
        if matches:
            for l_item in matches:
                result.append(_merge_items(l_item, r_item, suffix_left, suffix_right))
        else:
            result.append(r_item.copy())
    return result


def _full_join(
    left: list[dict[str, Any]],
    right: list[dict[str, Any]],
    left_key: str,
    right_key: str,
    suffix_left: str,
    suffix_right: str,
) -> list[dict[str, Any]]:
    """Full outer join implementation."""
    right_index = defaultdict(list)
    for item in right:
        right_index[item.get(right_key)].append(item)
    left_index = defaultdict(list)
    for item in left:
        left_index[item.get(left_key)].append(item)
    result = []
    all_keys = set(k for k in left_index) | set(k for k in right_index)
    for key in all_keys:
        l_items = left_index.get(key, [None])
        r_items = right_index.get(key, [None])
        for l_item in l_items:
            for r_item in r_items:
                if l_item and r_item:
                    result.append(_merge_items(l_item, r_item, suffix_left, suffix_right))
                elif l_item:
                    result.append(l_item.copy())
                elif r_item:
                    result.append(r_item.copy())
    return result


def _cross_join(
    left: list[dict[str, Any]],
    right: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Cross join (Cartesian product)."""
    result = []
    for l_item in left:
        for r_item in right:
            merged = l_item.copy()
            merged.update(r_item)
            result.append(merged)
    return result


def _merge_items(
    left: dict[str, Any],
    right: dict[str, Any],
    suffix_left: str,
    suffix_right: str,
) -> dict[str, Any]:
    """Merge two items with suffix handling for conflicts."""
    result = {}
    all_keys = set(left.keys()) | set(right.keys())
    for key in all_keys:
        if key in left and key in right:
            if left[key] == right[key]:
                result[key] = left[key]
            else:
                result[key + suffix_left] = left[key]
                result[key + suffix_right] = right[key]
        elif key in left:
            result[key] = left[key]
        else:
            result[key] = right[key]
    return result


def lookup(
    data: list[dict[str, Any]],
    key_field: str,
    lookup_key: Any,
) -> Optional[dict[str, Any]]:
    """Lookup a single item by key.

    Args:
        data: Collection to search.
        key_field: Field to match on.
        lookup_key: Value to find.

    Returns:
        Matched item or None.
    """
    for item in data:
        if item.get(key_field) == lookup_key:
            return item
    return None


def merge_dicts(
    dicts: list[dict[str, Any]],
    key_field: str,
    merge_strategy: str = "first",
) -> list[dict[str, Any]]:
    """Merge dicts with the same key value.

    Args:
        dicts: List of dicts to merge.
        key_field: Field to use as key.
        merge_strategy: How to handle duplicates (first, last, combine).

    Returns:
        Merged list with unique keys.
    """
    index: dict[Any, dict[str, Any]] = {}
    for d in dicts:
        key = d.get(key_field)
        if key not in index:
            index[key] = d.copy()
        else:
            if merge_strategy == "last":
                index[key].update(d)
            elif merge_strategy == "combine":
                for k, v in d.items():
                    if k != key_field:
                        if k in index[key]:
                            if isinstance(index[key][k], list):
                                index[key][k] = index[key][k] + [v]
                            else:
                                index[key][k] = [index[key][k], v]
                        else:
                            index[key][k] = v
    return list(index.values())


def union_all(*collections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Union of multiple collections (with duplicates).

    Args:
        *collections: Collections to union.

    Returns:
        Combined collection.
    """
    result = []
    for coll in collections:
        result.extend(coll)
    return result


def intersect_all(
    *collections: list[dict[str, Any]],
    key_field: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Intersection of multiple collections.

    Args:
        *collections: Collections to intersect.
        key_field: Optional field key for comparison.

    Returns:
        Items common to all collections.
    """
    if not collections:
        return []
    if key_field:
        keysets = [set(d.get(key_field) for d in coll) for coll in collections]
        common_keys = set.intersection(*keysets) if keysets else set()
        return [d for d in collections[0] if d.get(key_field) in common_keys]
    else:
        result = collections[0]
        for coll in collections[1:]:
            result = [item for item in result if item in coll]
        return result


def except_items(
    minuend: list[dict[str, Any]],
    subtrahend: list[dict[str, Any]],
    key_field: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Return items in minuend not in subtrahend.

    Args:
        minuend: Base collection.
        subtrahend: Collection to subtract.
        key_field: Optional field key for comparison.

    Returns:
        Items in minuend minus subtrahend.
    """
    if key_field:
        sub_keys = set(d.get(key_field) for d in subtrahend)
        return [d for d in minuend if d.get(key_field) not in sub_keys]
    else:
        sub_set = set(id(d) for d in subtrahend)
        return [d for d in minuend if id(d) not in sub_set]
