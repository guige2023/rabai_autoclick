"""Transform utilities for RabAI AutoClick.

Provides:
- Data transformation helpers
- Mapping utilities
- Conversion functions
"""

from __future__ import annotations

from typing import (
    Any,
    Callable,
    Dict,
    List,
    TypeVar,
)


T = TypeVar("T")
U = TypeVar("U")


def map_values(
    data: Dict[str, T],
    transform: Callable[[T], U],
) -> Dict[str, U]:
    """Transform all values in a dict.

    Args:
        data: Input dict.
        transform: Function to apply to each value.

    Returns:
        Dict with transformed values.
    """
    return {k: transform(v) for k, v in data.items()}


def map_keys(
    data: Dict[str, T],
    transform: Callable[[str], str],
) -> Dict[str, T]:
    """Transform all keys in a dict.

    Args:
        data: Input dict.
        transform: Function to apply to each key.

    Returns:
        Dict with transformed keys.
    """
    return {transform(k): v for k, v in data.items()}


def filter_values(
    data: Dict[str, T],
    predicate: Callable[[T], bool],
) -> Dict[str, T]:
    """Filter dict by values.

    Args:
        data: Input dict.
        predicate: Filter function.

    Returns:
        Filtered dict.
    """
    return {k: v for k, v in data.items() if predicate(v)}


def invert_dict(data: Dict[str, T]) -> Dict[T, str]:
    """Invert a dict (swap keys and values).

    Args:
        data: Input dict.

    Returns:
        Inverted dict.
    """
    return {v: k for k, v in data.items()}


def merge_dicts(
    *dicts: Dict[str, Any],
) -> Dict[str, Any]:
    """Merge multiple dicts.

    Args:
        *dicts: Dicts to merge.

    Returns:
        Merged dict.
    """
    result: Dict[str, Any] = {}
    for d in dicts:
        result.update(d)
    return result


def pluck(
    items: List[Dict[str, Any]],
    key: str,
) -> List[Any]:
    """Extract values for a key from list of dicts.

    Args:
        items: List of dicts.
        key: Key to extract.

    Returns:
        List of values.
    """
    return [item.get(key) for item in items if key in item]


__all__ = [
    "map_values",
    "map_keys",
    "filter_values",
    "invert_dict",
    "merge_dicts",
    "pluck",
]
