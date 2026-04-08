"""Tuple utilities for RabAI AutoClick.

Provides:
- Tuple operations and manipulation
- Named tuple helpers
- Tuple conversion
"""

from __future__ import annotations

from typing import (
    Any,
    Dict,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Type,
)


def first(t: Tuple[Any, ...]) -> Optional[Any]:
    """Get first element of tuple."""
    return t[0] if t else None


def last(t: Tuple[Any, ...]) -> Optional[Any]:
    """Get last element of tuple."""
    return t[-1] if t else None


def head(t: Tuple[Any, ...]) -> Optional[Any]:
    """Get first element (alias for first)."""
    return first(t)


def tail(t: Tuple[Any, ...]) -> Tuple[Any, ...]:
    """Get all elements except first."""
    return t[1:] if len(t) > 1 else ()


def init(t: Tuple[Any, ...]) -> Tuple[Any, ...]:
    """Get all elements except last."""
    return t[:-1] if len(t) > 1 else ()


def flatten(nested: Tuple[Tuple[Any, ...], ...]) -> Tuple[Any, ...]:
    """Flatten nested tuples.

    Args:
        nested: Tuple of tuples.

    Returns:
        Flattened tuple.
    """
    result: List[Any] = []
    for t in nested:
        result.extend(t)
    return tuple(result)


def map_tuple(func: Callable[[Any], Any], t: Tuple[Any, ...]) -> Tuple[Any, ...]:
    """Map a function over tuple elements.

    Args:
        func: Transformation function.
        t: Input tuple.

    Returns:
        Transformed tuple.
    """
    return tuple(func(x) for x in t)


def filter_tuple(pred: Callable[[Any], bool], t: Tuple[Any, ...]) -> Tuple[Any, ...]:
    """Filter tuple elements.

    Args:
        pred: Filter predicate.
        t: Input tuple.

    Returns:
        Filtered tuple.
    """
    return tuple(x for x in t if pred(x))


def zip_tuples(
    t1: Tuple[Any, ...],
    t2: Tuple[Any, ...],
) -> Tuple[Tuple[Any, Any], ...]:
    """Zip two tuples together.

    Args:
        t1: First tuple.
        t2: Second tuple.

    Returns:
        Zipped tuple of pairs.
    """
    return tuple(zip(t1, t2))


def tuple_to_list(t: Tuple[Any, ...]) -> List[Any]:
    """Convert tuple to list."""
    return list(t)


def list_to_tuple(lst: List[Any]) -> Tuple[Any, ...]:
    """Convert list to tuple."""
    return tuple(lst)


__all__ = [
    "first",
    "last",
    "head",
    "tail",
    "init",
    "flatten",
    "map_tuple",
    "filter_tuple",
    "zip_tuples",
    "tuple_to_list",
    "list_to_tuple",
]


from typing import Callable  # noqa: E402
