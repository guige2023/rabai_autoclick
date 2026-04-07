"""Copy utilities v4 - simple copy operations.

Simple shallow and deep copy utilities.
"""

from __future__ import annotations

import copy
from typing import Any, TypeVar

__all__ = [
    "shallow",
    "deep",
    "copy_list",
    "copy_dict",
    "copy_tuple",
    "clone",
]


T = TypeVar("T")


def shallow(obj: T) -> T:
    """Create shallow copy.

    Args:
        obj: Object to copy.

    Returns:
        Shallow copy.
    """
    return copy.copy(obj)


def deep(obj: T) -> T:
    """Create deep copy.

    Args:
        obj: Object to copy.

    Returns:
        Deep copy.
    """
    return copy.deepcopy(obj)


def copy_list(lst: list[T]) -> list[T]:
    """Copy a list.

    Args:
        lst: List to copy.

    Returns:
        New list.
    """
    return list(lst)


def copy_dict(d: dict[K, V]) -> dict[K, V]:
    """Copy a dictionary.

    Args:
        d: Dict to copy.

    Returns:
        New dict.
    """
    return dict(d)


def copy_tuple(t: tuple[T, ...]) -> tuple[T, ...]:
    """Copy a tuple.

    Args:
        t: Tuple to copy.

    Returns:
        New tuple.
    """
    return tuple(t)


def clone(obj: T) -> T:
    """Clone an object.

    Args:
        obj: Object to clone.

    Returns:
        Clone of object.
    """
    try:
        return copy.deepcopy(obj)
    except Exception:
        return copy.copy(obj)
