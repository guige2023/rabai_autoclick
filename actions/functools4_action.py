"""Functools utilities v4 - simple functional utilities.

Simple functional utilities including memoization.
"""

from __future__ import annotations

import functools
from functools import wraps
from typing import Any, Callable, TypeVar

__all__ = [
    "memoize",
    "lru_cache",
    "partial",
    "reduce",
    "wraps",
]


T = TypeVar("T")


def memoize(func: Callable[..., T]) -> Callable[..., T]:
    """Memoize a function.

    Args:
        func: Function to memoize.

    Returns:
        Memoized function.
    """
    cache: dict[Any, T] = {}
    @wraps(func)
    def memoized(*args: Any, **kwargs: Any) -> T:
        key = (args, tuple(sorted(kwargs.items())))
        if key not in cache:
            cache[key] = func(*args, **kwargs)
        return cache[key]
    return memoized


lru_cache = functools.lru_cache
partial = functools.partial
reduce = functools.reduce
wraps = functools.wraps
