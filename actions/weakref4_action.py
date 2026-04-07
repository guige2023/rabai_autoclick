"""Weakref utilities v4 - simple weak reference utilities.

Simple weak reference utilities.
"""

from __future__ import annotations

import weakref
from typing import Any, Callable, Generic, TypeVar

__all__ = [
    "weak_ref",
    "weak_ref_callback",
    "weak_cache",
    "weak_method",
]


T = TypeVar("T")


def weak_ref(obj: T, callback: Callable[[weakref], None] | None = None) -> weakref.ref[T]:
    """Create weak reference.

    Args:
        obj: Object to reference.
        callback: Optional callback.

    Returns:
        Weak reference.
    """
    return weakref.ref(obj, callback)


def weak_ref_callback(callback: Callable[[Any], None]) -> Callable[[weakref], None]:
    """Create weak reference callback.

    Args:
        callback: Function to call with dereferenced object.

    Returns:
        Callback for weakref.
    """
    def on_delete(ref: weakref) -> None:
        callback(ref())
    return on_delete


def weak_cache(func: Callable[..., T]) -> Callable[..., T | None]:
    """Create weak cache for function results.

    Args:
        func: Function to cache.

    Returns:
        Cached function.
    """
    cache: dict[Any, weakref.ref] = {}
    def cached(*args: Any, **kwargs: Any) -> T | None:
        key = (args, tuple(sorted(kwargs.items()))
        if key in cache:
            result = cache[key]()
            if result is not None:
                return result
            del cache[key]
        result = func(*args, **kwargs)
        cache[key] = weakref.ref(result)
        return result
    return cached


def weak_method(method: Callable[..., Any]) -> Callable[..., Any]:
    """Create weak reference to bound method.

    Args:
        method: Bound method.

    Returns:
        Weak reference wrapper.
    """
    ref = weakref.ref(method.__self__)
    func = method.__func__
    def weak_call(*args: Any, **kwargs: Any) -> Any:
        obj = ref()
        if obj is None:
            raise ReferenceError("Object no longer exists")
        return func(obj, *args, **kwargs)
    return weak_call
