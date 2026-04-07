"""Function utilities for RabAI AutoClick.

Provides:
- Function composition
- Partial application
- Memoization
- Function introspection
"""

from __future__ import annotations

import functools
from typing import (
    Any,
    Callable,
    Optional,
    TypeVar,
)


T = TypeVar("T")
U = TypeVar("U")
R = TypeVar("R")


def compose(*funcs: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Compose functions right-to-left.

    Args:
        *funcs: Functions to compose.

    Returns:
        Composed function.

    Example:
        f = compose(str.lower, str.strip)
        f("  Hello  ")  # "hello"
    """
    if not funcs:
        return lambda x: x

    def composed(x: Any) -> Any:
        result = x
        for func in reversed(funcs):
            result = func(result)
        return result

    return composed


def pipe(*funcs: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Pipe functions left-to-right.

    Args:
        *funcs: Functions to pipe.

    Returns:
        Piped function.

    Example:
        f = pipe(str.strip, str.lower)
        f("  Hello  ")  # "hello"
    """
    if not funcs:
        return lambda x: x

    def piped(x: Any) -> Any:
        result = x
        for func in funcs:
            result = func(result)
        return result

    return piped


def partial(
    func: Callable[..., R],
    *args: Any,
    **kwargs: Any,
) -> Callable[..., R]:
    """Create partial function application.

    Args:
        func: Function to partially apply.
        *args: Positional arguments to bind.
        **kwargs: Keyword arguments to bind.

    Returns:
        Partially applied function.
    """
    return functools.partial(func, *args, **kwargs)


def flip(func: Callable[[T, U], R]) -> Callable[[U, T], R]:
    """Flip argument order.

    Args:
        func: Function to flip.

    Returns:
        Flipped function.
    """
    @functools.wraps(func)
    def flipped(a: U, b: T) -> R:
        return func(b, a)
    return flipped


def memoize(func: Callable[..., R]) -> Callable[..., R]:
    """Memoize function results.

    Args:
        func: Function to memoize.

    Returns:
        Memoized function.
    """
    return functools.lru_cache(maxsize=None)(func)


def once(func: Callable[..., T]) -> Callable[..., T]:
    """Call function only once.

    Args:
        func: Function to decorate.

    Returns:
        Decorated function.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> T:
        if not wrapper._called:
            wrapper._called = True
            wrapper._result = func(*args, **kwargs)
        return wrapper._result

    wrapper._called = False  # type: ignore
    wrapper._result = None  # type: ignore
    return wrapper


def curry(func: Callable[..., R]) -> Callable[..., R]:
    """Curry a function.

    Args:
        func: Function to curry.

    Returns:
        Curried function.
    """
    @functools.wraps(func)
    def curried(*args, **kwargs) -> Any:
        if len(args) + len(kwargs) >= func.__code__.co_argcount:
            return func(*args, **kwargs)
        return partial(curried, *args, **kwargs)
    return curried


def identity(x: T) -> T:
    """Identity function.

    Args:
        x: Input value.

    Returns:
        Same value.
    """
    return x


def constant(x: T) -> Callable[..., T]:
    """Return a constant function.

    Args:
        x: Value to return.

    Returns:
        Function that always returns x.
    """
    return lambda *args, **kwargs: x


def noop(*args, **kwargs) -> None:
    """No-operation function.

    Returns:
        None.
    """
    pass


def getter(attr: str) -> Callable[[Any], Any]:
    """Create a getter function for attribute.

    Args:
        attr: Attribute name.

    Returns:
        Getter function.
    """
    return lambda obj: getattr(obj, attr)


def itemgetter(key: str) -> Callable[[dict], Any]:
    """Create a getter for dictionary item.

    Args:
        key: Dictionary key.

    Returns:
        Getter function.
    """
    return lambda d: d[key]
