"""
Functional partial application utilities.

Provides enhanced partial function creation with
chaining, composition, and placeholder support.
"""

from __future__ import annotations

from functools import partial
from typing import Any, Callable, TypeVar


T = TypeVar("T")
R = TypeVar("R")
P = TypeVar("P")


class _Placeholder:
    """Positional argument placeholder."""

    def __init__(self, index: int):
        self.index = index


def _arg(i: int) -> _Placeholder:
    """Create positional argument placeholder at index i."""
    return _Placeholder(i)


class Partial:
    """
    Enhanced partial with chaining and composition support.
    """

    def __init__(
        self,
        func: Callable,
        *args: object,
        **kwargs: object,
    ):
        self._func = func
        self._args = args
        self._kwargs = kwargs

    def __call__(self, *args: object, **kwargs: object) -> object:
        return self._func(*self._args, *args, **self._kwargs, **kwargs)

    def partial(self, *args: object, **kwargs: object) -> "Partial":
        """Create a new partial with additional arguments."""
        return Partial(self._func, *self._args, *args, **self._kwargs, **kwargs)

    def compose(self, other: Callable[[Any], R]) -> Callable[[Any], R]:
        """Compose this function with another: other(self(x))."""
        def composed(*args: object, **kwargs: object) -> R:
            return other(self(*args, **kwargs))
        return composed

    def rcompose(self, other: Callable[[Any], R]) -> Callable[[Any], R]:
        """Compose with reversed order: self(other(x))."""
        def composed(*args: object, **kwargs: object) -> R:
            return other(self(*args, **kwargs))
        return composed

    def map(self, func: Callable[[Any], R]) -> "Partial":
        """Create new partial that applies func to result."""
        def mapped(*args: object, **kwargs: object) -> R:
            return func(self(*args, **kwargs))
        return Partial(mapped)

    def tap(self, func: Callable[[Any], None]) -> "Partial":
        """Create new partial that calls func with result but returns result."""
        def tapped(*args: object, **kwargs: object) -> Any:
            result = self(*args, **kwargs)
            func(result)
            return result
        return Partial(tapped)

    def then(self, func: Callable[[Any], R]) -> Callable[[Any], R]:
        """Chain function after this partial."""
        def chained(*args: object, **kwargs: object) -> R:
            return func(self(*args, **kwargs))
        return chained

    def __repr__(self) -> str:
        return f"Partial({self._func.__name__}, {self._args}, {self._kwargs})"


def partialize(func: Callable) -> Callable[..., Partial]:
    """
    Wrap function to return Partial instead of partial.

    Args:
        func: Function to wrap

    Returns:
        Function that returns Partial objects
    """
    def wrapper(*args: object, **kwargs: object) -> Partial:
        return Partial(func, *args, **kwargs)
    return wrapper


def compose(*funcs: Callable) -> Callable:
    """
    Compose functions left-to-right: f(g(h(x))).

    Args:
        *funcs: Functions to compose

    Returns:
        Composed function
    """
    def composed(x: Any) -> Any:
        result = x
        for func in funcs:
            result = func(result)
        return result
    return composed


def compose_right(*funcs: Callable) -> Callable:
    """
    Compose functions right-to-left: h(g(f(x))).

    Args:
        *funcs: Functions to compose (rightmost first)

    Returns:
        Composed function
    """
    return compose(*reversed(funcs))


def curry(func: Callable[..., R], arity: int | None = None) -> Callable:
    """
    Curry a function to accept arguments one at a time.

    Args:
        func: Function to curry
        arity: Number of arguments (defaults to func.__code__.co_argcount)

    Returns:
        Curried function
    """
    if arity is None:
        arity = func.__code__.co_argcount

    def curried(*args: object) -> Callable | R:
        if len(args) >= arity:
            return func(*args[:arity])
        def next_arg(*more: object) -> Callable | R:
            return curried(*args, *more)
        return next_arg

    return curried


def flip(func: Callable[[T, P], R]) -> Callable[[P, T], R]:
    """Flip argument order of a function."""
    def flipped(p: P, t: T) -> R:
        return func(t, p)
    return flipped


def negate(func: Callable[..., bool]) -> Callable[..., bool]:
    """Return negation of a predicate function."""
    def negated(*args: object, **kwargs: object) -> bool:
        return not func(*args, **kwargs)
    return negated


def identity(x: T) -> T:
    """Identity function."""
    return x


def constant(x: T) -> Callable[[Any], T]:
    """Return constant function that always returns x."""
    return lambda _: x
