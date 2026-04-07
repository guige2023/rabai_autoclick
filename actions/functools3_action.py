"""Functools utilities v3 - advanced functional patterns.

Advanced functional utilities including monads,
 function composition, and async patterns.
"""

from __future__ import annotations

import functools
from functools import wraps
from typing import Any, Callable, Generic, TypeVar

__all__ = [
    "compose",
    "pipe",
    "curry",
    "rcurry",
    "maybe",
    "Option",
    "Result",
    "pipe_async",
    "memoize_async",
    "Lazy",
]


T = TypeVar("T")
U = TypeVar("U")
R = TypeVar("R")


def compose(*funcs: Callable) -> Callable:
    """Compose functions right-to-left.

    Args:
        *funcs: Functions to compose.

    Returns:
        Composed function.
    """
    if not funcs:
        return lambda x: x
    def composed(x: Any) -> Any:
        for f in reversed(funcs):
            x = f(x)
        return x
    return composed


def pipe(*funcs: Callable) -> Callable:
    """Pipe functions left-to-right."""
    return compose(*reversed(funcs))


def curry(func: Callable[..., R], arity: int | None = None) -> Callable[..., R]:
    """Curry a function.

    Args:
        func: Function to curry.
        arity: Number of arguments.

    Returns:
        Curried function.
    """
    if arity is None:
        arity = func.__code__.co_argcount
    @wraps(func)
    def curried(*args: Any, **kwargs: Any) -> R:
        if len(args) >= arity:
            return func(*args[:arity], **kwargs)
        def next_curry(arg: Any) -> Callable[..., R]:
            return curry(func(*args, arg), arity - len(args) - 1)
        return next_curry
    return curried


def rcurry(func: Callable[..., R], arity: int | None = None) -> Callable[..., R]:
    """Right curry (collect from right)."""
    if arity is None:
        arity = func.__code__.co_argcount
    @wraps(func)
    def curried(*args: Any, **kwargs: Any) -> R:
        if len(args) >= arity:
            return func(*args[-arity:], **kwargs)
        def next_curry(arg: Any) -> Callable[..., R]:
            return rcurry(func(arg, *args), arity - 1)
        return next_curry
    return curried


class Option(Generic[T]):
    """Option monad - Some or Nothing."""

    def __init__(self, value: T | None = None, is_some: bool = True) -> None:
        self._value = value
        self._is_some = is_some

    @classmethod
    def some(cls, value: T) -> Option[T]:
        """Create Some option."""
        return cls(value, True)

    @classmethod
    def nothing(cls) -> Option[T]:
        """Create Nothing option."""
        return cls(None, False)

    def is_some(self) -> bool:
        """Check if Some."""
        return self._is_some

    def is_nothing(self) -> bool:
        """Check if Nothing."""
        return not self._is_some

    def get(self) -> T:
        """Get value or raise."""
        if not self._is_some:
            raise ValueError("Cannot get from Nothing")
        return self._value

    def get_or(self, default: T) -> T:
        """Get value or default."""
        return self._value if self._is_some else default

    def map(self, fn: Callable[[T], Any]) -> Option[Any]:
        """Map over option."""
        if self._is_some:
            return Option.some(fn(self._value))
        return Option.nothing()


class Result(Generic[T, U]):
    """Result monad - Ok or Err."""

    def __init__(self, value: T | U, is_ok: bool) -> None:
        self._value = value
        self._is_ok = is_ok

    @classmethod
    def ok(cls, value: T) -> Result[T, U]:
        """Create Ok result."""
        return cls(value, True)

    @classmethod
    def err(cls, error: U) -> Result[T, U]:
        """Create Err result."""
        return cls(error, False)

    def is_ok(self) -> bool:
        """Check if Ok."""
        return self._is_ok

    def is_err(self) -> bool:
        """Check if Err."""
        return not self._is_ok

    def get(self) -> T:
        """Get value or raise."""
        if not self._is_ok:
            raise ValueError(f"Cannot get from Err: {self._value}")
        return self._value

    def map(self, fn: Callable[[T], Any]) -> Result[Any, U]:
        """Map over Ok."""
        if self._is_ok:
            return Result.ok(fn(self._value))
        return Result.err(self._value)


def maybe(value: T | None) -> Option[T]:
    """Create Option from nullable value."""
    if value is None:
        return Option.nothing()
    return Option.some(value)


async def pipe_async(*funcs: Callable) -> Callable:
    """Pipe async functions."""
    async def piped(input: Any) -> Any:
        result = input
        for f in funcs:
            if functools.iscoroutinefunction(f):
                result = await f(result)
            else:
                result = f(result)
        return result
    return piped


async def memoize_async(func: Callable) -> Callable:
    """Memoize async function."""
    cache = {}
    @wraps(func)
    async def memoized(*args: Any, **kwargs: Any) -> Any:
        key = (args, tuple(sorted(kwargs.items())))
        if key not in cache:
            cache[key] = await func(*args, **kwargs)
        return cache[key]
    return memoized


class Lazy(Generic[T]):
    """Lazy evaluation wrapper."""

    def __init__(self, factory: Callable[[], T]) -> None:
        self._factory = factory
        self._value: T | None = None
        self._evaluated = False

    def evaluate(self) -> T:
        """Evaluate lazy value."""
        if not self._evaluated:
            self._value = self._factory()
            self._evaluated = True
        return self._value

    @property
    def value(self) -> T:
        """Get value (evaluates if needed)."""
        return self.evaluate()
