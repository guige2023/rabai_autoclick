"""
Functional programming utilities.

Provides higher-order functions, function composition,
currying, memoization, and monadic operations.
"""

from __future__ import annotations

import functools
from typing import Callable, TypeVar, Generic


T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")


def compose(f: Callable[[T], U], g: Callable[[U], V]) -> Callable[[T], V]:
    """
    Compose two functions: compose(f, g)(x) = g(f(x)).

    Args:
        f: First function
        g: Second function

    Returns:
        Composed function g ∘ f

    Example:
        >>> double = lambda x: x * 2
        >>> square = lambda x: x ** 2
        >>> comp = compose(double, square)
        >>> comp(3)
        36
    """
    return lambda x: g(f(x))


def pipe(*funcs: Callable) -> Callable:
    """
    Pipe functions left-to-right: pipe(f, g, h)(x) = h(g(f(x))).

    Example:
        >>> pipe(double, square)(3)
        36
    """
    if not funcs:
        return lambda x: x
    return functools.reduce(lambda f, g: lambda x: g(f(x)), funcs)


def curry(func: Callable) -> Callable:
    """
    Curry a function: curry(f)(a)(b)(c) = f(a, b, c).
    """
    @functools.wraps(func)
    def curried(*args, **kwargs):
        if len(args) + len(kwargs) >= func.__code__.co_argcount:
            return func(*args, **kwargs)
        def next_curried(*a, **kw):
            return curried(*(args + a), **{**kwargs, **kw})
        return next_curried
    return curried


def memoize(func: Callable) -> Callable:
    """
    Memoize a function using LRU cache.

    Example:
        >>> @memoize
        ... def fib(n):
        ...     return n if n < 2 else fib(n-1) + fib(n-2)
        >>> fib(100)
        354224848179261915075
    """
    return functools.lru_cache(maxsize=None)(func)


def apply_ntimes(n: int) -> Callable[[Callable], Callable[[T], T]]:
    """
    Return a function that applies f n times.

    Example:
        >>> add5 = apply_ntimes(5)(lambda x: x + 1)
        >>> add5(0)
        5
    """
    def decorator(f: Callable) -> Callable[[T], T]:
        def applied(x: T) -> T:
            result = x
            for _ in range(n):
                result = f(result)
            return result
        return applied
    return decorator


def iterate(f: Callable, n: int) -> Callable[[T], list[T]]:
    """
    Return list of n applications of f: iterate(f, n)(x) = [x, f(x), f(f(x)), ...].

    Example:
        >>> iterate(lambda x: x*2, 5)(1)
        [1, 2, 4, 8, 16, 32]
    """
    def result(x: T) -> list[T]:
        vals = [x]
        cur = x
        for _ in range(n - 1):
            cur = f(cur)
            vals.append(cur)
        return vals
    return result


def unfold(
    f: Callable[[T], tuple[U, T] | None],
    seed: T,
) -> list[U]:
    """
    Generate sequence by repeatedly applying f to seed.

    Example:
        >>> unfold(lambda x: (x, x+1) if x < 5 else None, 0)
        [0, 1, 2, 3, 4, 5]
    """
    result = []
    cur = seed
    while True:
        val = f(cur)
        if val is None:
            break
        result.append(val[0])
        cur = val[1]
    return result


def flip(func: Callable) -> Callable:
    """Flip argument order of a binary function."""
    @functools.wraps(func)
    def flipped(a, b):
        return func(b, a)
    return flipped


def juxt(*funcs: Callable[[T], U]) -> Callable[[T], list[U]]:
    """
    Juxtapose: apply all functions to same argument.

    Example:
        >>> juxt(double, square, lambda x: x+1)(3)
        [6, 9, 4]
    """
    def result(x: T) -> list[U]:
        return [f(x) for f in funcs]
    return result


def constantly(x: T) -> Callable[..., T]:
    """Return function that always returns x, ignoring arguments."""
    def const(*args, **kwargs):
        return x
    return const


def trampoline(func: Callable) -> Callable:
    """
    Enable tail recursion via trampoline.

    Functions should return (result,) for value or (func, args) for recursion.
    """
    @functools.wraps(func)
    def trampolined(*args, **kwargs):
        result = func(*args, **kwargs)
        while callable(result):
            result = result()
        return result
    return trampolined


class Maybe(Generic[T):
    """Maybe monad for handling None values."""

    def __init__(self, value: T | None) -> None:
        self._value = value
        self._just = value is not None

    @classmethod
    def just(cls, value: T) -> Maybe[T]:
        return cls(value)

    @classmethod
    def nothing(cls) -> Maybe[T]:
        return cls(None)

    def is_just(self) -> bool:
        return self._just

    def is_nothing(self) -> bool:
        return not self._just

    def get(self, default: U = None) -> T | U:
        return self._value if self._just else default

    def map(self, f: Callable[[T], U]) -> Maybe[U]:
        if self._just:
            return Maybe.just(f(self._value))
        return Maybe.nothing()

    def flat_map(self, f: Callable[[T], Maybe[U]]) -> Maybe[U]:
        if self._just:
            return f(self._value)
        return Maybe.nothing()

    def __repr__(self) -> str:
        return f"Just({self._value})" if self._just else "Nothing"
