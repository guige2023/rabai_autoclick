"""Lambda utilities for RabAI AutoClick.

Provides:
- Lambda function builders
- Composable lambda expressions
- Shortcut lambda creators
"""

from __future__ import annotations

from typing import (
    Any,
    Callable,
    List,
    Tuple,
    TypeVar,
)


T = TypeVar("T")
U = TypeVar("U")
K = TypeVar("K")
V = TypeVar("V")


def identity(x: T) -> T:
    """Identity function - returns its argument unchanged.

    Args:
        x: Any value.

    Returns:
        The same value.
    """
    return x


def constant(x: T) -> Callable[..., T]:
    """Create a function that always returns the same value.

    Args:
        x: Value to always return.

    Returns:
        Function that returns x.
    """
    def const(*args: Any, **kwargs: Any) -> T:
        return x
    return const


def flip(func: Callable[[T, U], K]) -> Callable[[U, T], K]:
    """Flip argument order of a 2-argument function.

    Args:
        func: Function to flip.

    Returns:
        Function with reversed argument order.
    """
    def flipped(a: U, b: T) -> K:
        return func(b, a)
    return flipped


def curry_first(func: Callable[[T, U], K]) -> Callable[[T], Callable[[U], K]]:
    """Curry a 2-argument function, starting with first argument.

    Args:
        func: Function to curry.

    Returns:
        Curried function.
    """
    def curried(a: T) -> Callable[[U], K]:
        def inner(b: U) -> K:
            return func(a, b)
        return inner
    return curried


def juxt(*funcs: Callable[..., U]) -> Callable[..., List[U]]:
    """Create a function that applies multiple functions to same args.

    Args:
        *funcs: Functions to apply.

    Returns:
        Function returning list of results.
    """
    def juxtapos(*args: Any, **kwargs: Any) -> List[U]:
        return [f(*args, **kwargs) for f in funcs]
    return juxtapos


def comp(
    *funcs: Callable[[Any], Any],
) -> Callable[[Any], Any]:
    """Compose functions right-to-left.

    Args:
        *funcs: Functions to compose (last applied first).

    Returns:
        Composed function.
    """
    def composed(x: Any) -> Any:
        for f in reversed(funcs):
            x = f(x)
        return x
    return composed


def thread_first(value: T, *funcs: Callable[[Any], Any]) -> T:
    """Thread a value through a pipeline of functions (->).

    Args:
        value: Starting value.
        *funcs: Functions to apply in order.

    Returns:
        Result after all functions applied.
    """
    result = value
    for func in funcs:
        result = func(result)
    return result  # type: ignore


def thread_last(value: T, *funcs: Callable[[Any], Any]) -> T:
    """Thread a value through functions as last argument (->>).

    Args:
        value: Starting value.
        *funcs: Functions to apply in order.

    Returns:
        Result after all functions applied.
    """
    result = value
    for func in funcs:
        result = func(result)  # type: ignore
    return result  # type: ignore


def iterate_call(func: Callable[[T], T], n: int) -> Callable[[T], T]:
    """Create a function that applies func n times.

    Args:
        func: Function to apply repeatedly.
        n: Number of applications.

    Returns:
        Function that applies func n times.
    """
    def applied(x: T) -> T:
        result = x
        for _ in range(n):
            result = func(result)
        return result
    return applied


def when(
    predicate: Callable[..., bool],
    func: Callable[[T], T],
) -> Callable[[T], T]:
    """Conditional transformation - only apply if predicate is True.

    Args:
        predicate: Condition function.
        func: Transformation function.

    Returns:
        Function that conditionally transforms.
    """
    def transformer(x: T) -> T:
        if predicate(x):
            return func(x)
        return x
    return transformer


def unless(
    predicate: Callable[..., bool],
    func: Callable[[T], T],
) -> Callable[[T], T]:
    """Conditional transformation - apply when predicate is False.

    Args:
        predicate: Condition function.
        func: Transformation function.

    Returns:
        Function that conditionally transforms.
    """
    def transformer(x: T) -> T:
        if not predicate(x):
            return func(x)
        return x
    return transformer


def tmap(func: Callable[[T], U], iterable: List[T]) -> List[U]:
    """Map a function over a list.

    Args:
        func: Transformation function.
        iterable: Source list.

    Returns:
        Transformed list.
    """
    return [func(x) for x in iterable]


def tfilter(predicate: Callable[[T], bool], iterable: List[T]) -> List[T]:
    """Filter a list with a predicate.

    Args:
        predicate: Filter function.
        iterable: Source list.

    Returns:
        Filtered list.
    """
    return [x for x in iterable if predicate(x)]


def treduce(
    func: Callable[[T, T], T],
    iterable: List[T],
    initial: Optional[T] = None,
) -> T:
    """Reduce a list with a binary function.

    Args:
        func: Reduction function.
        iterable: Source list.
        initial: Initial value.

    Returns:
        Reduced value.
    """
    it = iter(iterable)
    if initial is None:
        result = next(it)
    else:
        result = initial
    for item in it:
        result = func(result, item)
    return result


__all__ = [
    "identity",
    "constant",
    "flip",
    "curry_first",
    "juxt",
    "comp",
    "thread_first",
    "thread_last",
    "iterate_call",
    "when",
    "unless",
    "tmap",
    "tfilter",
    "treduce",
]
