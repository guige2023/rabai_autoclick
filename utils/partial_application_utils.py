"""
Partial Application Utilities

Function currying, partial application, and function composition
tools for functional programming patterns.

License: MIT
"""

from __future__ import annotations

import functools
import inspect
from typing import (
    Any,
    Callable,
    TypeVar,
    Generic,
    Optional,
    Union,
    overload,
    ParamSpec,
)
from collections.abc import Callable as ABCCallable

P = ParamSpec("P")
T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")


def curry(func: Callable[P, T]) -> Callable[[Any], Callable[..., T]]:
    """Decorator to curry a function automatically.
    
    Example:
        @curry
        def add_three(a, b, c):
            return a + b + c
        
        add_three(1)(2)(3)  # returns 6
        add_three(1, 2)(3)  # returns 6
    """
    sig = inspect.signature(func)
    
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        if len(bound.arguments) >= len(sig.parameters):
            return func(*bound.args, **bound.kwargs)
        
        @functools.wraps(func)
        def partial(*more_args: Any, **more_kwargs: Any) -> T:
            new_args = tuple(bound.arguments.values()) + more_args
            new_kwargs = {**bound.kwargs, **more_kwargs}
            return func(*new_args, **new_kwargs)
        
        return partial
    
    return wrapper


def partial(func: Callable[..., T], /, *preset_args: Any, **preset_kwargs: Any) -> Callable[..., T]:
    """Create a partial application of a function.
    
    Similar to functools.partial but with enhanced type hints.
    """
    return functools.partial(func, *preset_args, **preset_kwargs)


def rpartial(func: Callable[..., T], /, *postset_args: Any, **postset_kwargs: Any) -> Callable[..., T]:
    """Create a partial application with arguments applied from the right."""
    def wrapper(*args: Any, **kwargs: Any) -> T:
        return func(*args, *postset_args, **{**kwargs, **postset_kwargs})
    return wrapper


def compose(*funcs: Callable[[T], U]) -> Callable[[T], U]:
    """Compose functions right-to-left.
    
    Example:
        add_one = lambda x: x + 1
        double = lambda x: x * 2
        comp = compose(double, add_one)
        comp(5)  # (5 + 1) * 2 = 12
    """
    if not funcs:
        raise ValueError("compose requires at least one function")
    
    def composed(x: T) -> U:
        result = x
        for func in reversed(funcs):
            result = func(result)
        return result
    
    return composed


def pipe(*funcs: Callable[[T], U]) -> Callable[[T], U]:
    """Pipe functions left-to-right.
    
    Example:
        add_one = lambda x: x + 1
        double = lambda x: x * 2
        piped = pipe(add_one, double)
        piped(5)  # (5 + 1) * 2 = 12
    """
    return compose(*reversed(funcs))


def juxt(*funcs: Callable[[T], U]) -> Callable[[T], tuple[U, ...]]:
    """Juxtapose: apply multiple functions to same argument.
    
    Example:
        juxt_sum_len = juxt(sum, len)
        juxt_sum_len([1, 2, 3, 4])  # (10, 4)
    """
    def juxtaposited(x: T) -> tuple[U, ...]:
        return tuple(f(x) for f in funcs)
    return juxtaposited


def flip(func: Callable[..., T]) -> Callable[..., T]:
    """Flip first two arguments of a function."""
    @functools.wraps(func)
    def flipped(a: Any, b: Any, *args: Any, **kwargs: Any) -> T:
        return func(b, a, *args, **kwargs)
    return flipped


def truediv(a: float, b: float) -> float:
    return a / b


def flip_truediv(a: float, b: float) -> float:
    return b / a


class Curried(Generic[T]):
    """Class-based curried function with explicit curry method."""
    
    def __init__(self, func: Callable[..., T]) -> None:
        self._func = func
        self._args: list = []
        self._kwargs: dict = {}
        self._sig = inspect.signature(func)
    
    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        combined_args = tuple(self._args) + args
        combined_kwargs = {**self._kwargs, **kwargs}
        
        try:
            bound = self._sig.bind(*combined_args, **combined_kwargs)
            bound.apply_defaults()
            if len(bound.arguments) >= len(self._sig.parameters):
                return self._func(*bound.args, **bound.kwargs)
            
            new_curried = Curried(self._func)
            new_curried._args = list(bound.args)
            new_curried._kwargs = dict(bound.kwargs)
            return new_curried
        except TypeError:
            return self._func(*combined_args, **combined_kwargs)
    
    def curry(self, *args: Any, **kwargs: Any) -> Curried[T]:
        combined_args = tuple(self._args) + args
        combined_kwargs = {**self._kwargs, **kwargs}
        new_curried = Curried(self._func)
        new_curried._args = list(combined_args)
        new_curried._kwargs = combined_kwargs
        return new_curried


def converge(
    converter: Callable[[T], U],
    functions: list[Callable[..., T]],
) -> Callable[..., U]:
    """Apply multiple functions and combine results with converter.
    
    Example:
        get_stats = converge(
            lambda x: sum(x) / len(x),
            [min, max, sum, len]
        )
        get_stats([1, 2, 3, 4, 5])  # returns average (3.0)
    """
    def converged(*args: Any, **kwargs: Any) -> U:
        results = [f(*args, **kwargs) for f in functions]
        return converter(results)
    return converged


def thread_first(value: T, *funcs: Callable[[Any], Any]) -> T:
    """Thread value through functions (pipe/thread-last macro).
    
    Example:
        thread_first(5, add(3), multiply(2))  # (5 + 3) * 2 = 16
    """
    result = value
    for func in funcs:
        result = func(result)
    return result


def thread_last(value: T, *funcs: Callable[[Any], Any]) -> T:
    """Thread value through functions as last argument.
    
    Example:
        thread_last(5, add_to, 3)  # add_to(3, 5) = 8
    """
    result = value
    for func in funcs:
        if isinstance(func, tuple):
            result = func[0](*func[1], result)
        else:
            result = func(result)
    return result


__all__ = [
    "curry",
    "partial",
    "rpartial",
    "compose",
    "pipe",
    "juxt",
    "flip",
    "Curried",
    "converge",
    "thread_first",
    "thread_last",
]
