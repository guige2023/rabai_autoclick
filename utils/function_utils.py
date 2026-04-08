"""Function utilities for RabAI AutoClick.

Provides:
- Function introspection helpers
- Argument binding and partial application
- Composition utilities
- Callable type checking and wrapping
"""

from __future__ import annotations

import functools
import inspect
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    ParamSpec,
    TypeVar,
    Union,
    overload,
)


P = ParamSpec("P")
T = TypeVar("T")
R = TypeVar("R")


def arity(func: Callable[..., Any]) -> int:
    """Get the number of parameters a function accepts.

    Args:
        func: Function to inspect.

    Returns:
        Number of parameters.
    """
    try:
        sig = inspect.signature(func)
        return len([
            p for p in sig.parameters.values()
            if p.default is inspect.Parameter.VAR_POSITIONAL
            and p.kind != inspect.Parameter.VAR_KEYWORD
        ])
    except (ValueError, TypeError):
        return -1


def full_arg_count(func: Callable[..., Any]) -> int:
    """Count total positional parameters (required + optional).

    Args:
        func: Function to inspect.

    Returns:
        Total parameter count.
    """
    try:
        sig = inspect.signature(func)
        return len(sig.parameters)
    except (ValueError, TypeError):
        return -1


def get_arg_names(func: Callable[..., Any]) -> List[str]:
    """Get names of all parameters.

    Args:
        func: Function to inspect.

    Returns:
        List of parameter names.
    """
    try:
        sig = inspect.signature(func)
        return list(sig.parameters.keys())
    except (ValueError, TypeError):
        return []


def bind_args(
    func: Callable[..., R],
    *args: Any,
    **kwargs: Any,
) -> Callable[[], R]:
    """Partially bind arguments to a function.

    Args:
        func: Function to bind.
        *args: Positional args to bind.
        **kwargs: Keyword args to bind.

    Returns:
        Callable with bound arguments.
    """
    @functools.wraps(func)
    def wrapper() -> R:
        return func(*args, **kwargs)
    return wrapper


def compose(
    *funcs: Callable[[Any], Any],
) -> Callable[[Any], Any]:
    """Compose functions right-to-left.

    Args:
        *funcs: Functions to compose (last is leftmost).

    Returns:
        Composed function.
    """
    if not funcs:
        raise ValueError("At least one function required")

    def composed(x: Any) -> Any:
        result = x
        for func in reversed(funcs):
            result = func(result)
        return result

    return composed


def pipe(
    *funcs: Callable[[Any], Any],
) -> Callable[[Any], Any]:
    """Pipe functions left-to-right.

    Args:
        *funcs: Functions to pipe (first is leftmost).

    Returns:
        Piped function.
    """
    if not funcs:
        raise ValueError("At least one function required")

    def piped(x: Any) -> Any:
        result = x
        for func in funcs:
            result = func(result)
        return result

    return piped


def curry(func: Callable[..., R]) -> Callable[..., R]:
    """Curry a function to accept arguments one at a time.

    Args:
        func: Function to curry.

    Returns:
        Curried function.
    """
    sig = inspect.signature(func)
    params = list(sig.parameters.values())

    @functools.wraps(func)
    def curried(*args: Any, **kwargs: Any) -> Any:
        all_args = list(args)
        remaining = len(params) - len(all_args)

        if remaining <= 0:
            return func(*all_args, **kwargs)

        def next_arg(a: Any, **kw: Any) -> Any:
            combined = tuple(list(all_args) + [a])
            if len(combined) >= len(params):
                return func(*combined, **kw)
            return next_arg

        if remaining == 1:
            return next_arg
        return next_arg

    return curried  # type: ignore


def memoize_call(func: Callable[..., R]) -> Callable[..., R]:
    """Memoize a function call (not on args, just repeat calls).

    Args:
        func: Function to memoize.

    Returns:
        Memoized function that caches the last result.
    """
    last_args: tuple = ()
    last_kwargs: tuple = ()
    last_result: Any = None
    cached = False

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> R:
        nonlocal last_args, last_kwargs, last_result, cached
        args_key = (args, tuple(sorted(kwargs.items())))
        if cached and args_key == last_args:
            return last_result  # type: ignore
        last_result = func(*args, **kwargs)
        last_args = args_key
        last_kwargs = tuple(sorted(kwargs.items()))
        cached = True
        return last_result  # type: ignore

    def clear() -> None:
        nonlocal cached
        cached = False

    wrapper.clear_cache = clear  # type: ignore
    return wrapper  # type: ignore


def call_if(
    condition: bool,
    func: Callable[[], T],
    default: Optional[T] = None,
) -> Optional[T]:
    """Call a function only if a condition is True.

    Args:
        condition: Whether to call func.
        func: Function to call.
        default: Value to return if condition is False.

    Returns:
        Result of func() or default.
    """
    if condition:
        return func()
    return default


def calls_pending(func: Callable[..., Any]) -> Callable[..., Any]:
    """Track whether a function has pending calls.

    Args:
        func: Function to wrap.

    Returns:
        Wrapped function with pending tracking.
    """
    pending = 0
    lock = __import__("threading").Lock()

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        nonlocal pending
        with lock:
            pending += 1
        try:
            return func(*args, **kwargs)
        finally:
            with lock:
                pending -= 1

    @property
    def is_pending() -> bool:
        return pending > 0

    wrapper.is_pending = is_pending  # type: ignore
    return wrapper


def is_coroutine_function(func: Callable[..., Any]) -> bool:
    """Check if a function is a coroutine function.

    Args:
        func: Function to check.

    Returns:
        True if it's async def.
    """
    return inspect.iscoroutinefunction(func)


def get_docstring(func: Callable[..., Any]) -> Optional[str]:
    """Get the docstring of a function.

    Args:
        func: Function to inspect.

    Returns:
        Docstring or None.
    """
    return inspect.getdoc(func)


def short_repr(func: Callable[..., Any]) -> str:
    """Get a short representation of a function.

    Args:
        func: Function to represent.

    Returns:
        Short string representation.
    """
    mod = getattr(func, "__module__", "?")
    name = getattr(func, "__name__", "?")
    qualname = getattr(func, "__qualname__", name)
    return f"{mod}.{qualname}"


__all__ = [
    "arity",
    "full_arg_count",
    "get_arg_names",
    "bind_args",
    "compose",
    "pipe",
    "curry",
    "memoize_call",
    "call_if",
    "calls_pending",
    "is_coroutine_function",
    "get_docstring",
    "short_repr",
]
