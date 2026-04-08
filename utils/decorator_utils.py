"""Decorator utilities for RabAI AutoClick.

Provides:
- Generic decorator factories
- Argument preservation helpers
- Retry and timeout decorators
- Caching decorators
- Rate limiting decorators
"""

from __future__ import annotations

import functools
import time
import threading
from typing import (
    Any,
    Callable,
    Optional,
    Type,
    TypeVar,
    Union,
    overload,
)

import random


T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])


def retry(
    max_attempts: int = 3,
    delay: float = 0.0,
    backoff: float = 2.0,
    exceptions: Type[Exception] = Exception,
) -> Callable[[F], F]:
    """Retry decorator with exponential backoff.

    Args:
        max_attempts: Maximum number of attempts.
        delay: Initial delay between retries in seconds.
        backoff: Multiplier for delay after each attempt.
        exceptions: Exception types to catch and retry.

    Returns:
        Decorated function.
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            current_delay = delay
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts - 1:
                        raise
                    time.sleep(current_delay)
                    current_delay *= backoff
            raise RuntimeError("Unreachable")
        return wrapper  # type: ignore
    return decorator


def timeout(seconds: float) -> Callable[[F], F]:
    """Timeout decorator for long-running functions.

    Args:
        seconds: Timeout duration in seconds.

    Returns:
        Decorated function.
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            result: Any = None
            exception: Optional[Exception] = None

            def target() -> None:
                nonlocal result, exception
                try:
                    result = func(*args, **kwargs)
                except Exception as e:
                    exception = e

            thread = threading.Thread(target=target)
            thread.daemon = True
            thread.start()
            thread.join(seconds)

            if thread.is_alive():
                raise TimeoutError(
                    f"{func.__name__} timed out after {seconds}s"
                )
            if exception is not None:
                raise exception
            return result  # type: ignore
        return wrapper  # type: ignore
    return decorator


def cache(
    maxsize: Optional[int] = 128,
    key_func: Optional[Callable[..., Any]] = None,
) -> Callable[[F], F]:
    """LRU cache decorator with custom key function.

    Args:
        maxsize: Maximum cache size. None for unlimited.
        key_func: Custom cache key function. None uses args/kwargs.

    Returns:
        Decorated function.
    """
    def decorator(func: F) -> F:
        cache_dict: Dict[str, Any] = {}
        cache_order: List[str] = []

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if key_func is not None:
                cache_key = str(key_func(*args, **kwargs))
            else:
                key = (args, tuple(sorted(kwargs.items())))
                cache_key = str(key)

            if cache_key in cache_dict:
                return cache_dict[cache_key]

            result = func(*args, **kwargs)

            if maxsize is not None and len(cache_dict) >= maxsize:
                oldest = cache_order.pop(0)
                cache_dict.pop(oldest, None)

            cache_dict[cache_key] = result
            cache_order.append(cache_key)

            return result

        def clear() -> None:
            cache_dict.clear()
            cache_order.clear()

        wrapper.clear_cache = clear  # type: ignore
        return wrapper  # type: ignore
    return decorator


def rate_limit(
    max_calls: int,
    period: float = 1.0,
) -> Callable[[F], F]:
    """Rate limiting decorator.

    Args:
        max_calls: Maximum calls allowed per period.
        period: Time period in seconds.

    Returns:
        Decorated function.
    """
    lock = threading.Lock()
    calls: List[float] = []

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with lock:
                now = time.monotonic()
                cutoff = now - period
                calls[:] = [t for t in calls if t > cutoff]

                if len(calls) >= max_calls:
                    sleep_time = calls[0] - cutoff
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                    now = time.monotonic()
                    cutoff = now - period
                    calls[:] = [t for t in calls if t > cutoff]

                calls.append(now)

            return func(*args, **kwargs)
        return wrapper  # type: ignore
    return decorator


def memoize(func: F) -> F:
    """Simple memoization decorator.

    Args:
        func: Function to memoize.

    Returns:
        Decorated function with caching.
    """
    cache: Dict[tuple, Any] = {}

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        key = (args, tuple(sorted(kwargs.items())))
        if key not in cache:
            cache[key] = func(*args, **kwargs)
        return cache[key]

    def clear() -> None:
        cache.clear()

    wrapper.clear_cache = clear  # type: ignore
    return wrapper  # type: ignore


def once_per_instance(func: F) -> F:
    """Ensure a method is called only once per instance.

    Args:
        func: Method to wrap.

    Returns:
        Decorated method.
    """
    attr_name = f"_once_called_{func.__name__}"

    @functools.wraps(func)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        if getattr(self, attr_name, False):
            return None
        setattr(self, attr_name, True)
        return func(self, *args, **kwargs)
    return wrapper  # type: ignore


def debug_calls(
    log_call: Callable[[str], None] = print,
) -> Callable[[F], F]:
    """Log function calls with arguments and return values.

    Args:
        log_call: Logging function to use.

    Returns:
        Decorated function.
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            sig = f"{func.__name__}({args}, {kwargs})"
            log_call(f"CALL: {sig}")
            try:
                result = func(*args, **kwargs)
                log_call(f"RETURN: {func.__name__} -> {result!r}")
                return result
            except Exception as e:
                log_call(f"RAISE: {func.__name__} -> {e!r}")
                raise
        return wrapper  # type: ignore
    return decorator


def validate_args(**validators: Any) -> Callable[[F], F]:
    """Argument validation decorator.

    Args:
        **validators: Map of argument name to validation callable.

    Returns:
        Decorated function.
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            for name, validator in validators.items():
                value = kwargs.get(name)
                if value is not None:
                    try:
                        validator(value)
                    except Exception as e:
                        raise ValueError(
                            f"Validation failed for '{name}': {e}"
                        )
            return func(*args, **kwargs)
        return wrapper  # type: ignore
    return decorator


# ─── Dict for type alias ────────────────────────────────────────────────────
from typing import Dict, List  # noqa: E402


__all__ = [
    "retry",
    "timeout",
    "cache",
    "rate_limit",
    "memoize",
    "once_per_instance",
    "debug_calls",
    "validate_args",
]
