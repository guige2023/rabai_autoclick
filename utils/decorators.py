"""Decorator utilities for RabAI AutoClick.

Provides:
- Common decorator utilities
- Function wrappers
"""

import functools
import time
from typing import Any, Callable, Optional, TypeVar


T = TypeVar("T")


def retry(max_attempts: int = 3, delay: float = 0):
    """Decorator to retry function on exception.

    Args:
        max_attempts: Maximum retry attempts.
        delay: Delay between retries.

    Returns:
        Decorated function.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        time.sleep(delay)
            raise last_exception
        return wrapper
    return decorator


def timing(func: Callable[..., T]) -> Callable[..., T]:
    """Decorator to time function execution.

    Returns:
        Decorated function.
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        start = time.perf_counter()
        result = func(*args, **kwargs)
        duration = time.perf_counter() - start
        return result
    return wrapper


def deprecated(message: Optional[str] = None) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to mark function as deprecated.

    Args:
        message: Deprecation message.

    Returns:
        Decorated function.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            import warnings
            msg = message or f"{func.__name__} is deprecated"
            warnings.warn(msg, DeprecationWarning, stacklevel=2)
            return func(*args, **kwargs)
        return wrapper
    return decorator


def cached(func: Callable[..., T]) -> Callable[..., T]:
    """Simple caching decorator.

    Returns:
        Decorated function.
    """
    cache: dict = {}

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        key = (args, tuple(sorted(kwargs.items())))
        if key not in cache:
            cache[key] = func(*args, **kwargs)
        return cache[key]

    wrapper.cache_clear = lambda: cache.clear()
    wrapper.cache = cache
    return wrapper


def rate_limit(calls: int, period: float) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to rate limit function calls.

    Args:
        calls: Number of calls allowed.
        period: Time period in seconds.

    Returns:
        Decorated function.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        call_times: list = []

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            now = time.time()

            # Remove old calls outside the window
            while call_times and now - call_times[0] > period:
                call_times.pop(0)

            if len(call_times) < calls:
                call_times.append(now)
                return func(*args, **kwargs)
            else:
                sleep_time = period - (now - call_times[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    call_times.append(time.time())
                return func(*args, **kwargs)

        return wrapper
    return decorator


def once(func: Callable[..., T]) -> Callable[..., T]:
    """Decorator to execute function only once.

    Returns:
        Decorated function.
    """
    executed = [False]
    result = [None]

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        if not executed[0]:
            executed[0] = True
            result[0] = func(*args, **kwargs)
        return result[0]

    return wrapper


def optional_args(func: Callable[..., T]) -> Callable[..., T]:
    """Decorator to make arguments optional.

    Returns:
        Decorated function.
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        return func(*args, **kwargs)
    return wrapper


def accepts(*types: type) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to enforce argument types.

    Args:
        *types: Expected types for arguments.

    Returns:
        Decorated function.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            for i, (arg, expected) in enumerate(zip(args, types)):
                if not isinstance(arg, expected):
                    raise TypeError(
                        f"Argument {i} must be {expected.__name__}, got {type(arg).__name__}"
                    )
            return func(*args, **kwargs)
        return wrapper
    return decorator


def returns(return_type: type) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to enforce return type.

    Args:
        return_type: Expected return type.

    Returns:
        Decorated function.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            result = func(*args, **kwargs)
            if not isinstance(result, return_type):
                raise TypeError(
                    f"Return type must be {return_type.__name__}, got {type(result).__name__}"
                )
            return result
        return wrapper
    return decorator


def debug(func: Callable[..., T]) -> Callable[..., T]:
    """Decorator to debug function calls.

    Returns:
        Decorated function.
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        import sys
        args_repr = [repr(a) for a in args]
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
        signature = ", ".join(args_repr + kwargs_repr)
        print(f"Calling {func.__name__}({signature})")
        try:
            result = func(*args, **kwargs)
            print(f"{func.__name__} returned {result!r}")
            return result
        except Exception as e:
            print(f"{func.__name__} raised {type(e).__name__}: {e}")
            raise
    return wrapper


def memoize(func: Callable[..., T]) -> Callable[..., T]:
    """Decorator to memoize function results.

    Returns:
        Decorated function.
    """
    cache: dict = {}

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        key = (args, tuple(sorted(kwargs.items())))
        if key not in cache:
            cache[key] = func(*args, **kwargs)
        return cache[key]

    wrapper.cache = cache
    wrapper.cache_clear = lambda: cache.clear()
    return wrapper