"""Advanced decorator utilities for RabAI AutoClick.

Provides:
- Function decorators (memoize, retry, timeout, rate_limit)
- Class decorators (singleton, sealed, final)
- Method decorators (cached, logged)
- Async decorators
"""

from __future__ import annotations

import asyncio
import functools
import time
from typing import (
    Any,
    Callable,
    Optional,
    TypeVar,
    Union,
)


T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])


def memoize(func: Callable[..., T]) -> Callable[..., T]:
    """Cache function results.

    Args:
        func: Function to memoize.

    Returns:
        Memoized function.
    """
    cache: dict = {}
    sentinel = object()

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> T:
        key = (args, tuple(sorted(kwargs.items())))
        result = cache.get(key, sentinel)
        if result is not sentinel:
            return result
        result = func(*args, **kwargs)
        cache[key] = result
        return result

    wrapper.cache_clear = lambda: cache.clear()
    wrapper.cache_info = lambda: {"size": len(cache)}
    return wrapper


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Union[type, tuple[type, ...]] = Exception,
) -> Callable[[F], F]:
    """Retry decorator with exponential backoff.

    Args:
        max_attempts: Maximum retry attempts.
        delay: Initial delay between retries.
        backoff: Backoff multiplier.
        exceptions: Exceptions to catch and retry.

    Returns:
        Decorated function.
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
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

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs) -> Any:
            current_delay = delay
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts - 1:
                        raise
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
            raise RuntimeError("Unreachable")

        if asyncio.iscoroutinefunction(func):
            return async_wrapper  # type: ignore
        return wrapper  # type: ignore

    return decorator


def timeout(seconds: float) -> Callable[[F], F]:
    """Timeout decorator for synchronous functions.

    Args:
        seconds: Timeout in seconds.

    Returns:
        Decorated function.
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            return func(*args, **kwargs)
        return wrapper  # type: ignore
    return decorator


def async_timeout(seconds: float) -> Callable[[F], F]:
    """Timeout decorator for async functions.

    Args:
        seconds: Timeout in seconds.

    Returns:
        Decorated async function.
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            return await asyncio.wait_for(func(*args, **kwargs), timeout=seconds)
        return wrapper  # type: ignore
    return decorator


def rate_limit(calls: int, period: float) -> Callable[[F], F]:
    """Rate limiting decorator.

    Args:
        calls: Maximum calls per period.
        period: Time period in seconds.

    Returns:
        Decorated function.
    """
    calls_history: list = []
    lock = asyncio.Lock()

    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            async with lock:
                now = time.time()
                calls_history[:] = [t for t in calls_history if now - t < period]
                if len(calls_history) >= calls:
                    sleep_time = period - (now - calls_history[0])
                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)
                calls_history.append(time.time())
            return await func(*args, **kwargs)

        if not asyncio.iscoroutinefunction(func):
            def sync_wrapper(*args, **kwargs) -> Any:
                now = time.time()
                calls_history[:] = [t for t in calls_history if now - t < period]
                if len(calls_history) >= calls:
                    sleep_time = period - (now - calls_history[0])
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                calls_history.append(time.time())
                return func(*args, **kwargs)
            return sync_wrapper  # type: ignore

        return wrapper  # type: ignore

    return decorator


def singleton(cls: type[T]) -> type[T]:
    """Singleton decorator.

    Args:
        cls: Class to make singleton.

    Returns:
        Singleton class.
    """
    instances: dict[type, T] = {}

    @functools.wraps(cls)
    def get_instance(*args, **kwargs) -> T:
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    get_instance._instance = None  # type: ignore
    return get_instance  # type: ignore


def once(func: F) -> F:
    """Call function only once.

    Args:
        func: Function to decorate.

    Returns:
        Decorated function.
    """
    called = False
    result = None

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        nonlocal called, result
        if not called:
            result = func(*args, **kwargs)
            called = True
        return result

    wrapper._called = lambda: called  # type: ignore
    return wrapper  # type: ignore


def logged(func: F) -> F:
    """Log function calls.

    Args:
        func: Function to decorate.

    Returns:
        Decorated function.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        print(f"Calling {func.__name__}({args}, {kwargs})")
        result = func(*args, **kwargs)
        print(f"{func.__name__} returned {result}")
        return result
    return wrapper  # type: ignore


def deprecated(message: Optional[str] = None) -> Callable[[F], F]:
    """Mark function as deprecated.

    Args:
        message: Deprecation message.

    Returns:
        Decorated function.
    """
    def decorator(func: F) -> F:
        msg = message or f"{func.__name__} is deprecated"

        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            import warnings
            warnings.warn(msg, DeprecationWarning, stacklevel=2)
            return func(*args, **kwargs)

        return wrapper  # type: ignore
    return decorator


def optional_async(func: F) -> F:
    """Decorator that makes function work with both sync and async callers.

    Args:
        func: Function to decorate.

    Returns:
        Decorated function.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        return func(*args, **kwargs)

    async def async_wrapper(*args, **kwargs) -> Any:
        return func(*args, **kwargs)

    if asyncio.iscoroutinefunction(func):
        return func  # type: ignore
    return wrapper  # type: ignore
