"""
Wrapper Action Module.

Provides generic wrapper patterns for actions including
retry, timeout, cache, and logging wrappers.
"""

import time
import asyncio
import threading
import functools
from typing import Callable, Any, Optional, TypeVar, Generic, Dict
from dataclasses import dataclass
from enum import Enum


T = TypeVar("T")
R = TypeVar("R")


class WrapperType(Enum):
    """Types of action wrappers."""
    RETRY = "retry"
    TIMEOUT = "timeout"
    CACHE = "cache"
    LOGGING = "logging"
    METRICS = "metrics"
    CIRCUIT_BREAKER = "circuit_breaker"
    BULHEAD = "bulkhead"


@dataclass
class WrapperConfig:
    """Base configuration for wrappers."""
    enabled: bool = True
    name: Optional[str] = None


@dataclass
class RetryConfig(WrapperConfig):
    """Configuration for retry wrapper."""
    max_attempts: int = 3
    delay: float = 1.0
    backoff_multiplier: float = 2.0
    max_delay: float = 30.0
    exceptions: tuple = (Exception,)


@dataclass
class TimeoutConfig(WrapperConfig):
    """Configuration for timeout wrapper."""
    timeout: float = 30.0


@dataclass
class CacheConfig(WrapperConfig):
    """Configuration for cache wrapper."""
    ttl: float = 300.0
    max_size: int = 1000


class WrapperAction(Generic[T, R]):
    """
    Wrapper that adds cross-cutting concerns to actions.

    Example:
        @wrapper.retry(max_attempts=3, delay=1.0)
        @wrapper.timeout(timeout=30.0)
        def fetch_data():
            return api.get()
    """

    def __init__(self, name: Optional[str] = None):
        self.name = name
        self._wrappers: list = []

    def wrap(self, func: Callable) -> Callable:
        """Apply all wrappers to a function."""
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            return self._execute_sync(func, args, kwargs)

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            return await self._execute_async(func, args, kwargs)

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    def _execute_sync(
        self,
        func: Callable,
        args: tuple,
        kwargs: Dict,
    ) -> Any:
        """Execute with sync wrappers applied."""
        return func(*args, **kwargs)

    async def _execute_async(
        self,
        func: Callable,
        args: tuple,
        kwargs: Dict,
    ) -> Any:
        """Execute with async wrappers applied."""
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        return func(*args, **kwargs)

    def retry(
        self,
        max_attempts: int = 3,
        delay: float = 1.0,
        backoff: float = 2.0,
        max_delay: float = 30.0,
        exceptions: tuple = (Exception,),
    ):
        """Decorator for retry logic."""
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                last_exception = None
                current_delay = delay

                for attempt in range(max_attempts):
                    try:
                        if asyncio.iscoroutinefunction(func):
                            return asyncio.run(func(*args, **kwargs))
                        return func(*args, **kwargs)
                    except exceptions as e:
                        last_exception = e
                        if attempt < max_attempts - 1:
                            time.sleep(current_delay)
                            current_delay = min(
                                current_delay * backoff,
                                max_delay,
                            )
                        else:
                            raise

                if last_exception:
                    raise last_exception

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                last_exception = None
                current_delay = delay

                for attempt in range(max_attempts):
                    try:
                        if asyncio.iscoroutinefunction(func):
                            return await func(*args, **kwargs)
                        return func(*args, **kwargs)
                    except exceptions as e:
                        last_exception = e
                        if attempt < max_attempts - 1:
                            await asyncio.sleep(current_delay)
                            current_delay = min(
                                current_delay * backoff,
                                max_delay,
                            )
                        else:
                            raise

                if last_exception:
                    raise last_exception

            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            return wrapper
        return decorator

    def timeout(self, seconds: float = 30.0):
        """Decorator for timeout logic."""
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                result = None
                exception = None

                def target():
                    nonlocal result, exception
                    try:
                        result = func(*args, **kwargs)
                    except Exception as e:
                        exception = e

                t = threading.Thread(target=target)
                t.daemon = True
                t.start()
                t.join(seconds)

                if t.is_alive():
                    raise TimeoutError(
                        f"Function {func.__name__} timed out after {seconds}s"
                    )
                if exception:
                    raise exception
                return result

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                try:
                    if asyncio.iscoroutinefunction(func):
                        return await asyncio.wait_for(
                            func(*args, **kwargs),
                            timeout=seconds,
                        )
                    return func(*args, **kwargs)
                except asyncio.TimeoutError:
                    raise TimeoutError(
                        f"Function {func.__name__} timed out after {seconds}s"
                    )

            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            return wrapper
        return decorator

    def with_cache(self, ttl: float = 300.0, max_size: int = 1000):
        """Decorator for caching results."""
        cache: Dict[str, tuple] = {}
        cache_order: list = []
        lock = threading.Lock()

        def make_key(args: tuple, kwargs: dict) -> str:
            key_parts = [str(a) for a in args]
            key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
            return "|".join(key_parts)

        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                key = make_key(args, kwargs)

                with lock:
                    if key in cache:
                        result, timestamp = cache[key]
                        if time.time() - timestamp < ttl:
                            return result

                    if asyncio.iscoroutinefunction(func):
                        result = asyncio.run(func(*args, **kwargs))
                    else:
                        result = func(*args, **kwargs)

                    cache[key] = (result, time.time())
                    cache_order.append(key)

                    if len(cache) > max_size:
                        oldest = cache_order.pop(0)
                        cache.pop(oldest, None)

                    return result

            wrapper.cache_clear = lambda: (
                cache.clear() or cache_order.clear()
            )
            wrapper.cache_info = lambda: {
                "size": len(cache),
                "keys": list(cache.keys()),
            }
            return wrapper
        return decorator

    def with_metrics(self):
        """Decorator for collecting metrics."""
        def decorator(func: Callable) -> Callable:
            func.call_count = 0
            func.error_count = 0
            func.total_time = 0.0

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                start = time.time()
                func.call_count += 1
                try:
                    if asyncio.iscoroutinefunction(func):
                        result = asyncio.run(func(*args, **kwargs))
                    else:
                        result = func(*args, **kwargs)
                    return result
                except Exception:
                    func.error_count += 1
                    raise
                finally:
                    func.total_time += time.time() - start

            @property
            def metrics(self):
                return {
                    "call_count": func.call_count,
                    "error_count": func.error_count,
                    "total_time": func.total_time,
                    "avg_time": (
                        func.total_time / func.call_count
                        if func.call_count > 0
                        else 0
                    ),
                }

            wrapper.metrics = metrics
            return wrapper
        return decorator

    def chain(self, *wrappers: Callable) -> Callable:
        """Chain multiple wrappers together."""
        def decorator(func: Callable) -> Callable:
            result = func
            for w in reversed(wrappers):
                result = w(result)
            return result
        return decorator
