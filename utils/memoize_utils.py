"""Memoization utilities for caching function results.

Provides flexible memoization decorators with TTL,
key generation, and cache management.
"""

import functools
import hashlib
import pickle
from typing import Any, Callable, Dict, Optional, Tuple, TypeVar


T = TypeVar("T")


class MemoCache:
    """Simple in-memory memoization cache."""

    def __init__(self, max_size: int = 256) -> None:
        self._cache: Dict[Tuple[Any, ...], Any] = {}
        self._max_size = max_size
        self._hits = 0
        self._misses = 0

    def get(self, key: Tuple[Any, ...]) -> Optional[Any]:
        """Get cached value."""
        if key in self._cache:
            self._hits += 1
            return self._cache[key]
        self._misses += 1
        return None

    def set(self, key: Tuple[Any, ...], value: Any) -> None:
        """Set cached value with LRU eviction."""
        if len(self._cache) >= self._max_size:
            oldest = next(iter(self._cache))
            del self._cache[oldest]
        self._cache[key] = value

    def clear(self) -> None:
        """Clear all cached values."""
        self._cache.clear()
        self._hits = 0
        self._misses = 0

    def stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        total = self._hits + self._misses
        return {
            "hits": self._hits,
            "misses": self._misses,
            "size": len(self._cache),
            "hit_rate": self._hits / total if total > 0 else 0.0,
        }


def default_key_func(args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Tuple[Any, ...]:
    """Default cache key function.

    Args:
        args: Positional arguments.
        kwargs: Keyword arguments.

    Returns:
        Hashable cache key.
    """
    try:
        return (args, tuple(sorted(kwargs.items())))
    except Exception:
        key_str = str((args, sorted(kwargs.items())))
        return (hashlib.sha256(key_str.encode()).digest(),)


def memoize(
    key_func: Optional[Callable[[Tuple[Any, ...], Dict[str, Any]], Tuple[Any, ...]]] = None,
    max_size: int = 256,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator for memoizing function results.

    Args:
        key_func: Custom key generation function.
        max_size: Maximum cache entries.

    Returns:
        Decorated function with memoization.
    """
    cache = MemoCache(max_size)
    key_fn = key_func or default_key_func

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            key = key_fn(args, kwargs)
            cached = cache.get(key)
            if cached is not None:
                return cached
            result = func(*args, **kwargs)
            cache.set(key, result)
            return result

        wrapper.cache_clear = cache.clear  # type: ignore
        wrapper.cache_stats = cache.stats  # type: ignore
        return wrapper

    return decorator


def memoize_with_ttl(
    ttl: float = 60.0,
    key_func: Optional[Callable[[Tuple[Any, ...], Dict[str, Any]], Tuple[Any, ...]]] = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator for memoizing with time-to-live.

    Args:
        ttl: Time-to-live in seconds.
        key_func: Custom key generation function.

    Returns:
        Decorated function with TTL memoization.
    """
    import time
    cache: Dict[Tuple[Any, ...], Tuple[Any, float]] = {}
    key_fn = key_func or default_key_func

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            key = key_fn(args, kwargs)
            now = time.time()
            if key in cache:
                value, timestamp = cache[key]
                if now - timestamp < ttl:
                    return value
            result = func(*args, **kwargs)
            cache[key] = (result, now)
            return result

        wrapper.cache_clear = lambda: cache.clear()  # type: ignore
        return wrapper

    return decorator


def memoize_hash(
    *arg_names: str,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Memoize by hashing specific keyword arguments.

    Args:
        *arg_names: Names of arguments to include in cache key.

    Returns:
        Decorated function.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        cache: Dict[str, T] = {}

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            sig = functools.signature(func)
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            key_parts = [str(bound.arguments.get(name, "")) for name in arg_names]
            key = "|".join(key_parts)
            if key in cache:
                return cache[key]
            result = func(*args, **kwargs)
            cache[key] = result
            return result

        wrapper.cache_clear = lambda: cache.clear()  # type: ignore
        return wrapper

    return decorator
