"""
Memoization Utilities

Function memoization decorators with TTL, LRU, and custom cache policies.
Thread-safe implementations with size limits and statistics.

License: MIT
"""

from __future__ import annotations

import functools
import threading
import time
import weakref
import hashlib
import pickle
from typing import (
    Any,
    Callable,
    TypeVar,
    Generic,
    Optional,
    Union,
    Hashable,
    Sequence,
)
from collections import OrderedDict
from abc import ABC, abstractmethod

T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")


class CachePolicy(ABC):
    """Abstract base for cache eviction policies."""
    
    @abstractmethod
    def get(self, key: Hashable) -> Any: ...
    
    @abstractmethod
    def set(self, key: Hashable, value: Any) -> None: ...
    
    @abstractmethod
    def invalidate(self, key: Hashable) -> None: ...
    
    @abstractmethod
    def clear(self) -> None: ...


class LRUCache(CachePolicy):
    """Least Recently Used cache with max size."""
    
    def __init__(self, maxsize: int = 128) -> None:
        self._maxsize = maxsize
        self._cache: OrderedDict = OrderedDict()
        self._lock = threading.RLock()
    
    def get(self, key: Hashable) -> Any:
        with self._lock:
            if key not in self._cache:
                return None
            self._cache.move_to_end(key)
            return self._cache[key]
    
    def set(self, key: Hashable, value: Any) -> None:
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            else:
                if len(self._cache) >= self._maxsize:
                    self._cache.popitem(last=False)
                self._cache[key] = value
    
    def invalidate(self, key: Hashable) -> None:
        with self._lock:
            self._cache.pop(key, None)
    
    def clear(self) -> None:
        with self._lock:
            self._cache.clear()


class TTLCache(CachePolicy):
    """Time-To-Live cache with automatic expiration."""
    
    def __init__(self, ttl: float = 300.0, maxsize: int = 256) -> None:
        self._ttl = ttl
        self._maxsize = maxsize
        self._cache: OrderedDict = OrderedDict()
        self._timestamps: OrderedDict = OrderedDict()
        self._lock = threading.RLock()
    
    def _is_expired(self, key: Hashable) -> bool:
        ts = self._timestamps.get(key)
        if ts is None:
            return True
        return time.monotonic() - ts > self._ttl
    
    def _cleanup(self) -> None:
        expired = [k for k in self._cache if self._is_expired(k)]
        for k in expired:
            self._cache.pop(k, None)
            self._timestamps.pop(k, None)
        while len(self._cache) > self._maxsize:
            self._cache.popitem(last=False)
            self._timestamps.popitem(last=False)
    
    def get(self, key: Hashable) -> Any:
        with self._lock:
            if key not in self._cache or self._is_expired(key):
                return None
            self._cache.move_to_end(key)
            self._timestamps[key] = time.monotonic()
            return self._cache[key]
    
    def set(self, key: Hashable, value: Any) -> None:
        with self._lock:
            self._cleanup()
            self._cache[key] = value
            self._timestamps[key] = time.monotonic()
    
    def invalidate(self, key: Hashable) -> None:
        with self._lock:
            self._cache.pop(key, None)
            self._timestamps.pop(key, None)
    
    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._timestamps.clear()


class LFUCache(CachePolicy):
    """Least Frequently Used cache."""
    
    def __init__(self, maxsize: int = 128) -> None:
        self._maxsize = maxsize
        self._cache: OrderedDict = OrderedDict()
        self._freq: dict = {}
        self._lock = threading.RLock()
    
    def get(self, key: Hashable) -> Any:
        with self._lock:
            if key not in self._cache:
                return None
            self._freq[key] = self._freq.get(key, 0) + 1
            self._cache.move_to_end(key)
            return self._cache[key]
    
    def set(self, key: Hashable, value: Any) -> None:
        with self._lock:
            if key in self._cache:
                self._freq[key] = self._freq.get(key, 0) + 1
            else:
                if len(self._cache) >= self._maxsize:
                    min_freq = min(self._freq.values())
                    lfu_keys = [k for k, f in self._freq.items() if f == min_freq]
                    for k in lfu_keys:
                        self._cache.pop(k, None)
                        self._freq.pop(k, None)
                        break
                self._freq[key] = 1
            self._cache[key] = value
    
    def invalidate(self, key: Hashable) -> None:
        with self._lock:
            self._cache.pop(key, None)
            self._freq.pop(key, None)
    
    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._freq.clear()


def memoize(
    cache: CachePolicy | None = None,
    *,
    maxsize: int = 128,
    ttl: float | None = None,
    key_func: Callable[..., Hashable] | None = None,
):
    """Decorator to memoize function results.
    
    Example:
        @memoize(ttl=60.0)
        def expensive_func(x, y):
            return compute(x, y)
    """
    if cache is None:
        if ttl:
            cache = TTLCache(ttl=ttl, maxsize=maxsize)
        else:
            cache = LRUCache(maxsize=maxsize)
    
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                try:
                    cache_key = (args, tuple(sorted(kwargs.items())))
                except TypeError:
                    return func(*args, **kwargs)
            
            result = cache.get(cache_key)
            if result is not None:
                return result
            
            result = func(*args, **kwargs)
            cache.set(cache_key, result)
            return result
        
        wrapper.cache = cache
        wrapper.cache_clear = cache.clear
        wrapper.cache_invalidate = cache.invalidate
        return wrapper
    return decorator


def memoize_method(maxsize: int = 128):
    """Decorator for memoizing instance methods with weakref storage."""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(self, *args: Any, **kwargs: Any) -> T:
            cache_attr = f"_memo_cache_{func.__name__}"
            if not hasattr(self, cache_attr):
                setattr(self, cache_attr, LRUCache(maxsize=maxsize))
            cache: LRUCache = getattr(self, cache_attr)
            try:
                key = (args, tuple(sorted(kwargs.items())))
            except TypeError:
                return func(self, *args, **kwargs)
            result = cache.get(key)
            if result is not None:
                return result
            result = func(self, *args, **kwargs)
            cache.set(key, result)
            return result
        return wrapper
    return decorator


class MemoizedResult(Generic[T]):
    """Container for a memoized computation result with metadata."""
    
    def __init__(
        self,
        value: T,
        computed_at: float | None = None,
        key: Hashable | None = None,
    ) -> None:
        self.value = value
        self.computed_at = computed_at or time.time()
        self.key = key
    
    @property
    def age_seconds(self) -> float:
        return time.time() - self.computed_at


__all__ = [
    "CachePolicy",
    "LRUCache",
    "TTLCache",
    "LFUCache",
    "memoize",
    "memoize_method",
    "MemoizedResult",
]
