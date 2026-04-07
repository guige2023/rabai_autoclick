"""debounce action module for rabai_autoclick.

Provides debounce and throttle utilities for rate-limiting function calls,
event handling, and coordinating concurrent access patterns.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Tuple
from enum import Enum, auto

__all__ = [
    "Debouncer",
    "Throttler",
    "RateLimiter",
    "debounce",
    "throttle",
    "rate_limit",
    "once",
    "memoize",
    "memoize_ttl",
    "CacheStrategy",
    "MemoCache",
    "LRUCache",
    "TTLCache",
    "CallTracker",
]


class CacheStrategy(Enum):
    """Cache eviction strategies."""
    LRU = auto()
    LFU = auto()
    FIFO = auto()
    TTL = auto()


@dataclass
class Debouncer:
    """Debouncer that delays function execution until after a quiet period.

    A debounced function will only execute after it hasn't been called
    for a specified delay period.
    """

    func: Callable
    delay: float = 0.5
    max_wait: Optional[float] = None
    leading: bool = False
    trailing: bool = True

    _timer: Optional[threading.Timer] = field(default=None, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _last_call: float = field(default_factory=lambda: time.time(), repr=False)
    _args: tuple = field(default_factory=tuple, repr=False)
    _kwargs: dict = field(default_factory=dict, repr=False)
    _cancelled: bool = field(default=False, repr=False)

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        """Call the debounced function."""
        self._args = args
        self._kwargs = kwargs
        self._last_call = time.time()

        if self.leading and self._timer is None:
            self._execute()

        self._cancel_timer()

        def on_timeout() -> None:
            if not self._cancelled:
                self._execute()
            self._timer = None

        if self.max_wait is not None:
            wait_time = min(self.delay, self.max_wait)
        else:
            wait_time = self.delay

        self._timer = threading.Timer(wait_time, on_timeout)
        self._timer.start()

    def _execute(self) -> None:
        """Execute the wrapped function."""
        if self.trailing:
            try:
                self.func(*self._args, **self._kwargs)
            except Exception:
                pass

    def _cancel_timer(self) -> None:
        """Cancel any pending timer."""
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def cancel(self) -> None:
        """Cancel any pending execution."""
        self._cancelled = True
        self._cancel_timer()

    def flush(self) -> None:
        """Execute immediately and cancel pending."""
        self._cancelled = False
        self._cancel_timer()
        self._execute()


@dataclass
class Throttler:
    """Throttler that limits function to at most once per interval.

    Unlike debounce, throttle guarantees the function is called at
    most once per interval, even if called many times.
    """

    func: Callable
    interval: float = 1.0
    leading: bool = True
    trailing: bool = True

    _last_call: float = field(default_factory=lambda: time.time() - 1000.0, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _pending: bool = field(default=False, repr=False)
    _timer: Optional[threading.Timer] = field(default=None, repr=False)
    _args: tuple = field(default_factory=tuple, repr=False)
    _kwargs: dict = field(default_factory=dict, repr=False)

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        """Call the throttled function."""
        now = time.time()
        elapsed = now - self._last_call

        if elapsed >= self.interval:
            if self.leading:
                self._execute(args, kwargs)
                self._last_call = now
            else:
                self._last_call = now
        else:
            if self.trailing and not self._pending:
                self._args = args
                self._kwargs = kwargs
                remaining = self.interval - elapsed
                self._pending = True
                self._timer = threading.Timer(remaining, self._on_timer)
                self._timer.start()

    def _on_timer(self) -> None:
        """Timer callback for trailing throttle."""
        self._execute(self._args, self._kwargs)
        self._last_call = time.time()
        self._pending = False
        self._timer = None

    def _execute(self, args: tuple, kwargs: dict) -> None:
        """Execute the wrapped function."""
        try:
            self.func(*args, **kwargs)
        except Exception:
            pass

    def cancel(self) -> None:
        """Cancel pending trailing execution."""
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        self._pending = False

    def flush(self) -> None:
        """Execute immediately if pending."""
        if self._pending:
            self._cancel_timer()
            self._execute(self._args, self._kwargs)
            self._last_call = time.time()
            self._pending = False

    def _cancel_timer(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None


class RateLimiter:
    """Token bucket rate limiter for controlling call frequency."""

    def __init__(self, rate: float, capacity: Optional[float] = None) -> None:
        self.rate = rate
        self.capacity = capacity if capacity is not None else rate
        self._tokens = float(self.capacity)
        self._last_update = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_update
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last_update = now

    def acquire(self, tokens: float = 1.0, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Acquire tokens, waiting if necessary.

        Args:
            tokens: Number of tokens to acquire.
            blocking: If True, wait for tokens; if False, return immediately.
            timeout: Maximum time to wait in seconds.

        Returns:
            True if tokens acquired, False on timeout.
        """
        deadline = None if timeout is None else time.monotonic() + timeout

        with self._lock:
            while True:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return True
                if not blocking:
                    return False
                wait_time = (tokens - self._tokens) / self.rate
                if timeout is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    wait_time = min(wait_time, remaining)
                ev = threading.Event()
                ev.wait(wait_time)

    def __call__(self, func: Callable) -> Callable:
        """Decorator form."""
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if self.acquire():
                return func(*args, **kwargs)
        return wrapper


def debounce(delay: float, leading: bool = False, trailing: bool = True) -> Callable:
    """Decorator to debounce a function.

    Args:
        delay: Seconds to wait after last call.
        leading: Execute on leading edge of first call.
        trailing: Execute after quiet period.

    Returns:
        Decorated function.
    """
    def decorator(func: Callable) -> Callable:
        debouncer = Debouncer(func, delay=delay, leading=leading, trailing=trailing)

        @property
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            debouncer(*args, **kwargs)
        wrapper.debouncer = debouncer
        wrapper.cancel = debouncer.cancel
        wrapper.flush = debouncer.flush
        return wrapper
    return decorator


def throttle(interval: float, leading: bool = True, trailing: bool = True) -> Callable:
    """Decorator to throttle a function.

    Args:
        interval: Minimum seconds between calls.
        leading: Execute on leading edge.
        trailing: Execute on trailing edge.

    Returns:
        Decorated function.
    """
    def decorator(func: Callable) -> Callable:
        throttler = Throttler(func, interval=interval, leading=leading, trailing=trailing)

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            throttler(*args, **kwargs)
        wrapper.throttler = throttler
        wrapper.cancel = throttler.cancel
        wrapper.flush = throttler.flush
        return wrapper
    return decorator


def rate_limit(rate: float, capacity: Optional[float] = None) -> Callable:
    """Decorator for rate limiting.

    Args:
        rate: Calls per second.
        capacity: Burst capacity.

    Returns:
        Decorated function.
    """
    limiter = RateLimiter(rate, capacity)

    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if limiter.acquire():
                return func(*args, **kwargs)
        return wrapper
    return decorator


def once(func: Callable) -> Callable:
    """Decorator that ensures function is only called once."""
    result: Tuple[bool, Any] = (False, None)
    lock = threading.Lock()

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with lock:
            if not result[0]:
                result[0] = True
                try:
                    result = (True, func(*args, **kwargs))
                except Exception as e:
                    result = (True, e)
                    raise
            if isinstance(result[1], Exception):
                raise result[1]
            return result[1]
    return wrapper


def memoize(func: Callable) -> Callable:
    """Simple memoization decorator (unbounded cache)."""
    cache: Dict[tuple, Any] = {}
    lock = threading.Lock()

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        key = (args, tuple(sorted(kwargs.items())))
        with lock:
            if key not in cache:
                cache[key] = func(*args, **kwargs)
            return cache[key]
    wrapper.cache = cache
    wrapper.clear = lambda: cache.clear()
    return wrapper


def memoize_ttl(seconds: float, max_size: int = 128) -> Callable:
    """Memoization with TTL (time-to-live) cache.

    Args:
        seconds: Cache TTL in seconds.
        max_size: Maximum cache entries.

    Returns:
        Decorated function.
    """
    def decorator(func: Callable) -> Callable:
        cache: Dict[tuple, Tuple[float, Any]] = {}
        lock = threading.Lock()

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            key = (args, tuple(sorted(kwargs.items())))
            now = time.time()
            with lock:
                if key in cache:
                    ts, value = cache[key]
                    if now - ts < seconds:
                        return value
                    del cache[key]
                if len(cache) >= max_size:
                    oldest_key = min(cache, key=lambda k: cache[k][0])
                    del cache[oldest_key]
                cache[key] = (now, func(*args, **kwargs))
                return cache[key][1]
        wrapper.cache = cache
        wrapper.clear = lambda: cache.clear()
        return wrapper
    return decorator


class MemoCache:
    """Simple in-memory memoization cache."""

    def __init__(self, max_size: int = 256) -> None:
        self.max_size = max_size
        self._cache: Dict[tuple, Any] = {}
        self._lock = threading.Lock()

    def get(self, key: tuple) -> Optional[Any]:
        """Get cached value."""
        with self._lock:
            return self._cache.get(key)

    def set(self, key: tuple, value: Any) -> None:
        """Set cached value."""
        with self._lock:
            if len(self._cache) >= self.max_size:
                first_key = next(iter(self._cache))
                del self._cache[first_key]
            self._cache[key] = value

    def clear(self) -> None:
        """Clear all cached values."""
        with self._lock:
            self._cache.clear()


class LRUCache:
    """Least Recently Used cache."""

    def __init__(self, max_size: int = 256) -> None:
        self.max_size = max_size
        self._cache: Dict[Any, Any] = {}
        self._order: deque = deque()
        self._lock = threading.Lock()

    def get(self, key: Any) -> Optional[Any]:
        """Get value, updating recency."""
        with self._lock:
            if key in self._cache:
                self._order.remove(key)
                self._order.append(key)
                return self._cache[key]
        return None

    def set(self, key: Any, value: Any) -> None:
        """Set value with LRU eviction."""
        with self._lock:
            if key in self._cache:
                self._order.remove(key)
            elif len(self._cache) >= self.max_size:
                oldest = self._order.popleft()
                del self._cache[oldest]
            self._cache[key] = value
            self._order.append(key)

    def clear(self) -> None:
        """Clear all entries."""
        with self._lock:
            self._cache.clear()
            self._order.clear()


class TTLCache:
    """Time-To-Live cache with automatic expiration."""

    def __init__(self, ttl: float, max_size: int = 256) -> None:
        self.ttl = ttl
        self.max_size = max_size
        self._cache: Dict[Any, Tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get(self, key: Any) -> Optional[Any]:
        """Get value if not expired."""
        with self._lock:
            if key in self._cache:
                ts, value = self._cache[key]
                if time.time() - ts < self.ttl:
                    return value
                del self._cache[key]
        return None

    def set(self, key: Any, value: Any) -> None:
        """Set value with TTL."""
        with self._lock:
            if len(self._cache) >= self.max_size:
                oldest_key = min(self._cache, key=lambda k: self._cache[k][0])
                del self._cache[oldest_key]
            self._cache[key] = (time.time(), value)

    def cleanup(self) -> int:
        """Remove all expired entries.

        Returns:
            Number of entries removed.
        """
        now = time.time()
        removed = 0
        with self._lock:
            expired = [k for k, (ts, _) in self._cache.items() if now - ts >= self.ttl]
            for k in expired:
                del self._cache[k]
                removed += 1
        return removed

    def clear(self) -> None:
        """Clear all entries."""
        with self._lock:
            self._cache.clear()


class CallTracker:
    """Track function call frequency and patterns."""

    def __init__(self, window_seconds: float = 60.0) -> None:
        self.window_seconds = window_seconds
        self._calls: deque = deque()
        self._lock = threading.Lock()

    def record(self) -> None:
        """Record a call."""
        now = time.time()
        with self._lock:
            self._calls.append(now)
            self._cleanup(now)

    def count(self) -> int:
        """Get call count in window."""
        with self._lock:
            self._cleanup(time.time())
            return len(self._calls)

    def rate(self) -> float:
        """Get calls per second."""
        count = self.count()
        return count / self.window_seconds if self.window_seconds > 0 else 0.0

    def _cleanup(self, now: float) -> None:
        """Remove calls outside window."""
        cutoff = now - self.window_seconds
        while self._calls and self._calls[0] < cutoff:
            self._calls.popleft()
