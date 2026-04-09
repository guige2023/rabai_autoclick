"""
Debounce and throttle utilities for UI automation event handling.

Provides debounce and throttle decorators for rate-limiting
function calls based on time and event patterns.
"""

from __future__ import annotations

import time
import threading
import functools
from typing import (
    TypeVar, Callable, Any, Optional, Union, 
    Generic, ParamSpec, overload
)
from dataclasses import dataclass, field
from collections import defaultdict
from collections.abc import Hashable


P = ParamSpec('P')
T = TypeVar('T')

Function = Callable[P, T]


@dataclass
class ThrottleState:
    """State tracking for throttled function."""
    last_call: float = 0.0
    result: Any = None
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    timer: Optional[threading.Timer] = None


@dataclass
class DebounceState:
    """State tracking for debounced function."""
    timer: Optional[threading.Timer] = None
    pending: bool = False


class Debouncer:
    """Manual debounce controller without decorators."""
    
    def __init__(
        self,
        wait: float,
        max_wait: Optional[float] = None,
        immediate: bool = False,
    ) -> None:
        """Initialize debouncer.
        
        Args:
            wait: Milliseconds to wait after last call
            max_wait: Maximum milliseconds to wait
            immediate: Execute on leading edge instead of trailing
        """
        self.wait = wait / 1000.0
        self.max_wait = max_wait / 1000.0 if max_wait else None
        self.immediate = immediate
        self._state: DebounceState = DebounceState()
        self._lock = threading.Lock()
    
    def __call__(self, func: Function[P, T]) -> Function[P, T]:
        """Wrap function with debounce."""
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            return self._debounce(func, args, kwargs)
        
        wrapper.cancel = self.cancel  # type: ignore
        wrapper.flush = self.flush  # type: ignore
        return wrapper
    
    def _debounce(self, func: Function[P, T], args: tuple, kwargs: dict) -> Optional[T]:
        """Execute debounce logic."""
        with self._lock:
            if self._state.timer is not None:
                self._state.timer.cancel()
            
            if self.immediate and not self._state.pending:
                self._state.pending = True
                try:
                    return func(*args, **kwargs)
                finally:
                    self._state.pending = False
            
            wait_time = self.wait
            if self.max_wait is not None:
                wait_time = self.max_wait
            
            self._state.timer = threading.Timer(wait_time, self._execute, [func, args, kwargs])
            self._state.timer.start()
            
            return None
    
    def _execute(self, func: Function[P, T], args: tuple, kwargs: dict) -> None:
        """Execute pending call."""
        with self._lock:
            self._state.pending = False
        func(*args, **kwargs)
    
    def cancel(self) -> None:
        """Cancel pending debounced call."""
        with self._lock:
            if self._state.timer is not None:
                self._state.timer.cancel()
                self._state.timer = None
    
    def flush(self) -> None:
        """Execute pending call immediately."""
        with self._lock:
            if self._state.timer is not None:
                self._state.timer.cancel()


class Throttler:
    """Manual throttle controller without decorators."""
    
    def __init__(
        self,
        rate: float,
        every: Optional[float] = None,
    ) -> None:
        """Initialize throttler.
        
        Args:
            rate: Maximum calls per interval
            every: Interval in milliseconds (defaults to 1/rate)
        """
        self.rate = rate
        self.interval = every / 1000.0 if every else (1.0 / rate)
        self._states: dict[str, ThrottleState] = defaultdict(ThrottleState)
        self._lock = threading.Lock()
    
    def __call__(self, func: Function[P, T]) -> Function[P, T]:
        """Wrap function with throttle."""
        key = f"{func.__module__}.{func.__qualname__}"
        
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Optional[T]:
            return self._throttle(key, func, args, kwargs)
        
        wrapper.cancel = lambda key=key: self.cancel(key)  # type: ignore
        return wrapper
    
    def _throttle(self, key: str, func: Function[P, T], args: tuple, kwargs: dict) -> Optional[T]:
        """Execute throttle logic."""
        state = self._states[key]
        now = time.time()
        
        with self._lock:
            elapsed = now - state.last_call
            
            if elapsed >= self.interval:
                state.last_call = now
                state.result = func(*args, **kwargs)
                state.args = args
                state.kwargs = kwargs
                return state.result
            
            if state.timer is None:
                remaining = self.interval - elapsed
                state.timer = threading.Timer(
                    remaining,
                    self._execute_delayed,
                    [key, func],
                )
                state.timer.start()
            
            return state.result
    
    def _execute_delayed(self, key: str, func: Function[P, T]) -> None:
        """Execute delayed throttled call."""
        with self._lock:
            state = self._states[key]
            state.timer = None
            state.last_call = time.time()
            state.result = func(*state.args, **state.result)


def debounce(
    wait: float,
    max_wait: Optional[float] = None,
    immediate: bool = False,
) -> Callable[[Function[P, T]], Function[P, Optional[T]]]:
    """Decorator: Debounce function calls.
    
    Args:
        wait: Milliseconds to wait after last call
        max_wait: Maximum milliseconds to wait
        immediate: Execute on leading edge instead of trailing
    
    Returns:
        Decorated function with debounce behavior
    
    Example:
        @debounce(300)
        def on_resize():
            print("Resized!")
    """
    def decorator(func: Function[P, T]) -> Function[P, Optional[T]]:
        debouncer = Debouncer(wait, max_wait, immediate)
        return debouncer(func)  # type: ignore
    
    return decorator


def throttle(
    rate: float,
    every: Optional[float] = None,
) -> Callable[[Function[P, T]], Function[P, Optional[T]]]:
    """Decorator: Throttle function calls.
    
    Args:
        rate: Maximum calls per interval
        every: Interval in milliseconds
    
    Returns:
        Decorated function with throttle behavior
    
    Example:
        @throttle(10, every=1000)
        def on_mouse_move(x, y):
            print(f"Moved to {x}, {y}")
    """
    def decorator(func: Function[P, T]) -> Function[P, Optional[T]]:
        throttler = Throttler(rate, every)
        return throttler(func)  # type: ignore
    
    return decorator


def rate_limit(
    max_calls: int,
    period: float,
) -> Callable[[Function[P, T]], Function[P, Optional[T]]]:
    """Decorator: Limit function to max_calls per period.
    
    Args:
        max_calls: Maximum calls allowed
        period: Period in seconds
    
    Returns:
        Decorated function with rate limit
    
    Example:
        @rate_limit(5, 60.0)
        def send_notification(msg):
            send(msg)
    """
    interval = period / max_calls
    throttler = Throttler(max_calls, every=int(period * 1000))
    
    def decorator(func: Function[P, T]) -> Function[P, Optional[T]]:
        return throttler(func)  # type: ignore
    
    return decorator


class AsyncDebouncer:
    """Async-aware debouncer for coroutine functions."""
    
    def __init__(
        self,
        wait: float,
        max_wait: Optional[float] = None,
    ) -> None:
        """Initialize async debouncer.
        
        Args:
            wait: Milliseconds to wait
            max_wait: Maximum milliseconds to wait
        """
        self.wait = wait / 1000.0
        self.max_wait = max_wait / 1000.0 if max_wait else None
        self._pending_task: Optional[threading.Timer] = None
        self._lock = threading.Lock()
    
    def __call__(self, func: Function[P, T]) -> Function[P, T]:
        """Wrap async function."""
        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> Optional[T]:
            return self._debounce(func, args, kwargs)
        
        wrapper.cancel = self.cancel  # type: ignore
        return wrapper  # type: ignore
    
    def _debounce(self, func: Function[P, T], args: tuple, kwargs: dict) -> Optional[T]:
        """Execute debounce."""
        with self._lock:
            if self._pending_task is not None:
                self._pending_task.cancel()
            
            wait_time = self.wait
            if self.max_wait is not None:
                wait_time = self.max_wait
        
        return None
    
    def cancel(self) -> None:
        """Cancel pending call."""
        with self._lock:
            if self._pending_task is not None:
                self._pending_task.cancel()


def batch_debounce(
    wait: float,
    batch_size: int = 10,
) -> Callable[[Function[P, T]], Function[P, list[T]]]:
    """Decorator: Batch calls and debounce the batch.
    
    Args:
        wait: Milliseconds to wait before executing batch
        batch_size: Maximum batch size before forced execution
    
    Returns:
        Decorated function that returns list of results
    """
    def decorator(func: Function[P, T]) -> Function[P, list[T]]:
        pending: list[tuple[tuple, dict]] = []
        timer: Optional[threading.Timer] = None
        lock = threading.Lock()
        
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> list[T]:
            nonlocal timer, pending
            
            with lock:
                pending.append((args, kwargs))
                
                if len(pending) >= batch_size:
                    if timer is not None:
                        timer.cancel()
                    results = _execute_batch(func, pending)
                    pending = []
                    return results
                
                if timer is not None:
                    timer.cancel()
                
                timer = threading.Timer(wait / 1000.0, _flush_batch)
                timer.start()
            
            return []
        
        def _flush_batch() -> None:
            nonlocal pending
            with lock:
                if pending:
                    results = _execute_batch(func, pending)
                    pending = []
        
        def _execute_batch(f: Function[P, T], batch: list) -> list[T]:
            return [f(*args, **kwargs) for args, kwargs in batch]
        
        return wrapper
    
    return decorator


class EventThrottler:
    """Per-event-type throttle with key-based tracking."""
    
    def __init__(
        self,
        default_rate: float = 10.0,
        default_interval: Optional[float] = None,
    ) -> None:
        """Initialize event throttler.
        
        Args:
            default_rate: Default max calls per second
            default_interval: Default interval in ms
        """
        self.default_rate = default_rate
        self.default_interval = (
            default_interval / 1000.0 if default_interval 
            else 1.0 / default_rate
        )
        self._throttlers: dict[str, Throttler] = {}
        self._lock = threading.Lock()
    
    def throttle(
        self,
        event_type: Hashable,
        rate: Optional[float] = None,
        interval: Optional[float] = None,
    ) -> Callable[[Function[P, T]], Function[P, Optional[T]]]:
        """Throttle specific event type.
        
        Args:
            event_type: Event identifier
            rate: Max calls per second
            interval: Interval in milliseconds
        
        Returns:
            Decorator for throttled function
        """
        throttler_key = str(event_type)
        
        with self._lock:
            if throttler_key not in self._throttlers:
                self._throttlers[throttler_key] = Throttler(
                    rate or self.default_rate,
                    interval or int(self.default_interval * 1000),
                )
        
        return self._throttlers[throttler_key].__call__  # type: ignore
