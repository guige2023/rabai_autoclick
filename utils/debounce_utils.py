"""Debouncing utilities for event handling.

Provides debounce functionality for filtering rapid events
and rate-limiting callbacks in automation workflows.
"""

import threading
import time
from typing import Any, Callable, Dict, Optional


D = TypeVar("D")


class Debouncer:
    """Debounces function calls with configurable delay.

    Example:
        debouncer = Debouncer(delay=0.3)
        for event in rapid_events:
            debouncer.debounce("save", save_callback)
    """

    def __init__(self, delay: float = 0.5) -> None:
        self._delay = delay
        self._timers: Dict[str, Optional[threading.Timer]] = {}
        self._lock = threading.Lock()
        self._last_args: Dict[str, tuple] = {}
        self._last_kwargs: Dict[str, dict] = {}

    def debounce(self, key: str, func: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        """Debounce a function call.

        Args:
            key: Unique key for this debounced action.
            func: Function to call.
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        """
        with self._lock:
            if key in self._timers and self._timers[key] is not None:
                self._timers[key].cancel()

            self._last_args[key] = args
            self._last_kwargs[key] = kwargs

            self._timers[key] = threading.Timer(
                self._delay,
                self._execute,
                args=(key, func),
            )
            self._timers[key].daemon = True
            self._timers[key].start()

    def _execute(self, key: str, func: Callable[..., Any]) -> None:
        args = ()
        kwargs = {}
        with self._lock:
            args = self._last_args.get(key, ())
            kwargs = self._last_kwargs.get(key, {})
            self._timers[key] = None

        try:
            func(*args, **kwargs)
        except Exception:
            pass

    def cancel(self, key: str) -> None:
        """Cancel pending debounced call.

        Args:
            key: Key to cancel.
        """
        with self._lock:
            if key in self._timers and self._timers[key] is not None:
                self._timers[key].cancel()
                self._timers[key] = None

    def cancel_all(self) -> None:
        """Cancel all pending debounced calls."""
        with self._lock:
            for timer in self._timers.values():
                if timer is not None:
                    timer.cancel()
            self._timers.clear()
            self._last_args.clear()
            self._last_kwargs.clear()

    def is_pending(self, key: str) -> bool:
        """Check if a debounced call is pending.

        Args:
            key: Key to check.

        Returns:
            True if call is pending.
        """
        with self._lock:
            return key in self._timers and self._timers[key] is not None


class Throttler:
    """Throttles function calls to a maximum rate.

    Example:
        throttler = Throttler(max_calls=5, period=1.0)
        for event in events:
            if throttler.should_proceed("action"):
                do_action()
    """

    def __init__(self, max_calls: int = 10, period: float = 1.0) -> None:
        self._max_calls = max_calls
        self._period = period
        self._calls: Dict[str, list] = {}
        self._lock = threading.Lock()

    def should_proceed(self, key: str) -> bool:
        """Check if action should proceed.

        Args:
            key: Action key.

        Returns:
            True if within rate limit.
        """
        with self._lock:
            now = time.time()
            cutoff = now - self._period

            if key not in self._calls:
                self._calls[key] = []

            self._calls[key] = [t for t in self._calls[key] if t > cutoff]

            if len(self._calls[key]) < self._max_calls:
                self._calls[key].append(now)
                return True
            return False

    def reset(self, key: Optional[str] = None) -> None:
        """Reset throttle counters.

        Args:
            key: Key to reset. None for all.
        """
        with self._lock:
            if key:
                self._calls[key] = []
            else:
                self._calls.clear()

    def wait_time(self, key: str) -> float:
        """Get time to wait before action can proceed.

        Args:
            key: Action key.

        Returns:
            Seconds to wait, 0 if can proceed.
        """
        with self._lock:
            if key not in self._calls or len(self._calls[key]) < self._max_calls:
                return 0.0
            oldest = min(self._calls[key])
            return max(0.0, self._period - (time.time() - oldest))

    def call_count(self, key: str) -> int:
        """Get number of calls in current period.

        Args:
            key: Action key.

        Returns:
            Number of calls.
        """
        with self._lock:
            if key not in self._calls:
                return 0
            now = time.time()
            cutoff = now - self._period
            return len([t for t in self._calls[key] if t > cutoff])


def debounce(delay: float = 0.5) -> Callable[[Callable[D], Callable[D]]:
    """Decorator to debounce function calls.

    Example:
        @debounce(delay=0.3)
        def on_change():
            save()
    """
    _debouncer: Dict[int, Debouncer] = {}

    def decorator(func: Callable[D]) -> Callable[D]:
        func_id = id(func)
        if func_id not in _debouncer:
            _debouncer[func_id] = Debouncer(delay)

        def wrapper(*args: Any, **kwargs: Any) -> None:
            _debouncer[func_id].debounce(str(func_id), func, *args, **kwargs)

        return wrapper  # type: ignore

    return decorator


def throttle(max_calls: int = 10, period: float = 1.0) -> Callable[[Callable[D], Callable[D]]:
    """Decorator to throttle function calls.

    Example:
        @throttle(max_calls=5, period=1.0)
        def on_click():
            track()
    """
    _throttler: Dict[int, Throttler] = {}

    def decorator(func: Callable[D]) -> Callable[D]:
        func_id = id(func)
        if func_id not in _throttler:
            _throttler[func_id] = Throttler(max_calls, period)

        def wrapper(*args: Any, **kwargs: Any) -> bool:
            return _throttler[func_id].should_proceed(str(func_id))

        return wrapper  # type: ignore

    return decorator


from typing import TypeVar
D = TypeVar("D")
