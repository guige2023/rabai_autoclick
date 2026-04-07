"""Performance optimization utilities for PyQt5 UI.

Provides lazy loading, caching, and UI optimization utilities
to improve application responsiveness and reduce memory usage.
"""

import functools
import threading
from typing import Any, Callable, Dict, Optional, TypeVar
from weakref import WeakValueDictionary

from PyQt5.QtCore import QObject, pyqtSignal, QTimer

T = TypeVar('T')


class LazyWidgetLoader:
    """Lazy loading wrapper for heavy widgets.

    Defers widget creation until the widget is actually accessed,
    improving initial startup time and reducing memory usage.
    """

    def __init__(self, factory: Callable[[], QObject]) -> None:
        """Initialize lazy loader.

        Args:
            factory: Factory function that creates the widget.
        """
        self._factory = factory
        self._instance: Optional[QObject] = None

    def get(self) -> QObject:
        """Get the widget instance, creating it if needed.

        Returns:
            The widget instance.
        """
        if self._instance is None:
            self._instance = self._factory()
        return self._instance

    @property
    def is_loaded(self) -> bool:
        """Check if the widget has been loaded.

        Returns:
            True if widget is loaded, False otherwise.
        """
        return self._instance is not None


class SignalThrottler:
    """Throttle rapid signal emissions to reduce UI updates.

    Useful for text change events, slider moves, etc.
    """

    def __init__(self, delay_ms: int = 100) -> None:
        """Initialize throttler.

        Args:
            delay_ms: Minimum delay between emissions in milliseconds.
        """
        self._delay_ms = delay_ms
        self._timer: Optional[QTimer] = None
        self._pending_func: Optional[Callable[..., None]] = None
        self._pending_args: tuple = ()
        self._pending_kwargs: Dict[str, Any] = {}

    def throttle(self, func: Callable[..., None], *args, **kwargs) -> None:
        """Throttle a function call.

        Args:
            func: Function to call.
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        """
        self._pending_func = func
        self._pending_args = args
        self._pending_kwargs = kwargs

        if self._timer is None:
            self._timer = QTimer()
            self._timer.timeout.connect(self._do_throttled_call)
            self._timer.setSingleShot(True)
            self._timer.start(self._delay_ms)

    def _do_throttled_call(self) -> None:
        """Execute the throttled call."""
        if self._pending_func:
            self._pending_func(*self._pending_args, **self._pending_kwargs)
        self._timer = None

    def set_callback(self, func: Callable[..., None]) -> None:
        """Set the function to be called when throttled.

        Args:
            func: Function to call.
        """
        self._pending_func = func


class Debouncer:
    """Debounce rapid calls to reduce processing.

    Only executes the function after a delay has passed
    without any new calls.
    """

    def __init__(self, delay_ms: int = 300) -> None:
        """Initialize debouncer.

        Args:
            delay_ms: Delay in milliseconds before executing.
        """
        self._delay_ms = delay_ms
        self._timer: Optional[QTimer] = None
        self._lock = threading.Lock()

    def debounce(self, func: Callable[..., None], *args, **kwargs) -> None:
        """Debounce a function call.

        Args:
            func: Function to call.
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        """
        if self._timer is not None:
            self._timer.stop()
            self._timer.deleteLater()

        self._timer = QTimer()
        self._timer.setSingleShot(True)
        self._timer.timeout.connect(lambda: func(*args, **kwargs))
        self._timer.start(self._delay_ms)

    def cancel(self) -> None:
        """Cancel any pending debounced call."""
        if self._timer is not None:
            self._timer.stop()
            self._timer.deleteLater()
            self._timer = None


class WidgetCache:
    """Cache for storing widget references with automatic cleanup.

    Uses WeakValueDictionary so cached widgets can still be garbage
    collected when they're no longer referenced elsewhere.
    """

    _instance: Optional['WidgetCache'] = None

    def __new__(cls) -> 'WidgetCache':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        self._cache: WeakValueDictionary[str, QObject] = WeakValueDictionary()

    def get_or_create(self, key: str, factory: Callable[[], QObject]) -> QObject:
        """Get a cached widget or create a new one.

        Args:
            key: Cache key.
            factory: Factory function to create widget if not cached.

        Returns:
            Cached or newly created widget.
        """
        if key not in self._cache:
            self._cache[key] = factory()
        return self._cache[key]

    def clear(self) -> None:
        """Clear all cached widgets."""
        self._cache.clear()

    def remove(self, key: str) -> None:
        """Remove a specific widget from cache.

        Args:
            key: Cache key to remove.
        """
        if key in self._cache:
            del self._cache[key]


# Global cache instance
widget_cache: WidgetCache = WidgetCache()


def lazy_property(func: Callable[[Any], T]) -> property:
    """Decorator for lazy-loaded properties.

    The property value is computed once on first access and
    then cached for subsequent accesses.

    Args:
        func: Function that computes the property value.

    Returns:
        A property descriptor.
    """
    attr_name = f'_lazy_{func.__name__}'

    @property
    @functools.wraps(func)
    def lazy_prop(self: Any) -> T:
        if not hasattr(self, attr_name):
            setattr(self, attr_name, func(self))
        return getattr(self, attr_name)

    return lazy_prop


def timer_delayed(delay_ms: int) -> Callable[[Callable[..., Any]], Callable[..., None]]:
    """Decorator to delay function execution using a timer.

    Args:
        delay_ms: Delay in milliseconds.

    Returns:
        Decorator function.
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., None]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> None:
            QTimer.singleShot(delay_ms, lambda: func(*args, **kwargs))
        return wrapper
    return decorator


class BatchOperation:
    """Context manager for batching multiple UI operations.

    Reduces repaints and updates by temporarily disabling
    updates on a widget while making changes.
    """

    def __init__(self, widget: QObject, delay_ms: int = 50) -> None:
        """Initialize batch operation.

        Args:
            widget: Widget to batch updates for.
            delay_ms: Delay before re-enabling updates.
        """
        self._widget = widget
        self._delay_ms = delay_ms

    def __enter__(self) -> 'BatchOperation':
        self._widget.setUpdatesEnabled(False)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        # Use a small delay to coalesce multiple updates
        QTimer.singleShot(
            self._delay_ms,
            lambda: (
                self._widget.setUpdatesEnabled(True),
                self._widget.update()
            )
        )


def cached_signal(sender: QObject, signal_name: str, timeout_ms: int = 100):
    """Create a cached version of a signal that throttles emissions.

    Args:
        sender: Object that emits the signal.
        signal_name: Name of the signal.
        timeout_ms: Minimum time between emissions.

    Returns:
        A throttled signal proxy.
    """
    signal = getattr(sender, signal_name)
    throttler = SignalThrottler(timeout_ms)

    def throttled_emit(*args, **kwargs):
        throttler.throttle(lambda: signal.emit(*args, **kwargs))

    return throttler_emit


class RateLimiter:
    """Rate limiter to control the frequency of function calls.

    Useful for limiting user input handling, search queries, etc.
    """

    def __init__(self, max_calls: int = 1, time_window_ms: int = 1000) -> None:
        """Initialize rate limiter.

        Args:
            max_calls: Maximum number of calls allowed.
            time_window_ms: Time window in milliseconds.
        """
        self._max_calls = max_calls
        self._time_window_ms = time_window_ms
        self._calls: list = []
        self._lock = threading.Lock()

    def is_allowed(self) -> bool:
        """Check if a new call is allowed under the rate limit.

        Returns:
            True if call is allowed, False otherwise.
        """
        import time
        with self._lock:
            now = time.time() * 1000
            # Remove old calls outside the time window
            self._calls = [t for t in self._calls if now - t < self._time_window_ms]

            if len(self._calls) < self._max_calls:
                self._calls.append(now)
                return True
            return False

    def reset(self) -> None:
        """Reset the rate limiter."""
        with self._lock:
            self._calls = []


def throttle_calls(max_calls: int = 1, time_window_ms: int = 1000):
    """Decorator to throttle function calls based on rate limit.

    Args:
        max_calls: Maximum number of calls allowed.
        time_window_ms: Time window in milliseconds.

    Returns:
        Decorator function.
    """
    limiter = RateLimiter(max_calls, time_window_ms)

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if limiter.is_allowed():
                return func(*args, **kwargs)
            return None
        return wrapper
    return decorator
