"""Debounce and throttle action for UI automation.

Provides input rate limiting:
- Debounce for text input
- Throttle for repeated actions
- Event coalescing
- Performance optimization
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Any


@dataclass
class DebounceOptions:
    """Debounce configuration."""
    delay: float = 0.3  # Seconds to wait
    leading: bool = False  # Execute on leading edge
    trailing: bool = True  # Execute on trailing edge
    max_wait: float | None = None  # Maximum wait time


@dataclass
class ThrottleOptions:
    """Throttle configuration."""
    interval: float = 0.1  # Minimum interval between calls
    leading: bool = True  # Execute on leading edge
    trailing: bool = True  # Execute on trailing edge


class Debouncer:
    """Debounces rapid events.

    Waits for a pause in events before executing.
    Useful for text input, resize handlers, etc.
    """

    def __init__(self, options: DebounceOptions | None = None):
        self.options = options or DebounceOptions()
        self._timer: float | None = None
        self._last_args: tuple = ()
        self._last_kwargs: dict = {}
        self._result: Any = None
        self._callback: Callable | None = None

    def __call__(
        self,
        func: Callable | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Call with debounce."""
        if func is not None:
            self._callback = func
        return self._debounced(*args, **kwargs)

    def _debounced(self, *args: Any, **kwargs: Any) -> Any:
        """Execute debounced function."""
        self._last_args = args
        self._last_kwargs = kwargs

        if self.options.leading:
            # Execute immediately on first call
            self._execute()
            return self._result

        # Reset timer
        self._cancel()
        self._timer = time.time()

        return self._result

    def _execute(self) -> None:
        """Execute the callback."""
        if self._callback:
            self._result = self._callback(*self._last_args, **self._last_kwargs)

    def _cancel(self) -> None:
        """Cancel pending execution."""
        self._timer = None

    def flush(self) -> Any:
        """Execute immediately and clear timer."""
        if self._timer is not None:
            self._execute()
            self._cancel()
        return self._result

    def cancel(self) -> None:
        """Cancel pending execution."""
        self._cancel()


class Throttler:
    """Throttles rapid events.

    Ensures function is called at most once per interval.
    Useful for scroll handlers, mouse move, etc.
    """

    def __init__(self, options: ThrottleOptions | None = None):
        self.options = options or ThrottleOptions()
        self._last_call: float = 0
        self._pending: bool = False
        self._timer: float | None = None
        self._args: tuple = ()
        self._kwargs: dict = {}
        self._callback: Callable | None = None

    def __call__(
        self,
        func: Callable | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Call with throttle."""
        if func is not None:
            self._callback = func
        return self._throttled(*args, **kwargs)

    def _throttled(self, *args: Any, **kwargs: Any) -> Any:
        """Execute throttled function."""
        now = time.time()
        elapsed = now - self._last_call

        self._args = args
        self._kwargs = kwargs

        if elapsed >= self.options.interval:
            # Enough time has passed
            if self.options.leading:
                self._execute()
            self._last_call = now
        elif self.options.trailing and not self._pending:
            # Schedule trailing edge call
            self._pending = True
            remaining = self.options.interval - elapsed
            self._timer = now + remaining

        return self._result

    def _execute(self) -> None:
        """Execute the callback."""
        self._pending = False
        if self._callback:
            self._result = self._callback(*self._args, **self._kwargs)

    def flush(self) -> Any:
        """Execute immediately if pending."""
        if self._pending:
            self._execute()
            self._last_call = time.time()
        return self._result

    def cancel(self) -> None:
        """Cancel pending execution."""
        self._pending = False
        self._timer = None


class EventCoalescer:
    """Coalesces rapid events.

    Collects multiple events and processes them together.
    Useful for batching rapid updates.
    """

    def __init__(self, interval: float = 0.1):
        self.interval = interval
        self._queue: deque = deque(maxlen=1000)
        self._last_process: float = 0
        self._callback: Callable | None = None
        self._pending: bool = False

    def add(self, event: Any) -> None:
        """Add event to coalescing queue."""
        self._queue.append(event)

    def process(self, callback: Callable[[list], Any]) -> Any:
        """Process all queued events.

        Args:
            callback: Function(list of events) -> result

        Returns:
            Result from callback
        """
        if not self._queue:
            return None

        events = list(self._queue)
        self._queue.clear()
        self._last_process = time.time()
        self._pending = False

        if self._callback:
            return self._callback(events)
        return callback(events)

    def set_callback(self, callback: Callable[[list], Any]) -> None:
        """Set processing callback."""
        self._callback = callback

    def should_process(self) -> bool:
        """Check if should process now."""
        if not self._queue:
            return False
        elapsed = time.time() - self._last_process
        return elapsed >= self.interval

    def clear(self) -> None:
        """Clear queue."""
        self._queue.clear()
        self._pending = False


def create_debouncer(
    delay: float = 0.3,
    leading: bool = False,
    trailing: bool = True,
) -> Debouncer:
    """Create debouncer."""
    options = DebounceOptions(delay=delay, leading=leading, trailing=trailing)
    return Debouncer(options)


def create_throttler(
    interval: float = 0.1,
    leading: bool = True,
    trailing: bool = True,
) -> Throttler:
    """Create throttler."""
    options = ThrottleOptions(interval=interval, leading=leading, trailing=trailing)
    return Throttler(options)
