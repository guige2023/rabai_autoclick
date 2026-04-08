"""Action filter and throttle utilities.

Provides rate limiting and action filtering for
managing event and action flow in automation workflows.
"""

import threading
import time
from collections import defaultdict
from typing import Any, Callable, Dict, Optional


ActionHandler = Callable[..., Any]


class ActionThrottle:
    """Throttles actions to a maximum rate.

    Example:
        throttle = ActionThrottle(max_calls=5, period=1.0)
        for event in events:
            if throttle.should_proceed("action1"):
                do_action()
    """

    def __init__(self, max_calls: int = 10, period: float = 1.0) -> None:
        self._max_calls = max_calls
        self._period = period
        self._calls: Dict[str, list] = defaultdict(list)
        self._lock = threading.Lock()

    def should_proceed(self, action: str) -> bool:
        """Check if action should proceed.

        Args:
            action: Action name/key.

        Returns:
            True if within rate limit.
        """
        with self._lock:
            now = time.time()
            cutoff = now - self._period

            self._calls[action] = [t for t in self._calls[action] if t > cutoff]

            if len(self._calls[action]) < self._max_calls:
                self._calls[action].append(now)
                return True
            return False

    def reset(self, action: Optional[str] = None) -> None:
        """Reset throttle counters.

        Args:
            action: Action to reset. None for all.
        """
        with self._lock:
            if action:
                self._calls[action].clear()
            else:
                self._calls.clear()

    def wait_time(self, action: str) -> float:
        """Get time to wait before action can proceed.

        Args:
            action: Action name.

        Returns:
            Seconds to wait, 0 if can proceed now.
        """
        with self._lock:
            if action not in self._calls or len(self._calls[action]) < self._max_calls:
                return 0.0

            oldest = min(self._calls[action])
            return max(0, self._period - (time.time() - oldest))


class Debouncer:
    """Debounces rapid events.

    Example:
        debouncer = Debouncer(delay=0.3)
        for keypress in keypresses:
            debouncer.debounce("save", do_save)
    """

    def __init__(self, delay: float = 0.5) -> None:
        self._delay = delay
        self._timers: Dict[str, Optional[threading.Timer]] = {}
        self._lock = threading.Lock()

    def debounce(self, key: str, func: ActionHandler, *args: Any, **kwargs: Any) -> None:
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

            self._timers[key] = threading.Timer(
                self._delay,
                self._execute,
                args=(key, func, args, kwargs),
            )
            self._timers[key].daemon = True
            self._timers[key].start()

    def _execute(self, key: str, func: ActionHandler, args: tuple, kwargs: dict) -> None:
        try:
            func(*args, **kwargs)
        except Exception:
            pass
        finally:
            with self._lock:
                self._timers[key] = None

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


class ActionFilter:
    """Filters and manages action handlers.

    Example:
        filter = ActionFilter()
        filter.register("click", handle_click)
        filter.register("click", log_click)
        filter.fire("click", x=100, y=200)
    """

    def __init__(self) -> None:
        self._handlers: Dict[str, list] = defaultdict(list)
        self._throttle: Optional[ActionThrottle] = None

    def set_throttle(self, max_calls: int = 10, period: float = 1.0) -> None:
        """Set throttle for all actions."""
        self._throttle = ActionThrottle(max_calls, period)

    def register(self, action: str, handler: ActionHandler, prepend: bool = False) -> None:
        """Register an action handler.

        Args:
            action: Action name.
            handler: Handler function.
            prepend: If True, add to beginning of handlers.
        """
        if prepend:
            self._handlers[action].insert(0, handler)
        else:
            self._handlers[action].append(handler)

    def unregister(self, action: str, handler: ActionHandler) -> bool:
        """Unregister an action handler.

        Args:
            action: Action name.
            handler: Handler to remove.

        Returns:
            True if handler was found and removed.
        """
        if action in self._handlers:
            try:
                self._handlers[action].remove(handler)
                return True
            except ValueError:
                pass
        return False

    def fire(self, action: str, *args: Any, **kwargs: Any) -> None:
        """Fire an action to all handlers.

        Args:
            action: Action name.
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        """
        if self._throttle and not self._throttle.should_proceed(action):
            return

        for handler in self._handlers.get(action, []):
            try:
                handler(*args, **kwargs)
            except Exception:
                pass

    def has_handlers(self, action: str) -> bool:
        """Check if action has handlers.

        Args:
            action: Action name.

        Returns:
            True if handlers exist.
        """
        return len(self._handlers.get(action, [])) > 0

    def clear(self, action: Optional[str] = None) -> None:
        """Clear action handlers.

        Args:
            action: Action to clear. None for all.
        """
        if action:
            self._handlers[action].clear()
        else:
            self._handlers.clear()
