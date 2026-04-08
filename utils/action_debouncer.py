"""Action debouncing utilities for UI automation.

Prevents duplicate or rapid-fire execution of actions caused by
duplicate events, UI flicker, or network latency.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class DebounceStrategy(Enum):
    """Debounce strategies for handling rapid events."""
    LEADING = auto()   # Execute on first call, ignore subsequent until cooldown
    TRAILING = auto()   # Execute only after events stop for the cooldown period
    COMBINED = auto()   # Execute on first call, and also on last call after cooldown


@dataclass
class DebounceOptions:
    """Options for debounce behavior.

    Attributes:
        wait: Cooldown period in seconds.
        strategy: LEADING, TRAILING, or COMBINED.
        max_wait: Maximum time to wait before forcing execution (for TRAILING).
        leading_before: Extra leading delay (seconds).
        on_execute: Callback to check if action should actually execute.
    """
    wait: float = 0.5
    strategy: DebounceStrategy = DebounceStrategy.COMBINED
    max_wait: float = 5.0
    leading_before: float = 0.0
    on_execute: Optional[Callable[[], bool]] = None


@dataclass
class DebounceState:
    """Internal state for a debounced action."""
    timer: Optional[object] = None
    last_call_time: float = 0.0
    last_exec_time: float = 0.0
    call_count: int = 0
    result: Any = None
    is_pending: bool = False
    pending_args: tuple[Any, ...] = field(default_factory=tuple)
    pending_kwargs: dict[str, Any] = field(default_factory=dict)


class ActionDebouncer:
    """Debounces action calls to prevent rapid-fire execution.

    Supports LEADING, TRAILING, and COMBINED strategies.
    Thread-safe for single-threaded UI automation use.
    """

    def __init__(
        self,
        action: Callable[..., Any],
        options: Optional[DebounceOptions] = None,
    ) -> None:
        """Initialize debouncer with an action callable.

        Args:
            action: The callable to debounce.
            options: Debounce configuration options.
        """
        self._action = action
        self._options = options or DebounceOptions()
        self._states: dict[str, DebounceState] = {}

    def __call__(
        self,
        *args: Any,
        key: str = "default",
        **kwargs: Any,
    ) -> Optional[Any]:
        """Call the debounced action.

        Args:
            *args: Positional arguments passed to the action.
            **kwargs: Keyword arguments passed to the action.
            key: Identifier for this debounce slot (multiple slots supported).

        Returns:
            The action's return value, or None if debounced.
        """
        state = self._states.setdefault(key, DebounceState())
        now = time.time()
        wait = self._options.wait
        strategy = self._options.strategy
        result: Optional[Any] = None

        state.call_count += 1
        state.last_call_time = now
        state.pending_args = args
        state.pending_kwargs = kwargs

        if self._options.leading_before > 0:
            time.sleep(self._options.leading_before)

        if strategy == DebounceStrategy.LEADING:
            if now - state.last_exec_time >= wait:
                result = self._execute(state, args, kwargs)
                state.last_exec_time = now
            else:
                state.is_pending = True

        elif strategy == DebounceStrategy.TRAILING:
            if not state.is_pending:
                state.is_pending = True
                self._schedule_trailing(key, state)
            result = state.result

        elif strategy == DebounceStrategy.COMBINED:
            if now - state.last_exec_time >= wait:
                result = self._execute(state, args, kwargs)
                state.last_exec_time = now
            else:
                state.is_pending = True
                self._schedule_trailing(key, state)

        return result

    def _execute(
        self,
        state: DebounceState,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        """Execute the wrapped action."""
        if self._options.on_execute and not self._options.on_execute():
            return None
        try:
            state.result = self._action(*args, **kwargs)
        except Exception as e:
            state.result = e
        state.is_pending = False
        state.call_count = 0
        return state.result

    def _schedule_trailing(self, key: str, state: DebounceState) -> None:
        """Schedule trailing execution."""
        state.is_pending = True

    def cancel(self, key: str = "default") -> None:
        """Cancel pending debounced call for a key."""
        if key in self._states:
            self._states[key].is_pending = False

    def flush(self, key: str = "default") -> Optional[Any]:
        """Immediately execute pending call for a key."""
        state = self._states.get(key)
        if not state or not state.is_pending:
            return None
        return self._execute(
            state, state.pending_args, state.pending_kwargs,
        )

    def get_state(self, key: str = "default") -> Optional[DebounceState]:
        """Get the current debounce state for a key."""
        return self._states.get(key)

    def reset(self, key: Optional[str] = None) -> None:
        """Reset state for a key, or all keys if key is None."""
        if key:
            self._states.pop(key, None)
        else:
            self._states.clear()


def debounce(
    wait: float = 0.5,
    strategy: DebounceStrategy = DebounceStrategy.COMBINED,
) -> Callable[[Callable[..., Any]], ActionDebouncer]:
    """Decorator to debounce a function.

    Usage:
        @debounce(wait=0.3, strategy=DebounceStrategy.LEADING)
        def on_click():
            ...
    """
    def decorator(func: Callable[..., Any]) -> ActionDebouncer:
        options = DebounceOptions(wait=wait, strategy=strategy)
        return ActionDebouncer(func, options)
    return decorator


class ThrottledAction:
    """Rate-limited action execution.

    Ensures an action is not called more than once per interval.
    Unlike debouncer, throttle tracks call rate, not event timing.
    """

    def __init__(
        self,
        action: Callable[..., Any],
        min_interval: float = 1.0,
    ) -> None:
        """Initialize throttled action.

        Args:
            action: The callable to throttle.
            min_interval: Minimum seconds between executions.
        """
        self._action = action
        self._min_interval = min_interval
        self._last_exec_time: float = 0.0
        self._result: Any = None

    def __call__(self, *args: Any, **kwargs: Any) -> Optional[Any]:
        """Call the throttled action if interval has passed."""
        now = time.time()
        if now - self._last_exec_time >= self._min_interval:
            self._last_exec_time = now
            try:
                self._result = self._action(*args, **kwargs)
            except Exception as e:
                self._result = e
        return self._result

    def reset(self) -> None:
        """Reset last execution time, allowing immediate call."""
        self._last_exec_time = 0.0

    @property
    def last_result(self) -> Any:
        """Return the result from the last successful execution."""
        return self._result


def throttle(min_interval: float = 1.0) -> Callable[[Callable[..., Any]], ThrottledAction]:
    """Decorator to throttle a function's call rate.

    Usage:
        @throttle(min_interval=2.0)
        def check_for_updates():
            ...
    """
    def decorator(func: Callable[..., Any]) -> ThrottledAction:
        return ThrottledAction(func, min_interval)
    return decorator
