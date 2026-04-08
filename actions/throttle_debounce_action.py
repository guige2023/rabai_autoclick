"""Throttle Debounce Action Module.

Provides throttle and debounce
for function call rate control.
"""

import time
import threading
from typing import Any, Callable, Dict
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class ThrottleState:
    """Throttle state."""
    last_call: float = 0
    last_result: Any = None
    locked: bool = False


class ThrottleDebounceManager:
    """Manages throttle and debounce."""

    def __init__(self):
        self._throttle_states: Dict[str, ThrottleState] = {}
        self._debounce_timers: Dict[str, threading.Timer] = {}
        self._lock = threading.RLock()

    def throttle(
        self,
        key: str,
        func: Callable,
        interval: float,
        *args,
        **kwargs
    ) -> Any:
        """Throttle function calls."""
        with self._lock:
            if key not in self._throttle_states:
                self._throttle_states[key] = ThrottleState()

            state = self._throttle_states[key]
            now = time.time()

            if now - state.last_call >= interval:
                state.last_call = now
                try:
                    state.last_result = func(*args, **kwargs)
                except Exception as e:
                    state.last_result = None
                    raise e

            return state.last_result

    def debounce(
        self,
        key: str,
        func: Callable,
        delay: float,
        *args,
        **kwargs
    ) -> None:
        """Debounce function calls."""
        with self._lock:
            if key in self._debounce_timers:
                self._debounce_timers[key].cancel()

            timer = threading.Timer(delay, self._execute_debounce, [key, func, args, kwargs])
            self._debounce_timers[key] = timer
            timer.start()

    def _execute_debounce(
        self,
        key: str,
        func: Callable,
        args: tuple,
        kwargs: dict
    ) -> None:
        """Execute debounced function."""
        with self._lock:
            if key in self._debounce_timers:
                del self._debounce_timers[key]

        try:
            func(*args, **kwargs)
        except Exception:
            pass


class ThrottleDebounceAction(BaseAction):
    """Action for throttle/debounce operations."""

    def __init__(self):
        super().__init__("throttle_debounce")
        self._manager = ThrottleDebounceManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute throttle/debounce action."""
        try:
            operation = params.get("operation", "throttle")

            if operation == "throttle":
                return self._throttle(params)
            elif operation == "debounce":
                return self._debounce(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _throttle(self, params: Dict) -> ActionResult:
        """Throttle function."""
        def identity(x):
            return x

        result = self._manager.throttle(
            key=params.get("key", ""),
            func=params.get("func") or identity,
            interval=params.get("interval", 1.0)
        )
        return ActionResult(success=True, data={"result": result})

    def _debounce(self, params: Dict) -> ActionResult:
        """Debounce function."""
        def noop():
            pass

        self._manager.debounce(
            key=params.get("key", ""),
            func=params.get("func") or noop,
            delay=params.get("delay", 0.5)
        )
        return ActionResult(success=True)
