"""Throttle action module for RabAI AutoClick.

Provides throttling utilities:
- Throttler: Function throttling
- RateThrottler: Rate limiting throttle
- Debouncer: Debounce rapid calls
"""

from typing import Any, Callable, Dict, List, Optional
import threading
import time
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class Throttler:
    """Function throttler."""

    def __init__(self, min_interval: float = 1.0):
        self.min_interval = min_interval
        self._last_call = 0.0
        self._lock = threading.Lock()
        self._call_count = 0
        self._total_calls = 0

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Call function with throttling."""
        with self._lock:
            now = time.time()
            elapsed = now - self._last_call

            if elapsed < self.min_interval:
                wait_time = self.min_interval - elapsed
                time.sleep(wait_time)

            self._last_call = time.time()
            self._call_count += 1
            self._total_calls += 1

        return func(*args, **kwargs)

    def can_call(self) -> bool:
        """Check if can call now."""
        with self._lock:
            elapsed = time.time() - self._last_call
            return elapsed >= self.min_interval

    def get_stats(self) -> Dict[str, Any]:
        """Get throttle stats."""
        with self._lock:
            return {
                "call_count": self._call_count,
                "total_calls": self._total_calls,
                "min_interval": self.min_interval,
                "last_call": self._last_call,
            }

    def reset(self) -> None:
        """Reset throttler."""
        with self._lock:
            self._last_call = 0.0
            self._call_count = 0


class RateThrottler:
    """Rate limiting throttler."""

    def __init__(self, max_calls: int, time_window: float = 1.0):
        self.max_calls = max_calls
        self.time_window = time_window
        self._calls: List[float] = []
        self._lock = threading.Lock()
        self._total_calls = 0

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Call function with rate limiting."""
        with self._lock:
            now = time.time()
            self._cleanup(now)

            if len(self._calls) >= self.max_calls:
                sleep_time = self._calls[0] + self.time_window - now
                if sleep_time > 0:
                    time.sleep(sleep_time)
                now = time.time()
                self._cleanup(now)

            self._calls.append(now)
            self._total_calls += 1

        return func(*args, **kwargs)

    def can_call(self) -> bool:
        """Check if can call now."""
        with self._lock:
            self._cleanup(time.time())
            return len(self._calls) < self.max_calls

    def _cleanup(self, now: float) -> None:
        """Clean up old calls."""
        cutoff = now - self.time_window
        self._calls = [c for c in self._calls if c > cutoff]

    def get_remaining(self) -> int:
        """Get remaining calls."""
        with self._lock:
            self._cleanup(time.time())
            return self.max_calls - len(self._calls)

    def get_stats(self) -> Dict[str, Any]:
        """Get throttle stats."""
        with self._lock:
            self._cleanup(time.time())
            return {
                "remaining": self.max_calls - len(self._calls),
                "used": len(self._calls),
                "max_calls": self.max_calls,
                "time_window": self.time_window,
                "total_calls": self._total_calls,
            }


class Debouncer:
    """Debounce rapid calls."""

    def __init__(self, delay: float = 0.5):
        self.delay = delay
        self._pending_call: Optional[threading.Event] = None
        self._lock = threading.Lock()
        self._last_call_time = 0.0

    def call(self, func: Callable, *args, **kwargs) -> None:
        """Debounce function call."""
        with self._lock:
            if self._pending_call is not None:
                self._pending_call.set()

            self._pending_call = threading.Event()

        def delayed():
            self._pending_call.wait(self.delay)
            func(*args, **kwargs)

        thread = threading.Thread(target=delayed, daemon=True)
        thread.start()

    def flush(self) -> None:
        """Flush pending call."""
        with self._lock:
            if self._pending_call is not None:
                self._pending_call.set()


class ThrottleAction(BaseAction):
    """Throttle action."""
    action_type = "throttle"
    display_name = "节流器"
    description = "函数节流控制"

    def __init__(self):
        super().__init__()
        self._throttlers: Dict[str, Throttler] = {}
        self._rate_throttlers: Dict[str, RateThrottler] = {}
        self._debouncers: Dict[str, Debouncer] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "call")

            if operation == "call":
                return self._call(params)
            elif operation == "create":
                return self._create_throttler(params)
            elif operation == "create_rate":
                return self._create_rate_throttler(params)
            elif operation == "can_call":
                return self._can_call(params)
            elif operation == "stats":
                return self._stats(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Throttle error: {str(e)}")

    def _create_throttler(self, params: Dict[str, Any]) -> ActionResult:
        """Create a throttler."""
        name = params.get("name", str(uuid.uuid4()))
        interval = params.get("interval", 1.0)

        throttler = Throttler(min_interval=interval)
        self._throttlers[name] = throttler

        return ActionResult(success=True, message=f"Throttler created: {name}", data={"name": name})

    def _create_rate_throttler(self, params: Dict[str, Any]) -> ActionResult:
        """Create a rate throttler."""
        name = params.get("name", str(uuid.uuid4()))
        max_calls = params.get("max_calls", 10)
        time_window = params.get("time_window", 1.0)

        throttler = RateThrottler(max_calls, time_window)
        self._rate_throttlers[name] = throttler

        return ActionResult(success=True, message=f"Rate throttler created: {name}", data={"name": name})

    def _call(self, params: Dict[str, Any]) -> ActionResult:
        """Call throttled function."""
        name = params.get("name")
        throttle_type = params.get("type", "throttler")

        if throttle_type == "throttler":
            if name not in self._throttlers:
                return ActionResult(success=False, message=f"Throttler not found: {name}")
            throttler = self._throttlers[name]

            def dummy_func():
                pass

            throttler.call(dummy_func)
            return ActionResult(success=True, message=f"Throttled call: {name}")

        elif throttle_type == "rate":
            if name not in self._rate_throttlers:
                return ActionResult(success=False, message=f"Rate throttler not found: {name}")
            throttler = self._rate_throttlers[name]

            def dummy_func():
                pass

            throttler.call(dummy_func)
            return ActionResult(success=True, message=f"Rate throttled call: {name}")

        return ActionResult(success=False, message=f"Unknown throttle type: {throttle_type}")

    def _can_call(self, params: Dict[str, Any]) -> ActionResult:
        """Check if can call."""
        name = params.get("name")
        throttle_type = params.get("type", "throttler")

        if throttle_type == "throttler":
            if name not in self._throttlers:
                return ActionResult(success=False, message=f"Throttler not found: {name}")
            can = self._throttlers[name].can_call()
        elif throttle_type == "rate":
            if name not in self._rate_throttlers:
                return ActionResult(success=False, message=f"Rate throttler not found: {name}")
            can = self._rate_throttlers[name].can_call()
        else:
            return ActionResult(success=False, message=f"Unknown throttle type: {throttle_type}")

        return ActionResult(success=True, message="Can call" if can else "Cannot call", data={"can_call": can})

    def _stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get stats."""
        name = params.get("name")
        throttle_type = params.get("type", "throttler")

        if throttle_type == "throttler":
            if name not in self._throttlers:
                return ActionResult(success=False, message=f"Throttler not found: {name}")
            stats = self._throttlers[name].get_stats()
        elif throttle_type == "rate":
            if name not in self._rate_throttlers:
                return ActionResult(success=False, message=f"Rate throttler not found: {name}")
            stats = self._rate_throttlers[name].get_stats()
        else:
            return ActionResult(success=False, message=f"Unknown throttle type: {throttle_type}")

        return ActionResult(success=True, message="Stats retrieved", data={"name": name, "stats": stats})
