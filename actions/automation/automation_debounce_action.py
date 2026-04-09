"""
Automation Debounce Action Module.

Debouncing utilities for automation to coalesce rapid events
and prevent excessive task executions.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class DebounceStats:
    """Debounce statistics."""
    total_invocations: int = 0
    debounced_invocations: int = 0
    executed_invocations: int = 0
    last_execution_time: float = 0.0


class AutomationDebounceAction:
    """
    Debounce mechanism for automation events.

    Coalesces rapid successive calls into a single execution,
    waiting for a quiet period before triggering.

    Example:
        debouncer = AutomationDebounceAction(wait_ms=500)

        async def my_task(data):
            print(f"Executing with {data}")

        # Multiple rapid calls only trigger one execution
        debouncer.call(my_task, "data1")
        debouncer.call(my_task, "data2")
        debouncer.call(my_task, "data3")
        await asyncio.sleep(0.6)  # Wait for debounce window
    """

    def __init__(
        self,
        wait_ms: float = 300.0,
        leading: bool = False,
        trailing: bool = True,
    ) -> None:
        self.wait_ms = wait_ms
        self.leading = leading
        self.trailing = trailing
        self._pending_call: Optional[asyncio.Task] = None
        self._debounce_timer: Optional[asyncio.Task] = None
        self._last_call_time: float = 0.0
        self._stats = DebounceStats()
        self._pending_args: tuple = ()
        self._pending_kwargs: Dict[str, Any] = {}
        self._pending_func: Optional[Callable] = None

    def call(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Schedule a debounced call."""
        self._stats.total_invocations += 1
        self._pending_func = func
        self._pending_args = args
        self._pending_kwargs = kwargs

        if self.leading and self._debounce_timer is None:
            # Execute immediately on leading edge
            self._execute_now()
            return

        # Cancel existing timer
        if self._debounce_timer is not None:
            self._debounce_timer.cancel()
            self._debounce_timer = None

        # Schedule trailing execution
        loop = asyncio.get_event_loop()
        self._debounce_timer = loop.create_task(self._debounce_wait())

    async def _debounce_wait(self) -> None:
        """Wait for debounce period then execute."""
        try:
            await asyncio.sleep(self.wait_ms / 1000.0)
            self._execute_now()
        except asyncio.CancelledError:
            pass

    def _execute_now(self) -> None:
        """Execute the pending call immediately."""
        if not self._pending_func:
            return

        self._last_call_time = time.time()
        self._stats.executed_invocations += 1
        self._stats.last_execution_time = self._last_call_time

        try:
            if asyncio.iscoroutinefunction(self._pending_func):
                loop = asyncio.get_event_loop()
                loop.create_task(
                    self._pending_func(*self._pending_args, **self._pending_kwargs)
                )
            else:
                self._pending_func(*self._pending_args, **self._pending_kwargs)

        except Exception as e:
            logger.error(f"Debounced execution failed: {e}")

        finally:
            self._pending_func = None
            self._pending_args = ()
            self._pending_kwargs = {}
            self._debounce_timer = None

    def cancel(self) -> None:
        """Cancel any pending debounced call."""
        if self._debounce_timer:
            self._debounce_timer.cancel()
            self._debounce_timer = None
        self._pending_func = None
        self._pending_args = ()
        self._pending_kwargs = {}

    def flush(self) -> None:
        """Execute any pending call immediately."""
        if self._pending_func:
            self._execute_now()

    def get_stats(self) -> DebounceStats:
        """Get debounce statistics."""
        return self._stats


class MultiDebouncer:
    """
    Manages multiple debounced functions with shared or individual timers.
    """

    def __init__(self, wait_ms: float = 300.0) -> None:
        self.wait_ms = wait_ms
        self._debouncers: Dict[str, AutomationDebounceAction] = {}

    def get_debouncer(
        self,
        key: str,
        wait_ms: Optional[float] = None,
    ) -> AutomationDebounceAction:
        """Get or create a debouncer for a key."""
        if key not in self._debouncers:
            self._debouncers[key] = AutomationDebounceAction(
                wait_ms=wait_ms or self.wait_ms
            )
        return self._debouncers[key]

    def call(
        self,
        key: str,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Call a debounced function by key."""
        debouncer = self.get_debouncer(key)
        debouncer.call(func, *args, **kwargs)

    def flush_all(self) -> None:
        """Flush all pending debounced calls."""
        for debouncer in self._debouncers.values():
            debouncer.flush()

    def cancel_all(self) -> None:
        """Cancel all pending calls."""
        for debouncer in self._debouncers.values():
            debouncer.cancel()


def debounce(wait_ms: float = 300.0) -> Callable:
    """
    Decorator to debounce a function.

    Example:
        @debounce(wait_ms=500)
        async def my_function(data):
            await process(data)
    """
    def decorator(func: Callable[..., T]) -> Callable[..., None]:
        debouncer = AutomationDebounceAction(wait_ms=wait_ms)

        def wrapper(*args: Any, **kwargs: Any) -> None:
            debouncer.call(func, *args, **kwargs)

        return wrapper

    return decorator
