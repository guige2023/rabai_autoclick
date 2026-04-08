"""
Automation Timeout Action Module.

Timeout management for automation tasks with
graceful cancellation, timeout callbacks, and deadline tracking.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from dataclasses import dataclass, field
import logging
import asyncio
import time
from enum import Enum

logger = logging.getLogger(__name__)


class TimeoutStrategy(Enum):
    """What to do when timeout occurs."""
    CANCEL = "cancel"
    RAISE = "raise"
    CONTINUE = "continue"
    CALLBACK = "callback"


@dataclass
class TimeoutConfig:
    """Configuration for a timeout."""
    duration: float
    strategy: TimeoutStrategy = TimeoutStrategy.RAISE
    callback: Optional[Callable[[], None]] = None
    name: str = ""


@dataclass
class TimeoutResult:
    """Result of a timed operation."""
    completed: bool
    value: Any = None
    error: Optional[str] = None
    elapsed_seconds: float = 0.0
    timed_out: bool = False


class AutomationTimeoutAction:
    """
    Timeout management for automation tasks.

    Wraps async/sync functions with timeout enforcement,
    supports callbacks and graceful cancellation.

    Example:
        timeout_mgr = AutomationTimeoutAction()
        result = await timeout_mgr.run(my_async_func, timeout=30.0)
    """

    def __init__(
        self,
        default_timeout: float = 60.0,
        default_strategy: TimeoutStrategy = TimeoutStrategy.RAISE,
    ) -> None:
        self.default_timeout = default_timeout
        self.default_strategy = default_strategy
        self._active_timers: dict[str, asyncio.Task] = {}

    async def run(
        self,
        func: Callable[..., Any],
        *args: Any,
        timeout: Optional[float] = None,
        strategy: Optional[TimeoutStrategy] = None,
        callback: Optional[Callable[[], None]] = None,
        task_id: Optional[str] = None,
        **kwargs: Any,
    ) -> TimeoutResult:
        """Run a function with timeout protection."""
        duration = timeout or self.default_timeout
        strat = strategy or self.default_strategy
        tid = task_id or f"timeout_{time.time()}"

        start_time = time.perf_counter()

        try:
            if asyncio.iscoroutinefunction(func):
                result = await asyncio.wait_for(
                    func(*args, **kwargs),
                    timeout=duration,
                )
            else:
                result = await asyncio.wait_for(
                    asyncio.to_thread(func, *args, **kwargs),
                    timeout=duration,
                )

            elapsed = time.perf_counter() - start_time

            return TimeoutResult(
                completed=True,
                value=result,
                elapsed_seconds=elapsed,
            )

        except asyncio.TimeoutError:
            elapsed = time.perf_counter() - start_time

            if callback:
                try:
                    callback()
                except Exception as e:
                    logger.error("Timeout callback failed: %s", e)

            if strat == TimeoutStrategy.RAISE:
                return TimeoutResult(
                    completed=False,
                    error=f"Operation timed out after {duration}s",
                    elapsed_seconds=elapsed,
                    timed_out=True,
                )

            elif strat == TimeoutStrategy.CONTINUE:
                return TimeoutResult(
                    completed=False,
                    error=f"Timeout exceeded ({duration}s), continuing...",
                    elapsed_seconds=elapsed,
                    timed_out=True,
                )

            elif strat == TimeoutStrategy.CANCEL:
                if tid in self._active_timers:
                    self._active_timers[tid].cancel()
                return TimeoutResult(
                    completed=False,
                    error="Task cancelled due to timeout",
                    elapsed_seconds=elapsed,
                    timed_out=True,
                )

            return TimeoutResult(
                completed=False,
                elapsed_seconds=elapsed,
                timed_out=True,
            )

    def run_sync(
        self,
        func: Callable[..., Any],
        *args: Any,
        timeout: Optional[float] = None,
        **kwargs: Any,
    ) -> TimeoutResult:
        """Run a synchronous function with timeout."""
        duration = timeout or self.default_timeout
        start_time = time.perf_counter()

        try:
            import threading
            result_container = []
            error_container = []

            def target():
                try:
                    result_container.append(func(*args, **kwargs))
                except Exception as e:
                    error_container.append(e)

            thread = threading.Thread(target=target)
            thread.start()
            thread.join(timeout=duration)

            elapsed = time.perf_counter() - start_time

            if thread.is_alive():
                return TimeoutResult(
                    completed=False,
                    error=f"Operation timed out after {duration}s",
                    elapsed_seconds=elapsed,
                    timed_out=True,
                )

            if error_container:
                return TimeoutResult(
                    completed=False,
                    error=str(error_container[0]),
                    elapsed_seconds=elapsed,
                )

            return TimeoutResult(
                completed=True,
                value=result_container[0] if result_container else None,
                elapsed_seconds=elapsed,
            )

        except Exception as e:
            elapsed = time.perf_counter() - start_time
            return TimeoutResult(
                completed=False,
                error=str(e),
                elapsed_seconds=elapsed,
            )
