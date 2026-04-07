"""Timer utilities for RabAI AutoClick.

Provides:
- Timer: Context manager for timing code blocks
- IntervalTimer: Periodic task execution
- Debouncer: Debounce rapid calls
- Throttler: Rate-limit function calls
"""

import asyncio
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, TypeVar


T = TypeVar("T")


@dataclass
class TimingResult:
    """Result of a timed operation."""
    duration: float
    value: Any = None


class Timer:
    """Context manager for timing code execution.

    Usage:
        with Timer() as t:
            # code to time
            pass
        print(f"Elapsed: {t.duration}")
    """

    def __init__(self) -> None:
        self.start_time: float = 0
        self.end_time: float = 0
        self._duration: float = 0

    def __enter__(self) -> 'Timer':
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, *args: Any) -> None:
        self.end_time = time.perf_counter()
        self._duration = self.end_time - self.start_time

    @property
    def duration(self) -> float:
        """Get elapsed time in seconds."""
        if self.end_time == 0:
            return time.perf_counter() - self.start_time
        return self._duration


def timed(func: Callable[..., T]) -> Callable[..., T]:
    """Decorator to time function execution.

    Returns tuple of (duration, result) if return_result is True.
    """
    def wrapper(*args: Any, **kwargs: Any) -> T:
        start = time.perf_counter()
        result = func(*args, **kwargs)
        duration = time.perf_counter() - start
        # Attach duration to result if it's a tuple
        if isinstance(result, tuple):
            result = (*result, duration) if len(result) < 3 else result
        return result

    return wrapper


class IntervalTimer:
    """Timer that executes a callback at regular intervals.

    Usage:
        def my_task():
            print("Running")

        timer = IntervalTimer(1.0, my_task)  # Every 1 second
        timer.start()
        timer.stop()
    """

    def __init__(
        self,
        interval: float,
        callback: Callable[[], None],
        name: Optional[str] = None,
    ) -> None:
        """Initialize interval timer.

        Args:
            interval: Seconds between executions.
            callback: Function to call.
            name: Optional name for the timer.
        """
        self.interval = interval
        self.callback = callback
        self.name = name or f"IntervalTimer-{id(self)}"
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        """Start the timer."""
        if self._running:
            return

        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the timer."""
        if not self._running:
            return

        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5.0)

    def _run(self) -> None:
        """Main timer loop."""
        while not self._stop_event.is_set():
            self._stop_event.wait(self.interval)
            if self._stop_event.is_set():
                break
            try:
                self.callback()
            except Exception:
                pass

    @property
    def is_running(self) -> bool:
        """Check if timer is running."""
        return self._running


class AsyncIntervalTimer:
    """Async version of IntervalTimer.

    Usage:
        async def my_task():
            print("Running")

        timer = AsyncIntervalTimer(1.0, my_task)
        await timer.start()
        await timer.stop()
    """

    def __init__(
        self,
        interval: float,
        callback: Callable[[], Any],
        name: Optional[str] = None,
    ) -> None:
        """Initialize async interval timer.

        Args:
            interval: Seconds between executions.
            callback: Async function to call.
            name: Optional name for the timer.
        """
        self.interval = interval
        self.callback = callback
        self.name = name or f"AsyncIntervalTimer-{id(self)}"
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start the timer."""
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Stop the timer."""
        if not self._running:
            return

        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        """Main timer loop."""
        while self._running:
            try:
                await self.callback()
            except Exception:
                pass
            await asyncio.sleep(self.interval)

    @property
    def is_running(self) -> bool:
        """Check if timer is running."""
        return self._running


class Debouncer:
    """Debounce rapid calls to a function.

    Only executes the function after calls stop for a specified delay.

    Usage:
        @Debouncer(0.5).debounce
        def my_func():
            print("Executed")
    """

    def __init__(self, delay: float, timer_factory: Callable[[], Any] = threading.Timer) -> None:
        """Initialize debouncer.

        Args:
            delay: Seconds to wait before executing.
            timer_factory: Factory for creating timers.
        """
        self.delay = delay
        self.timer_factory = timer_factory
        self._timer: Optional[Any] = None
        self._lock = threading.Lock()

    def debounce(self, func: Callable[..., T]) -> Callable[..., Optional[T]]:
        """Decorator to debounce function calls."""
        def wrapper(*args: Any, **kwargs: Any) -> Optional[T]:
            with self._lock:
                if self._timer:
                    self._timer.cancel()

                def call():
                    func(*args, **kwargs)

                self._timer = self.timer_factory(self.delay, call)

            return None  # Debounced functions don't return immediate values

        return wrapper

    def execute(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> None:
        """Immediately execute after canceling pending debounced calls.

        Args:
            func: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        """
        with self._lock:
            if self._timer:
                self._timer.cancel()
                self._timer = None
            func(*args, **kwargs)


class Throttler:
    """Throttle function calls to a maximum rate.

    Usage:
        @Throttler(10).throttle  # Max 10 calls per second
        def my_func():
            print("Called")
    """

    def __init__(self, rate: float) -> None:
        """Initialize throttler.

        Args:
            rate: Maximum calls per second.
        """
        self.rate = rate
        self.min_interval = 1.0 / rate
        self._last_call = 0.0
        self._lock = threading.Lock()

    def throttle(self, func: Callable[..., T]) -> Callable[..., Optional[T]]:
        """Decorator to throttle function calls."""
        def wrapper(*args: Any, **kwargs: Any) -> Optional[T]:
            with self._lock:
                now = time.perf_counter()
                elapsed = now - self._last_call

                if elapsed < self.min_interval:
                    time.sleep(self.min_interval - elapsed)

                self._last_call = time.perf_counter()
                return func(*args, **kwargs)

        return wrapper

    def __call__(self, func: Callable[..., T]) -> Callable[..., Optional[T]]:
        """Allow using throttler as a decorator."""
        return self.throttle(func)


@dataclass
class PerformanceTimer:
    """High-resolution performance timer with lap support."""
    _start: float = field(default_factory=time.perf_counter)
    _laps: list = field(default_factory=list)

    def lap(self) -> float:
        """Record a lap time and return elapsed since last lap (or start)."""
        now = time.perf_counter()
        if not self._laps:
            lap_time = now - self._start
        else:
            lap_time = now - self._laps[-1]
        self._laps.append(now)
        return lap_time

    @property
    def elapsed(self) -> float:
        """Get total elapsed time."""
        return time.perf_counter() - self._start

    @property
    def laps(self) -> list:
        """Get list of lap times."""
        return self._laps.copy()

    def reset(self) -> None:
        """Reset the timer."""
        self._start = time.perf_counter()
        self._laps.clear()