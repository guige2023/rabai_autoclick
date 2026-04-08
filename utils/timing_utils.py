"""Timing and precision delay utilities for automation workflows.

Provides high-resolution timing, jitter-controllable delays,
rate-limited action execution, and timing-related context managers
for instrumentation and benchmarking of automation tasks.

Example:
    >>> from utils.timing_utils import precise_delay, RateLimiter, Timer
    >>> precise_delay(0.1)  # 100ms with sub-ms precision
    >>> limiter = RateLimiter(max_calls=10, period=1.0)
    >>> limiter.throttle(lambda: click(100, 200))
"""

from __future__ import annotations

import time
import threading
from typing import Callable, Optional
from dataclasses import dataclass, field

__all__ = [
    "precise_delay",
    "Timer",
    "RateLimiter",
    "JitterDelay",
    "Timeout",
    "measure",
    "timestamp",
]


def precise_delay(seconds: float, busy_wait: bool = True) -> None:
    """Delay for a precise duration with optional busy-wait.

    Uses a combination of sleep and busy-wait to achieve higher
    precision than standard time.sleep alone.

    Args:
        seconds: Duration to sleep in seconds. Supports sub-millisecond
            precision when busy_wait=True.
        busy_wait: If True, use busy-wait loop for the final portion
            to improve precision.
    """
    if seconds <= 0:
        return

    if busy_wait and seconds > 0.001:
        # Use sleep for the bulk of the delay
        sleep_time = seconds - 0.002
        if sleep_time > 0:
            time.sleep(sleep_time)
        # Busy-wait for the remaining 2ms
        deadline = time.perf_counter() + 0.002
        while time.perf_counter() < deadline:
            pass
    else:
        time.sleep(seconds)


def timestamp() -> float:
    """Return the current monotonic timestamp in seconds."""
    return time.monotonic()


@dataclass
class Timer:
    """Simple timer for measuring elapsed time.

    Example:
        >>> t = Timer()
        >>> t.start()
        >>> # ... do work ...
        >>> t.stop()
        >>> print(f"Elapsed: {t.elapsed:.3f}s")
    """

    _start: Optional[float] = field(default=None, repr=False)
    _stop: Optional[float] = field(default=None, repr=False)
    _running: bool = field(default=False, repr=False)

    def start(self) -> "Timer":
        self._start = time.perf_counter()
        self._stop = None
        self._running = True
        return self

    def stop(self) -> "Timer":
        if self._running:
            self._stop = time.perf_counter()
            self._running = False
        return self

    def reset(self) -> "Timer":
        self._start = None
        self._stop = None
        self._running = False
        return self

    @property
    def elapsed(self) -> float:
        """Elapsed time in seconds."""
        if self._start is None:
            return 0.0
        if self._running:
            return time.perf_counter() - self._start
        if self._stop is not None:
            return self._stop - self._start
        return 0.0

    def __enter__(self) -> "Timer":
        return self.start()

    def __exit__(self, *args) -> None:
        self.stop()

    @property
    def is_running(self) -> bool:
        return self._running


def measure(func: Callable, *args, **kwargs) -> tuple[float, any]:
    """Measure the execution time of a function call.

    Args:
        func: Callable to measure.
        *args: Positional arguments to pass to func.
        **kwargs: Keyword arguments to pass to func.

    Returns:
        Tuple of (elapsed_seconds, func_return_value).
    """
    t = Timer().start()
    result = func(*args, **kwargs)
    t.stop()
    return t.elapsed, result


class JitterDelay:
    """Delay with configurable random jitter for natural timing.

    Useful for making automation timing less predictable/robot-like.

    Example:
        >>> jitter = JitterDelay(base=0.5, jitter=0.1)
        >>> jitter.sleep()  # sleeps 0.45-0.55 seconds
    """

    def __init__(self, base: float = 1.0, jitter: float = 0.1):
        """Configure jitter delay.

        Args:
            base: Base delay in seconds.
            jitter: Maximum random deviation in seconds (added/subtracted).
        """
        self.base = base
        self.jitter = jitter

    def sleep(self) -> None:
        """Sleep for base ± random(jitter)."""
        import random

        deviation = random.uniform(-self.jitter, self.jitter)
        precise_delay(max(0, self.base + deviation), busy_wait=False)

    def delay_for(self, seconds: float, jitter_fraction: float = 0.1) -> None:
        """Delay for a specific duration with relative jitter.

        Args:
            seconds: Target delay.
            jitter_fraction: Jitter as a fraction of the delay.
        """
        import random

        jit = seconds * jitter_fraction
        deviation = random.uniform(-jit, jit)
        precise_delay(max(0, seconds + deviation), busy_wait=False)


class RateLimiter:
    """Token-bucket rate limiter for throttling action frequency.

    Example:
        >>> limiter = RateLimiter(max_calls=5, period=2.0)
        >>> for action in many_actions:
        ...     limiter.throttle(lambda: do_action(action))
    """

    def __init__(self, max_calls: int = 10, period: float = 1.0):
        """Configure the rate limiter.

        Args:
            max_calls: Maximum number of calls allowed per period.
            period: Time window in seconds.
        """
        self.max_calls = max_calls
        self.period = period
        self._lock = threading.Lock()
        self._timestamps: list[float] = []

    def throttle(self, func: Callable, *args, **kwargs) -> any:
        """Execute func, blocking if rate limit would be exceeded.

        Args:
            func: Callable to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            The return value of func.
        """
        self._wait_if_needed()
        result = func(*args, **kwargs)
        self._record()
        return result

    def _wait_if_needed(self) -> None:
        now = time.monotonic()
        with self._lock:
            cutoff = now - self.period
            self._timestamps = [t for t in self._timestamps if t > cutoff]
            if len(self._timestamps) >= self.max_calls:
                sleep_time = self._timestamps[0] - cutoff
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    now = time.monotonic()
                    cutoff = now - self.period
                    self._timestamps = [t for t in self._timestamps if t > cutoff]

    def _record(self) -> None:
        with self._lock:
            self._timestamps.append(time.monotonic())

    def try_acquire(self) -> bool:
        """Try to acquire a slot without blocking.

        Returns:
            True if a slot was available, False otherwise.
        """
        now = time.monotonic()
        with self._lock:
            cutoff = now - self.period
            self._timestamps = [t for t in self._timestamps if t > cutoff]
            if len(self._timestamps) < self.max_calls:
                self._timestamps.append(now)
                return True
            return False

    @property
    def available(self) -> int:
        """Number of available slots in the current window."""
        now = time.monotonic()
        with self._lock:
            cutoff = now - self.period
            active = sum(1 for t in self._timestamps if t > cutoff)
            return max(0, self.max_calls - active)


class Timeout:
    """Context manager for executing code with a timeout.

    Example:
        >>> with Timeout(5.0) as t:
        ...     result = wait_for_condition()
        ...     print(f"Completed in {t.elapsed:.2f}s")
    """

    def __init__(self, seconds: float):
        self.seconds = seconds
        self.started_at: Optional[float] = None
        self.ended_at: Optional[float] = None

    def __enter__(self) -> "Timeout":
        self.started_at = time.monotonic()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.ended_at = time.monotonic()
        return False  # Don't suppress exceptions

    @property
    def elapsed(self) -> float:
        if self.started_at is None:
            return 0.0
        end = self.ended_at if self.ended_at else time.monotonic()
        return end - self.started_at

    @property
    def remaining(self) -> float:
        if self.started_at is None:
            return self.seconds
        elapsed = self.elapsed
        return max(0, self.seconds - elapsed)
