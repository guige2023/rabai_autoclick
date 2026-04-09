"""Timing and clock utilities for automation workflows.

Provides high-precision timing, rate limiting, throttling,
clock sources, and temporal utilities.
"""

from __future__ import annotations

from typing import Optional, Callable, List, Any, Dict, Tuple
from dataclasses import dataclass, field
from enum import Enum, auto
import time
import threading
import math


class ClockSource(Enum):
    """Clock source types."""
    MONOTONIC = auto()
    MONOTONIC_RAW = auto()
    REALTIME = auto()
    THREAD_TIME = auto()
    PROCESS_TIME = auto()


@dataclass
class Timestamp:
    """High-precision timestamp with metadata."""
    value: float
    source: ClockSource = ClockSource.MONOTONIC
    source_value: Optional[float] = field(default=None, repr=False)

    @property
    def unix(self) -> float:
        """Get Unix timestamp."""
        return self.value

    @classmethod
    def now(cls, source: ClockSource = ClockSource.MONOTONIC) -> Timestamp:
        """Create timestamp for current time."""
        return cls(value=cls._get_time(source), source=source)

    @staticmethod
    def _get_time(source: ClockSource) -> float:
        """Get time from specified source."""
        if source == ClockSource.MONOTONIC:
            return time.monotonic()
        elif source == ClockSource.MONOTONIC_RAW:
            try:
                return time.monotonic_raw()
            except AttributeError:
                return time.monotonic()
        elif source == ClockSource.REALTIME:
            return time.time()
        elif source == ClockSource.THREAD_TIME:
            return time.thread_time()
        elif source == ClockSource.PROCESS_TIME:
            return time.process_time()
        return time.monotonic()

    def elapsed_since(self, other: Timestamp) -> float:
        """Get elapsed time between this and another timestamp."""
        return abs(self.value - other.value)

    def __sub__(self, other: Timestamp) -> float:
        return self.value - other.value

    def __add__(self, seconds: float) -> Timestamp:
        return Timestamp(value=self.value + seconds, source=self.source)

    def __repr__(self) -> str:
        return f"Timestamp({self.value:.6f}, {self.source.name})"


class RateLimiter:
    """Token bucket rate limiter.

    Example:
        limiter = RateLimiter(rate=10.0, capacity=20)
        if limiter.try_acquire():
            do_action()
        else:
            print("Rate limited")
    """

    def __init__(
        self,
        rate: float,
        capacity: Optional[float] = None,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        """
        Args:
            rate: Tokens added per second.
            capacity: Maximum tokens (defaults to rate).
            clock: Custom clock function.
        """
        if rate <= 0:
            raise ValueError("Rate must be positive")
        self._rate = rate
        self._capacity = capacity if capacity is not None else rate
        self._clock = clock or time.monotonic
        self._tokens = float(self._capacity)
        self._last_update = self._clock()
        self._lock = threading.Lock()

    def try_acquire(self, tokens: float = 1.0) -> bool:
        """Try to acquire tokens. Returns True if successful."""
        with self._lock:
            now = self._clock()
            elapsed = now - self._last_update
            self._tokens = min(
                self._capacity,
                self._tokens + elapsed * self._rate
            )
            self._last_update = now
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def acquire(self, tokens: float = 1.0, timeout: Optional[float] = None) -> bool:
        """Acquire tokens, waiting if necessary.

        Returns:
            True if acquired within timeout, False otherwise.
        """
        start = self._clock()
        while True:
            if self.try_acquire(tokens):
                return True
            if timeout is not None:
                elapsed = self._clock() - start
                if elapsed >= timeout:
                    return False
            sleep_time = 1.0 / (self._rate * 2)
            if timeout is not None:
                sleep_time = min(sleep_time, timeout - (self._clock() - start))
            time.sleep(max(0.001, sleep_time))

    @property
    def available_tokens(self) -> float:
        """Get current available tokens."""
        with self._lock:
            now = self._clock()
            elapsed = now - self._last_update
            return min(
                self._capacity,
                self._tokens + elapsed * self._rate
            )


class Throttler:
    """Throttler that limits execution rate.

    Example:
        throttler = Throttler(calls_per_second=5.0)
        while True:
            throttler.throttle()
            do_periodic_work()
    """

    def __init__(
        self,
        calls_per_second: float,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        if calls_per_second <= 0:
            raise ValueError("calls_per_second must be positive")
        self._min_interval = 1.0 / calls_per_second
        self._clock = clock or time.monotonic
        self._last_call = 0.0
        self._lock = threading.Lock()

    def throttle(self) -> None:
        """Wait until throttle allows execution."""
        with self._lock:
            now = self._clock()
            elapsed = now - self._last_call
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_call = self._clock()

    def try_throttle(self) -> bool:
        """Try to throttle. Returns True if execution is allowed now."""
        with self._lock:
            now = self._clock()
            elapsed = now - self._last_call
            if elapsed >= self._min_interval:
                self._last_call = now
                return True
            return False

    @property
    def next_allowed_time(self) -> float:
        """Get time when next call is allowed."""
        with self._lock:
            return self._last_call + self._min_interval

    def reset(self) -> None:
        """Reset throttler state."""
        with self._lock:
            self._last_call = 0.0


class Debouncer:
    """Debouncer that delays execution until after quiet period.

    Example:
        debouncer = Debouncer(delay_seconds=0.5)
        def on_change():
            debouncer.debounce(lambda: print("Changed!"))
        # Multiple rapid calls only trigger once after 0.5s of silence
    """

    def __init__(
        self,
        delay_seconds: float,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        if delay_seconds < 0:
            raise ValueError("delay_seconds must be non-negative")
        self._delay = delay_seconds
        self._clock = clock or time.monotonic
        self._pending_call: Optional[Tuple[float, Callable[[], None]]] = None
        self._lock = threading.Lock()

    def debounce(self, func: Callable[[], None]) -> None:
        """Schedule function to be called after delay if no new calls."""
        with self._lock:
            deadline = self._clock() + self._delay
            self._pending_call = (deadline, func)

    def flush(self) -> None:
        """Immediately execute pending call if any."""
        with self._lock:
            if self._pending_call is not None:
                deadline, func = self._pending_call
                self._pending_call = None
                func()

    def cancel(self) -> None:
        """Cancel pending call."""
        with self._lock:
            self._pending_call = None

    def tick(self) -> bool:
        """Check and execute if delay has passed. Returns True if executed."""
        with self._lock:
            if self._pending_call is None:
                return False
            deadline, func = self._pending_call
            if self._clock() >= deadline:
                self._pending_call = None
                func()
                return True
            return False


class Timer:
    """High-precision timer for measuring durations.

    Example:
        timer = Timer()
        do_work()
        elapsed = timer.elapsed()  # Returns seconds
    """

    def __init__(self, auto_start: bool = True, source: ClockSource = ClockSource.MONOTONIC) -> None:
        self._source = source
        self._start_time: Optional[float] = None
        self._stop_time: Optional[float] = None
        if auto_start:
            self.start()

    def start(self) -> Timer:
        """Start the timer."""
        self._start_time = Timestamp._get_time(self._source)
        self._stop_time = None
        return self

    def stop(self) -> float:
        """Stop the timer and return elapsed time."""
        if self._start_time is None:
            raise RuntimeError("Timer not started")
        self._stop_time = Timestamp._get_time(self._source)
        return self.elapsed()

    def elapsed(self) -> float:
        """Get elapsed time in seconds."""
        if self._start_time is None:
            return 0.0
        end = self._stop_time if self._stop_time else Timestamp._get_time(self._source)
        return end - self._start_time

    def reset(self) -> None:
        """Reset the timer."""
        self._start_time = None
        self._stop_time = None

    @property
    def is_running(self) -> bool:
        """Check if timer is running."""
        return self._start_time is not None and self._stop_time is None

    def __enter__(self) -> Timer:
        self.start()
        return self

    def __exit__(self, *args: Any) -> None:
        self.stop()


class IntervalTimer:
    """Timer that fires callback at fixed intervals.

    Example:
        def on_tick():
            print("Tick!")

        timer = IntervalTimer(interval_seconds=1.0, callback=on_tick)
        timer.start()
        time.sleep(5)
        timer.stop()
    """

    def __init__(
        self,
        interval_seconds: float,
        callback: Callable[[], None],
        oneshot: bool = False,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self._interval = interval_seconds
        self._callback = callback
        self._oneshot = oneshot
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start the interval timer."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()

    def stop(self) -> None:
        """Stop the interval timer."""
        with self._lock:
            self._running = False
        if self._thread is not None:
            self._thread.join(timeout=1.0)
            self._thread = None

    def _run(self) -> None:
        """Timer thread loop."""
        next_time = time.monotonic() + self._interval
        while True:
            sleep_time = next_time - time.monotonic()
            if sleep_time > 0:
                time.sleep(sleep_time)
            with self._lock:
                if not self._running:
                    break
            try:
                self._callback()
            except Exception:
                pass
            if self._oneshot:
                break
            next_time += self._interval

    @property
    def is_running(self) -> bool:
        """Check if timer is running."""
        return self._running


@dataclass
class TimeWindow:
    """Fixed-size time window for tracking events."""
    window_seconds: float
    _events: List[float] = field(default_factory=list)

    def record(self, timestamp: Optional[float] = None) -> None:
        """Record an event at current time or specified timestamp."""
        ts = timestamp if timestamp is not None else time.monotonic()
        self._events.append(ts)
        self._prune(ts)

    def _prune(self, now: float) -> None:
        """Remove events outside the window."""
        cutoff = now - self.window_seconds
        self._events = [e for e in self._events if e >= cutoff]

    def count(self) -> int:
        """Get count of events in window."""
        self._prune(time.monotonic())
        return len(self._events)

    def rate(self) -> float:
        """Get events per second in window."""
        self._prune(time.monotonic())
        if not self._events:
            return 0.0
        if len(self._events) < 2:
            return 0.0
        duration = self._events[-1] - self._events[0]
        if duration <= 0:
            return 0.0
        return (len(self._events) - 1) / duration
