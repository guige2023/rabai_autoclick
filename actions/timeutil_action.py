"""timeutil_action module for rabai_autoclick.

Provides time utilities: duration formatting, timezone conversion,
sleep utilities, periodic execution, and scheduling helpers.
"""

from __future__ import annotations

import time
import threading
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, List, Optional

__all__ = [
    "Duration",
    "Timer",
    "PeriodicTimer",
    "format_duration",
    "parse_duration",
    "sleep_until",
    "sleep_random",
    "format_timestamp",
    "parse_timestamp",
    "get_timezone_offset",
    "TimeUnit",
    "format_relative_time",
]


class TimeUnit:
    """Time unit constants."""
    SECOND = 1.0
    MINUTE = 60.0
    HOUR = 3600.0
    DAY = 86400.0
    WEEK = 604800.0
    MILLISECOND = 0.001
    MICROSECOND = 0.000001


@dataclass
class Duration:
    """Represents a duration of time."""
    seconds: float

    @classmethod
    def seconds(cls, value: float) -> "Duration":
        return cls(value)

    @classmethod
    def minutes(cls, value: float) -> "Duration":
        return cls(value * 60)

    @classmethod
    def hours(cls, value: float) -> "Duration":
        return cls(value * 3600)

    @classmethod
    def days(cls, value: float) -> "Duration":
        return cls(value * 86400)

    @classmethod
    def weeks(cls, value: float) -> "Duration":
        return cls(value * 604800)

    def __str__(self) -> str:
        return format_duration(self.seconds)

    def __add__(self, other: "Duration") -> "Duration":
        return Duration(self.seconds + other.seconds)

    def __mul__(self, factor: float) -> "Duration":
        return Duration(self.seconds * factor)


class Timer:
    """High-precision timer context manager."""

    def __init__(self) -> None:
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.elapsed: float = 0.0

    def __enter__(self) -> "Timer":
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, *args: Any) -> None:
        self.end_time = time.perf_counter()
        if self.start_time is not None:
            self.elapsed = self.end_time - self.start_time

    def reset(self) -> None:
        """Reset the timer."""
        self.start_time = None
        self.end_time = None
        self.elapsed = 0.0

    @property
    def running(self) -> bool:
        return self.start_time is not None and self.end_time is None

    def elapsed_ms(self) -> float:
        """Get elapsed time in milliseconds."""
        return self.elapsed * 1000

    def elapsed_us(self) -> float:
        """Get elapsed time in microseconds."""
        return self.elapsed * 1_000_000


class PeriodicTimer:
    """Periodic task executor."""

    def __init__(
        self,
        interval: float,
        func: Callable,
        args: tuple = (),
        daemon: bool = True,
    ) -> None:
        self.interval = interval
        self.func = func
        self.args = args
        self.daemon = daemon
        self._timer: Optional[threading.Timer] = None
        self._running = False
        self._skip_next = False
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start the periodic timer."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._schedule()

    def stop(self) -> None:
        """Stop the periodic timer."""
        with self._lock:
            self._running = False
            if self._timer:
                self._timer.cancel()
                self._timer = None

    def _schedule(self) -> None:
        """Schedule next execution."""
        if not self._running:
            return
        self._timer = threading.Timer(
            self.interval,
            self._run,
            daemon=self.daemon,
        )
        self._timer.start()

    def _run(self) -> None:
        """Execute the task."""
        if not self._running:
            return
        try:
            self.func(*self.args)
        except Exception:
            pass
        with self._lock:
            if self._running:
                self._schedule()

    def __enter__(self) -> "PeriodicTimer":
        self.start()
        return self

    def __exit__(self, *args: Any) -> None:
        self.stop()


def format_duration(seconds: float, precision: int = 2) -> str:
    """Format duration in human-readable form.

    Args:
        seconds: Duration in seconds.
        precision: Number of decimal places.

    Returns:
        Formatted string like "1h 23m 45s".
    """
    if seconds < 0:
        return f"-{format_duration(-seconds, precision)}"

    units = [
        ("w", 604800),
        ("d", 86400),
        ("h", 3600),
        ("m", 60),
        ("s", 1),
    ]

    parts = []
    remaining = seconds

    for unit, value in units:
        if remaining >= value:
            count = int(remaining // value)
            remaining -= count * value
            parts.append(f"{count}{unit}")

    if not parts:
        return f"{seconds:.{precision}f}s"

    return " ".join(parts)


def parse_duration(duration_str: str) -> float:
    """Parse duration string to seconds.

    Args:
        duration_str: String like "1h30m", "2.5h", "30s".

    Returns:
        Duration in seconds.
    """
    import re
    total = 0.0
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}

    parts = re.findall(r"([\d.]+)([smhdw])", duration_str.lower())
    for value, unit in parts:
        total += float(value) * units.get(unit, 1)

    if not parts and duration_str:
        try:
            total = float(duration_str)
        except ValueError:
            pass

    return total


def sleep_until(timestamp: float) -> None:
    """Sleep until specified Unix timestamp.

    Args:
        timestamp: Target Unix timestamp.
    """
    sleep_time = timestamp - time.time()
    if sleep_time > 0:
        time.sleep(sleep_time)


def sleep_random(min_seconds: float, max_seconds: float) -> float:
    """Sleep for random duration between min and max.

    Args:
        min_seconds: Minimum sleep time.
        max_seconds: Maximum sleep time.

    Returns:
        Actual sleep duration.
    """
    import random
    duration = random.uniform(min_seconds, max_seconds)
    time.sleep(duration)
    return duration


def format_timestamp(
    timestamp: Optional[float] = None,
    fmt: str = "%Y-%m-%d %H:%M:%S",
    tz: Optional[timezone] = None,
) -> str:
    """Format Unix timestamp as string.

    Args:
        timestamp: Unix timestamp (now if None).
        fmt: Format string.
        tz: Timezone (UTC if None).

    Returns:
        Formatted timestamp string.
    """
    if timestamp is None:
        timestamp = time.time()
    dt = datetime.fromtimestamp(timestamp, tz=tz or timezone.utc)
    return dt.strftime(fmt)


def parse_timestamp(
    timestamp_str: str,
    fmt: str = "%Y-%m-%d %H:%M:%S",
    tz: Optional[timezone] = None,
) -> float:
    """Parse timestamp string to Unix timestamp.

    Args:
        timestamp_str: Timestamp string.
        fmt: Format string.
        tz: Timezone (UTC if None).

    Returns:
        Unix timestamp.
    """
    dt = datetime.strptime(timestamp_str, fmt)
    if tz:
        dt = dt.replace(tzinfo=tz)
    return dt.timestamp()


def get_timezone_offset(tz_name: str = "local") -> int:
    """Get timezone offset in seconds.

    Args:
        tz_name: Timezone name or "local".

    Returns:
        Offset in seconds (e.g., 28800 for UTC+8).
    """
    if tz_name == "local":
        offset = time.timezone if not time.daylight else time.altzone
    else:
        now = datetime.now()
        offset = now.astimezone().utcoffset().total_seconds()
    return int(offset)


def format_relative_time(timestamp: float) -> str:
    """Format timestamp as relative time (e.g., "2 hours ago").

    Args:
        timestamp: Unix timestamp.

    Returns:
        Relative time string.
    """
    now = time.time()
    diff = now - timestamp

    if diff < 0:
        return "in the future"

    if diff < 60:
        return f"{int(diff)} seconds ago"
    if diff < 3600:
        return f"{int(diff // 60)} minutes ago"
    if diff < 86400:
        return f"{int(diff // 3600)} hours ago"
    if diff < 604800:
        return f"{int(diff // 86400)} days ago"
    if diff < 2592000:
        return f"{int(diff // 604800)} weeks ago"
    return format_timestamp(timestamp, "%Y-%m-%d")
