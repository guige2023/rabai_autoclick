"""Time utilities for RabAI AutoClick.

Provides:
- Duration formatting and parsing
- Time measurement decorators
- Timestamp helpers
- Timezone utilities
"""

from __future__ import annotations

import time
from typing import (
    Any,
    Callable,
    Optional,
    TypeVar,
)


T = TypeVar("T")


def measure_time(func: Callable[..., T]) -> Callable[..., T]:
    """Decorator that measures function execution time.

    Args:
        func: Function to measure.

    Returns:
        Decorated function.
    """
    def wrapper(*args: Any, **kwargs: Any) -> T:
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        print(f"{func.__name__} took {elapsed:.4f}s")
        return result
    return wrapper


def measure_time_ctx(
    name: Optional[str] = None,
) -> "ElapsedTimer":
    """Context manager to measure elapsed time.

    Args:
        name: Optional label for the timer.

    Returns:
        ElapsedTimer context manager.
    """
    return ElapsedTimer(name)


class ElapsedTimer:
    """Context manager for measuring elapsed time."""

    def __init__(self, name: Optional[str] = None) -> None:
        self.name = name or "Elapsed"
        self.start: Optional[float] = None
        self.elapsed: Optional[float] = None

    def __enter__(self) -> "ElapsedTimer":
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args: Any) -> None:
        self.elapsed = time.perf_counter() - self.start

    def __str__(self) -> str:
        if self.elapsed is None:
            return f"{self.name}: running"
        return f"{self.name}: {self.elapsed:.4f}s"


def format_duration(seconds: float) -> str:
    """Format seconds as human-readable duration.

    Args:
        seconds: Duration in seconds.

    Returns:
        Formatted string (e.g., '1h 23m 45s').
    """
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes = int(seconds // 60)
    seconds = seconds % 60
    if minutes < 60:
        return f"{minutes}m {seconds:.0f}s"
    hours = minutes // 60
    minutes = minutes % 60
    return f"{hours}h {minutes}m"


def parse_duration(duration_str: str) -> float:
    """Parse a duration string to seconds.

    Args:
        duration_str: String like '1h30m', '45s', '2.5h'.

    Returns:
        Duration in seconds.
    """
    import re
    total = 0.0
    patterns = [
        (r"(\d+(?:\.\d+)?)h", 3600),
        (r"(\d+(?:\.\d+)?)m", 60),
        (r"(\d+(?:\.\d+)?)s", 1),
    ]
    for pattern, multiplier in patterns:
        match = re.search(pattern, duration_str)
        if match:
            total += float(match.group(1)) * multiplier
    return total


def timestamp_now() -> float:
    """Get current Unix timestamp."""
    return time.time()


def timestamp_ms() -> int:
    """Get current timestamp in milliseconds."""
    return int(time.time() * 1000)


def sleep(seconds: float) -> None:
    """Sleep for a given number of seconds.

    Args:
        seconds: Duration to sleep.
    """
    time.sleep(seconds)


def retry_delay(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential: bool = True,
) -> float:
    """Calculate delay before next retry attempt.

    Args:
        attempt: Attempt number (1-based).
        base_delay: Base delay in seconds.
        max_delay: Maximum delay.
        exponential: Use exponential backoff if True.

    Returns:
        Delay in seconds.
    """
    if exponential:
        delay = base_delay * (2 ** (attempt - 1))
    else:
        delay = base_delay * attempt
    return min(delay, max_delay)


__all__ = [
    "measure_time",
    "measure_time_ctx",
    "ElapsedTimer",
    "format_duration",
    "parse_duration",
    "timestamp_now",
    "timestamp_ms",
    "sleep",
    "retry_delay",
]
