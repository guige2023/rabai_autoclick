"""
Action timing and performance measurement utilities.

This module provides utilities for measuring execution time,
tracking performance metrics, and generating timing reports.
"""

from __future__ import annotations

import time
import threading
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Any
from contextlib import contextmanager


@dataclass
class TimingRecord:
    """
    Record of a timed execution.

    Attributes:
        name: Name of the timed operation.
        start_time: When the operation started.
        end_time: When the operation ended.
        duration: Total duration in seconds.
        success: Whether operation completed successfully.
        metadata: Additional timing metadata.
    """
    name: str
    start_time: float
    end_time: float = 0.0
    duration: float = 0.0
    success: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.end_time > 0:
            self.duration = self.end_time - self.start_time


class Timer:
    """
    Simple timer for measuring elapsed time.

    Can be used as context manager or manually.
    """

    def __init__(self) -> None:
        self._start: Optional[float] = None
        self._end: Optional[float] = None
        self._running: bool = False

    def start(self) -> Timer:
        """Start the timer."""
        self._start = time.time()
        self._running = True
        return self

    def stop(self) -> float:
        """Stop the timer and return elapsed time."""
        if not self._running:
            return 0.0
        self._end = time.time()
        self._running = False
        return self.elapsed

    def reset(self) -> None:
        """Reset the timer."""
        self._start = None
        self._end = None
        self._running = False

    @property
    def elapsed(self) -> float:
        """Get elapsed time in seconds."""
        if self._start is None:
            return 0.0
        if self._end is None:
            return time.time() - self._start
        return self._end - self._start

    def __enter__(self) -> Timer:
        """Start timer as context manager."""
        self.start()
        return self

    def __exit__(self, *args: Any) -> None:
        """Stop timer on context exit."""
        self.stop()


class PerformanceTracker:
    """
    Tracks performance metrics for multiple operations.

    Stores timing records and provides aggregation
    and reporting capabilities.
    """

    def __init__(self) -> None:
        self._records: List[TimingRecord] = []
        self._lock = threading.Lock()
        self._active_timers: Dict[str, Timer] = {}

    def start(self, name: str) -> None:
        """Start timing an operation."""
        with self._lock:
            self._active_timers[name] = Timer()
            self._active_timers[name].start()

    def stop(
        self,
        name: str,
        success: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[float]:
        """Stop timing an operation and record it."""
        with self._lock:
            timer = self._active_timers.pop(name, None)
            if timer is None:
                return None

            duration = timer.stop()
            record = TimingRecord(
                name=name,
                start_time=timer._start or 0,
                end_time=(timer._start or 0) + duration,
                duration=duration,
                success=success,
                metadata=metadata or {},
            )
            self._records.append(record)
            return duration

    @contextmanager
    def measure(self, name: str, **kwargs: Any):
        """Context manager for measuring operations."""
        self.start(name)
        try:
            yield
            self.stop(name, success=True, metadata=kwargs)
        except Exception:
            self.stop(name, success=False, metadata=kwargs)
            raise

    def get_records(
        self,
        name: Optional[str] = None,
        since: Optional[float] = None,
    ) -> List[TimingRecord]:
        """Get timing records, optionally filtered."""
        with self._lock:
            records = self._records

            if name:
                records = [r for r in records if r.name == name]

            if since:
                records = [r for r in records if r.start_time >= since]

            return records

    def get_stats(self, name: str) -> Dict[str, float]:
        """Get aggregated statistics for an operation."""
        with self._lock:
            records = [r for r in self._records if r.name == name]

            if not records:
                return {}

            durations = [r.duration for r in records]
            durations.sort()

            return {
                "count": len(durations),
                "total": sum(durations),
                "mean": sum(durations) / len(durations),
                "min": min(durations),
                "max": max(durations),
                "median": durations[len(durations) // 2],
                "p95": durations[int(len(durations) * 0.95)] if len(durations) >= 20 else durations[-1],
                "p99": durations[int(len(durations) * 0.99)] if len(durations) >= 100 else durations[-1],
            }

    def get_all_stats(self) -> Dict[str, Dict[str, float]]:
        """Get statistics for all tracked operations."""
        names = set(r.name for r in self._records)
        return {name: self.get_stats(name) for name in names}

    def clear(self) -> None:
        """Clear all timing records."""
        with self._lock:
            self._records.clear()
            self._active_timers.clear()

    def generate_report(self) -> str:
        """Generate a formatted timing report."""
        lines = ["=== Performance Report ===", ""]

        stats = self.get_all_stats()

        for name, stat in stats.items():
            lines.append(f"Operation: {name}")
            lines.append(f"  Count:   {stat['count']}")
            lines.append(f"  Total:   {stat['total']:.3f}s")
            lines.append(f"  Mean:    {stat['mean']:.3f}s")
            lines.append(f"  Min:     {stat['min']:.3f}s")
            lines.append(f"  Max:     {stat['max']:.3f}s")
            lines.append(f"  Median:  {stat['median']:.3f}s")
            if stat['count'] >= 20:
                lines.append(f"  P95:     {stat['p95']:.3f}s")
            lines.append("")

        return "\n".join(lines)


class ActionTimer:
    """
    Decorator-based timer for functions.

    Wraps functions to automatically track execution time.
    """

    def __init__(self, tracker: PerformanceTracker) -> None:
        self._tracker = tracker

    def __call__(self, name: Optional[str] = None) -> Callable:
        """Decorate a function to track its timing."""
        def decorator(func: Callable) -> Callable:
            operation_name = name or f"{func.__module__}.{func.__name__}"

            def wrapper(*args: Any, **kwargs: Any) -> Any:
                with self._tracker.measure(operation_name):
                    return func(*args, **kwargs)

            wrapper.__name__ = func.__name__
            wrapper.__doc__ = func.__doc__
            return wrapper

        return decorator


class RateMeter:
    """
    Measures execution rate (operations per second).

    Tracks how many operations complete in a time window.
    """

    def __init__(self, window_size: float = 60.0) -> None:
        self._window_size = window_size
        self._timestamps: List[float] = []
        self._lock = threading.Lock()

    def record(self) -> None:
        """Record an operation completion."""
        with self._lock:
            now = time.time()
            self._timestamps.append(now)
            self._cleanup(now)

    def _cleanup(self, now: float) -> None:
        """Remove timestamps outside the window."""
        cutoff = now - self._window_size
        self._timestamps = [t for t in self._timestamps if t > cutoff]

    @property
    def count(self) -> int:
        """Get number of operations in window."""
        with self._lock:
            self._cleanup(time.time())
            return len(self._timestamps)

    @property
    def rate(self) -> float:
        """Get operations per second."""
        with self._lock:
            self._cleanup(time.time())
            if not self._timestamps:
                return 0.0
            elapsed = time.time() - self._timestamps[0]
            if elapsed == 0:
                return 0.0
            return len(self._timestamps) / elapsed


# Global default tracker
_default_tracker: Optional[PerformanceTracker] = None


def get_tracker() -> PerformanceTracker:
    """Get or create the default performance tracker."""
    global _default_tracker
    if _default_tracker is None:
        _default_tracker = PerformanceTracker()
    return _default_tracker


def timed(name: Optional[str] = None) -> Callable:
    """
    Decorator to time a function.

    Usage:
        @timed("my_operation")
        def my_function():
            ...
    """
    tracker = get_tracker()

    def decorator(func: Callable) -> Callable:
        operation_name = name or func.__name__

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with tracker.measure(operation_name):
                return func(*args, **kwargs)

        return wrapper

    return decorator
