"""Performance Profiling Utilities.

Profiles UI automation performance and identifies bottlenecks.
Tracks execution time, call counts, and resource usage.
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Optional, TypeVar


T = TypeVar("T")


@dataclass
class ProfilerStats:
    """Statistics for a profiled operation.

    Attributes:
        name: Operation name.
        call_count: Number of times the operation was called.
        total_time_ms: Total time spent in the operation.
        min_time_ms: Minimum single-call duration.
        max_time_ms: Maximum single-call duration.
        avg_time_ms: Average single-call duration.
    """

    name: str
    call_count: int = 0
    total_time_ms: float = 0.0
    min_time_ms: float = float("inf")
    max_time_ms: float = 0.0
    avg_time_ms: float = 0.0

    def add_sample(self, duration_ms: float) -> None:
        """Add a timing sample.

        Args:
            duration_ms: Duration in milliseconds.
        """
        self.call_count += 1
        self.total_time_ms += duration_ms
        self.min_time_ms = min(self.min_time_ms, duration_ms)
        self.max_time_ms = max(self.max_time_ms, duration_ms)
        self.avg_time_ms = self.total_time_ms / self.call_count

    @property
    def total_time_formatted(self) -> str:
        """Get formatted total time."""
        return f"{self.total_time_ms:.2f}ms"

    @property
    def avg_time_formatted(self) -> str:
        """Get formatted average time."""
        return f"{self.avg_time_ms:.2f}ms"


@dataclass
class ProfileResult:
    """Result of a profiling session.

    Attributes:
        operation_stats: Statistics by operation name.
        wall_time_ms: Total wall-clock time.
        start_time: Session start timestamp.
        end_time: Session end timestamp.
    """

    operation_stats: dict[str, ProfilerStats] = field(default_factory=dict)
    wall_time_ms: float = 0.0
    start_time: float = 0.0
    end_time: float = 0.0

    def get_stats(self, name: str) -> Optional[ProfilerStats]:
        """Get stats for an operation.

        Args:
            name: Operation name.

        Returns:
            ProfilerStats or None if not found.
        """
        return self.operation_stats.get(name)

    def get_top_operations(self, limit: int = 10) -> list[ProfilerStats]:
        """Get top operations by total time.

        Args:
            limit: Maximum number to return.

        Returns:
            List of ProfilerStats sorted by total time.
        """
        stats = list(self.operation_stats.values())
        stats.sort(key=lambda s: s.total_time_ms, reverse=True)
        return stats[:limit]


class Profiler:
    """Performance profiler for automation operations.

    Example:
        profiler = Profiler()
        with profiler.profile("find_element"):
            element = find_element("button")
        result = profiler.get_result()
    """

    def __init__(self):
        """Initialize the profiler."""
        self._stats: dict[str, ProfilerStats] = {}
        self._start_time: Optional[float] = None
        self._end_time: Optional[float] = None
        self._current_context: list[tuple[str, float]] = []

    def profile(self, operation_name: str) -> "ProfileContext":
        """Create a profiling context for an operation.

        Args:
            operation_name: Name of the operation.

        Returns:
            ProfileContext for the operation.
        """
        return ProfileContext(self, operation_name)

    def _start_operation(self, name: str) -> float:
        """Start timing an operation.

        Args:
            name: Operation name.

        Returns:
            Start timestamp.
        """
        if self._start_time is None:
            self._start_time = time.time()

        start = time.time()
        self._current_context.append((name, start))
        return start

    def _end_operation(self, name: str, start_time: float) -> None:
        """End timing an operation.

        Args:
            name: Operation name.
            start_time: Start timestamp.
        """
        end = time.time()
        duration_ms = (end - start_time) * 1000

        if name not in self._stats:
            self._stats[name] = ProfilerStats(name=name)
        self._stats[name].add_sample(duration_ms)

        if self._current_context and self._current_context[-1][0] == name:
            self._current_context.pop()

    def start_session(self) -> None:
        """Start a profiling session."""
        self._start_time = time.time()
        self._end_time = None
        self._stats.clear()
        self._current_context.clear()

    def end_session(self) -> ProfileResult:
        """End the profiling session.

        Returns:
            ProfileResult with session statistics.
        """
        self._end_time = time.time()
        wall_time_ms = 0.0

        if self._start_time and self._end_time:
            wall_time_ms = (self._end_time - self._start_time) * 1000

        return ProfileResult(
            operation_stats=dict(self._stats),
            wall_time_ms=wall_time_ms,
            start_time=self._start_time or 0,
            end_time=self._end_time or 0,
        )

    def reset(self) -> None:
        """Reset all profiling data."""
        self._stats.clear()
        self._start_time = None
        self._end_time = None
        self._current_context.clear()


class ProfileContext:
    """Context manager for profiling a single operation."""

    def __init__(self, profiler: Profiler, operation_name: str):
        """Initialize the context.

        Args:
            profiler: Parent Profiler instance.
            operation_name: Name of the operation.
        """
        self._profiler = profiler
        self._name = operation_name
        self._start_time: Optional[float] = None

    def __enter__(self) -> "ProfileContext":
        """Enter the profiling context."""
        self._start_time = self._profiler._start_operation(self._name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the profiling context."""
        if self._start_time is not None:
            self._profiler._end_operation(self._name, self._start_time)


class PerformanceTracker:
    """Tracks performance metrics over time.

    Example:
        tracker = PerformanceTracker()
        tracker.record("element_find", 50.0)
        tracker.record("element_click", 30.0)
        summary = tracker.get_summary()
    """

    def __init__(self):
        """Initialize the tracker."""
        self._samples: dict[str, list[float]] = defaultdict(list)

    def record(self, metric_name: str, value: float) -> None:
        """Record a performance sample.

        Args:
            metric_name: Name of the metric.
            value: Metric value.
        """
        self._samples[metric_name].append(value)

    def record_timed(
        self,
        metric_name: str,
        func: Callable[[], T],
    ) -> T:
        """Record timing for a function execution.

        Args:
            metric_name: Name of the metric.
            func: Function to time.

        Returns:
            Function result.
        """
        start = time.time()
        try:
            return func()
        finally:
            duration_ms = (time.time() - start) * 1000
            self.record(metric_name, duration_ms)

    def get_samples(self, metric_name: str) -> list[float]:
        """Get all samples for a metric.

        Args:
            metric_name: Metric name.

        Returns:
            List of sample values.
        """
        return self._samples.get(metric_name, [])

    def get_summary(self) -> dict[str, dict]:
        """Get summary statistics for all metrics.

        Returns:
            Dictionary of metric summaries.
        """
        summary = {}

        for metric_name, samples in self._samples.items():
            if samples:
                summary[metric_name] = {
                    "count": len(samples),
                    "min": min(samples),
                    "max": max(samples),
                    "avg": sum(samples) / len(samples),
                    "total": sum(samples),
                }

        return summary

    def clear(self) -> None:
        """Clear all recorded samples."""
        self._samples.clear()


def profile_function(
    profiler: Optional[Profiler] = None,
    operation_name: Optional[str] = None,
) -> Callable[[Callable[T]], Callable[T]]:
    """Decorator to profile a function.

    Args:
        profiler: Profiler to use (creates new if None).
        operation_name: Operation name (uses function name if None).

    Returns:
        Decorator function.
    """
    def decorator(func: Callable[T]) -> Callable[T]:
        name = operation_name or func.__name__

        def wrapper(*args, **kwargs):
            p = profiler or Profiler()
            with p.profile(name):
                return func(*args, **kwargs)

        return wrapper
    return decorator
