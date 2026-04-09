"""
Performance Monitor Action Module.

Monitors automation performance metrics including execution
time, resource usage, and operation throughput.
"""

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Metric:
    """A performance metric sample."""
    name: str
    value: float
    timestamp: float
    unit: str = ""


@dataclass
class PerformanceSnapshot:
    """A snapshot of performance metrics."""
    operation: str
    duration_ms: float
    timestamp: float
    success: bool


class PerformanceMonitor:
    """Monitors automation performance metrics."""

    def __init__(self, window_size: int = 100):
        """
        Initialize performance monitor.

        Args:
            window_size: Number of samples to retain.
        """
        self.window_size = window_size
        self._metrics: dict[str, deque[float]] = {}
        self._operation_times: deque[PerformanceSnapshot] = deque(maxlen=window_size)
        self._start_times: dict[str, float] = {}
        self._operation_counts: dict[str, int] = {}
        self._operation_errors: dict[str, int] = {}

    def start_operation(self, name: str) -> None:
        """
        Start timing an operation.

        Args:
            name: Operation name.
        """
        self._start_times[name] = time.time()
        self._operation_counts[name] = self._operation_counts.get(name, 0) + 1

    def end_operation(
        self,
        name: str,
        success: bool = True,
    ) -> Optional[float]:
        """
        End timing an operation.

        Args:
            name: Operation name.
            success: Whether operation succeeded.

        Returns:
            Operation duration in milliseconds or None.
        """
        start_time = self._start_times.pop(name, None)
        if start_time is None:
            return None

        duration_ms = (time.time() - start_time) * 1000

        snapshot = PerformanceSnapshot(
            operation=name,
            duration_ms=duration_ms,
            timestamp=time.time(),
            success=success,
        )
        self._operation_times.append(snapshot)

        if not self._metrics.get(name):
            self._metrics[name] = deque(maxlen=self.window_size)
        self._metrics[name].append(duration_ms)

        if not success:
            self._operation_errors[name] = self._operation_errors.get(name, 0) + 1

        return duration_ms

    def record_metric(
        self,
        name: str,
        value: float,
        unit: str = "",
    ) -> Metric:
        """
        Record a custom metric.

        Args:
            name: Metric name.
            value: Metric value.
            unit: Optional unit string.

        Returns:
            Recorded Metric.
        """
        metric = Metric(
            name=name,
            value=value,
            timestamp=time.time(),
            unit=unit,
        )

        if not self._metrics.get(name):
            self._metrics[name] = deque(maxlen=self.window_size)
        self._metrics[name].append(value)

        return metric

    def get_stats(self, name: str) -> dict:
        """
        Get statistics for an operation.

        Args:
            name: Operation name.

        Returns:
            Dictionary with min, max, avg, count, error_rate.
        """
        if name not in self._metrics or not self._metrics[name]:
            return {
                "min_ms": 0,
                "max_ms": 0,
                "avg_ms": 0,
                "count": 0,
                "error_count": 0,
                "error_rate": 0.0,
            }

        values = list(self._metrics[name])
        count = self._operation_counts.get(name, 0)
        errors = self._operation_errors.get(name, 0)

        return {
            "min_ms": min(values),
            "max_ms": max(values),
            "avg_ms": sum(values) / len(values),
            "count": count,
            "error_count": errors,
            "error_rate": errors / count if count > 0 else 0.0,
        }

    def get_throughput(self, name: str, window_seconds: float = 60.0) -> float:
        """
        Calculate operations per second.

        Args:
            name: Operation name.
            window_seconds: Time window in seconds.

        Returns:
            Operations per second.
        """
        cutoff = time.time() - window_seconds
        recent = [
            s for s in self._operation_times
            if s.operation == name and s.timestamp >= cutoff
        ]
        return len(recent) / window_seconds if window_seconds > 0 else 0.0

    def get_all_stats(self) -> dict:
        """Get statistics for all operations."""
        return {
            name: self.get_stats(name)
            for name in self._metrics.keys()
        }
