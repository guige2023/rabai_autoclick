"""
Input Profiler Action Module

Profiles input latency, tracks performance metrics,
and identifies bottlenecks in automation workflows.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of performance metrics."""

    LATENCY = "latency"
    THROUGHPUT = "throughput"
    ERROR_RATE = "error_rate"
    CPU_USAGE = "cpu_usage"
    MEMORY_USAGE = "memory_usage"
    DURATION = "duration"


@dataclass
class MetricPoint:
    """A single metric measurement."""

    timestamp: float
    metric_type: MetricType
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class ProfilerConfig:
    """Configuration for input profiler."""

    window_size: int = 1000
    sample_rate: float = 1.0
    enable_latency_tracking: bool = True
    enable_throughput_tracking: bool = True
    alert_threshold_ms: float = 100.0
    metrics_retention_seconds: int = 3600


class InputProfiler:
    """
    Profiles input performance and automation execution.

    Tracks latency, throughput, error rates, and other
    performance metrics with statistical analysis.
    """

    def __init__(
        self,
        config: Optional[ProfilerConfig] = None,
        alert_callback: Optional[Callable[[MetricType, float], None]] = None,
    ):
        self.config = config or ProfilerConfig()
        self.alert_callback = alert_callback
        self._metrics: Dict[MetricType, deque] = defaultdict(lambda: deque(maxlen=self.config.window_size))
        self._counters: Dict[str, int] = defaultdict(int)
        self._timers: Dict[str, Tuple[str, float]] = {}
        self._start_time: float = time.time()

    def start_timer(self, name: str, label: str = "default") -> None:
        """
        Start a named timer.

        Args:
            name: Timer identifier
            label: Optional label for grouping
        """
        self._timers[name] = (label, time.time())

    def end_timer(self, name: str) -> Optional[float]:
        """
        End a named timer and record the duration.

        Args:
            name: Timer identifier

        Returns:
            Duration in seconds or None if timer not found
        """
        if name not in self._timers:
            return None

        label, start = self._timers.pop(name)
        duration = time.time() - start

        self.record_metric(
            MetricType.DURATION,
            duration * 1000,
            {"timer": name, "label": label},
        )

        return duration

    def record_metric(
        self,
        metric_type: MetricType,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Record a metric measurement.

        Args:
            metric_type: Type of metric
            value: Metric value
            labels: Optional labels for categorization
        """
        point = MetricPoint(
            timestamp=time.time(),
            metric_type=metric_type,
            value=value,
            labels=labels or {},
        )

        self._metrics[metric_type].append(point)

        if (
            metric_type == MetricType.LATENCY
            and value > self.config.alert_threshold_ms
        ):
            self._trigger_alert(metric_type, value)

    def record_latency(
        self,
        operation: str,
        duration_ms: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Convenience method to record latency."""
        labels = labels or {}
        labels["operation"] = operation
        self.record_metric(MetricType.LATENCY, duration_ms, labels)

    def increment_counter(self, name: str, value: int = 1) -> None:
        """
        Increment a counter.

        Args:
            name: Counter identifier
            value: Amount to increment
        """
        self._counters[name] += value

    def get_counter(self, name: str) -> int:
        """Get current counter value."""
        return self._counters.get(name, 0)

    def get_latency_stats(
        self,
        operation: Optional[str] = None,
    ) -> Dict[str, float]:
        """
        Get latency statistics.

        Args:
            operation: Optional filter by operation name

        Returns:
            Dict with min, max, mean, median, p95, p99
        """
        latencies = self._get_filtered_latencies(operation)

        if not latencies:
            return {"count": 0}

        sorted_latencies = sorted(latencies)
        count = len(sorted_latencies)

        return {
            "count": count,
            "min": min(latencies),
            "max": max(latencies),
            "mean": statistics.mean(latencies),
            "median": statistics.median(latencies),
            "p95": sorted_latencies[int(count * 0.95)] if count > 0 else 0,
            "p99": sorted_latencies[int(count * 0.99)] if count > 0 else 0,
            "stdev": statistics.stdev(latencies) if count > 1 else 0,
        }

    def _get_filtered_latencies(self, operation: Optional[str]) -> List[float]:
        """Get filtered latency values."""
        points = self._metrics.get(MetricType.LATENCY, [])

        if operation is None:
            return [p.value for p in points]

        return [p.value for p in points if p.labels.get("operation") == operation]

    def get_throughput(
        self,
        counter_name: str,
        time_window: Optional[float] = None,
    ) -> float:
        """
        Calculate throughput (operations per second).

        Args:
            counter_name: Counter to calculate from
            time_window: Time window in seconds (full window if None)

        Returns:
            Operations per second
        """
        count = self.get_counter(counter_name)
        elapsed = time.time() - self._start_time

        if time_window:
            elapsed = min(elapsed, time_window)

        return count / elapsed if elapsed > 0 else 0

    def get_error_rate(
        self,
        success_counter: str,
        error_counter: str,
    ) -> float:
        """
        Calculate error rate.

        Args:
            success_counter: Name of success counter
            error_counter: Name of error counter

        Returns:
            Error rate as percentage (0-100)
        """
        success = self.get_counter(success_counter)
        errors = self.get_counter(error_counter)
        total = success + errors

        return (errors / total * 100) if total > 0 else 0

    def get_summary(self) -> Dict[str, Any]:
        """
        Get a summary of all profiling data.

        Returns:
            Dict with summary statistics
        """
        summary: Dict[str, Any] = {
            "uptime_seconds": time.time() - self._start_time,
            "counters": dict(self._counters),
            "metrics": {},
        }

        for metric_type in MetricType:
            points = self._metrics.get(metric_type, [])
            if not points:
                continue

            values = [p.value for p in points]

            summary["metrics"][metric_type.value] = {
                "count": len(values),
                "min": min(values),
                "max": max(values),
                "mean": statistics.mean(values),
                "last": values[-1] if values else 0,
            }

        return summary

    def _trigger_alert(self, metric_type: MetricType, value: float) -> None:
        """Trigger an alert callback."""
        if self.alert_callback:
            try:
                self.alert_callback(metric_type, value)
            except Exception as e:
                logger.error(f"Alert callback failed: {e}")

    def export_metrics(
        self,
        metric_type: Optional[MetricType] = None,
    ) -> List[Dict[str, Any]]:
        """
        Export metrics for external analysis.

        Args:
            metric_type: Specific metric type (all if None)

        Returns:
            List of metric dictionaries
        """
        if metric_type:
            points = self._metrics.get(metric_type, [])
            return [self._point_to_dict(p) for p in points]

        result = []
        for mtype, points in self._metrics.items():
            result.extend([self._point_to_dict(p) for p in points])

        return result

    def _point_to_dict(self, point: MetricPoint) -> Dict[str, Any]:
        """Convert a metric point to dictionary."""
        return {
            "timestamp": point.timestamp,
            "type": point.metric_type.value,
            "value": point.value,
            "labels": point.labels,
        }

    def reset(self) -> None:
        """Reset all profiling data."""
        self._metrics.clear()
        self._counters.clear()
        self._timers.clear()
        self._start_time = time.time()


def create_input_profiler(
    config: Optional[ProfilerConfig] = None,
) -> InputProfiler:
    """Factory function to create an InputProfiler."""
    return InputProfiler(config=config)
