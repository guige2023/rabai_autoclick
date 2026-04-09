"""UI Performance utilities for measuring and optimizing UI operations.

This module provides utilities for measuring UI performance,
including frame timing, rendering metrics, and operation profiling.
"""

from typing import Dict, List, Optional, Callable, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum, auto
import time
import threading
from collections import deque
from contextlib import contextmanager
import statistics


class PerformanceMetric(Enum):
    """Types of performance metrics."""
    FRAME_TIME = "frame_time"
    FPS = "fps"
    RENDER_TIME = "render_time"
    LAYOUT_TIME = "layout_time"
    PAINT_TIME = "paint_time"
    INPUT_LATENCY = "input_latency"
    MEMORY_USAGE = "memory_usage"
    CPU_USAGE = "cpu_usage"


@dataclass
class PerformanceSample:
    """A single performance measurement sample."""
    metric: PerformanceMetric
    value: float
    timestamp: float
    tags: Dict[str, str] = field(default_factory=dict)

    def __repr__(self) -> str:
        return f"PerformanceSample({self.metric.value}={self.value:.2f} @ {self.timestamp:.3f})"


@dataclass
class PerformanceStats:
    """Statistics for a performance metric."""
    metric: PerformanceMetric
    count: int = 0
    total: float = 0.0
    min_value: float = float('inf')
    max_value: float = float('-inf')
    mean: float = 0.0
    median: float = 0.0
    p95: float = 0.0
    p99: float = 0.0
    std_dev: float = 0.0

    def update(self, value: float) -> None:
        """Update stats with a new value."""
        self.count += 1
        self.total += value
        self.min_value = min(self.min_value, value)
        self.max_value = max(self.max_value, value)

        if self.count == 1:
            self.mean = value
            self.median = value
            self.p95 = value
            self.p99 = value
            self.std_dev = 0.0
        else:
            self.mean = self.total / self.count

    def finalize(self, values: List[float]) -> None:
        """Finalize stats from a list of values."""
        if not values:
            return
        self.count = len(values)
        self.total = sum(values)
        self.min_value = min(values)
        self.max_value = max(values)
        self.mean = self.total / self.count
        self.median = statistics.median(values)
        if len(values) > 1:
            self.std_dev = statistics.stdev(values)
        else:
            self.std_dev = 0.0
        sorted_values = sorted(values)
        self.p95 = sorted_values[int(len(sorted_values) * 0.95)]
        self.p99 = sorted_values[int(len(sorted_values) * 0.99)]


class PerformanceMonitor:
    """Monitors and collects performance metrics."""

    def __init__(self, max_samples: int = 1000):
        self.max_samples = max_samples
        self.samples: deque[PerformanceSample] = deque(maxlen=max_samples)
        self.stats: Dict[PerformanceMetric, PerformanceStats] = {
            m: PerformanceStats(metric=m) for m in PerformanceMetric
        }
        self._lock = threading.Lock()
        self._start_time = time.time()

    def record(self, metric: PerformanceMetric, value: float,
               tags: Optional[Dict[str, str]] = None) -> None:
        """Record a performance sample."""
        sample = PerformanceSample(
            metric=metric,
            value=value,
            timestamp=time.time() - self._start_time,
            tags=tags or {}
        )
        with self._lock:
            self.samples.append(sample)
            self.stats[metric].update(value)

    def get_recent_samples(self, metric: PerformanceMetric,
                          count: int = 100) -> List[PerformanceSample]:
        """Get recent samples for a metric."""
        with self._lock:
            return [s for s in self.samples if s.metric == metric][-count:]

    def get_stats(self, metric: PerformanceMetric) -> PerformanceStats:
        """Get statistics for a metric."""
        with self._lock:
            recent = [s.value for s in self.samples if s.metric == metric]
            stats = PerformanceStats(metric=metric)
            stats.finalize(recent)
            return stats

    def calculate_fps(self, window_seconds: float = 1.0) -> float:
        """Calculate frames per second over a time window."""
        cutoff_time = time.time() - self._start_time - window_seconds
        with self._lock:
            frame_times = [s.timestamp for s in self.samples
                          if s.metric == PerformanceMetric.FRAME_TIME
                          and s.timestamp >= cutoff_time]
        if len(frame_times) < 2:
            return 0.0
        frame_times.sort()
        total_time = frame_times[-1] - frame_times[0]
        if total_time == 0:
            return 0.0
        return (len(frame_times) - 1) / total_time

    def get_metric_summary(self, metric: PerformanceMetric) -> Dict[str, float]:
        """Get summary of a metric."""
        stats = self.get_stats(metric)
        return {
            "count": stats.count,
            "mean": stats.mean,
            "median": stats.median,
            "min": stats.min_value,
            "max": stats.max_value,
            "p95": stats.p95,
            "p99": stats.p99,
            "std_dev": stats.std_dev,
        }

    def clear(self) -> None:
        """Clear all samples and reset stats."""
        with self._lock:
            self.samples.clear()
            for metric in PerformanceMetric:
                self.stats[metric] = PerformanceStats(metric=metric)


class FrameTimer:
    """High-precision frame timing."""

    def __init__(self):
        self.frame_count = 0
        self.start_time = time.perf_counter()
        self.last_frame_time = self.start_time
        self.frame_deltas: deque[float] = deque(maxlen=60)

    def tick(self) -> float:
        """Mark a frame as completed, return delta time."""
        current_time = time.perf_counter()
        delta = current_time - self.last_frame_time
        self.last_frame_time = current_time
        self.frame_count += 1
        self.frame_deltas.append(delta)
        return delta

    def get_fps(self) -> float:
        """Calculate current FPS."""
        if not self.frame_deltas:
            return 0.0
        avg_delta = sum(self.frame_deltas) / len(self.frame_deltas)
        if avg_delta == 0:
            return 0.0
        return 1.0 / avg_delta

    def get_frame_time_ms(self) -> float:
        """Get average frame time in milliseconds."""
        if not self.frame_deltas:
            return 0.0
        return (sum(self.frame_deltas) / len(self.frame_deltas)) * 1000

    def reset(self) -> None:
        """Reset the timer."""
        self.frame_count = 0
        self.start_time = time.perf_counter()
        self.last_frame_time = self.start_time
        self.frame_deltas.clear()


class OperationProfiler:
    """Profiles the execution time of operations."""

    def __init__(self):
        self.operations: Dict[str, List[float]] = {}
        self._lock = threading.Lock()

    @contextmanager
    def profile(self, operation_name: str):
        """Context manager for profiling an operation."""
        start = time.perf_counter()
        try:
            yield
        finally:
            duration = time.perf_counter() - start
            with self._lock:
                if operation_name not in self.operations:
                    self.operations[operation_name] = []
                self.operations[operation_name].append(duration)

    def get_stats(self, operation_name: str) -> Optional[Dict[str, float]]:
        """Get statistics for an operation."""
        with self._lock:
            if operation_name not in self.operations:
                return None
            values = self.operations[operation_name]
            if not values:
                return None

            sorted_values = sorted(values)
            return {
                "count": len(values),
                "total": sum(values),
                "mean": sum(values) / len(values),
                "min": min(values),
                "max": max(values),
                "median": sorted_values[len(sorted_values) // 2],
                "p95": sorted_values[int(len(sorted_values) * 0.95)],
                "p99": sorted_values[int(len(sorted_values) * 0.99)],
            }

    def get_all_stats(self) -> Dict[str, Dict[str, float]]:
        """Get statistics for all operations."""
        with self._lock:
            return {name: self.get_stats(name)
                   for name in self.operations if self.get_stats(name)}


class LatencyTracker:
    """Tracks input-to-action latency."""

    def __init__(self, max_samples: int = 1000):
        self.max_samples = max_samples
        self.samples: deque[Tuple[float, float]] = deque(maxlen=max_samples)
        self._lock = threading.Lock()

    def record(self, event_time: float, processing_time: float) -> None:
        """Record a latency sample."""
        with self._lock:
            self.samples.append((event_time, processing_time))

    def get_average_latency(self, window_seconds: float = 60.0) -> float:
        """Get average latency over time window."""
        cutoff = time.time() - window_seconds
        with self._lock:
            recent = [(e, p) for e, p in self.samples if e >= cutoff]
        if not recent:
            return 0.0
        total_latency = sum(p - e for e, p in recent)
        return total_latency / len(recent)

    def get_percentile_latency(self, percentile: float,
                               window_seconds: float = 60.0) -> float:
        """Get percentile latency over time window."""
        cutoff = time.time() - window_seconds
        with self._lock:
            recent = sorted([p - e for e, p in self.samples if e >= cutoff])
        if not recent:
            return 0.0
        index = int(len(recent) * (percentile / 100.0))
        return recent[min(index, len(recent) - 1)]


@contextmanager
def timed_operation(name: str, monitor: Optional[PerformanceMonitor] = None):
    """Context manager to time an operation and record to monitor."""
    start = time.perf_counter()
    try:
        yield
    finally:
        duration = time.perf_counter() - start
        if monitor:
            monitor.record(PerformanceMetric.RENDER_TIME, duration,
                         tags={"operation": name})


class PerformanceBenchmark:
    """Runs performance benchmarks."""

    def __init__(self, name: str):
        self.name = name
        self.results: Dict[str, List[float]] = {}

    def run(self, func: Callable[[], Any],
           iterations: int = 10,
           warmup: int = 3) -> Dict[str, float]:
        """Run benchmark for a function."""
        for _ in range(warmup):
            func()

        times = []
        for _ in range(iterations):
            start = time.perf_counter()
            func()
            duration = time.perf_counter() - start
            times.append(duration)

        self.results[self.name] = times

        sorted_times = sorted(times)
        return {
            "name": self.name,
            "iterations": iterations,
            "mean": sum(times) / len(times),
            "min": min(times),
            "max": max(times),
            "median": sorted_times[len(sorted_times) // 2],
            "p95": sorted_times[int(len(sorted_times) * 0.95)],
        }

    def compare(self, other_name: str) -> Optional[Dict[str, float]]:
        """Compare this benchmark with another."""
        if self.name not in self.results or other_name not in self.results:
            return None

        times1 = self.results[self.name]
        times2 = self.results[other_name]

        mean1 = sum(times1) / len(times1)
        mean2 = sum(times2) / len(times2)

        speedup = mean2 / mean1 if mean1 > 0 else 0.0

        return {
            "baseline": self.name,
            "comparison": other_name,
            "baseline_mean": mean1,
            "comparison_mean": mean2,
            "speedup": speedup,
            "improvement_percent": (1 - speedup) * 100 if speedup > 0 else 0,
        }


def estimate_memory_usage(element_count: int,
                         average_text_length: int = 50) -> int:
    """Estimate memory usage for UI elements in bytes."""
    per_element = 500
    per_text_char = 2
    return (element_count * per_element +
            element_count * average_text_length * per_text_char)
