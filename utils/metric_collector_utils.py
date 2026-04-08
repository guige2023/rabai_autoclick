"""Metrics collection and reporting utilities."""

from typing import Dict, List, Optional, Callable, Any
import time
import threading
from collections import defaultdict
import statistics


class Metric:
    """Single metric value with timestamp."""

    def __init__(self, name: str, value: float, tags: Optional[Dict[str, str]] = None):
        """Initialize metric.
        
        Args:
            name: Metric name.
            value: Metric value.
            tags: Optional tags/labels.
        """
        self.name = name
        self.value = value
        self.tags = tags or {}
        self.timestamp = time.time()


class MetricsCollector:
    """Collects and aggregates metrics."""

    def __init__(self):
        """Initialize metrics collector."""
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = defaultdict(list)
        self._timers: Dict[str, List[float]] = defaultdict(list)
        self._lock = threading.RLock()

    def increment(self, name: str, value: float = 1.0, tags: Optional[Dict[str, str]] = None) -> None:
        """Increment a counter metric.
        
        Args:
            name: Metric name.
            value: Value to add.
            tags: Optional tags.
        """
        with self._lock:
            self._counters[name] += value

    def gauge(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """Set a gauge metric.
        
        Args:
            name: Metric name.
            value: Gauge value.
            tags: Optional tags.
        """
        with self._lock:
            self._gauges[name] = value

    def histogram(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """Record a histogram value.
        
        Args:
            name: Metric name.
            value: Histogram value.
            tags: Optional tags.
        """
        with self._lock:
            self._histograms[name].append(value)

    def timing(self, name: str, duration_ms: float, tags: Optional[Dict[str, str]] = None) -> None:
        """Record a timing metric.
        
        Args:
            name: Metric name.
            duration_ms: Duration in milliseconds.
            tags: Optional tags.
        """
        with self._lock:
            self._timers[name].append(duration_ms)

    def get_counter(self, name: str) -> float:
        """Get counter value."""
        with self._lock:
            return self._counters.get(name, 0.0)

    def get_gauge(self, name: str) -> Optional[float]:
        """Get gauge value."""
        with self._lock:
            return self._gauges.get(name)

    def get_histogram_stats(self, name: str) -> Optional[Dict[str, float]]:
        """Get histogram statistics."""
        with self._lock:
            values = self._histograms.get(name)
            if not values:
                return None
            return {
                "count": len(values),
                "min": min(values),
                "max": max(values),
                "mean": statistics.mean(values),
                "median": statistics.median(values),
                "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
            }

    def get_timing_stats(self, name: str) -> Optional[Dict[str, float]]:
        """Get timing statistics in milliseconds."""
        return self.get_histogram_stats(name)

    def get_all(self) -> Dict[str, Any]:
        """Get all metrics as dictionary."""
        with self._lock:
            return {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "histograms": {k: self.get_histogram_stats(k) for k in self._histograms},
                "timers": {k: self.get_timing_stats(k) for k in self._timers},
            }

    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._histograms.clear()
            self._timers.clear()


_global_collector: Optional[MetricsCollector] = None


def get_collector() -> MetricsCollector:
    """Get the global metrics collector."""
    global _global_collector
    if _global_collector is None:
        _global_collector = MetricsCollector()
    return _global_collector


class Timer:
    """Context manager for timing code blocks."""

    def __init__(self, name: str, collector: Optional[MetricsCollector] = None):
        """Initialize timer.
        
        Args:
            name: Metric name for timing.
            collector: Optional metrics collector.
        """
        self.name = name
        self.collector = collector or get_collector()
        self._start: Optional[float] = None

    def __enter__(self) -> "Timer":
        self._start = time.time()
        return self

    def __exit__(self, *args: Any) -> None:
        duration_ms = (time.time() - self._start) * 1000
        self.collector.timing(self.name, duration_ms)
