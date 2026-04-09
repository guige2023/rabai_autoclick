"""
Metrics Collection Utilities for UI Automation.

This module provides utilities for collecting and analyzing performance
metrics during automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional
from collections import defaultdict
import statistics


class MetricType(Enum):
    """Types of metrics."""
    COUNTER = auto()
    GAUGE = auto()
    HISTOGRAM = auto()
    TIMER = auto()
    RATE = auto()


@dataclass
class Metric:
    """Base metric class."""
    name: str
    metric_type: MetricType
    value: float = 0.0
    timestamp: float = field(default_factory=time.time)
    tags: dict[str, str] = field(default_factory=dict)


@dataclass
class Counter:
    """Counter metric that only increments."""
    name: str
    value: int = 0
    tags: dict[str, str] = field(default_factory=dict)
    
    def increment(self, amount: int = 1) -> None:
        """Increment the counter."""
        self.value += amount
    
    def decrement(self, amount: int = 1) -> None:
        """Decrement the counter."""
        self.value -= amount
    
    def reset(self) -> None:
        """Reset the counter to zero."""
        self.value = 0
    
    def to_metric(self) -> Metric:
        """Convert to Metric object."""
        return Metric(
            name=self.name,
            metric_type=MetricType.COUNTER,
            value=float(self.value),
            tags=self.tags
        )


@dataclass
class Gauge:
    """Gauge metric that can go up or down."""
    name: str
    value: float = 0.0
    tags: dict[str, str] = field(default_factory=dict)
    
    def set(self, value: float) -> None:
        """Set the gauge value."""
        self.value = value
    
    def increment(self, amount: float = 1.0) -> None:
        """Increment the gauge."""
        self.value += amount
    
    def decrement(self, amount: float = 1.0) -> None:
        """Decrement the gauge."""
        self.value -= amount
    
    def to_metric(self) -> Metric:
        """Convert to Metric object."""
        return Metric(
            name=self.name,
            metric_type=MetricType.GAUGE,
            value=self.value,
            tags=self.tags
        )


@dataclass
class Histogram:
    """Histogram metric for tracking distributions."""
    name: str
    values: list[float] = field(default_factory=list)
    tags: dict[str, str] = field(default_factory=dict)
    
    def observe(self, value: float) -> None:
        """Add an observation to the histogram."""
        self.values.append(value)
    
    @property
    def count(self) -> int:
        """Get number of observations."""
        return len(self.values)
    
    @property
    def sum(self) -> float:
        """Get sum of all observations."""
        return sum(self.values)
    
    @property
    def mean(self) -> float:
        """Get mean of observations."""
        return statistics.mean(self.values) if self.values else 0.0
    
    @property
    def median(self) -> float:
        """Get median of observations."""
        return statistics.median(self.values) if self.values else 0.0
    
    @property
    def std_dev(self) -> float:
        """Get standard deviation of observations."""
        return statistics.stdev(self.values) if len(self.values) > 1 else 0.0
    
    def percentile(self, p: float) -> float:
        """Get the p-th percentile (0-100)."""
        if not self.values:
            return 0.0
        sorted_values = sorted(self.values)
        index = int(len(sorted_values) * p / 100)
        return sorted_values[min(index, len(sorted_values) - 1)]
    
    def reset(self) -> None:
        """Reset the histogram."""
        self.values.clear()
    
    def to_metric(self) -> Metric:
        """Convert to Metric object."""
        return Metric(
            name=self.name,
            metric_type=MetricType.HISTOGRAM,
            value=self.mean,
            tags=self.tags
        )


class Timer:
    """Timer context manager for measuring durations."""
    
    def __init__(self, name: str, histogram: Optional[Histogram] = None):
        self.name = name
        self.histogram = histogram
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.duration_ms: float = 0.0
    
    def __enter__(self) -> 'Timer':
        """Start the timer."""
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Stop the timer and record the duration."""
        self.end_time = time.time()
        self.duration_ms = (self.end_time - self.start_time) * 1000
        
        if self.histogram:
            self.histogram.observe(self.duration_ms)
    
    def stop(self) -> float:
        """Manually stop the timer."""
        if self.start_time:
            self.end_time = time.time()
            self.duration_ms = (self.end_time - self.start_time) * 1000
        return self.duration_ms


class MetricsCollector:
    """
    Central metrics collection and management.
    
    Example:
        collector = MetricsCollector()
        collector.counter("requests").increment()
        collector.gauge("memory").set(1024)
        
        with collector.timer("operation"):
            do_work()
    """
    
    def __init__(self):
        self._counters: dict[str, Counter] = {}
        self._gauges: dict[str, Gauge] = {}
        self._histograms: dict[str, Histogram] = {}
        self._timers: dict[str, Timer] = {}
    
    def counter(self, name: str, tags: Optional[dict[str, str]] = None) -> Counter:
        """Get or create a counter."""
        key = self._make_key(name, tags)
        if key not in self._counters:
            self._counters[key] = Counter(name=name, tags=tags or {})
        return self._counters[key]
    
    def gauge(self, name: str, tags: Optional[dict[str, str]] = None) -> Gauge:
        """Get or create a gauge."""
        key = self._make_key(name, tags)
        if key not in self._gauges:
            self._gauges[key] = Gauge(name=name, tags=tags or {})
        return self._gauges[key]
    
    def histogram(
        self, 
        name: str, 
        tags: Optional[dict[str, str]] = None
    ) -> Histogram:
        """Get or create a histogram."""
        key = self._make_key(name, tags)
        if key not in self._histograms:
            self._histograms[key] = Histogram(name=name, tags=tags or {})
        return self._histograms[key]
    
    def timer(self, name: str, tags: Optional[dict[str, str]] = None) -> Timer:
        """Create a timer context manager."""
        hist = self.histogram(name, tags)
        return Timer(name=name, histogram=hist)
    
    def time_function(
        self, 
        name: str, 
        func: Callable[..., Any],
        *args,
        **kwargs
    ) -> tuple[Any, float]:
        """
        Time a function execution.
        
        Returns:
            Tuple of (function_result, duration_ms)
        """
        start = time.time()
        result = func(*args, **kwargs)
        duration_ms = (time.time() - start) * 1000
        
        self.histogram(name).observe(duration_ms)
        
        return result, duration_ms
    
    def get_all_metrics(self) -> list[Metric]:
        """Get all current metrics."""
        metrics = []
        
        for counter in self._counters.values():
            metrics.append(counter.to_metric())
        
        for gauge in self._gauges.values():
            metrics.append(gauge.to_metric())
        
        for histogram in self._histograms.values():
            metrics.append(histogram.to_metric())
        
        return metrics
    
    def get_counter_value(self, name: str, tags: Optional[dict[str, str]] = None) -> int:
        """Get the current value of a counter."""
        key = self._make_key(name, tags)
        return self._counters.get(key, Counter(name=name)).value
    
    def reset_all(self) -> None:
        """Reset all metrics."""
        for counter in self._counters.values():
            counter.reset()
        for histogram in self._histograms.values():
            histogram.reset()
    
    def _make_key(self, name: str, tags: Optional[dict[str, str]]) -> str:
        """Create a unique key for a metric."""
        if not tags:
            return name
        tag_str = ",".join(f"{k}={v}" for k, v in sorted(tags.items()))
        return f"{name}[{tag_str}]"


# Global default collector
_default_collector: Optional[MetricsCollector] = None


def get_collector() -> MetricsCollector:
    """Get the global default metrics collector."""
    global _default_collector
    if _default_collector is None:
        _default_collector = MetricsCollector()
    return _default_collector


def counter(name: str, tags: Optional[dict[str, str]] = None) -> Counter:
    """Convenience function for global counter."""
    return get_collector().counter(name, tags)


def gauge(name: str, tags: Optional[dict[str, str]] = None) -> Gauge:
    """Convenience function for global gauge."""
    return get_collector().gauge(name, tags)


def histogram(name: str, tags: Optional[dict[str, str]] = None) -> Histogram:
    """Convenience function for global histogram."""
    return get_collector().histogram(name, tags)


def timer(name: str, tags: Optional[dict[str, str]] = None) -> Timer:
    """Convenience function for global timer."""
    return get_collector().timer(name, tags)
