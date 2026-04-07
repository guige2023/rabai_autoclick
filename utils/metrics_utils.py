"""Metrics utilities for RabAI AutoClick.

Provides:
- Counter, Gauge, Histogram metrics
- Simple metrics collection and reporting
- Timer context managers
"""

import time
import threading
from typing import (
    Callable,
    Dict,
    List,
    Optional,
)


class Counter:
    """Thread-safe counter metric."""

    def __init__(self, initial: float = 0.0) -> None:
        self._value = initial
        self._lock = threading.Lock()

    def increment(self, delta: float = 1.0) -> float:
        """Increment the counter.

        Args:
            delta: Amount to increment.

        Returns:
            New value.
        """
        with self._lock:
            self._value += delta
            return self._value

    def decrement(self, delta: float = 1.0) -> float:
        """Decrement the counter.

        Args:
            delta: Amount to decrement.

        Returns:
            New value.
        """
        with self._lock:
            self._value -= delta
            return self._value

    def get(self) -> float:
        """Get current value."""
        with self._lock:
            return self._value

    def set(self, value: float) -> None:
        """Set the counter value."""
        with self._lock:
            self._value = value

    def reset(self) -> None:
        """Reset counter to zero."""
        with self._lock:
            self._value = 0.0


class Gauge:
    """Thread-safe gauge metric."""

    def __init__(self, initial: float = 0.0) -> None:
        self._value = initial
        self._lock = threading.Lock()

    def set(self, value: float) -> None:
        """Set the gauge value."""
        with self._lock:
            self._value = value

    def get(self) -> float:
        """Get current value."""
        with self._lock:
            return self._value

    def increment(self, delta: float = 1.0) -> float:
        """Increment the gauge."""
        with self._lock:
            self._value += delta
            return self._value

    def decrement(self, delta: float = 1.0) -> float:
        """Decrement the gauge."""
        with self._lock:
            self._value -= delta
            return self._value

    def reset(self) -> None:
        """Reset gauge to zero."""
        with self._lock:
            self._value = 0.0


class Histogram:
    """Histogram metric for tracking distributions."""

    def __init__(
        self,
        buckets: Optional[List[float]] = None,
    ) -> None:
        """Initialize histogram.

        Args:
            buckets: Bucket boundaries (e.g., [0.1, 0.5, 1.0, 5.0]).
        """
        if buckets is None:
            buckets = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        self._buckets = sorted(buckets)
        self._counts: Dict[float, int] = {b: 0 for b in self._buckets}
        self._sum = 0.0
        self._count = 0
        self._min = float("inf")
        self._max = float("-inf")
        self._lock = threading.Lock()

    def observe(self, value: float) -> None:
        """Record an observation.

        Args:
            value: Observed value.
        """
        with self._lock:
            self._sum += value
            self._count += 1
            self._min = min(self._min, value)
            self._max = max(self._max, value)

            for bucket in self._buckets:
                if value <= bucket:
                    self._counts[bucket] += 1

    def get_stats(self) -> dict:
        """Get histogram statistics.

        Returns:
            Dict with count, sum, min, max, mean, buckets.
        """
        with self._lock:
            return {
                "count": self._count,
                "sum": self._sum,
                "min": self._min if self._count > 0 else 0,
                "max": self._max if self._count > 0 else 0,
                "mean": self._sum / self._count if self._count > 0 else 0,
                "buckets": dict(self._counts),
            }


class Timer:
    """Timer metric for measuring durations."""

    def __init__(self) -> None:
        self._histogram = Histogram()
        self._start_time: Optional[float] = None
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start the timer."""
        self._start_time = time.perf_counter()

    def stop(self) -> float:
        """Stop the timer and record the duration.

        Returns:
            Elapsed time in seconds.
        """
        if self._start_time is None:
            return 0.0
        elapsed = time.perf_counter() - self._start_time
        self._histogram.observe(elapsed)
        self._start_time = None
        return elapsed

    def __enter__(self) -> "Timer":
        self.start()
        return self

    def __exit__(self, *args: Any) -> None:
        self.stop()

    def get_stats(self) -> dict:
        """Get timer statistics."""
        return self._histogram.get_stats()


class MetricsRegistry:
    """Global metrics registry."""

    _instance: Optional["MetricsRegistry"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "MetricsRegistry":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._metrics: Dict[str, any] = {}
                    cls._instance._metric_lock = threading.Lock()
        return cls._instance

    def counter(self, name: str, **kwargs: Any) -> Counter:
        """Get or create a counter."""
        with self._metric_lock:
            if name not in self._metrics:
                self._metrics[name] = Counter(**kwargs)
            return self._metrics[name]

    def gauge(self, name: str, **kwargs: Any) -> Gauge:
        """Get or create a gauge."""
        with self._metric_lock:
            if name not in self._metrics:
                self._metrics[name] = Gauge(**kwargs)
            return self._metrics[name]

    def histogram(self, name: str, **kwargs: Any) -> Histogram:
        """Get or create a histogram."""
        with self._metric_lock:
            if name not in self._metrics:
                self._metrics[name] = Histogram(**kwargs)
            return self._metrics[name]

    def timer(self, name: str) -> Timer:
        """Get or create a timer."""
        with self._metric_lock:
            if name not in self._metrics:
                self._metrics[name] = Timer()
            return self._metrics[name]

    def get_all(self) -> dict:
        """Get all metrics."""
        with self._metric_lock:
            result = {}
            for name, metric in self._metrics.items():
                if isinstance(metric, (Counter, Gauge)):
                    result[name] = {"type": type(metric).__name__, "value": metric.get()}
                elif isinstance(metric, (Histogram, Timer)):
                    result[name] = {"type": type(metric).__name__, "stats": metric.get_stats()}
            return result

    def reset(self) -> None:
        """Reset all metrics."""
        with self._metric_lock:
            for metric in self._metrics.values():
                if hasattr(metric, "reset"):
                    metric.reset()


# Global registry instance
registry = MetricsRegistry()


def timed(metric_name: Optional[str] = None) -> Callable:
    """Decorator to time a function.

    Args:
        metric_name: Optional metric name (default: function name).

    Returns:
        Decorated function.
    """
    def decorator(func: Callable) -> Callable:
        name = metric_name or func.__name__

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            timer = registry.timer(name)
            with timer:
                return func(*args, **kwargs)

        return wrapper
    return decorator


def count_calls(metric_name: Optional[str] = None) -> Callable:
    """Decorator to count function calls.

    Args:
        metric_name: Optional metric name.

    Returns:
        Decorated function.
    """
    def decorator(func: Callable) -> Callable:
        name = metric_name or func.__name__

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            counter = registry.counter(name)
            counter.increment()
            return func(*args, **kwargs)

        return wrapper
    return decorator


def observe(metric_name: str) -> Callable:
    """Decorator to observe return values as histogram samples.

    Args:
        metric_name: Metric name.

    Returns:
        Decorated function.
    """
    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            result = func(*args, **kwargs)
            if isinstance(result, (int, float)):
                histogram = registry.histogram(metric_name)
                histogram.observe(float(result))
            return result
        return wrapper
    return decorator
