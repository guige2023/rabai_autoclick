"""
Input response time measurement utilities.

This module provides utilities for measuring and analyzing
input-to-action response times in automation workflows.
"""

from __future__ import annotations

import time
import math
from typing import List, Optional, Dict, Any, Callable
from dataclasses import dataclass, field
from dataclasses import dataclass as dc
from enum import Enum, auto


class TimingMetric(Enum):
    """Types of timing measurements."""
    LATENCY = auto()
    PROCESSING_TIME = auto()
    ACTION_DELAY = auto()
    END_TO_END = auto()


@dataclass
class TimingSample:
    """A single timing measurement."""
    metric: TimingMetric
    value_ms: float
    timestamp: float
    context: Dict[str, Any] = field(default_factory=dict)

    @property
    def value_s(self) -> float:
        """Value in seconds."""
        return self.value_ms / 1000.0


@dataclass
class TimingStats:
    """Statistical summary of timing measurements."""
    count: int = 0
    mean_ms: float = 0.0
    median_ms: float = 0.0
    min_ms: float = 0.0
    max_ms: float = 0.0
    std_dev_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0

    def __str__(self) -> str:
        return (f"TimingStats(count={self.count}, mean={self.mean_ms:.2f}ms, "
                f"median={self.median_ms:.2f}ms, p95={self.p95_ms:.2f}ms, "
                f"p99={self.p99_ms:.2f}ms, max={self.max_ms:.2f}ms)")


@dataclass
class ResponseTimeMonitor:
    """Monitor for tracking input response times."""
    samples: List[TimingSample] = field(default_factory=list)
    _pending_start: Optional[float] = field(default=None, repr=False)

    def start_timer(self) -> None:
        """Start the response timer."""
        self._pending_start = time.perf_counter()

    def stop_timer(self, metric: TimingMetric = TimingMetric.LATENCY, context: Optional[Dict[str, Any]] = None) -> Optional[float]:
        """
        Stop the timer and record a sample.

        Args:
            metric: Type of timing metric.
            context: Optional context to attach.

        Returns:
            Elapsed time in milliseconds, or None if timer wasn't started.
        """
        if self._pending_start is None:
            return None
        elapsed = (time.perf_counter() - self._pending_start) * 1000.0
        self._pending_start = None
        sample = TimingSample(
            metric=metric,
            value_ms=elapsed,
            timestamp=time.time(),
            context=context or {},
        )
        self.samples.append(sample)
        return elapsed

    def add_sample(self, value_ms: float, metric: TimingMetric = TimingMetric.LATENCY, context: Optional[Dict[str, Any]] = None) -> TimingSample:
        """
        Manually record a timing sample.

        Args:
            value_ms: Timing value in milliseconds.
            metric: Type of timing metric.
            context: Optional context to attach.

        Returns:
            The created TimingSample.
        """
        sample = TimingSample(
            metric=metric,
            value_ms=value_ms,
            timestamp=time.time(),
            context=context or {},
        )
        self.samples.append(sample)
        return sample

    def get_stats(self, metric: Optional[TimingMetric] = None) -> TimingStats:
        """
        Compute statistics for recorded samples.

        Args:
            metric: Filter to specific metric type, or None for all.

        Returns:
            TimingStats summary.
        """
        values = [s.value_ms for s in self.samples if metric is None or s.metric == metric]
        if not values:
            return TimingStats()

        sorted_values = sorted(values)
        count = len(sorted_values)
        mean = sum(sorted_values) / count
        variance = sum((v - mean) ** 2 for v in sorted_values) / count

        return TimingStats(
            count=count,
            mean_ms=mean,
            median_ms=sorted_values[count // 2],
            min_ms=sorted_values[0],
            max_ms=sorted_values[-1],
            std_dev_ms=math.sqrt(variance),
            p95_ms=sorted_values[int(count * 0.95)],
            p99_ms=sorted_values[int(count * 0.99)],
        )

    def clear(self) -> None:
        """Clear all recorded samples."""
        self.samples.clear()
        self._pending_start = None

    def __enter__(self) -> "ResponseTimeMonitor":
        self.start_timer()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop_timer(context={"error": str(exc_val) if exc_val else None})


class Timer:
    """Simple context manager for timing code blocks."""

    def __init__(self, name: str = "block"):
        self.name = name
        self.start_time: Optional[float] = None
        self.elapsed_ms: float = 0.0

    def __enter__(self) -> "Timer":
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.start_time is not None:
            self.elapsed_ms = (time.perf_counter() - self.start_time) * 1000.0

    def __str__(self) -> str:
        return f"{self.name}: {self.elapsed_ms:.2f}ms"


def compute_percentile(values: List[float], percentile: float) -> float:
    """
    Compute a percentile value from a list.

    Args:
        values: List of numeric values.
        percentile: Percentile to compute (0-100).

    Returns:
        The percentile value.
    """
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = int(len(sorted_values) * percentile / 100.0)
    index = min(index, len(sorted_values) - 1)
    return sorted_values[index]


def is_within_threshold(sample: TimingSample, threshold_ms: float) -> bool:
    """Check if a timing sample is within acceptable threshold."""
    return sample.value_ms <= threshold_ms


def filter_by_threshold(samples: List[TimingSample], threshold_ms: float) -> List[TimingSample]:
    """Filter samples that are within threshold."""
    return [s for s in samples if s.value_ms <= threshold_ms]
