"""
Input Latency Utilities

Measure, track, and report on input latency in automation pipelines.
Covers event queuing latency, dispatch latency, and end-to-end latency.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class LatencyMeasurement:
    """A single latency measurement."""
    label: str
    latency_ms: float
    timestamp_ms: float = field(default_factory=lambda: time.time() * 1000)


@dataclass
class LatencyStats:
    """Statistical summary of latency measurements."""
    label: str
    count: int
    mean_ms: float
    min_ms: float
    max_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float


class InputLatencyTracker:
    """Track input latency with a rolling window."""

    def __init__(self, label: str, window_size: int = 100):
        self.label = label
        self.window_size = window_size
        self._measurements: deque[LatencyMeasurement] = deque(maxlen=window_size)

    def record(self, latency_ms: float) -> LatencyMeasurement:
        """Record a new latency measurement."""
        m = LatencyMeasurement(label=self.label, latency_ms=latency_ms)
        self._measurements.append(m)
        return m

    def get_stats(self) -> LatencyStats:
        """Compute statistics from the rolling window."""
        if not self._measurements:
            return LatencyStats(
                label=self.label, count=0,
                mean_ms=0.0, min_ms=0.0, max_ms=0.0,
                p50_ms=0.0, p95_ms=0.0, p99_ms=0.0,
            )
        values = sorted(m.latency_ms for m in self._measurements)
        n = len(values)
        return LatencyStats(
            label=self.label,
            count=n,
            mean_ms=sum(values) / n,
            min_ms=values[0],
            max_ms=values[-1],
            p50_ms=values[int(n * 0.50)],
            p95_ms=values[int(n * 0.95)],
            p99_ms=values[int(n * 0.99)],
        )


class MultiLatencyTracker:
    """Track latency across multiple pipeline stages."""

    def __init__(self):
        self._trackers: dict[str, InputLatencyTracker] = {}

    def get_or_create(self, label: str, window_size: int = 100) -> InputLatencyTracker:
        """Get an existing tracker or create a new one."""
        if label not in self._trackers:
            self._trackers[label] = InputLatencyTracker(label, window_size)
        return self._trackers[label]

    def record(self, label: str, latency_ms: float) -> LatencyMeasurement:
        """Record a latency measurement for a given label."""
        return self.get_or_create(label).record(latency_ms)

    def get_all_stats(self) -> dict[str, LatencyStats]:
        """Get statistics for all tracked stages."""
        return {label: t.get_stats() for label, t in self._trackers.items()}


def measure_event_latency(
    event_timestamp_ms: float,
    processing_start_ms: Optional[float] = None,
) -> float:
    """Measure latency between an event timestamp and processing start."""
    start = processing_start_ms if processing_start_ms is not None else time.time() * 1000
    return max(0.0, start - event_timestamp_ms)
