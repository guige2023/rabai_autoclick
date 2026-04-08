"""
Data Rate Tracker Action Module.

Tracks rate of events/values over sliding windows,
computes throughput, velocity, and trend analysis.
"""

from __future__ import annotations

from typing import Any, Optional
from dataclasses import dataclass, field
import logging
import time
import math
from collections import deque

logger = logging.getLogger(__name__)


@dataclass
class RateSample:
    """Single rate sample."""
    timestamp: float
    value: float


@dataclass
class RateStats:
    """Rate statistics."""
    rate: float
    samples: int
    window_seconds: float
    trend: float
    min_value: float
    max_value: float


class DataRateTrackerAction:
    """
    Tracks rate of events over sliding time windows.

    Computes events per second/minute, trends, and
    velocity changes with configurable window sizes.

    Example:
        tracker = DataRateTrackerAction(window_seconds=60)
        tracker.add(1)  # event with value 1
        stats = tracker.get_stats()
        print(stats.rate)  # events per second
    """

    def __init__(
        self,
        window_seconds: float = 60.0,
        max_samples: int = 1000,
    ) -> None:
        self.window_seconds = window_seconds
        self.max_samples = max_samples
        self._samples: deque[RateSample] = deque(maxlen=max_samples)
        self._total_count: int = 0
        self._total_value: float = 0.0

    def add(
        self,
        value: float = 1.0,
        timestamp: Optional[float] = None,
    ) -> None:
        """Add a sample to the rate tracker."""
        ts = timestamp or time.time()
        self._samples.append(RateSample(timestamp=ts, value=value))
        self._total_count += 1
        self._total_value += value
        self._prune_old_samples(ts)

    def get_rate(
        self,
        window_seconds: Optional[float] = None,
    ) -> float:
        """Get events per second over the window."""
        window = window_seconds or self.window_seconds
        self._prune_old_samples(time.time())

        if not self._samples:
            return 0.0

        elapsed = time.time() - self._samples[0].timestamp
        if elapsed <= 0:
            return 0.0

        count = len(self._samples)
        return count / min(elapsed, window)

    def get_throughput(
        self,
        window_seconds: Optional[float] = None,
    ) -> float:
        """Get total value per second over the window."""
        window = window_seconds or self.window_seconds
        self._prune_old_samples(time.time())

        if not self._samples:
            return 0.0

        elapsed = time.time() - self._samples[0].timestamp
        if elapsed <= 0:
            return 0.0

        total = sum(s.value for s in self._samples)
        return total / min(elapsed, window)

    def get_stats(
        self,
        window_seconds: Optional[float] = None,
    ) -> RateStats:
        """Get comprehensive rate statistics."""
        window = window_seconds or self.window_seconds
        self._prune_old_samples(time.time())

        if not self._samples:
            return RateStats(
                rate=0.0,
                samples=0,
                window_seconds=window,
                trend=0.0,
                min_value=0.0,
                max_value=0.0,
            )

        values = [s.value for s in self._samples]
        rate = self.get_rate(window)
        trend = self._compute_trend()

        return RateStats(
            rate=rate,
            samples=len(self._samples),
            window_seconds=window,
            trend=trend,
            min_value=min(values),
            max_value=max(values),
        )

    def get_percentile(
        self,
        percentile: float,
        window_seconds: Optional[float] = None,
    ) -> float:
        """Get value at given percentile."""
        window = window_seconds or self.window_seconds
        self._prune_old_samples(time.time())

        if not self._samples:
            return 0.0

        values = sorted(s.value for s in self._samples)
        idx = int(len(values) * percentile / 100)
        idx = min(idx, len(values) - 1)
        return values[idx]

    def reset(self) -> None:
        """Reset all tracking data."""
        self._samples.clear()
        self._total_count = 0
        self._total_value = 0.0

    def _prune_old_samples(self, current_time: float) -> None:
        """Remove samples outside the window."""
        cutoff = current_time - self.window_seconds

        while self._samples and self._samples[0].timestamp < cutoff:
            self._samples.popleft()

    def _compute_trend(self) -> float:
        """Compute trend (slope) of recent samples."""
        if len(self._samples) < 3:
            return 0.0

        recent = list(self._samples)[-10:]
        times = [s.timestamp - recent[0].timestamp for s in recent]
        values = [s.value for s in recent]

        if all(t == times[0] for t in times):
            return 0.0

        n = len(recent)
        sum_x = sum(times)
        sum_y = sum(values)
        sum_xy = sum(t * y for t, y in zip(times, values))
        sum_x2 = sum(t * t for t in times)

        denominator = n * sum_x2 - sum_x * sum_x
        if abs(denominator) < 1e-10:
            return 0.0

        slope = (n * sum_xy - sum_x * sum_y) / denominator
        return slope
