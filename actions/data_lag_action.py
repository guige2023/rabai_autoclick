"""
Data Lag Action Module.

Measures and monitors lag in data pipelines,
tracks processing delays and throughput differences.
"""

from __future__ import annotations

from typing import Any, Optional
from dataclasses import dataclass, field
import logging
import time
from collections import deque

logger = logging.getLogger(__name__)


@dataclass
class LagMetrics:
    """Lag measurement results."""
    current_lag_seconds: float
    avg_lag_seconds: float
    max_lag_seconds: float
    min_lag_seconds: float
    samples: int
    lag_trend: float


class DataLagAction:
    """
    Measures lag between data production and consumption.

    Tracks processing delays, computes statistics,
    and alerts when lag exceeds thresholds.

    Example:
        lag_monitor = DataLagAction(window=100)
        lag_monitor.record(produced_ts=1000, consumed_ts=1005)
        metrics = lag_monitor.get_metrics()
    """

    def __init__(
        self,
        window: int = 100,
        max_lag_threshold: Optional[float] = None,
    ) -> None:
        self.window = window
        self.max_lag_threshold = max_lag_threshold
        self._lag_samples: deque[float] = deque(maxlen=window)
        self._production_times: deque[float] = deque(maxlen=window)
        self._consumption_times: deque[float] = deque(maxlen=window)
        self._total_processed: int = 0
        self._total_lag: float = 0.0

    def record(
        self,
        produced_ts: Optional[float] = None,
        consumed_ts: Optional[float] = None,
        lag_seconds: Optional[float] = None,
    ) -> float:
        """Record a lag measurement."""
        if lag_seconds is None:
            if produced_ts is None or consumed_ts is None:
                raise ValueError("Must provide either lag_seconds or both produced_ts and consumed_ts")
            lag_seconds = consumed_ts - produced_ts

        self._lag_samples.append(lag_seconds)
        self._total_processed += 1
        self._total_lag += lag_seconds

        if produced_ts is not None:
            self._production_times.append(produced_ts)
        if consumed_ts is not None:
            self._consumption_times.append(consumed_ts)

        if self.max_lag_threshold and lag_seconds > self.max_lag_threshold:
            logger.warning(
                "Lag threshold exceeded: %.2fs > %.2fs",
                lag_seconds, self.max_lag_threshold
            )

        return lag_seconds

    def record_now(
        self,
        produced_ts: float,
    ) -> float:
        """Record lag using current time as consumption timestamp."""
        return self.record(
            produced_ts=produced_ts,
            consumed_ts=time.time(),
        )

    def get_current_lag(self) -> float:
        """Get the most recent lag measurement."""
        if not self._lag_samples:
            return 0.0
        return self._lag_samples[-1]

    def get_metrics(self) -> LagMetrics:
        """Get comprehensive lag metrics."""
        if not self._lag_samples:
            return LagMetrics(
                current_lag_seconds=0.0,
                avg_lag_seconds=0.0,
                max_lag_seconds=0.0,
                min_lag_seconds=0.0,
                samples=0,
                lag_trend=0.0,
            )

        samples = list(self._lag_samples)

        return LagMetrics(
            current_lag_seconds=samples[-1],
            avg_lag_seconds=sum(samples) / len(samples),
            max_lag_seconds=max(samples),
            min_lag_seconds=min(samples),
            samples=len(samples),
            lag_trend=self._compute_trend(samples),
        )

    def is_healthy(self) -> bool:
        """Check if lag is within acceptable threshold."""
        if self.max_lag_threshold is None:
            return True
        return self.get_current_lag() <= self.max_lag_threshold

    def reset(self) -> None:
        """Reset all lag tracking data."""
        self._lag_samples.clear()
        self._production_times.clear()
        self._consumption_times.clear()
        self._total_processed = 0
        self._total_lag = 0.0

    @property
    def total_processed(self) -> int:
        """Total number of records processed."""
        return self._total_processed

    @property
    def avg_lag(self) -> float:
        """Average lag over all processed records."""
        if self._total_processed == 0:
            return 0.0
        return self._total_lag / self._total_processed

    def _compute_trend(self, samples: list[float]) -> float:
        """Compute lag trend (positive = increasing lag)."""
        if len(samples) < 3:
            return 0.0

        recent = samples[-10:]
        n = len(recent)

        if n < 2:
            return 0.0

        indices = list(range(n))
        sum_x = sum(indices)
        sum_y = sum(recent)
        sum_xy = sum(i * y for i, y in zip(indices, recent))
        sum_x2 = sum(i * i for i in indices)

        denominator = n * sum_x2 - sum_x * sum_x
        if abs(denominator) < 1e-10:
            return 0.0

        return (n * sum_xy - sum_x * sum_y) / denominator
