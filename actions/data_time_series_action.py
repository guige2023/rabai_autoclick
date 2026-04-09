"""Data Time Series Action module.

Provides time series data processing capabilities including
resampling, interpolation, smoothing, trend detection,
and seasonal decomposition.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Optional


class ResampleRule(Enum):
    """Resampling aggregation rules."""

    MEAN = "mean"
    SUM = "sum"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    COUNT = "count"


@dataclass
class TimeSeriesPoint:
    """A single time series data point."""

    timestamp: datetime
    value: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TimeSeries:
    """Time series data container."""

    name: str
    points: list[TimeSeriesPoint] = field(default_factory=list)
    unit: str = ""

    def add(self, timestamp: datetime, value: float, **metadata: Any) -> None:
        """Add a data point."""
        self.points.append(
            TimeSeriesPoint(timestamp=timestamp, value=value, metadata=metadata)
        )

    def values(self) -> list[float]:
        """Get all values."""
        return [p.value for p in self.points]

    def timestamps(self) -> list[datetime]:
        """Get all timestamps."""
        return [p.timestamp for p in self.points]

    def __len__(self) -> int:
        return len(self.points)


def resample_timeseries(
    ts: TimeSeries,
    interval_seconds: float,
    rule: ResampleRule = ResampleRule.MEAN,
) -> TimeSeries:
    """Resample time series to new interval.

    Args:
        ts: Source time series
        interval_seconds: Target interval in seconds
        rule: Aggregation rule

    Returns:
        Resampled time series
    """
    if len(ts) == 0:
        return TimeSeries(name=ts.name, unit=ts.unit)

    result = TimeSeries(name=ts.name, unit=ts.unit)

    sorted_points = sorted(ts.points, key=lambda p: p.timestamp)
    start_time = sorted_points[0].timestamp
    end_time = sorted_points[-1].timestamp

    current_bucket_start = start_time
    current_bucket: list[float] = []

    for point in sorted_points:
        bucket_end = current_bucket_start + timedelta(seconds=interval_seconds)

        if point.timestamp < bucket_end:
            current_bucket.append(point.value)
        else:
            if current_bucket:
                aggregated = aggregate_bucket(current_bucket, rule)
                result.add(current_bucket_start, aggregated)

            while point.timestamp >= bucket_end:
                current_bucket_start = bucket_end
                bucket_end = current_bucket_start + timedelta(seconds=interval_seconds)

            current_bucket = [point.value]

    if current_bucket:
        aggregated = aggregate_bucket(current_bucket, rule)
        result.add(current_bucket_start, aggregated)

    return result


def aggregate_bucket(values: list[float], rule: ResampleRule) -> float:
    """Aggregate a bucket of values."""
    if not values:
        return 0.0

    if rule == ResampleRule.MEAN:
        return sum(values) / len(values)
    elif rule == ResampleRule.SUM:
        return sum(values)
    elif rule == ResampleRule.MIN:
        return min(values)
    elif rule == ResampleRule.MAX:
        return max(values)
    elif rule == ResampleRule.FIRST:
        return values[0]
    elif rule == ResampleRule.LAST:
        return values[-1]
    elif rule == ResampleRule.COUNT:
        return float(len(values))
    return 0.0


def interpolate_missing(
    ts: TimeSeries,
    max_gap_seconds: float,
    method: str = "linear",
) -> TimeSeries:
    """Interpolate missing values in time series.

    Args:
        ts: Time series with potentially missing points
        max_gap_seconds: Maximum gap to interpolate
        method: Interpolation method ('linear', 'forward', 'backward')

    Returns:
        Time series with interpolated values
    """
    if len(ts) < 2:
        return TimeSeries(name=ts.name, unit=ts.unit, points=list(ts.points))

    result = TimeSeries(name=ts.name, unit=ts.unit)

    sorted_points = sorted(ts.points, key=lambda p: p.timestamp)

    for i, point in enumerate(sorted_points):
        result.points.append(point)

        if i < len(sorted_points) - 1:
            next_point = sorted_points[i + 1]
            gap = (next_point.timestamp - point.timestamp).total_seconds()

            if 0 < gap <= max_gap_seconds:
                steps = int(gap / (max_gap_seconds / 10))
                if steps > 1:
                    for step in range(1, steps):
                        alpha = step / steps
                        if method == "linear":
                            interp_value = point.value + alpha * (
                                next_point.value - point.value
                            )
                        elif method == "forward":
                            interp_value = point.value
                        else:
                            interp_value = next_point.value

                        interp_time = point.timestamp + timedelta(
                            seconds=gap * alpha
                        )
                        result.add(interp_time, interp_value)

    return result


@dataclass
class MovingAverageConfig:
    """Configuration for moving average."""

    window_size: int = 5
    min_periods: int = 1
    center: bool = False


def moving_average(
    ts: TimeSeries,
    config: Optional[MovingAverageConfig] = None,
) -> TimeSeries:
    """Compute moving average of time series.

    Args:
        ts: Source time series
        config: Moving average configuration

    Returns:
        Smoothed time series
    """
    config = config or MovingAverageConfig()
    result = TimeSeries(name=ts.name, unit=ts.unit)

    values = ts.values()
    n = len(values)

    for i in range(n):
        if config.center:
            half = config.window_size // 2
            start = max(0, i - half)
            end = min(n, i + half + 1)
        else:
            start = max(0, i - config.window_size + 1)
            end = i + 1

        window = values[start:end]
        if len(window) >= config.min_periods:
            avg = sum(window) / len(window)
            result.add(ts.points[i].timestamp, avg)

    return result


@dataclass
class TrendResult:
    """Result of trend analysis."""

    slope: float
    intercept: float
    r_squared: float
    direction: str
    strength: str


def detect_trend(ts: TimeSeries) -> TrendResult:
    """Detect linear trend in time series using least squares.

    Args:
        ts: Source time series

    Returns:
        TrendResult with slope, intercept, and metrics
    """
    if len(ts) < 3:
        return TrendResult(slope=0, intercept=0, r_squared=0, direction="flat", strength="unknown")

    n = len(ts)
    times = [(p.timestamp - ts.points[0].timestamp).total_seconds() for p in ts.points]
    values = ts.values()

    mean_t = sum(times) / n
    mean_v = sum(values) / n

    numerator = sum((t - mean_t) * (v - mean_v) for t, v in zip(times, values))
    denominator = sum((t - mean_t) ** 2 for t in times)

    if denominator == 0:
        slope = 0
    else:
        slope = numerator / denominator

    intercept = mean_v - slope * mean_t

    ss_res = sum((v - (slope * t + intercept)) ** 2 for t, v in zip(times, values))
    ss_tot = sum((v - mean_v) ** 2 for v in values)

    r_squared = 1 - ss_res / ss_tot if ss_tot != 0 else 0

    if abs(slope) < 1e-10:
        direction = "flat"
    elif slope > 0:
        direction = "increasing"
    else:
        direction = "decreasing"

    strength = "weak"
    if r_squared > 0.7:
        strength = "strong"
    elif r_squared > 0.4:
        strength = "moderate"

    return TrendResult(
        slope=slope,
        intercept=intercept,
        r_squared=r_squared,
        direction=direction,
        strength=strength,
    )


class ExponentialSmoother:
    """Exponential smoothing for time series."""

    def __init__(self, alpha: float = 0.3):
        if not 0 < alpha <= 1:
            raise ValueError("alpha must be in (0, 1]")
        self.alpha = alpha
        self._smoothed: Optional[float] = None

    def add(self, value: float) -> float:
        """Add value and return smoothed result."""
        if self._smoothed is None:
            self._smoothed = value
        else:
            self._smoothed = self.alpha * value + (1 - self.alpha) * self._smoothed
        return self._smoothed

    def reset(self) -> None:
        """Reset smoother state."""
        self._smoothed = None

    @property
    def smoothed(self) -> Optional[float]:
        """Get current smoothed value."""
        return self._smoothed
