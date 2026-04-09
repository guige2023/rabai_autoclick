"""
Data Rolling Statistics Action Module

Computes rolling window statistics for streaming data.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple, Deque
from dataclasses import dataclass
from collections import deque
import math


@dataclass
class RollingStats:
    """Rolling window statistics."""
    count: int = 0
    sum: float = 0.0
    mean: float = 0.0
    min: float = 0.0
    max: float = 0.0
    std_dev: float = 0.0
    variance: float = 0.0


class RollingWindow:
    """Fixed-size rolling window buffer."""

    def __init__(self, size: int):
        self.size = size
        self.buffer: Deque[float] = deque(maxlen=size)

    def push(self, value: float) -> None:
        """Add value to window."""
        self.buffer.append(value)

    def get_all(self) -> List[float]:
        """Get all values in window."""
        return list(self.buffer)

    def __len__(self) -> int:
        return len(self.buffer)

    def is_full(self) -> bool:
        return len(self.buffer) >= self.size


class RollingMean:
    """Welford's online algorithm for rolling mean and variance."""

    def __init__(self):
        self.count = 0
        self.mean = 0.0
        self.m2 = 0.0
        self.min = float('inf')
        self.max = float('-inf')

    def update(self, value: float) -> None:
        """Update with new value."""
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self.m2 += delta * delta2
        self.min = min(self.min, value)
        self.max = max(self.max, value)

    def get_stats(self) -> RollingStats:
        """Get current statistics."""
        variance = self.m2 / self.count if self.count > 1 else 0.0
        return RollingStats(
            count=self.count,
            sum=self.mean * self.count,
            mean=self.mean,
            min=self.min,
            max=self.max,
            std_dev=math.sqrt(variance),
            variance=variance
        )


class RollingQuantile:
    """T-digest algorithm for approximate quantiles."""

    def __init__(self, compression: float = 100.0):
        self.compression = compression
        self.centroids: List[Tuple[float, float]] = []  # (mean, weight)
        self.count = 0.0

    def update(self, value: float) -> None:
        """Update with new value."""
        self.centroids.append((value, 1.0))
        self.count += 1
        self._compress()

    def _compress(self) -> None:
        """Merge centroids to maintain compression constraint."""
        if len(self.centroids) <= self.compression:
            return

        self.centroids.sort(key=lambda x: x[0])
        merged: List[Tuple[float, float]] = []

        i = 0
        while i < len(self.centroids):
            if i + 1 < len(self.centroids):
                # Merge with next centroid
                w1 = self.centroids[i][1]
                w2 = self.centroids[i + 1][1]
                new_w = w1 + w2
                new_mean = (self.centroids[i][0] * w1 + self.centroids[i + 1][0] * w2) / new_w
                merged.append((new_mean, new_w))
                i += 2
            else:
                merged.append(self.centroids[i])
                i += 1

        self.centroids = merged

    def quantile(self, q: float) -> float:
        """Get approximate quantile value."""
        if not self.centroids:
            return 0.0

        self.centroids.sort(key=lambda x: x[0])
        total_weight = sum(c[1] for c in self.centroids)
        target = q * total_weight

        cumulative = 0.0
        for mean, weight in self.centroids:
            if cumulative + weight >= target:
                return mean
            cumulative += weight

        return self.centroids[-1][0]


class RollingCorrelation:
    """Online rolling correlation computation."""

    def __init__(self):
        self.x_stats = RollingMean()
        self.y_stats = RollingMean()
        self.xy_sum = 0.0
        self.n = 0

    def update(self, x: float, y: float) -> None:
        """Update with new (x, y) pair."""
        self.x_stats.update(x)
        self.y_stats.update(y)

        # Update covariance sum
        dx = x - self.x_stats.mean
        dy = y - self.y_stats.mean
        self.xy_sum += dx * dy
        self.n += 1

    def get_correlation(self) -> float:
        """Get current Pearson correlation coefficient."""
        if self.n < 2:
            return 0.0

        cov = self.xy_sum / self.n
        x_std = self.x_stats.get_stats().std_dev
        y_std = self.y_stats.get_stats().std_dev

        if x_std == 0 or y_std == 0:
            return 0.0

        return cov / (x_std * y_std)


class ExponentialMovingAverage:
    """Exponential moving average (EMA)."""

    def __init__(self, alpha: Optional[float] = None, span: Optional[float] = None):
        if alpha is not None:
            self.alpha = alpha
        elif span is not None:
            self.alpha = 2.0 / (span + 1)
        else:
            self.alpha = 0.1

        self.value: Optional[float] = None
        self.count = 0

    def update(self, new_value: float) -> float:
        """Update EMA with new value."""
        self.count += 1
        if self.value is None:
            self.value = new_value
        else:
            self.value = self.alpha * new_value + (1 - self.alpha) * self.value
        return self.value

    def get_value(self) -> Optional[float]:
        """Get current EMA value."""
        return self.value


class DataRollingStatisticsAction:
    """
    Rolling window statistics for streaming data.

    Example:
        stats = DataRollingStatisticsAction(window_size=100)
        stats.push(1.0)
        stats.push(2.0)
        result = stats.get_stats()
        print(f"Mean: {result.mean}, StdDev: {result.std_dev}")

        # EMA
        ema = stats.create_ema(span=10)
        ema.update(10.0)
    """

    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self.window = RollingWindow(window_size)
        self.rolling_stats = RollingMean()

    def push(self, value: float) -> None:
        """Add value to rolling window."""
        self.window.push(value)
        self.rolling_stats.update(value)

    def get_stats(self) -> RollingStats:
        """Get current rolling statistics."""
        return self.rolling_stats.get_stats()

    def get_quantile(self, q: float) -> float:
        """Get approximate quantile from window."""
        tdigest = RollingQuantile()
        for v in self.window.get_all():
            tdigest.update(v)
        return tdigest.quantile(q)

    def create_ema(self, alpha: Optional[float] = None, span: Optional[float] = None) -> ExponentialMovingAverage:
        """Create EMA calculator."""
        return ExponentialMovingAverage(alpha=alpha, span=span)

    def create_correlation(self) -> RollingCorrelation:
        """Create correlation calculator for bivariate data."""
        return RollingCorrelation()

    def reset(self) -> None:
        """Reset all statistics."""
        self.window = RollingWindow(self.window_size)
        self.rolling_stats = RollingMean()
