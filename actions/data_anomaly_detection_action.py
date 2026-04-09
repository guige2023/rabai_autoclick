"""Data Anomaly Detection Action module.

Provides anomaly detection capabilities for data streams and datasets.
Supports statistical methods (z-score, IQR), isolation forest,
and custom detection rules.
"""

from __future__ import annotations

import math
import statistics
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

import numpy as np


class AnomalyType(Enum):
    """Types of anomalies."""

    POINT = "point"  # Single point anomaly
    CONTEXTUAL = "contextual"  # Anomaly given context
    COLLECTIVE = "collective"  # Collection of anomalous points


@dataclass
class AnomalyResult:
    """Result of anomaly detection."""

    is_anomaly: bool
    score: float
    anomaly_type: AnomalyType
    details: dict[str, Any] = field(default_factory=dict)
    timestamp: Optional[float] = None


@dataclass
class ZScoreConfig:
    """Configuration for z-score anomaly detection."""

    threshold: float = 3.0
    min_samples: int = 3
    use_median: bool = False


class ZScoreDetector:
    """Z-score based anomaly detector."""

    def __init__(self, config: Optional[ZScoreConfig] = None):
        self.config = config or ZScoreConfig()
        self._values: list[float] = []
        self._mean: Optional[float] = None
        self._std: Optional[float] = None

    def add(self, value: float) -> AnomalyResult:
        """Add a value and check for anomaly."""
        self._values.append(value)

        if len(self._values) < self.config.min_samples:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                anomaly_type=AnomalyType.POINT,
                details={"reason": "insufficient_samples"},
            )

        if self.config.use_median:
            center = statistics.median(self._values)
            spread = statistics.stdev(self._values) if len(self._values) > 1 else 1.0
        else:
            center = statistics.mean(self._values)
            spread = statistics.stdev(self._values) if len(self._values) > 1 else 1.0

        z_score = abs((value - center) / spread) if spread > 0 else 0.0

        is_anomaly = z_score > self.config.threshold

        return AnomalyResult(
            is_anomaly=is_anomaly,
            score=z_score,
            anomaly_type=AnomalyType.POINT,
            details={
                "z_score": z_score,
                "threshold": self.config.threshold,
                "center": center,
                "spread": spread,
            },
        )

    def reset(self) -> None:
        """Reset detector state."""
        self._values.clear()
        self._mean = None
        self._std = None


@dataclass
class IQRConfig:
    """Configuration for IQR-based anomaly detection."""

    multiplier: float = 1.5
    min_samples: int = 4


class IQRDetector:
    """Interquartile range anomaly detector."""

    def __init__(self, config: Optional[IQRConfig] = None):
        self.config = config or IQRConfig()
        self._values: list[float] = []

    def add(self, value: float) -> AnomalyResult:
        """Add a value and check for anomaly."""
        self._values.append(value)

        if len(self._values) < self.config.min_samples:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                anomaly_type=AnomalyType.POINT,
                details={"reason": "insufficient_samples"},
            )

        sorted_vals = sorted(self._values)
        n = len(sorted_vals)

        q1_idx = n // 4
        q3_idx = (3 * n) // 4
        q1 = sorted_vals[q1_idx]
        q3 = sorted_vals[q3_idx]
        iqr = q3 - q1

        lower_bound = q1 - self.config.multiplier * iqr
        upper_bound = q3 + self.config.multiplier * iqr

        is_anomaly = value < lower_bound or value > upper_bound

        distance = 0.0
        if is_anomaly:
            if value < lower_bound:
                distance = lower_bound - value
            else:
                distance = value - upper_bound

        return AnomalyResult(
            is_anomaly=is_anomaly,
            score=distance,
            anomaly_type=AnomalyType.POINT,
            details={
                "q1": q1,
                "q3": q3,
                "iqr": iqr,
                "lower_bound": lower_bound,
                "upper_bound": upper_bound,
            },
        )

    def reset(self) -> None:
        """Reset detector state."""
        self._values.clear()


@dataclass
class RollingStatsConfig:
    """Configuration for rolling window statistics."""

    window_size: int = 100


class RollingAnomalyDetector:
    """Rolling window anomaly detector."""

    def __init__(self, config: Optional[RollingStatsConfig] = None):
        self.config = config or RollingStatsConfig()
        self._window: deque[float] = deque(maxlen=self.config.window_size)

    def add(self, value: float) -> AnomalyResult:
        """Add value and detect anomaly."""
        self._window.append(value)

        if len(self._window) < self.config.window_size // 2:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                anomaly_type=AnomalyType.POINT,
                details={"reason": "window_not_full"},
            )

        values = list(self._window)
        mean = statistics.mean(values)
        std = statistics.stdev(values) if len(values) > 1 else 1.0

        z_score = abs((value - mean) / std) if std > 0 else 0.0
        is_anomaly = z_score > 2.5

        return AnomalyResult(
            is_anomaly=is_anomaly,
            score=z_score,
            anomaly_type=AnomalyType.POINT,
            details={
                "window_size": len(self._window),
                "mean": mean,
                "std": std,
                "z_score": z_score,
            },
        )

    def reset(self) -> None:
        """Reset detector."""
        self._window.clear()


class ChangePointDetector:
    """Cumulative sum (CUSUM) based change point detector."""

    def __init__(
        self,
        threshold: float = 5.0,
        drift: float = 0.5,
        min_samples: int = 30,
    ):
        self.threshold = threshold
        self.drift = drift
        self.min_samples = min_samples
        self._values: list[float] = []
        self._cusum_pos = 0.0
        self._cusum_neg = 0.0

    def add(self, value: float, expected: float = 0.0) -> AnomalyResult:
        """Add value and check for change point."""
        self._values.append(value)

        if len(self._values) < self.min_samples:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                anomaly_type=AnomalyType.COLLECTIVE,
                details={"reason": "insufficient_samples"},
            )

        deviation = value - expected

        self._cusum_pos = max(0.0, self._cusum_pos + deviation - self.drift)
        self._cusum_neg = max(0.0, self._cusum_neg - deviation - self.drift)

        cusum_score = max(self._cusum_pos, self._cusum_neg)
        is_anomaly = cusum_score > self.threshold

        return AnomalyResult(
            is_anomaly=is_anomaly,
            score=cusum_score,
            anomaly_type=AnomalyType.COLLECTIVE if is_anomaly else AnomalyType.POINT,
            details={
                "cusum_pos": self._cusum_pos,
                "cusum_neg": self._cusum_neg,
                "threshold": self.threshold,
            },
        )

    def reset(self) -> None:
        """Reset detector."""
        self._values.clear()
        self._cusum_pos = 0.0
        self._cusum_neg = 0.0
