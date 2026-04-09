"""Anomaly Detection Action Module.

Detect anomalies in data streams using statistical methods.
"""

from __future__ import annotations

import asyncio
import math
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

from .data_validator_action import ValidationResult


class AnomalyType(Enum):
    """Type of anomaly detected."""
    NONE = "none"
    SPIKE = "spike"
    DROP = "drop"
    VALUE_OUTLIER = "value_outlier"
    RATE_CHANGE = "rate_change"
    PATTERN_BREAK = "pattern_break"


@dataclass
class AnomalyResult:
    """Result of anomaly detection."""
    is_anomaly: bool
    anomaly_type: AnomalyType
    score: float
    threshold: float
    message: str
    timestamp: float


class StatisticalAnomalyDetector:
    """Statistical anomaly detector using moving average and standard deviation."""

    def __init__(
        self,
        window_size: int = 100,
        threshold_std: float = 3.0,
        min_data_points: int = 10
    ) -> None:
        self.window_size = window_size
        self.threshold_std = threshold_std
        self.min_data_points = min_data_points
        self._values: deque[float] = deque(maxlen=window_size)
        self._timestamps: deque[float] = deque(maxlen=window_size)
        self._lock = asyncio.Lock()

    async def add(self, value: float, timestamp: float | None = None) -> AnomalyResult:
        """Add a value and check for anomaly."""
        async with self._lock:
            timestamp = timestamp or time.time()
            self._values.append(value)
            self._timestamps.append(timestamp)
            return self._detect()

    def _detect(self) -> AnomalyResult:
        """Detect anomaly using statistical methods."""
        if len(self._values) < self.min_data_points:
            return AnomalyResult(
                is_anomaly=False,
                anomaly_type=AnomalyType.NONE,
                score=0.0,
                threshold=self.threshold_std,
                message="Insufficient data points",
                timestamp=time.time()
            )
        values = list(self._values)
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        std = math.sqrt(variance)
        latest = values[-1]
        if std == 0:
            if latest != mean:
                return AnomalyResult(
                    is_anomaly=True,
                    anomaly_type=AnomalyType.VALUE_OUTLIER,
                    score=float('inf'),
                    threshold=self.threshold_std,
                    message=f"Constant value {mean} but got {latest}",
                    timestamp=time.time()
                )
            return AnomalyResult(
                is_anomaly=False,
                anomaly_type=AnomalyType.NONE,
                score=0.0,
                threshold=self.threshold_std,
                message="No variance in data",
                timestamp=time.time()
            )
        z_score = abs((latest - mean) / std)
        if latest > mean:
            anomaly_type = AnomalyType.SPIKE
        elif latest < mean:
            anomaly_type = AnomalyType.DROP
        else:
            anomaly_type = AnomalyType.VALUE_OUTLIER
        is_anomaly = z_score > self.threshold_std
        return AnomalyResult(
            is_anomaly=is_anomaly,
            anomaly_type=anomaly_type if is_anomaly else AnomalyType.NONE,
            score=z_score,
            threshold=self.threshold_std,
            message=f"Value {latest} has z-score {z_score:.2f}",
            timestamp=time.time()
        )


class ThresholdAnomalyDetector:
    """Anomaly detector using fixed or dynamic thresholds."""

    def __init__(
        self,
        min_threshold: float | None = None,
        max_threshold: float | None = None
    ) -> None:
        self.min_threshold = min_threshold
        self.max_threshold = max_threshold

    async def check(self, value: float) -> AnomalyResult:
        """Check value against thresholds."""
        is_anomaly = False
        anomaly_type = AnomalyType.NONE
        message = "Value within thresholds"
        if self.min_threshold is not None and value < self.min_threshold:
            is_anomaly = True
            anomaly_type = AnomalyType.DROP
            message = f"Value {value} below minimum {self.min_threshold}"
        elif self.max_threshold is not None and value > self.max_threshold:
            is_anomaly = True
            anomaly_type = AnomalyType.SPIKE
            message = f"Value {value} above maximum {self.max_threshold}"
        return AnomalyResult(
            is_anomaly=is_anomaly,
            anomaly_type=anomaly_type,
            score=0.0,
            threshold=max(self.min_threshold or 0, self.max_threshold or float('inf')),
            message=message,
            timestamp=time.time()
        )


class CompositeAnomalyDetector:
    """Composite detector combining multiple detection methods."""

    def __init__(self) -> None:
        self._detectors: list[tuple[str, Callable]] = []

    def add_detector(self, name: str, detector: Callable) -> None:
        """Add a detector function."""
        self._detectors.append((name, detector))

    async def check(self, value: float) -> list[AnomalyResult]:
        """Check value with all detectors."""
        results = []
        for name, detector in self._detectors:
            if asyncio.iscoroutinefunction(detector):
                result = await detector(value)
            else:
                result = detector(value)
            if hasattr(result, 'is_anomaly'):
                results.append(result)
        return results
