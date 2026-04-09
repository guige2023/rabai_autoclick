"""Data anomaly detection action."""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional, Sequence


class AnomalyType(str, Enum):
    """Type of anomaly detected."""

    NONE = "none"
    POINT = "point"  # Single point anomaly
    CONTEXTUAL = "contextual"  # Anomaly in specific context
    COLLECTIVE = "collective"  # Sequence of anomalous points


@dataclass
class AnomalyResult:
    """Result of anomaly detection."""

    is_anomaly: bool
    anomaly_type: AnomalyType
    score: float  # 0-1, higher = more anomalous
    location: Optional[int] = None
    value: Optional[Any] = None
    expected: Optional[Any] = None
    deviation: Optional[float] = None
    message: Optional[str] = None


class DataAnomalyDetectorAction:
    """Detects anomalies in data streams and datasets."""

    def __init__(
        self,
        sensitivity: float = 0.5,
        window_size: int = 100,
        z_threshold: float = 3.0,
    ):
        """Initialize anomaly detector.

        Args:
            sensitivity: Detection sensitivity (0-1).
            window_size: Size of rolling window for stats.
            z_threshold: Z-score threshold for point anomalies.
        """
        self._sensitivity = sensitivity
        self._window_size = window_size
        self._z_threshold = z_threshold
        self._history: deque[float] = deque(maxlen=window_size)
        self._on_anomaly: Optional[Callable[[AnomalyResult], None]] = None

    def _mean(self, values: list[float]) -> float:
        """Calculate mean."""
        return sum(values) / len(values) if values else 0.0

    def _std(self, values: list[float]) -> float:
        """Calculate standard deviation."""
        if len(values) < 2:
            return 0.0
        mean = self._mean(values)
        variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
        return math.sqrt(variance)

    def _z_score(self, value: float, values: list[float]) -> float:
        """Calculate z-score."""
        mean = self._mean(values)
        std = self._std(values)
        if std == 0:
            return 0.0
        return abs(value - mean) / std

    def detect_z_score(
        self,
        data: Sequence[float],
        threshold: Optional[float] = None,
    ) -> list[AnomalyResult]:
        """Detect anomalies using z-score method.

        Args:
            data: Numeric data sequence.
            threshold: Z-score threshold.

        Returns:
            List of AnomalyResult for each point.
        """
        threshold = threshold or self._z_threshold
        results = []

        for i, value in enumerate(data):
            if i < 3:
                results.append(
                    AnomalyResult(
                        is_anomaly=False,
                        anomaly_type=AnomalyType.NONE,
                        score=0.0,
                        location=i,
                        value=value,
                    )
                )
                continue

            window = list(data[max(0, i - self._window_size) : i])
            z = self._z_score(value, window)
            score = min(z / (threshold * 2), 1.0)

            if z > threshold * self._sensitivity:
                expected = self._mean(window)
                results.append(
                    AnomalyResult(
                        is_anomaly=True,
                        anomaly_type=AnomalyType.POINT,
                        score=score,
                        location=i,
                        value=value,
                        expected=expected,
                        deviation=z,
                        message=f"Z-score {z:.2f} exceeds threshold {threshold}",
                    )
                )
            else:
                results.append(
                    AnomalyResult(
                        is_anomaly=False,
                        anomaly_type=AnomalyType.NONE,
                        score=score,
                        location=i,
                        value=value,
                    )
                )

        return results

    def detect_iqr(
        self,
        data: Sequence[float],
        multiplier: float = 1.5,
    ) -> list[AnomalyResult]:
        """Detect anomalies using IQR (Interquartile Range) method.

        Args:
            data: Numeric data sequence.
            multiplier: IQR multiplier for bounds.

        Returns:
            List of AnomalyResult for each point.
        """
        sorted_data = sorted(data)
        n = len(sorted_data)

        def percentile(values: list[float], p: float) -> float:
            idx = p * (len(values) - 1)
            lower = int(idx)
            upper = lower + 1
            if upper >= len(values):
                return values[lower]
            return values[lower] * (upper - idx) + values[upper] * (idx - lower)

        q1 = percentile(sorted_data, 0.25)
        q3 = percentile(sorted_data, 0.75)
        iqr = q3 - q1

        lower_bound = q1 - multiplier * iqr
        upper_bound = q3 + multiplier * iqr

        results = []
        for i, value in enumerate(data):
            is_anomaly = value < lower_bound or value > upper_bound
            score = 0.0

            if is_anomaly:
                if value < lower_bound:
                    deviation = lower_bound - value
                    score = min(deviation / abs(lower_bound) if lower_bound != 0 else 1.0, 1.0)
                else:
                    deviation = value - upper_bound
                    score = min(deviation / abs(upper_bound) if upper_bound != 0 else 1.0, 1.0)

            results.append(
                AnomalyResult(
                    is_anomaly=is_anomaly,
                    anomaly_type=AnomalyType.POINT if is_anomaly else AnomalyType.NONE,
                    score=score * self._sensitivity,
                    location=i,
                    value=value,
                    expected=(lower_bound + upper_bound) / 2,
                )
            )

        return results

    def detect_moving_average(
        self,
        data: Sequence[float],
        window_size: Optional[int] = None,
        threshold_multiplier: float = 2.0,
    ) -> list[AnomalyResult]:
        """Detect anomalies using moving average deviation.

        Args:
            data: Numeric data sequence.
            window_size: Size of moving window.
            threshold_multiplier: Std dev multiplier for threshold.

        Returns:
            List of AnomalyResult for each point.
        """
        window_size = window_size or self._window_size
        results = []

        for i in range(len(data)):
            if i < window_size:
                results.append(
                    AnomalyResult(
                        is_anomaly=False,
                        anomaly_type=AnomalyType.NONE,
                        score=0.0,
                        location=i,
                        value=data[i],
                    )
                )
                continue

            window = list(data[i - window_size : i])
            mean = self._mean(window)
            std = self._std(window)

            deviation = abs(data[i] - mean)
            threshold = threshold_multiplier * std if std > 0 else float("inf")

            is_anomaly = deviation > threshold * self._sensitivity
            score = min(deviation / threshold if threshold > 0 else 0.0, 1.0)

            results.append(
                AnomalyResult(
                    is_anomaly=is_anomaly,
                    anomaly_type=AnomalyType.POINT if is_anomaly else AnomalyType.NONE,
                    score=score,
                    location=i,
                    value=data[i],
                    expected=mean,
                    deviation=deviation,
                )
            )

        return results

    def set_anomaly_callback(
        self,
        callback: Callable[[AnomalyResult], None],
    ) -> None:
        """Set callback for anomaly detection."""
        self._on_anomaly = callback

    def get_summary(
        self,
        results: list[AnomalyResult],
    ) -> dict[str, Any]:
        """Get summary of detection results."""
        anomalies = [r for r in results if r.is_anomaly]
        return {
            "total_points": len(results),
            "anomaly_count": len(anomalies),
            "anomaly_rate": len(anomalies) / len(results) if results else 0,
            "avg_score": sum(r.score for r in anomalies) / len(anomalies)
            if anomalies
            else 0,
            "max_score": max((r.score for r in anomalies), default=0),
            "anomaly_types": {
                at.value: sum(1 for r in anomalies if r.anomaly_type == at)
                for at in AnomalyType
                if at != AnomalyType.NONE
            },
        }
