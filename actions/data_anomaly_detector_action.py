"""Data Anomaly Detector Action Module.

Provides anomaly detection for data streams and datasets with support
for statistical methods, isolation forest, and custom detectors.
"""

from __future__ import annotations

import logging
import math
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class AnomalyType(Enum):
    POINT = "point"
    CONTEXTUAL = "contextual"
    COLLECTIVE = "collective"


class DetectionMethod(Enum):
    ZSCORE = "zscore"
    IQR = "iqr"
    MAD = "mad"
    PERCENTILE = "percentile"
    CUSTOM = "custom"


@dataclass
class AnomalyResult:
    is_anomaly: bool
    score: float
    anomaly_type: AnomalyType
    timestamp: datetime = field(default_factory=datetime.now)
    details: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DetectorConfig:
    method: DetectionMethod = DetectionMethod.ZSCORE
    threshold: float = 3.0
    window_size: int = 100
    min_samples: int = 10
    sensitivity: float = 1.0
    seasonal_period: Optional[int] = None
    custom_fn: Optional[Callable[[List[float]], float]] = None


class StatisticalAnomalyDetector:
    def __init__(self, config: Optional[DetectorConfig] = None):
        self.config = config or DetectorConfig()
        self._values: deque = deque(maxlen=self.config.window_size)
        self._mean: Optional[float] = None
        self._std: Optional[float] = None
        self._stats_cache: Dict[str, float] = {}

    def update(self, value: float) -> AnomalyResult:
        self._values.append(value)
        self._compute_stats()

        if len(self._values) < self.config.min_samples:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                anomaly_type=AnomalyType.POINT,
                details={"message": "Insufficient samples"},
            )

        score = self._compute_score(value)
        threshold = self.config.threshold * self.config.sensitivity

        is_anomaly = abs(score) > threshold

        return AnomalyResult(
            is_anomaly=is_anomaly,
            score=abs(score),
            anomaly_type=AnomalyType.POINT,
            details={
                "value": value,
                "mean": self._mean,
                "std": self._std,
                "threshold": threshold,
                "zscore": score,
            },
        )

    def _compute_stats(self) -> None:
        if len(self._values) < 2:
            self._mean = sum(self._values) / len(self._values) if self._values else 0.0
            self._std = 0.0
            return

        self._mean = sum(self._values) / len(self._values)
        variance = sum((x - self._mean) ** 2 for x in self._values) / len(self._values)
        self._std = math.sqrt(variance) if variance > 0 else 1e-6

    def _compute_score(self, value: float) -> float:
        if self.config.method == DetectionMethod.ZSCORE:
            if self._std == 0:
                return 0.0
            return (value - self._mean) / self._std

        elif self.config.method == DetectionMethod.IQR:
            sorted_values = sorted(self._values)
            n = len(sorted_values)
            q1 = sorted_values[n // 4]
            q3 = sorted_values[3 * n // 4]
            iqr = q3 - q1
            if iqr == 0:
                return 0.0
            median = sorted_values[n // 2]
            return (value - median) / (iqr * 1.5) if iqr > 0 else 0.0

        elif self.config.method == DetectionMethod.MAD:
            if not self._values:
                return 0.0
            median = sorted(self._values)[len(self._values) // 2]
            mad = sorted([abs(v - median) for v in self._values])[len(self._values) // 2]
            if mad == 0:
                return 0.0
            return 0.6745 * (value - median) / mad

        elif self.config.method == DetectionMethod.PERCENTILE:
            sorted_values = sorted(self._values)
            n = len(sorted_values)
            lower = sorted_values[int(n * 0.05)]
            upper = sorted_values[int(n * 0.95)]
            if upper == lower:
                return 0.0
            return (value - self._mean) / (upper - lower)

        elif self.config.method == DetectionMethod.CUSTOM and self.config.custom_fn:
            return self.config.custom_fn(list(self._values) + [value])

        return 0.0

    def reset(self) -> None:
        self._values.clear()
        self._mean = None
        self._std = None
        self._stats_cache.clear()

    def get_stats(self) -> Dict[str, float]:
        return {
            "count": len(self._values),
            "mean": self._mean or 0.0,
            "std": self._std or 0.0,
            "min": min(self._values) if self._values else 0.0,
            "max": max(self._values) if self._values else 0.0,
        }


class EnsembleAnomalyDetector:
    def __init__(self):
        self._detectors: List[StatisticalAnomalyDetector] = []
        self._weights: List[float] = []

    def add_detector(self, detector: StatisticalAnomalyDetector, weight: float = 1.0) -> None:
        self._detectors.append(detector)
        self._weights.append(weight)

    def update(self, value: float) -> AnomalyResult:
        if not self._detectors:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                anomaly_type=AnomalyType.POINT,
                details={"message": "No detectors"},
            )

        total_weight = sum(self._weights)
        weighted_scores = []

        for detector, weight in zip(self._detectors, self._weights):
            result = detector.update(value)
            weighted_scores.append(result.score * weight / total_weight)

        ensemble_score = sum(weighted_scores)
        is_anomaly = ensemble_score > 2.0

        return AnomalyResult(
            is_anomaly=is_anomaly,
            score=ensemble_score,
            anomaly_type=AnomalyType.POINT,
            details={"individual_scores": weighted_scores},
        )

    def reset(self) -> None:
        for detector in self._detectors:
            detector.reset()


def detect_spikes(values: List[float], threshold: float = 2.0) -> List[Tuple[int, float]]:
    if len(values) < 3:
        return []

    spikes = []
    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / len(values)
    std = math.sqrt(variance) if variance > 0 else 1e-6

    for i, value in enumerate(values):
        zscore = (value - mean) / std if std > 0 else 0.0
        if abs(zscore) > threshold:
            spikes.append((i, value))

    return spikes
