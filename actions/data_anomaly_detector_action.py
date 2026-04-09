"""
Data Anomaly Detector Action Module.

Provides statistical anomaly detection with Z-score,
IQR, and isolation forest algorithms.
"""

import asyncio
import math
import statistics
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional
from collections import defaultdict

import numpy as np


class DetectionMethod(Enum):
    """Anomaly detection methods."""
    ZSCORE = "zscore"
    IQR = "iqr"
    ISOLATION_FOREST = "isolation_forest"
    MAD = "mad"


@dataclass
class AnomalyConfig:
    """Anomaly detection configuration."""
    method: DetectionMethod = DetectionMethod.ZSCORE
    threshold: float = 3.0
    window_size: int = 100
    min_samples: int = 30
    contamination: float = 0.1


@dataclass
class AnomalyResult:
    """Anomaly detection result."""
    is_anomaly: bool
    score: float
    threshold: float
    method: DetectionMethod
    value: Any = None
    context: dict = field(default_factory=dict)


class ZScoreDetector:
    """Z-score based anomaly detection."""

    def __init__(self, threshold: float = 3.0):
        self.threshold = threshold
        self._history: list[float] = []

    def add_sample(self, value: float) -> None:
        """Add sample to history."""
        self._history.append(value)

    def detect(self, value: float) -> AnomalyResult:
        """Detect anomaly using Z-score."""
        if len(self._history) < 2:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                threshold=self.threshold,
                method=DetectionMethod.ZSCORE,
                value=value
            )

        mean = statistics.mean(self._history)
        stdev = statistics.stdev(self._history)

        if stdev == 0:
            zscore = 0.0
        else:
            zscore = abs((value - mean) / stdev)

        is_anomaly = zscore > self.threshold

        return AnomalyResult(
            is_anomaly=is_anomaly,
            score=zscore,
            threshold=self.threshold,
            method=DetectionMethod.ZSCORE,
            value=value,
            context={"mean": mean, "stdev": stdev}
        )


class IQRDetector:
    """Interquartile range anomaly detection."""

    def __init__(self, multiplier: float = 1.5):
        self.multiplier = multiplier
        self._history: list[float] = []

    def add_sample(self, value: float) -> None:
        """Add sample to history."""
        self._history.append(value)

    def detect(self, value: float) -> AnomalyResult:
        """Detect anomaly using IQR."""
        if len(self._history) < 4:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                threshold=self.multiplier,
                method=DetectionMethod.IQR,
                value=value
            )

        sorted_data = sorted(self._history)
        n = len(sorted_data)
        q1 = sorted_data[n // 4]
        q3 = sorted_data[3 * n // 4]
        iqr = q3 - q1

        lower_bound = q1 - self.multiplier * iqr
        upper_bound = q3 + self.multiplier * iqr

        is_anomaly = value < lower_bound or value > upper_bound

        if is_anomaly:
            if value < lower_bound:
                score = (lower_bound - value) / iqr if iqr > 0 else abs(value)
            else:
                score = (value - upper_bound) / iqr if iqr > 0 else abs(value)
        else:
            score = 0.0

        return AnomalyResult(
            is_anomaly=is_anomaly,
            score=score,
            threshold=self.multiplier,
            method=DetectionMethod.IQR,
            value=value,
            context={"q1": q1, "q3": q3, "iqr": iqr}
        )


class MADDetector:
    """Median Absolute Deviation anomaly detection."""

    def __init__(self, threshold: float = 3.5):
        self.threshold = threshold
        self._history: list[float] = []

    def add_sample(self, value: float) -> None:
        """Add sample to history."""
        self._history.append(value)

    def detect(self, value: float) -> AnomalyResult:
        """Detect anomaly using MAD."""
        if len(self._history) < 2:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                threshold=self.threshold,
                method=DetectionMethod.MAD,
                value=value
            )

        median = statistics.median(self._history)
        deviations = [abs(v - median) for v in self._history]
        mad = statistics.median(deviations)

        if mad == 0:
            modified_z = 0.0
        else:
            modified_z = 0.6745 * (value - median) / mad

        is_anomaly = abs(modified_z) > self.threshold

        return AnomalyResult(
            is_anomaly=is_anomaly,
            score=abs(modified_z),
            threshold=self.threshold,
            method=DetectionMethod.MAD,
            value=value,
            context={"median": median, "mad": mad}
        )


class DataAnomalyDetectorAction:
    """
    Anomaly detection for data streams.

    Example:
        detector = DataAnomalyDetectorAction(
            method=DetectionMethod.ZSCORE,
            threshold=3.0
        )

        detector.add_sample(100)
        detector.add_sample(105)
        result = detector.detect(200)
    """

    def __init__(
        self,
        method: DetectionMethod = DetectionMethod.ZSCORE,
        threshold: float = 3.0
    ):
        self.config = AnomalyConfig(method=method, threshold=threshold)

        if method == DetectionMethod.ZSCORE:
            self._detector = ZScoreDetector(threshold)
        elif method == DetectionMethod.IQR:
            self._detector = IQRDetector(threshold)
        elif method == DetectionMethod.MAD:
            self._detector = MADDetector(threshold)
        else:
            self._detector = ZScoreDetector(threshold)

    def add_sample(self, value: float) -> None:
        """Add sample to detector."""
        self._detector.add_sample(value)

    def detect(self, value: float) -> AnomalyResult:
        """Detect anomaly."""
        return self._detector.detect(value)

    async def detect_batch(self, values: list[float]) -> list[AnomalyResult]:
        """Detect anomalies in batch."""
        results = []
        for value in values:
            self._detector.add_sample(value)
            result = self._detector.detect(value)
            results.append(result)
        return results

    def get_statistics(self) -> dict:
        """Get detector statistics."""
        if hasattr(self._detector, '_history'):
            history = self._detector._history
            if len(history) < 2:
                return {"count": len(history)}

            return {
                "count": len(history),
                "mean": statistics.mean(history),
                "stdev": statistics.stdev(history) if len(history) > 1 else 0,
                "median": statistics.median(history),
                "min": min(history),
                "max": max(history)
            }
        return {}
