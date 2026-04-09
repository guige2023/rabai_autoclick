"""
Anomaly Detection Utilities for UI Automation.

This module provides anomaly detection utilities for identifying
unusual patterns in UI elements, user behavior, and test results.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Tuple, Dict, Any
from enum import Enum


class AnomalyMethod(Enum):
    """Anomaly detection methods."""
    ZSCORE = "zscore"
    IQR = "iqr"
    MAD = "mad"
    ISOLATION_FOREST = "isolation_forest"
    DBSCAN = "dbscan"


@dataclass
class DataPoint:
    """A single data point with timestamp and value."""
    timestamp: float
    value: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AnomalyResult:
    """Result of anomaly detection."""
    is_anomaly: bool
    score: float
    threshold: float
    method: AnomalyMethod
    details: Dict[str, Any] = field(default_factory=dict)


class ZScoreDetector:
    """Z-score based anomaly detector."""

    def __init__(self, threshold: float = 3.0, min_samples: int = 5):
        """
        Initialize Z-score detector.

        Args:
            threshold: Z-score threshold for anomaly (default: 3.0)
            min_samples: Minimum samples needed before detection
        """
        self.threshold = threshold
        self.min_samples = min_samples
        self._values: List[float] = []
        self._mean: Optional[float] = None
        self._std: Optional[float] = None

    def add(self, value: float) -> AnomalyResult:
        """
        Add a value and check for anomaly.

        Args:
            value: The value to check

        Returns:
            AnomalyResult with detection details
        """
        self._values.append(value)
        return self.detect()

    def detect(self) -> AnomalyResult:
        """
        Detect anomaly in current data.

        Returns:
            AnomalyResult
        """
        if len(self._values) < self.min_samples:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                threshold=self.threshold,
                method=AnomalyMethod.ZSCORE,
                details={"reason": "insufficient_samples"}
            )

        self._mean = statistics.mean(self._values)
        self._std = statistics.stdev(self._values)

        if self._std == 0:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                threshold=self.threshold,
                method=AnomalyMethod.ZSCORE,
                details={"reason": "no_variance"}
            )

        latest = self._values[-1]
        zscore = abs(latest - self._mean) / self._std

        return AnomalyResult(
            is_anomaly=zscore > self.threshold,
            score=zscore,
            threshold=self.threshold,
            method=AnomalyMethod.ZSCORE,
            details={
                "mean": self._mean,
                "std": self._std,
                "latest": latest
            }
        )

    def reset(self) -> None:
        """Reset detector state."""
        self._values.clear()
        self._mean = None
        self._std = None


class IQRDetector:
    """Interquartile range based anomaly detector."""

    def __init__(self, k: float = 1.5, min_samples: int = 5):
        """
        Initialize IQR detector.

        Args:
            k: Multiplier for IQR (default: 1.5)
            min_samples: Minimum samples needed before detection
        """
        self.k = k
        self.min_samples = min_samples
        self._values: List[float] = []

    def add(self, value: float) -> AnomalyResult:
        """Add a value and check for anomaly."""
        self._values.append(value)
        return self.detect()

    def detect(self) -> AnomalyResult:
        """Detect anomaly using IQR method."""
        if len(self._values) < self.min_samples:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                threshold=0.0,
                method=AnomalyMethod.IQR,
                details={"reason": "insufficient_samples"}
            )

        sorted_vals = sorted(self._values)
        n = len(sorted_vals)
        q1_idx = n // 4
        q3_idx = 3 * n // 4
        q1 = sorted_vals[q1_idx]
        q3 = sorted_vals[q3_idx]
        iqr = q3 - q1

        lower_bound = q1 - self.k * iqr
        upper_bound = q3 + self.k * iqr

        latest = self._values[-1]
        distance = 0.0
        if latest < lower_bound:
            distance = lower_bound - latest
        elif latest > upper_bound:
            distance = latest - upper_bound

        max_distance = max(abs(latest - lower_bound), abs(latest - upper_bound))
        score = distance / max_distance if max_distance > 0 else 0.0

        return AnomalyResult(
            is_anomaly=latest < lower_bound or latest > upper_bound,
            score=score,
            threshold=0.0,
            method=AnomalyMethod.IQR,
            details={
                "q1": q1,
                "q3": q3,
                "iqr": iqr,
                "lower_bound": lower_bound,
                "upper_bound": upper_bound
            }
        )

    def reset(self) -> None:
        """Reset detector state."""
        self._values.clear()


class MADDetector:
    """Median Absolute Deviation based anomaly detector."""

    def __init__(self, threshold: float = 3.5, min_samples: int = 5):
        """
        Initialize MAD detector.

        Args:
            threshold: Threshold multiplier for MAD (default: 3.5)
            min_samples: Minimum samples needed before detection
        """
        self.threshold = threshold
        self.min_samples = min_samples
        self._values: List[float] = []

    def add(self, value: float) -> AnomalyResult:
        """Add a value and check for anomaly."""
        self._values.append(value)
        return self.detect()

    def detect(self) -> AnomalyResult:
        """Detect anomaly using MAD method."""
        if len(self._values) < self.min_samples:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                threshold=self.threshold,
                method=AnomalyMethod.MAD,
                details={"reason": "insufficient_samples"}
            )

        median = statistics.median(self._values)
        deviations = [abs(v - median) for v in self._values]
        mad = statistics.median(deviations)

        if mad == 0:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                threshold=self.threshold,
                method=AnomalyMethod.MAD,
                details={"reason": "zero_mad"}
            )

        latest = self._values[-1]
        modified_z = 0.6745 * abs(latest - median) / mad

        return AnomalyResult(
            is_anomaly=modified_z > self.threshold,
            score=modified_z,
            threshold=self.threshold,
            method=AnomalyMethod.MAD,
            details={
                "median": median,
                "mad": mad,
                "latest": latest
            }
        )

    def reset(self) -> None:
        """Reset detector state."""
        self._values.clear()


class AnomalyDetector:
    """
    Unified anomaly detector supporting multiple methods.

    This is the main entry point for anomaly detection in UI automation.
    """

    DETECTOR_MAP = {
        AnomalyMethod.ZSCORE: ZScoreDetector,
        AnomalyMethod.IQR: IQRDetector,
        AnomalyMethod.MAD: MADDetector,
    }

    def __init__(
        self,
        method: AnomalyMethod = AnomalyMethod.ZSCORE,
        threshold: float = 3.0,
        min_samples: int = 5,
        **kwargs
    ):
        """
        Initialize anomaly detector.

        Args:
            method: Detection method to use
            threshold: Threshold for the method
            min_samples: Minimum samples before detection starts
            **kwargs: Additional method-specific arguments
        """
        self.method = method
        self.detector = self.DETECTOR_MAP[method](
            threshold=threshold,
            min_samples=min_samples,
            **kwargs
        )

    def add(self, value: float) -> AnomalyResult:
        """Add a value and check for anomaly."""
        return self.detector.add(value)

    def detect(self) -> AnomalyResult:
        """Detect anomaly in current data."""
        return self.detector.detect()

    def reset(self) -> None:
        """Reset detector state."""
        self.detector.reset()


def detect_anomalies_in_series(
    values: List[float],
    method: AnomalyMethod = AnomalyMethod.ZSCORE,
    threshold: float = 3.0,
    min_samples: int = 5
) -> List[AnomalyResult]:
    """
    Detect anomalies in a series of values.

    Args:
        values: List of values to analyze
        method: Detection method to use
        threshold: Threshold for the method
        min_samples: Minimum samples before detection starts

    Returns:
        List of AnomalyResult for each value
    """
    detector = AnomalyDetector(method=method, threshold=threshold, min_samples=min_samples)
    results = []
    for value in values:
        result = detector.add(value)
        results.append(result)
    return results


def calculate_anomaly_score(
    values: List[float],
    point_index: int,
    window_size: int = 10,
    method: str = "zscore"
) -> float:
    """
    Calculate anomaly score for a specific point in a window.

    Args:
        values: Full list of values
        point_index: Index of point to score
        window_size: Size of sliding window
        method: Scoring method ("zscore" or "iqr")

    Returns:
        Anomaly score (higher = more anomalous)
    """
    if point_index < window_size:
        return 0.0

    start = max(0, point_index - window_size)
    window = values[start:point_index + 1]

    if len(window) < 3:
        return 0.0

    if method == "zscore":
        mean = statistics.mean(window[:-1])
        std = statistics.stdev(window[:-1]) if len(window) > 2 else 1.0
        if std == 0:
            return 0.0
        zscore = abs(window[-1] - mean) / std
        return zscore
    elif method == "iqr":
        sorted_window = sorted(window[:-1])
        n = len(sorted_window)
        q1 = sorted_window[n // 4]
        q3 = sorted_window[3 * n // 4]
        iqr = q3 - q1
        if iqr == 0:
            return 0.0
        latest = window[-1]
        if latest < q1:
            return (q1 - latest) / iqr
        elif latest > q3:
            return (latest - q3) / iqr
        return 0.0
    return 0.0
