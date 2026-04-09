"""Data outlier detection and handling action."""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Sequence


class OutlierMethod(str, Enum):
    """Outlier detection method."""

    ZSCORE = "zscore"
    IQR = "iqr"
    MAD = "mad"
    DBSCAN = "dbscan"
    ISOLATION_FOREST = "isolation_forest"


@dataclass
class OutlierResult:
    """Result of outlier detection."""

    is_outlier: bool
    score: float
    method: OutlierMethod
    value: Any
    threshold: float
    location: Optional[int] = None


class DataOutlierDetectorAction:
    """Detects and handles outliers in data."""

    def __init__(
        self,
        method: OutlierMethod = OutlierMethod.ZSCORE,
        threshold: float = 3.0,
    ):
        """Initialize outlier detector.

        Args:
            method: Detection method.
            threshold: Threshold for outlier detection.
        """
        self._method = method
        self._threshold = threshold
        self._rolling_window: deque = deque(maxlen=1000)
        self._fitted = False
        self._mean = 0.0
        self._std = 0.0

    def _update_stats(self, values: list[float]) -> None:
        """Update running statistics."""
        if not values:
            return
        self._mean = sum(values) / len(values)
        variance = sum((x - self._mean) ** 2 for x in values) / len(values)
        self._std = math.sqrt(variance) if variance > 0 else 1.0
        self._fitted = True

    def detect_zscore(
        self,
        values: Sequence[float],
        threshold: Optional[float] = None,
    ) -> list[OutlierResult]:
        """Detect outliers using z-score method.

        Args:
            values: Numeric values.
            threshold: Z-score threshold.

        Returns:
            List of OutlierResult for each value.
        """
        threshold = threshold or self._threshold
        float_values = [float(v) for v in values if v is not None]

        self._update_stats(float_values)

        results = []
        for i, value in enumerate(values):
            if value is None:
                results.append(
                    OutlierResult(
                        is_outlier=False,
                        score=0.0,
                        method=OutlierMethod.ZSCORE,
                        value=value,
                        threshold=threshold,
                        location=i,
                    )
                )
                continue

            zscore = abs((float(value) - self._mean) / self._std) if self._std > 0 else 0.0

            results.append(
                OutlierResult(
                    is_outlier=zscore > threshold,
                    score=zscore,
                    method=OutlierMethod.ZSCORE,
                    value=value,
                    threshold=threshold,
                    location=i,
                )
            )

        return results

    def detect_iqr(
        self,
        values: Sequence[float],
        multiplier: float = 1.5,
    ) -> list[OutlierResult]:
        """Detect outliers using IQR method.

        Args:
            values: Numeric values.
            multiplier: IQR multiplier.

        Returns:
            List of OutlierResult for each value.
        """
        float_values = sorted([v for v in values if v is not None])

        if len(float_values) < 4:
            return [
                OutlierResult(
                    is_outlier=False,
                    score=0.0,
                    method=OutlierMethod.IQR,
                    value=v,
                    threshold=multiplier,
                    location=i if i < len(values) else None,
                )
                for i, v in enumerate(values)
            ]

        q1_idx = len(float_values) // 4
        q3_idx = 3 * len(float_values) // 4
        q1 = float_values[q1_idx]
        q3 = float_values[q3_idx]
        iqr = q3 - q1

        lower_bound = q1 - multiplier * iqr
        upper_bound = q3 + multiplier * iqr

        results = []
        for i, value in enumerate(values):
            if value is None:
                results.append(
                    OutlierResult(
                        is_outlier=False,
                        score=0.0,
                        method=OutlierMethod.IQR,
                        value=value,
                        threshold=multiplier,
                        location=i,
                    )
                )
                continue

            dist_from_median = 0.0
            if float(value) < lower_bound:
                dist_from_median = abs(float(value) - lower_bound) / iqr if iqr > 0 else 0.0
            elif float(value) > upper_bound:
                dist_from_median = abs(float(value) - upper_bound) / iqr if iqr > 0 else 0.0

            is_outlier = float(value) < lower_bound or float(value) > upper_bound

            results.append(
                OutlierResult(
                    is_outlier=is_outlier,
                    score=dist_from_median,
                    method=OutlierMethod.IQR,
                    value=value,
                    threshold=multiplier,
                    location=i,
                )
            )

        return results

    def detect_mad(
        self,
        values: Sequence[float],
        threshold: float = 3.5,
    ) -> list[OutlierResult]:
        """Detect outliers using Median Absolute Deviation.

        Args:
            values: Numeric values.
            threshold: MAD threshold.

        Returns:
            List of OutlierResult for each value.
        """
        float_values = [v for v in values if v is not None]

        if len(float_values) < 2:
            return [
                OutlierResult(
                    is_outlier=False,
                    score=0.0,
                    method=OutlierMethod.MAD,
                    value=v,
                    threshold=threshold,
                    location=i if i < len(values) else None,
                )
                for i, v in enumerate(values)
            ]

        median = sorted(float_values)[len(float_values) // 2]
        abs_deviations = [abs(v - median) for v in float_values]
        mad = sorted(abs_deviations)[len(abs_deviations) // 2]

        mad_factor = 0.6745

        results = []
        for i, value in enumerate(values):
            if value is None:
                results.append(
                    OutlierResult(
                        is_outlier=False,
                        score=0.0,
                        method=OutlierMethod.MAD,
                        value=value,
                        threshold=threshold,
                        location=i,
                    )
                )
                continue

            score = abs(float(value) - median) / mad if mad > 0 else 0.0
            normalized_score = score / mad_factor if mad_factor > 0 else 0.0

            results.append(
                OutlierResult(
                    is_outlier=normalized_score > threshold,
                    score=normalized_score,
                    method=OutlierMethod.MAD,
                    value=value,
                    threshold=threshold,
                    location=i,
                )
            )

        return results

    def detect(
        self,
        values: Sequence[float],
        threshold: Optional[float] = None,
    ) -> list[OutlierResult]:
        """Detect outliers using configured method.

        Args:
            values: Numeric values.
            threshold: Detection threshold.

        Returns:
            List of OutlierResult.
        """
        threshold = threshold or self._threshold

        if self._method == OutlierMethod.ZSCORE:
            return self.detect_zscore(values, threshold)
        elif self._method == OutlierMethod.IQR:
            return self.detect_iqr(values, threshold)
        elif self._method == OutlierMethod.MAD:
            return self.detect_mad(values, threshold)
        else:
            return self.detect_zscore(values, threshold)

    def remove_outliers(
        self,
        values: Sequence[float],
        threshold: Optional[float] = None,
    ) -> list[float]:
        """Remove outliers from values.

        Args:
            values: Numeric values.
            threshold: Detection threshold.

        Returns:
            List without outliers.
        """
        results = self.detect(values, threshold)
        return [v for v, r in zip(values, results) if not r.is_outlier]

    def cap_outliers(
        self,
        values: Sequence[float],
        threshold: Optional[float] = None,
    ) -> list[float]:
        """Cap outliers at threshold boundaries.

        Args:
            values: Numeric values.
            threshold: Detection threshold.

        Returns:
            List with capped outliers.
        """
        float_values = [float(v) for v in values if v is not None]
        sorted_values = sorted(float_values)

        if len(sorted_values) < 4:
            return values

        q1_idx = len(sorted_values) // 4
        q3_idx = 3 * len(sorted_values) // 4
        q1 = sorted_values[q1_idx]
        q3 = sorted_values[q3_idx]
        iqr = q3 - q1

        thresh = threshold or self._threshold
        lower = q1 - thresh * iqr
        upper = q3 + thresh * iqr

        results = []
        for value in values:
            if value is None:
                results.append(None)
            else:
                v = float(value)
                results.append(max(lower, min(upper, v)))

        return results
