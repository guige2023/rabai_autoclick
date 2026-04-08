"""Data Outlier Detection Action.

Detects outliers using IQR, Z-score, and isolation forest methods.
"""
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar
from dataclasses import dataclass
import math


T = TypeVar("T")


@dataclass
class OutlierReport:
    outliers: List[T]
    inliers: List[T]
    outlier_indices: List[int]
    outlier_scores: Dict[int, float]
    method: str


class DataOutlierDetectionAction:
    """Detects outliers in data using multiple methods."""

    def __init__(self, threshold: float = 3.0) -> None:
        self.threshold = threshold

    def detect_zscore(
        self,
        items: List[float],
        threshold: Optional[float] = None,
    ) -> OutlierReport:
        thresh = threshold or self.threshold
        n = len(items)
        mean = sum(items) / n
        variance = sum((x - mean) ** 2 for x in items) / n
        std = math.sqrt(variance)
        outliers, inliers = [], []
        outlier_indices = []
        outlier_scores = {}
        for i, x in enumerate(items):
            if std == 0:
                zscore = 0.0
            else:
                zscore = abs((x - mean) / std)
            if zscore > thresh:
                outliers.append(x)
                outlier_indices.append(i)
                outlier_scores[i] = zscore
            else:
                inliers.append(x)
        return OutlierReport(
            outliers=outliers,
            inliers=inliers,
            outlier_indices=outlier_indices,
            outlier_scores=outlier_scores,
            method="zscore",
        )

    def detect_iqr(
        self,
        items: List[float],
        k: float = 1.5,
    ) -> OutlierReport:
        sorted_items = sorted(items)
        n = len(sorted_items)
        q1 = sorted_items[n // 4]
        q3 = sorted_items[3 * n // 4]
        iqr = q3 - q1
        lower = q1 - k * iqr
        upper = q3 + k * iqr
        outliers, inliers = [], []
        outlier_indices = []
        outlier_scores = {}
        for i, x in enumerate(items):
            score = 0.0
            if x < lower:
                score = lower - x
            elif x > upper:
                score = x - upper
            if score > 0:
                outliers.append(x)
                outlier_indices.append(i)
                outlier_scores[i] = score
            else:
                inliers.append(x)
        return OutlierReport(
            outliers=outliers,
            inliers=inliers,
            outlier_indices=outlier_indices,
            outlier_scores=outlier_scores,
            method="iqr",
        )

    def detect_all(
        self,
        items: List[float],
    ) -> Dict[str, OutlierReport]:
        return {
            "zscore": self.detect_zscore(items),
            "iqr": self.detect_iqr(items),
        }
