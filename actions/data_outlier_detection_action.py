"""Data Outlier Detection Action module.

Detects outliers and anomalies in datasets using statistical
methods, clustering, and isolation-based techniques.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Optional

import numpy as np


@dataclass
class OutlierResult:
    """Result of outlier detection."""

    is_outlier: bool
    score: float
    method: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "is_outlier": self.is_outlier,
            "score": self.score,
            "method": self.method,
            "details": self.details,
        }


class ZScoreOutlierDetector:
    """Z-score based outlier detector.

    Flags values beyond a threshold number of standard
    deviations from the mean.
    """

    def __init__(
        self,
        threshold: float = 3.0,
        min_std: float = 1e-6,
    ):
        self.threshold = threshold
        self.min_std = min_std
        self._mean: Optional[float] = None
        self._std: Optional[float] = None
        self._values: list[float] = []

    def fit(self, values: list[float]) -> "ZScoreOutlierDetector":
        """Fit detector to data.

        Args:
            values: Training data

        Returns:
            Self
        """
        self._values = values
        self._mean = np.mean(values)
        self._std = np.std(values)
        return self

    def detect(self, value: float) -> OutlierResult:
        """Detect if value is an outlier.

        Args:
            value: Value to check

        Returns:
            OutlierResult
        """
        if self._mean is None or self._std is None:
            return OutlierResult(
                is_outlier=False,
                score=0.0,
                method="zscore",
                details={"error": "not fitted"},
            )

        if self._std < self.min_std:
            return OutlierResult(
                is_outlier=False,
                score=0.0,
                method="zscore",
                details={"error": "insufficient variance"},
            )

        z_score = abs((value - self._mean) / self._std)
        is_outlier = z_score > self.threshold

        return OutlierResult(
            is_outlier=is_outlier,
            score=z_score,
            method="zscore",
            details={
                "z_score": z_score,
                "threshold": self.threshold,
                "mean": self._mean,
                "std": self._std,
            },
        )

    def fit_detect(self, values: list[float]) -> list[OutlierResult]:
        """Fit and detect outliers in same data.

        Args:
            values: Data to check

        Returns:
            List of OutlierResults
        """
        self.fit(values)
        return [self.detect(v) for v in values]


class IQROutlierDetector:
    """Interquartile range (IQR) outlier detector.

    Flags values outside of Q1 - k*IQR to Q3 + k*IQR.
    """

    def __init__(
        self,
        multiplier: float = 1.5,
    ):
        self.multiplier = multiplier
        self._q1: Optional[float] = None
        self._q3: Optional[float] = None
        self._iqr: Optional[float] = None

    def fit(self, values: list[float]) -> "IQROutlierDetector":
        """Fit detector to data.

        Args:
            values: Training data

        Returns:
            Self
        """
        arr = np.array(values)
        self._q1 = np.percentile(arr, 25)
        self._q3 = np.percentile(arr, 75)
        self._iqr = self._q3 - self._q1
        return self

    def detect(self, value: float) -> OutlierResult:
        """Detect if value is an outlier.

        Args:
            value: Value to check

        Returns:
            OutlierResult
        """
        if any(v is None for v in [self._q1, self._q3, self._iqr]):
            return OutlierResult(
                is_outlier=False,
                score=0.0,
                method="iqr",
                details={"error": "not fitted"},
            )

        lower = self._q1 - self.multiplier * self._iqr
        upper = self._q3 + self.multiplier * self._iqr

        distance = 0.0
        if value < lower:
            distance = lower - value
        elif value > upper:
            distance = value - upper

        is_outlier = value < lower or value > upper

        return OutlierResult(
            is_outlier=is_outlier,
            score=distance,
            method="iqr",
            details={
                "q1": self._q1,
                "q3": self._q3,
                "iqr": self._iqr,
                "lower_bound": lower,
                "upper_bound": upper,
                "distance": distance,
            },
        )

    def fit_detect(self, values: list[float]) -> list[OutlierResult]:
        """Fit and detect outliers in same data."""
        self.fit(values)
        return [self.detect(v) for v in values]


class MADOutlierDetector:
    """Median Absolute Deviation (MAD) outlier detector.

    Robust to extreme values, uses median instead of mean.
    """

    def __init__(
        self,
        threshold: float = 3.5,
        multiplier: float = 1.4826,
    ):
        self.threshold = threshold
        self.multiplier = multiplier
        self._median: Optional[float] = None
        self._mad: Optional[float] = None

    def fit(self, values: list[float]) -> "MADOutlierDetector":
        """Fit detector to data.

        Args:
            values: Training data

        Returns:
            Self
        """
        self._median = np.median(values)
        median_abs_dev = [abs(v - self._median) for v in values]
        self._mad = self.multiplier * np.median(median_abs_dev)
        return self

    def detect(self, value: float) -> OutlierResult:
        """Detect if value is an outlier."""
        if self._median is None or self._mad is None:
            return OutlierResult(
                is_outlier=False,
                score=0.0,
                method="mad",
                details={"error": "not fitted"},
            )

        if self._mad < 1e-6:
            return OutlierResult(
                is_outlier=False,
                score=0.0,
                method="mad",
                details={"error": "zero MAD"},
            )

        score = abs(value - self._median) / self._mad
        is_outlier = score > self.threshold

        return OutlierResult(
            is_outlier=is_outlier,
            score=score,
            method="mad",
            details={
                "median": self._median,
                "mad": self._mad,
                "threshold": self.threshold,
                "modified_z_score": score,
            },
        )

    def fit_detect(self, values: list[float]) -> list[OutlierResult]:
        """Fit and detect outliers."""
        self.fit(values)
        return [self.detect(v) for v in values]


class IsolationForestOutlierDetector:
    """Isolation Forest based outlier detector.

    Works well for high-dimensional data and doesn't
    require normal distribution assumption.
    """

    def __init__(
        self,
        num_trees: int = 100,
        sample_size: Optional[int] = None,
        contamination: float = 0.1,
    ):
        self.num_trees = num_trees
        self.sample_size = sample_size
        self.contamination = contamination
        self._trees: list[dict[str, Any]] = []
        self._threshold: Optional[float] = None

    def fit(self, values: list[list[float]]) -> "IsolationForestOutlierDetector":
        """Fit detector to data."""
        import random

        n = len(values)
        sample_size = self.sample_size or min(256, n)

        self._trees = []

        for _ in range(self.num_trees):
            sample_indices = random.sample(range(n), sample_size)
            sample = [values[i] for i in sample_indices]

            tree = self._build_tree(sample)
            self._trees.append(tree)

        scores = self.score_samples(values)
        self._threshold = np.percentile(scores, (1 - self.contamination) * 100)

        return self

    def _build_tree(self, data: list[list[float]]) -> dict[str, Any]:
        """Build isolation tree recursively."""
        if len(data) <= 1:
            return {"leaf": True, "size": len(data)}

        num_features = len(data[0])
        feature_idx = random.randint(0, num_features - 1)
        min_val = min(d[feature_idx] for d in data)
        max_val = max(d[feature_idx] for d in data)

        if min_val == max_val:
            return {"leaf": True, "size": len(data)}

        split_val = random.uniform(min_val, max_val)

        left = [d for d in data if d[feature_idx] < split_val]
        right = [d for d in data if d[feature_idx] >= split_val]

        return {
            "leaf": False,
            "feature": feature_idx,
            "split": split_val,
            "left": self._build_tree(left) if left else {"leaf": True, "size": 0},
            "right": self._build_tree(right) if right else {"leaf": True, "size": 0},
        }

    def _path_length(self, point: list[float], tree: dict[str, Any], depth: int = 0) -> float:
        """Calculate path length for a point."""
        if tree.get("leaf"):
            if tree["size"] <= 1:
                return depth
            return depth + np.log(tree["size"])
        feature = tree["feature"]
        split = tree["split"]
        if point[feature] < split:
            return self._path_length(point, tree["left"], depth + 1)
        else:
            return self._path_length(point, tree["right"], depth + 1)

    def score_samples(self, data: list[list[float]]) -> list[float]:
        """Score samples for outlier-ness."""
        import math

        n = len(data)
        avg_path_length = []

        for point in data:
            total = 0
            for tree in self._trees:
                total += self._path_length(point, tree)
            avg_path_length.append(total / self.num_trees)

        c_n = 2 * (math.log(n - 1) + 0.5772156649) - (2 * (n - 1) / (n - 1))

        scores = []
        for path_len in avg_path_length:
            score = math.pow(2, -path_len / c_n)
            scores.append(score)

        return scores

    def detect(self, point: list[float]) -> OutlierResult:
        """Detect if point is an outlier."""
        if not self._trees or self._threshold is None:
            return OutlierResult(
                is_outlier=False,
                score=0.0,
                method="isolation_forest",
                details={"error": "not fitted"},
            )

        scores = self.score_samples([point])
        score = scores[0]
        is_outlier = score > self._threshold

        return OutlierResult(
            is_outlier=is_outlier,
            score=score,
            method="isolation_forest",
            details={
                "threshold": self._threshold,
                "contamination": self.contamination,
            },
        )


class DBSCANOutlierDetector:
    """DBSCAN-based outlier detector.

    Points not in any cluster are considered outliers.
    """

    def __init__(
        self,
        eps: float = 0.5,
        min_samples: int = 5,
    ):
        self.eps = eps
        self.min_samples = min_samples
        self._labels: Optional[list[int]] = None

    def fit(self, values: list[list[float]]) -> "DBSCANOutlierDetector":
        """Fit detector to data."""
        import random

        n = len(values)
        labels = [-1] * n
        cluster_id = 0

        for i in range(n):
            if labels[i] != -1:
                continue

            neighbors = self._region_query(values, i)

            if len(neighbors) < self.min_samples:
                continue

            labels[i] = cluster_id
            seed_list = list(neighbors)

            j = 0
            while j < len(seed_list):
                p = seed_list[j]
                if labels[p] == -1:
                    labels[p] = cluster_id
                    p_neighbors = self._region_query(values, p)
                    if len(p_neighbors) >= self.min_samples:
                        seed_list.extend(p_neighbors)
                j += 1

            cluster_id += 1

        self._labels = labels
        return self

    def _region_query(self, values: list[list[float]], idx: int) -> list[int]:
        """Find neighbors within eps distance."""
        neighbors = []
        point = values[idx]
        for i, other in enumerate(values):
            if self._distance(point, other) <= self.eps:
                neighbors.append(i)
        return neighbors

    def _distance(self, a: list[float], b: list[float]) -> float:
        """Calculate Euclidean distance."""
        return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))

    def detect(self, idx: int) -> OutlierResult:
        """Detect if point is an outlier."""
        if self._labels is None:
            return OutlierResult(
                is_outlier=False,
                score=0.0,
                method="dbscan",
                details={"error": "not fitted"},
            )

        label = self._labels[idx]
        is_outlier = label == -1

        return OutlierResult(
            is_outlier=is_outlier,
            score=1.0 if is_outlier else 0.0,
            method="dbscan",
            details={
                "cluster_id": label,
                "is_noise": is_outlier,
            },
        )
