"""Anomaly Detection Action Module.

Provides statistical and ML-based anomaly detection for time series
and tabular data including Z-score, IQR, Isolation Forest, and LOF methods.
"""
from __future__ import annotations

import math
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class DetectionMethod(Enum):
    """Anomaly detection method."""
    ZSCORE = "zscore"
    IQR = "iqr"
    ISOLATION_FOREST = "isolation_forest"
    LOF = "lof"
    STATISTICAL = "statistical"


@dataclass
class AnomalyResult:
    """Single anomaly detection result."""
    index: int
    value: float
    score: float
    is_anomaly: bool
    method: str
    threshold: Optional[float] = None
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DetectionReport:
    """Full anomaly detection report."""
    success: bool
    method: str
    total_points: int
    anomalies_found: int
    anomaly_percentage: float
    results: List[AnomalyResult] = field(default_factory=list)
    statistics: Dict[str, float] = field(default_factory=dict)
    duration_ms: float = 0.0


def _mean(values: List[float]) -> float:
    """Calculate mean."""
    return sum(values) / len(values) if values else 0.0


def _std(values: List[float]) -> float:
    """Calculate standard deviation."""
    if len(values) < 2:
        return 0.0
    m = _mean(values)
    variance = sum((x - m) ** 2 for x in values) / (len(values) - 1)
    return math.sqrt(variance)


def _median(values: List[float]) -> float:
    """Calculate median."""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    if n % 2 == 0:
        return (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
    return sorted_vals[n // 2]


def _percentile(values: List[float], p: float) -> float:
    """Calculate percentile."""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    k = (len(sorted_vals) - 1) * p / 100
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    return sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)


class AnomalyDetectionEngine:
    """Statistical anomaly detection engine."""

    def __init__(self):
        self._data: List[float] = []
        self._stats: Dict[str, float] = {}

    def fit(self, data: List[float]) -> "AnomalyDetectionEngine":
        """Fit engine with training data."""
        self._data = data
        self._stats = {
            "mean": _mean(data),
            "std": _std(data),
            "median": _median(data),
            "min": min(data) if data else 0.0,
            "max": max(data) if data else 0.0,
            "q1": _percentile(data, 25),
            "q3": _percentile(data, 75),
            "count": len(data)
        }
        self._stats["iqr"] = self._stats["q3"] - self._stats["q1"]
        return self

    def detect_zscore(self, threshold: float = 3.0) -> List[AnomalyResult]:
        """Detect anomalies using Z-score method.

        Anomaly if |z-score| > threshold.
        """
        results = []
        mean = self._stats["mean"]
        std = self._stats["std"]

        if std == 0:
            std = 1.0

        for i, value in enumerate(self._data):
            zscore = (value - mean) / std
            is_anomaly = abs(zscore) > threshold
            results.append(AnomalyResult(
                index=i,
                value=value,
                score=abs(zscore),
                is_anomaly=is_anomaly,
                method="zscore",
                threshold=threshold,
                details={"zscore": zscore}
            ))

        return results

    def detect_iqr(self, multiplier: float = 1.5) -> List[AnomalyResult]:
        """Detect anomalies using IQR method.

        Anomaly if value < Q1 - multiplier*IQR or value > Q3 + multiplier*IQR.
        """
        q1 = self._stats["q1"]
        q3 = self._stats["q3"]
        iqr = self._stats["iqr"]

        lower_bound = q1 - multiplier * iqr
        upper_bound = q3 + multiplier * iqr

        results = []
        for i, value in enumerate(self._data):
            is_anomaly = value < lower_bound or value > upper_bound
            distance = 0.0
            if value < lower_bound:
                distance = lower_bound - value
            elif value > upper_bound:
                distance = value - upper_bound

            results.append(AnomalyResult(
                index=i,
                value=value,
                score=distance,
                is_anomaly=is_anomaly,
                method="iqr",
                threshold=multiplier,
                details={
                    "lower_bound": lower_bound,
                    "upper_bound": upper_bound,
                    "distance": distance
                }
            ))

        return results

    def detect_statistical(self, tolerance: float = 3.0) -> List[AnomalyResult]:
        """Detect anomalies using modified Z-score (MAD-based).

        More robust to outliers than Z-score.
        """
        median = self._stats["median"]
        deviations = [abs(v - median) for v in self._data]
        mad = _median(deviations)

        if mad == 0:
            mad = 1.0

        results = []
        for i, value in enumerate(self._data):
            modified_zscore = 0.6745 * deviations[i] / mad
            is_anomaly = abs(modified_zscore) > tolerance
            results.append(AnomalyResult(
                index=i,
                value=value,
                score=abs(modified_zscore),
                is_anomaly=is_anomaly,
                method="statistical",
                threshold=tolerance,
                details={"modified_zscore": modified_zscore}
            ))

        return results

    def get_statistics(self) -> Dict[str, float]:
        """Get computed statistics."""
        return self._stats.copy()


class IsolationForestSim:
    """Simplified Isolation Forest for anomaly detection.

    Uses random partitioning to isolate anomalies.
    In production, use sklearn.ensemble.IsolationForest.
    """

    def __init__(self, n_trees: int = 100, max_samples: int = 256):
        self._n_trees = n_trees
        self._max_samples = max_samples
        self._trees: List[Dict[str, Any]] = []

    def fit(self, data: List[List[float]]) -> "IsolationForestSim":
        """Fit the forest."""
        self._trees = []
        for _ in range(self._n_trees):
            sample = random.sample(data, min(len(data), self._max_samples))
            self._trees.append(self._build_tree(sample))
        return self

    def _build_tree(self, samples: List[List[float]]) -> Dict[str, Any]:
        """Build single isolation tree."""
        if not samples or len(samples) <= 1:
            return {"type": "leaf", "size": len(samples)}

        n_features = len(samples[0])
        feature_idx = random.randint(0, n_features - 1)
        min_val = min(s[feature_idx] for s in samples)
        max_val = max(s[feature_idx] for s in samples)

        if min_val == max_val:
            return {"type": "leaf", "size": len(samples)}

        split_val = random.uniform(min_val, max_val)
        left = [s for s in samples if s[feature_idx] < split_val]
        right = [s for s in samples if s[feature_idx] >= split_val]

        return {
            "type": "node",
            "feature_idx": feature_idx,
            "split_val": split_val,
            "left": self._build_tree(left),
            "right": self._build_tree(right)
        }

    def _path_length(self, point: List[float], tree: Dict[str, Any], depth: int = 0) -> float:
        """Calculate path length for a point."""
        if tree["type"] == "leaf":
            return depth + (tree.get("size", 1) - 1) / (self._max_samples - 1)

        feat_idx = tree["feature_idx"]
        if point[feat_idx] < tree["split_val"]:
            return self._path_length(point, tree["left"], depth + 1)
        else:
            return self._path_length(point, tree["right"], depth + 1)

    def score(self, data: List[List[float]]) -> List[float]:
        """Get anomaly scores (lower = more anomalous)."""
        if not self._trees:
            return [0.0] * len(data)

        scores = []
        c = 2 * (math.log(self._max_samples - 1) + 0.5772156649) - (2 * (self._max_samples - 1) / self._max_samples)

        for point in data:
            avg_path = sum(
                self._path_length(point, tree)
                for tree in self._trees
            ) / self._n_trees
            score = math.pow(2, -avg_path / c)
            scores.append(score)

        return scores


class AnomalyDetectionAction:
    """Anomaly detection action supporting multiple methods.

    Example:
        action = AnomalyDetectionAction()
        action.fit([1, 2, 3, 4, 5, 100, 2, 3, 4, 5])
        report = action.detect(method="zscore", threshold=2.0)
    """

    def __init__(self):
        self._engine: Optional[AnomalyDetectionEngine] = None
        self._forest: Optional[IsolationForestSim] = None

    def fit(self, data: List[float]) -> Dict[str, Any]:
        """Fit detection engine with data.

        Args:
            data: 1D list of numerical values

        Returns:
            Dict with fit status and statistics
        """
        try:
            self._engine = AnomalyDetectionEngine()
            self._engine.fit(data)
            return {
                "success": True,
                "statistics": self._engine.get_statistics(),
                "message": f"Fitted with {len(data)} data points"
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Fit error: {str(e)}"
            }

    def fit_multivariate(self, data: List[List[float]]) -> Dict[str, Any]:
        """Fit multivariate detection model.

        Args:
            data: 2D list where each row is a sample

        Returns:
            Dict with fit status
        """
        try:
            self._forest = IsolationForestSim(n_trees=50)
            self._forest.fit(data)
            return {
                "success": True,
                "n_samples": len(data),
                "n_features": len(data[0]) if data else 0,
                "message": f"Fitted multivariate model with {len(data)} samples"
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Fit error: {str(e)}"
            }

    def detect(self, method: str = "zscore", threshold: float = 3.0,
              multiplier: float = 1.5) -> DetectionReport:
        """Detect anomalies using specified method.

        Args:
            method: Detection method (zscore, iqr, statistical)
            threshold: Detection threshold
            multiplier: IQR multiplier (for IQR method)

        Returns:
            DetectionReport with all results
        """
        start = time.time()

        if self._engine is None:
            return DetectionReport(
                success=False,
                method=method,
                total_points=0,
                anomalies_found=0,
                anomaly_percentage=0.0,
                errors=["Engine not fitted. Call fit() first."]
            )

        try:
            if method == "zscore":
                results = self._engine.detect_zscore(threshold)
            elif method == "iqr":
                results = self._engine.detect_iqr(multiplier)
            elif method == "statistical":
                results = self._engine.detect_statistical(threshold)
            else:
                return DetectionReport(
                    success=False,
                    method=method,
                    total_points=len(self._engine._data),
                    anomalies_found=0,
                    anomaly_percentage=0.0,
                    errors=[f"Unknown method: {method}"]
                )

            anomalies = [r for r in results if r.is_anomaly]
            duration_ms = (time.time() - start) * 1000

            return DetectionReport(
                success=True,
                method=method,
                total_points=len(results),
                anomalies_found=len(anomalies),
                anomaly_percentage=len(anomalies) / len(results) * 100 if results else 0.0,
                results=results,
                statistics=self._engine.get_statistics(),
                duration_ms=duration_ms
            )

        except Exception as e:
            return DetectionReport(
                success=False,
                method=method,
                total_points=len(self._engine._data) if self._engine else 0,
                anomalies_found=0,
                anomaly_percentage=0.0,
                errors=[str(e)]
            )

    def detect_multivariate(self, data: List[List[float]],
                           score_threshold: float = 0.5) -> DetectionReport:
        """Detect anomalies in multivariate data.

        Args:
            data: 2D list of samples
            score_threshold: Score above this is anomaly (0-1)

        Returns:
            DetectionReport with multivariate results
        """
        start = time.time()

        if self._forest is None:
            self.fit_multivariate(data)

        try:
            scores = self._forest.score(data)
            results = [
                AnomalyResult(
                    index=i,
                    value=sum(row) / len(row) if row else 0.0,
                    score=scores[i],
                    is_anomaly=scores[i] >= score_threshold,
                    method="isolation_forest",
                    threshold=score_threshold,
                    details={"raw_score": scores[i]}
                )
                for i, row in enumerate(data)
            ]

            anomalies = [r for r in results if r.is_anomaly]
            duration_ms = (time.time() - start) * 1000

            return DetectionReport(
                success=True,
                method="isolation_forest",
                total_points=len(results),
                anomalies_found=len(anomalies),
                anomaly_percentage=len(anomalies) / len(results) * 100 if results else 0.0,
                results=results,
                duration_ms=duration_ms
            )

        except Exception as e:
            return DetectionReport(
                success=False,
                method="isolation_forest",
                total_points=len(data),
                anomalies_found=0,
                anomaly_percentage=0.0,
                errors=[str(e)]
            )


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute anomaly detection action.

    Args:
        context: Execution context
        params: Dict with keys:
            - operation: "fit", "detect", "fit_multivariate", "detect_multivariate"
            - data: List of values for univariate detection
            - multivariate_data: 2D list for multivariate detection
            - method: Detection method (zscore, iqr, statistical)
            - threshold: Detection threshold
            - multiplier: IQR multiplier
            - score_threshold: Score threshold for multivariate

    Returns:
        Dict with success, results, statistics
    """
    operation = params.get("operation", "detect")
    action = AnomalyDetectionAction()

    try:
        if operation in ("fit", "detect"):
            data = params.get("data", [])
            if not data:
                return {"success": False, "message": "data required"}
            fit_result = action.fit(data)
            if not fit_result["success"]:
                return fit_result

        if operation == "fit":
            return fit_result

        elif operation in ("detect", "detect_univariate"):
            data = params.get("data", [])
            if data:
                action.fit(data)
            method = params.get("method", "zscore")
            threshold = params.get("threshold", 3.0)
            multiplier = params.get("multiplier", 1.5)
            report = action.detect(method, threshold, multiplier)
            return {
                "success": report.success,
                "method": report.method,
                "total_points": report.total_points,
                "anomalies_found": report.anomalies_found,
                "anomaly_percentage": report.anomaly_percentage,
                "statistics": report.statistics,
                "duration_ms": report.duration_ms,
                "anomalies": [
                    {"index": r.index, "value": r.value, "score": r.score, "details": r.details}
                    for r in report.results if r.is_anomaly
                ][:100],
                "message": f"Found {report.anomalies_found} anomalies"
            }

        elif operation in ("fit_multivariate", "detect_multivariate"):
            multivariate_data = params.get("multivariate_data", [])
            if not multivariate_data:
                return {"success": False, "message": "multivariate_data required"}
            action.fit_multivariate(multivariate_data)

            if operation == "fit_multivariate":
                return {"success": True, "message": "Multivariate model fitted"}

            score_threshold = params.get("score_threshold", 0.5)
            report = action.detect_multivariate(multivariate_data, score_threshold)
            return {
                "success": report.success,
                "method": report.method,
                "total_points": report.total_points,
                "anomalies_found": report.anomalies_found,
                "anomaly_percentage": report.anomaly_percentage,
                "duration_ms": report.duration_ms,
                "anomalies": [
                    {"index": r.index, "score": r.score}
                    for r in report.results if r.is_anomaly
                ][:100],
                "message": f"Found {report.anomalies_found} anomalies"
            }

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"Anomaly detection error: {str(e)}"}
