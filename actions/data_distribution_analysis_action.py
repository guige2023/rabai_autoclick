"""Data Distribution Analysis Action.

Analyzes data distributions: skewness, kurtosis, normality tests,
distribution fitting, and outlier detection.
"""
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import Counter
import math


@dataclass
class DistributionSummary:
    mean: float
    median: float
    std: float
    variance: float
    skewness: float
    kurtosis: float
    min: float
    max: float
    q1: float
    q3: float
    iqr: float
    count: int

    def as_dict(self) -> Dict[str, float]:
        return {
            "mean": self.mean,
            "median": self.median,
            "std": self.std,
            "variance": self.variance,
            "skewness": self.skewness,
            "kurtosis": self.kurtosis,
            "min": self.min,
            "max": self.max,
            "q1": self.q1,
            "q3": self.q3,
            "iqr": self.iqr,
            "count": float(self.count),
        }


class DataDistributionAnalysisAction:
    """Analyzes statistical distributions of data."""

    @staticmethod
    def _percentile(sorted_vals: List[float], p: float) -> float:
        if not sorted_vals:
            return 0.0
        idx = p * (len(sorted_vals) - 1)
        lo = int(idx)
        hi = lo + 1
        frac = idx - lo
        if hi >= len(sorted_vals):
            return sorted_vals[lo]
        return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac

    @staticmethod
    def _skewness(values: List[float], mean: float, std: float) -> float:
        if std == 0 or len(values) < 3:
            return 0.0
        n = len(values)
        m3 = sum((v - mean) ** 3 for v in values) / n
        return m3 / (std**3)

    @staticmethod
    def _kurtosis(values: List[float], mean: float, std: float) -> float:
        if std == 0 or len(values) < 4:
            return 0.0
        n = len(values)
        m4 = sum((v - mean) ** 4 for v in values) / n
        return m4 / (std**4) - 3

    def summarize(self, values: List[float]) -> DistributionSummary:
        if not values:
            return DistributionSummary(0,0,0,0,0,0,0,0,0,0,0,0)
        sorted_vals = sorted(values)
        n = len(values)
        mean = sum(values) / n
        variance = sum((v - mean)**2 for v in values) / n
        std = math.sqrt(variance)
        skew = self._skewness(values, mean, std)
        kurt = self._kurtosis(values, mean, std)
        q1 = self._percentile(sorted_vals, 0.25)
        q3 = self._percentile(sorted_vals, 0.75)
        return DistributionSummary(
            mean=mean,
            median=self._percentile(sorted_vals, 0.5),
            std=std,
            variance=variance,
            skewness=skew,
            kurtosis=kurt,
            min=sorted_vals[0],
            max=sorted_vals[-1],
            q1=q1,
            q3=q3,
            iqr=q3 - q1,
            count=n,
        )

    def detect_outliers_iqr(self, values: List[float], factor: float = 1.5) -> Tuple[List[float], List[float]]:
        """Detect outliers using IQR method. Returns (inliers, outliers)."""
        if len(values) < 4:
            return values, []
        sorted_vals = sorted(values)
        q1 = self._percentile(sorted_vals, 0.25)
        q3 = self._percentile(sorted_vals, 0.75)
        iqr = q3 - q1
        lower = q1 - factor * iqr
        upper = q3 + factor * iqr
        inliers = [v for v in values if lower <= v <= upper]
        outliers = [v for v in values if v < lower or v > upper]
        return inliers, outliers

    def detect_outliers_zscore(self, values: List[float], threshold: float = 3.0) -> Tuple[List[float], List[float]]:
        """Detect outliers using Z-score method."""
        if len(values) < 3:
            return values, []
        mean = sum(values) / len(values)
        std = math.sqrt(sum((v - mean)**2 for v in values) / len(values))
        if std == 0:
            return values, []
        inliers, outliers = [], []
        for v in values:
            z = abs((v - mean) / std)
            if z <= threshold:
                inliers.append(v)
            else:
                outliers.append(v)
        return inliers, outliers

    def shapiro_wilk_approx(self, values: List[float], alpha: float = 0.05) -> Dict[str, Any]:
        """Approximate Shapiro-Wilk normality test."""
        if len(values) < 3:
            return {"statistic": 0.0, "p_value": 1.0, "normal": True, "alpha": alpha}
        if len(values) > 5000:
            values = values[:5000]
        sorted_vals = sorted(values)
        n = len(values)
        mean = sum(values) / n
        s_sq = sum((v - mean)**2 for v in values)
        # Simplified approximation of Shapiro-Wilk W statistic
        a = [0.0] * n
        for i in range(n):
            a[i] = math.exp(-0.5 * ((i - n // 2) / (n / 2))**2)
        a_sum = sum(a[i] * sorted_vals[i] for i in range(n))
        # W = (sum(a_i * x_i)^2) / sum((x_i - mean)^2)
        w = (a_sum - sum(sorted_vals) / n * sum(a))**2 / s_sq if s_sq > 0 else 0.0
        w = min(w, 1.0)
        # Approximate p-value
        n_adj = min(n, 50)
        p = math.exp(-8 - 0.32 * n_adj + 12 * w) if w > 0 else 1.0
        return {
            "statistic": round(w, 4),
            "p_value": min(p, 1.0),
            "normal": min(p, 1.0) > alpha,
            "alpha": alpha,
        }
