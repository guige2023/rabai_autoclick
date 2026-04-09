"""Data Outlier Handling Action.

Strategies for handling outliers: capping, winsorization, imputation,
removal, and transformation. Supports multiple detection methods.
"""
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import math


class OutlierMethod(Enum):
    IQR = "iqr"
    ZSCORE = "zscore"
    MODIFIED_ZSCORE = "modified_zscore"
    PERCENTILE = "percentile"
    GRUBBS = "grubbs"


@dataclass
class OutlierHandlingConfig:
    method: OutlierMethod = OutlierMethod.IQR
    iqr_factor: float = 1.5
    zscore_threshold: float = 3.0
    modified_zscore_threshold: float = 3.5
    percentile_lower: float = 0.01
    percentile_upper: float = 0.99
    handling_strategy: Literal["cap", "remove", "winsorize", "impute", "transform"] = "winsorize"


class DataOutlierHandlingAction:
    """Handles outliers in datasets using various strategies."""

    def detect_outliers(
        self,
        values: List[float],
        method: OutlierMethod = OutlierMethod.IQR,
        **kwargs,
    ) -> Tuple[List[int], List[float]]:
        """Returns (indices, outlier_values) of detected outliers."""
        if not values:
            return [], []
        if method == OutlierMethod.IQR:
            return self._detect_iqr(values, kwargs.get("iqr_factor", 1.5))
        elif method == OutlierMethod.ZSCORE:
            return self._detect_zscore(values, kwargs.get("zscore_threshold", 3.0))
        elif method == OutlierMethod.MODIFIED_ZSCORE:
            return self._detect_modified_zscore(values, kwargs.get("modified_zscore_threshold", 3.5))
        elif method == OutlierMethod.PERCENTILE:
            return self._detect_percentile(values, kwargs.get("percentile_lower", 0.01), kwargs.get("percentile_upper", 0.99))
        elif method == OutlierMethod.GRUBBS:
            return self._detect_grubbs(values)
        return [], []

    def _detect_iqr(self, values: List[float], factor: float) -> Tuple[List[int], List[float]]:
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        q1_idx = int(n * 0.25)
        q3_idx = int(n * 0.75)
        q1, q3 = sorted_vals[q1_idx], sorted_vals[q3_idx]
        iqr = q3 - q1
        lower = q1 - factor * iqr
        upper = q3 + factor * iqr
        indices = [i for i, v in enumerate(values) if v < lower or v > upper]
        return indices, [values[i] for i in indices]

    def _detect_zscore(self, values: List[float], threshold: float) -> Tuple[List[int], List[float]]:
        mean = sum(values) / len(values)
        std = math.sqrt(sum((v - mean) ** 2 for v in values) / len(values))
        if std == 0:
            return [], []
        indices = [i for i, v in enumerate(values) if abs((v - mean) / std) > threshold]
        return indices, [values[i] for i in indices]

    def _detect_modified_zscore(self, values: List[float], threshold: float) -> Tuple[List[int], List[float]]:
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        median = sorted_vals[n // 2]
        mad = sorted([abs(v - median) for v in values])
        mad_median = mad[n // 2] if mad else 1.0
        if mad_median == 0:
            return [], []
        indices = [i for i, v in enumerate(values) if 0.6745 * abs(v - median) / mad_median > threshold]
        return indices, [values[i] for i in indices]

    def _detect_percentile(self, values: List[float], p_low: float, p_high: float) -> Tuple[List[int], List[float]]:
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        lower = sorted_vals[int(n * p_low)]
        upper = sorted_vals[int(n * p_high)]
        indices = [i for i, v in enumerate(values) if v < lower or v > upper]
        return indices, [values[i] for i in indices]

    def _detect_grubbs(self, values: List[float], alpha: float = 0.05) -> Tuple[List[int], List[float]]:
        n = len(values)
        if n < 3:
            return [], []
        mean = sum(values) / n
        std = math.sqrt(sum((v - mean) ** 2 for v in values) / n)
        if std == 0:
            return [], []
        sorted_vals = sorted(values)
        # Two-sided Grubbs test
        g_stat = max(abs(v - mean) / std for v in values)
        t_crit = 2.31  # approximate for alpha=0.05, df=n-2
        grubbs_crit = ((n - 1) / math.sqrt(n)) * math.sqrt(t_crit**2 / (n - 2 + t_crit**2))
        indices = [i for i, v in enumerate(values) if abs(v - mean) / std > grubbs_crit]
        return indices, [values[i] for i in indices]

    def handle_outliers(
        self,
        values: List[float],
        strategy: Literal["cap", "remove", "winsorize", "impute", "transform"] = "winsorize",
        method: OutlierMethod = OutlierMethod.IQR,
        **kwargs,
    ) -> List[float]:
        result = list(values)
        indices, outlier_values = self.detect_outliers(result, method, **kwargs)
        if not indices:
            return result
        if strategy == "remove":
            for i in sorted(indices, reverse=True):
                del result[i]
        elif strategy == "cap":
            sorted_vals = sorted(values)
            n = len(sorted_vals)
            p_low = sorted_vals[int(n * 0.01)]
            p_high = sorted_vals[int(n * 0.99)]
            for i in indices:
                if result[i] < p_low:
                    result[i] = p_low
                else:
                    result[i] = p_high
        elif strategy == "winsorize":
            sorted_vals = sorted(values)
            n = len(sorted_vals)
            lower = sorted_vals[int(n * 0.01)]
            upper = sorted_vals[int(n * 0.99)]
            for i in indices:
                result[i] = max(lower, min(upper, result[i]))
        elif strategy == "impute":
            non_outliers = [v for i, v in enumerate(values) if i not in indices]
            if non_outliers:
                fill_value = sum(non_outliers) / len(non_outliers)
            else:
                fill_value = sum(values) / len(values)
            for i in indices:
                result[i] = fill_value
        elif strategy == "transform":
            # Log transform for positive values
            for i in indices:
                if result[i] > 0:
                    result[i] = math.log(result[i])
                else:
                    result[i] = 0.0
        return result
