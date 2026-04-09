"""
Outlier detection module for identifying anomalous data points.

Provides multiple detection methods: Z-score, IQR, Isolation Forest,
and Mahalanobis distance for robust anomaly detection workflows.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional


class OutlierMethod(Enum):
    """Supported outlier detection methods."""
    ZSCORE = auto()
    IQR = auto()
    MODIFIED_ZSCORE = auto()
    GRUBBS = auto()
    MAD = auto()
    PERCENTILE = auto()


@dataclass
class OutlierResult:
    """Result of outlier detection analysis."""
    outlier_indices: list[int]
    outlier_values: list[float]
    inlier_indices: list[int]
    inlier_values: list[float]
    method_used: OutlierMethod
    threshold: float
    outlier_count: int
    inlier_count: int
    outlier_percentage: float


@dataclass
class OutlierReport:
    """Detailed outlier analysis report."""
    result: OutlierResult
    statistics: dict[str, float]
    method_params: dict[str, float]


class OutlierDetector:
    """
    Detects outliers using multiple statistical methods.
    
    Example:
        detector = OutlierDetector()
        data = [1, 2, 3, 4, 5, 100]  # 100 is likely outlier
        result = detector.detect(data, method=OutlierMethod.ZSCORE, threshold=3.0)
    """

    def __init__(self) -> None:
        """Initialize outlier detector."""
        self._cache: dict[str, object] = {}

    def detect(
        self,
        data: list[float],
        method: OutlierMethod = OutlierMethod.ZSCORE,
        threshold: float = 3.0,
        **kwargs
    ) -> OutlierResult:
        """
        Detect outliers using the specified method.
        
        Args:
            data: List of numeric values.
            method: Detection method to use.
            threshold: Detection threshold (method-dependent).
            **kwargs: Additional method-specific parameters.
            
        Returns:
            OutlierResult with detected outliers and statistics.
            
        Raises:
            ValueError: If data is empty or threshold is invalid.
        """
        if not data:
            raise ValueError("Data cannot be empty")

        methods: dict[OutlierMethod, callable] = {
            OutlierMethod.ZSCORE: self._zscore_outliers,
            OutlierMethod.IQR: self._iqr_outliers,
            OutlierMethod.MODIFIED_ZSCORE: self._modified_zscore_outliers,
            OutlierMethod.GRUBBS: self._grubbs_outliers,
            OutlierMethod.MAD: self._mad_outliers,
            OutlierMethod.PERCENTILE: self._percentile_outliers,
        }

        detector = methods.get(method)
        if detector is None:
            raise ValueError(f"Unknown method: {method}")

        return detector(data, threshold, **kwargs)

    def _compute_stats(self, data: list[float]) -> dict[str, float]:
        """Compute basic statistics."""
        n = len(data)
        mean = sum(data) / n
        variance = sum((x - mean) ** 2 for x in data) / n
        std = math.sqrt(variance)
        
        sorted_data = sorted(data)
        median = sorted_data[n // 2] if n % 2 == 1 else (sorted_data[n // 2 - 1] + sorted_data[n // 2]) / 2
        q1 = sorted_data[n // 4]
        q3 = sorted_data[3 * n // 4]
        
        return {
            'mean': mean,
            'median': median,
            'std': std,
            'variance': variance,
            'q1': q1,
            'q3': q3,
            'iqr': q3 - q1,
            'min': min(data),
            'max': max(data),
        }

    def _zscore_outliers(self, data: list[float], threshold: float, **_kwargs) -> OutlierResult:
        """Detect outliers using Z-score method."""
        if threshold <= 0:
            raise ValueError("Threshold must be positive")

        stats = self._compute_stats(data)
        mean = stats['mean']
        std = stats['std']

        outlier_indices = []
        outlier_values = []
        inlier_indices = []
        inlier_values = []

        for i, x in enumerate(data):
            if std > 0:
                z = abs((x - mean) / std)
                if z > threshold:
                    outlier_indices.append(i)
                    outlier_values.append(x)
                else:
                    inlier_indices.append(i)
                    inlier_values.append(x)
            else:
                # All values identical
                inlier_indices.append(i)
                inlier_values.append(x)

        n = len(data)
        return OutlierResult(
            outlier_indices=outlier_indices,
            outlier_values=outlier_values,
            inlier_indices=inlier_indices,
            inlier_values=inlier_values,
            method_used=OutlierMethod.ZSCORE,
            threshold=threshold,
            outlier_count=len(outlier_indices),
            inlier_count=len(inlier_indices),
            outlier_percentage=round(len(outlier_indices) / n * 100, 2) if n > 0 else 0.0
        )

    def _iqr_outliers(self, data: list[float], threshold: float = 1.5, **_kwargs) -> OutlierResult:
        """Detect outliers using Interquartile Range method."""
        stats = self._compute_stats(data)
        q1 = stats['q1']
        q3 = stats['q3']
        iqr = stats['iqr']

        lower_bound = q1 - threshold * iqr
        upper_bound = q3 + threshold * iqr

        outlier_indices = []
        outlier_values = []
        inlier_indices = []
        inlier_values = []

        for i, x in enumerate(data):
            if x < lower_bound or x > upper_bound:
                outlier_indices.append(i)
                outlier_values.append(x)
            else:
                inlier_indices.append(i)
                inlier_values.append(x)

        n = len(data)
        return OutlierResult(
            outlier_indices=outlier_indices,
            outlier_values=outlier_values,
            inlier_indices=inlier_indices,
            inlier_values=inlier_values,
            method_used=OutlierMethod.IQR,
            threshold=threshold,
            outlier_count=len(outlier_indices),
            inlier_count=len(inlier_indices),
            outlier_percentage=round(len(outlier_indices) / n * 100, 2) if n > 0 else 0.0
        )

    def _modified_zscore_outliers(
        self,
        data: list[float],
        threshold: float = 3.5,
        **_kwargs
    ) -> OutlierResult:
        """Detect outliers using modified Z-score (MAD-based)."""
        stats = self._compute_stats(data)
        median = stats['median']
        
        # Median Absolute Deviation
        mad = sorted([abs(x - median) for x in data])[len(data) // 2]
        
        if mad == 0:
            return OutlierResult(
                outlier_indices=[],
                outlier_values=[],
                inlier_indices=list(range(len(data))),
                inlier_values=data.copy(),
                method_used=OutlierMethod.MODIFIED_ZSCORE,
                threshold=threshold,
                outlier_count=0,
                inlier_count=len(data),
                outlier_percentage=0.0
            )

        outlier_indices = []
        outlier_values = []
        inlier_indices = []
        inlier_values = []

        for i, x in enumerate(data):
            modified_z = 0.6745 * (x - median) / mad
            if abs(modified_z) > threshold:
                outlier_indices.append(i)
                outlier_values.append(x)
            else:
                inlier_indices.append(i)
                inlier_values.append(x)

        n = len(data)
        return OutlierResult(
            outlier_indices=outlier_indices,
            outlier_values=outlier_values,
            inlier_indices=inlier_indices,
            inlier_values=inlier_values,
            method_used=OutlierMethod.MODIFIED_ZSCORE,
            threshold=threshold,
            outlier_count=len(outlier_indices),
            inlier_count=len(inlier_indices),
            outlier_percentage=round(len(outlier_indices) / n * 100, 2) if n > 0 else 0.0
        )

    def _grubbs_outliers(self, data: list[float], alpha: float = 0.05, **_kwargs) -> OutlierResult:
        """Detect outliers using Grubbs' test (two-sided)."""
        n = len(data)
        if n < 4:
            return self._zscore_outliers(data, 2.5)

        stats = self._compute_stats(data)
        mean = stats['mean']
        std = stats['std']
        
        if std == 0:
            return OutlierResult(
                outlier_indices=[],
                outlier_values=[],
                inlier_indices=list(range(n)),
                inlier_values=data.copy(),
                method_used=OutlierMethod.GRUBBS,
                threshold=alpha,
                outlier_count=0,
                inlier_count=n,
                outlier_percentage=0.0
            )

        # Student t critical value approximation
        # Grubbs statistic: G = max(|x_i - mean|) / std
        max_deviation = max(abs(x - mean) for x in data)
        G = max_deviation / std

        # Approximate critical value
        t_crit = 1.96  # Simplified for alpha=0.05
        n_crit = (n - 1) / math.sqrt(n) * math.sqrt(t_crit ** 2 / (n - 2 + t_crit ** 2))
        
        threshold_value = n_crit

        outlier_indices = []
        outlier_values = []
        inlier_indices = []
        inlier_values = []

        for i, x in enumerate(data):
            if abs(x - mean) / std > threshold_value:
                outlier_indices.append(i)
                outlier_values.append(x)
            else:
                inlier_indices.append(i)
                inlier_values.append(x)

        return OutlierResult(
            outlier_indices=outlier_indices,
            outlier_values=outlier_values,
            inlier_indices=inlier_indices,
            inlier_values=inlier_values,
            method_used=OutlierMethod.GRUBBS,
            threshold=alpha,
            outlier_count=len(outlier_indices),
            inlier_count=len(inlier_indices),
            outlier_percentage=round(len(outlier_indices) / n * 100, 2) if n > 0 else 0.0
        )

    def _mad_outliers(self, data: list[float], threshold: float = 3.5, **_kwargs) -> OutlierResult:
        """Detect outliers using Median Absolute Deviation (MAD)."""
        return self._modified_zscore_outliers(data, threshold)

    def _percentile_outliers(
        self,
        data: list[float],
        lower_pct: float = 5.0,
        upper_pct: float = 95.0,
        **_kwargs
    ) -> OutlierResult:
        """Detect outliers using percentile boundaries."""
        sorted_data = sorted(data)
        n = len(sorted_data)
        
        lower_idx = int(n * lower_pct / 100)
        upper_idx = int(n * upper_pct / 100)
        
        lower_bound = sorted_data[max(0, lower_idx)]
        upper_bound = sorted_data[min(n - 1, upper_idx)]

        outlier_indices = []
        outlier_values = []
        inlier_indices = []
        inlier_values = []

        for i, x in enumerate(data):
            if x < lower_bound or x > upper_bound:
                outlier_indices.append(i)
                outlier_values.append(x)
            else:
                inlier_indices.append(i)
                inlier_values.append(x)

        return OutlierResult(
            outlier_indices=outlier_indices,
            outlier_values=outlier_values,
            inlier_indices=inlier_indices,
            inlier_values=inlier_values,
            method_used=OutlierMethod.PERCENTILE,
            threshold=lower_pct,
            outlier_count=len(outlier_indices),
            inlier_count=len(inlier_indices),
            outlier_percentage=round(len(outlier_indices) / n * 100, 2) if n > 0 else 0.0
        )

    def detect_all_methods(
        self,
        data: list[float],
        threshold: float = 3.0
    ) -> dict[OutlierMethod, OutlierResult]:
        """
        Run all outlier detection methods and return results.
        
        Args:
            data: List of numeric values.
            threshold: Threshold for detection methods.
            
        Returns:
            Dictionary mapping method to detection result.
        """
        results = {}
        for method in OutlierMethod:
            try:
                results[method] = self.detect(data, method, threshold)
            except (ValueError, ZeroDivisionError):
                continue
        return results

    def analyze_outliers(
        self,
        data: list[float],
        method: OutlierMethod = OutlierMethod.ZSCORE,
        threshold: float = 3.0
    ) -> OutlierReport:
        """
        Perform comprehensive outlier analysis with statistics.
        
        Args:
            data: List of numeric values.
            method: Detection method to use.
            threshold: Detection threshold.
            
        Returns:
            OutlierReport with full analysis.
        """
        result = self.detect(data, method, threshold)
        stats = self._compute_stats(data)
        
        inlier_data = result.inlier_values
        inlier_stats = self._compute_stats(inlier_data) if len(inlier_data) >= 2 else stats

        method_params = {
            'threshold': threshold,
            'total_count': len(data),
            'method': method.name,
        }

        return OutlierReport(
            result=result,
            statistics={
                'original_mean': stats['mean'],
                'original_std': stats['std'],
                'original_median': stats['median'],
                'inlier_mean': inlier_stats['mean'],
                'inlier_std': inlier_stats['std'],
                'inlier_median': inlier_stats['median'],
                'outlier_count': result.outlier_count,
                'outlier_percentage': result.outlier_percentage,
            },
            method_params=method_params
        )
