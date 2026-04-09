"""
Data Normalizer Action Module.

Provides data normalization and transformation utilities including
scaling, standardization, min-max normalization, and robust scaling.
"""

import math
from typing import Optional, List, Dict, Any, Callable, Union
from dataclasses import dataclass
from enum import Enum
import numpy as np


class NormalizationMethod(Enum):
    """Normalization method types."""
    MIN_MAX = "min_max"
    Z_SCORE = "z_score"
    ROBUST = "robust"
    LOG = "log"
    LOG10 = "log10"
    SQUARE_ROOT = "square_root"
    MAX_ABS = "max_abs"
    L2 = "l2"


@dataclass
class NormalizerConfig:
    """Configuration for data normalization."""
    method: NormalizationMethod = NormalizationMethod.MIN_MAX
    feature_range: tuple = (0, 1)  # for MIN_MAX
    robust_quantiles: tuple = (0.25, 0.75)  # for ROBUST
    log_offset: float = 1.0  # offset for log normalization
    epsilon: float = 1e-10  # small value to prevent division by zero


class DataStatistics:
    """Statistics for a data series."""

    def __init__(self, data: List[float]):
        self.data = data
        self.n = len(data)
        self.mean = sum(data) / self.n if self.n > 0 else 0
        self.min_val = min(data) if self.n > 0 else 0
        self.max_val = max(data) if self.n > 0 else 0
        self.range = self.max_val - self.min_val

        # Calculate variance and std
        if self.n > 0:
            variance = sum((x - self.mean) ** 2 for x in data) / self.n
            self.std = math.sqrt(variance)
            self.var = variance
        else:
            self.std = 0
            self.var = 0

        # Median
        sorted_data = sorted(data)
        if self.n % 2 == 0:
            self.median = (sorted_data[self.n // 2 - 1] + sorted_data[self.n // 2]) / 2
        else:
            self.median = sorted_data[self.n // 2]

        # Quartiles for robust scaling
        q1_idx = self.n // 4
        q3_idx = 3 * self.n // 4
        self.q1 = sorted_data[q1_idx] if self.n > 0 else 0
        self.q3 = sorted_data[q3_idx] if self.n > 0 else 0
        self.iqr = self.q3 - self.q1


class DataNormalizerAction:
    """
    Data normalization and transformation action.

    Provides various normalization methods for preparing data
    for machine learning or analysis pipelines.
    """

    def __init__(self, config: Optional[NormalizerConfig] = None):
        self.config = config or NormalizerConfig()
        self._stats: Dict[str, DataStatistics] = {}
        self._fitted = False
        self._feature_range = self.config.feature_range

    def fit(self, data: Union[List[float], List[List[float]]]) -> "DataNormalizerAction":
        """
        Fit the normalizer to the data.

        Args:
            data: Training data for computing normalization parameters

        Returns:
            self for chaining
        """
        if isinstance(data[0], (int, float)):
            # 1D data
            self._stats["default"] = DataStatistics(data)
        else:
            # 2D data (multiple features)
            n_features = len(data[0])
            for i in range(n_features):
                feature_data = [row[i] for row in data]
                self._stats[f"feature_{i}"] = DataStatistics(feature_data)

        self._fitted = True
        return self

    def transform(self, data: Union[List[float], List[List[float]]]) -> List[float]:
        """
        Transform data using fitted normalization.

        Args:
            data: Data to normalize

        Returns:
            Normalized data
        """
        if not self._fitted:
            raise ValueError("Normalizer must be fitted before transform")

        if isinstance(data[0], (int, float)):
            # 1D data
            stats = self._stats.get("default")
            return self._normalize_1d(data, stats)
        else:
            # 2D data
            result = []
            for row in data:
                normalized_row = []
                for i, val in enumerate(row):
                    stats = self._stats.get(f"feature_{i}")
                    if stats:
                        normalized_row.append(
                            self._normalize_value(val, stats)[0]
                        )
                    else:
                        normalized_row.append(val)
                result.append(normalized_row)
            return result

    def fit_transform(self, data: Union[List[float], List[List[float]]]) -> List[float]:
        """Fit and transform in one step."""
        return self.fit(data).transform(data)

    def _normalize_1d(self, data: List[float], stats: DataStatistics) -> List[float]:
        """Normalize 1D data."""
        return [self._normalize_value(val, stats)[0] for val in data]

    def _normalize_value(
        self, value: float, stats: DataStatistics
    ) -> tuple[float, str]:
        """Normalize a single value using configured method."""
        method = self.config.method
        epsilon = self.config.epsilon

        if method == NormalizationMethod.MIN_MAX:
            if stats.range < epsilon:
                return (self._feature_range[0], "clipped")
            normalized = self._feature_range[0] + (
                (value - stats.min_val) / stats.range
            ) * (self._feature_range[1] - self._feature_range[0])
            return (normalized, "ok")

        elif method == NormalizationMethod.Z_SCORE:
            if stats.std < epsilon:
                return (0.0, "clipped")
            normalized = (value - stats.mean) / stats.std
            return (normalized, "ok")

        elif method == NormalizationMethod.ROBUST:
            iqr = stats.q3 - stats.q1
            if iqr < epsilon:
                return (0.0, "clipped")
            normalized = (value - stats.median) / iqr
            return (normalized, "ok")

        elif method == NormalizationMethod.LOG:
            normalized = math.log(value + self.config.log_offset)
            return (normalized, "ok")

        elif method == NormalizationMethod.LOG10:
            normalized = math.log10(value + self.config.log_offset)
            return (normalized, "ok")

        elif method == NormalizationMethod.SQUARE_ROOT:
            normalized = math.sqrt(max(0, value))
            return (normalized, "ok")

        elif method == NormalizationMethod.MAX_ABS:
            max_abs = max(abs(stats.min_val), abs(stats.max_val))
            if max_abs < epsilon:
                return (0.0, "clipped")
            return (value / max_abs, "ok")

        elif method == NormalizationMethod.L2:
            norm = math.sqrt(sum(x ** 2 for x in [value]))
            if norm < epsilon:
                return (0.0, "clipped")
            return (value / norm, "ok")

        return (value, "unsupported")

    def inverse_transform(
        self, data: Union[List[float], List[List[float]]]
    ) -> List[float]:
        """
        Reverse normalization to original scale.

        Args:
            data: Normalized data

        Returns:
            Original scale data
        """
        if not self._fitted:
            raise ValueError("Normalizer must be fitted before inverse_transform")

        # This is a simplified version - full implementation would
        # need to track the actual normalization parameters
        if isinstance(data[0], (int, float)):
            stats = self._stats.get("default")
            if stats is None:
                return data

            if self.config.method == NormalizationMethod.MIN_MAX:
                min_f, max_f = self._feature_range
                result = []
                for val in data:
                    original = min_f + val * (stats.max_val - stats.min_val) / (max_f - min_f)
                    result.append(original)
                return result

        return data

    def get_stats(self, feature: str = "default") -> Optional[DataStatistics]:
        """Get statistics for a feature."""
        return self._stats.get(feature)

    def is_fitted(self) -> bool:
        """Check if normalizer has been fitted."""
        return self._fitted


class OutlierClipper:
    """Clip outliers to specified quantiles."""

    def __init__(
        self,
        lower_quantile: float = 0.05,
        upper_quantile: float = 0.95,
    ):
        self.lower_quantile = lower_quantile
        self.upper_quantile = upper_quantile
        self._lower_bound: Optional[float] = None
        self._upper_bound: Optional[float] = None

    def fit(self, data: List[float]) -> "OutlierClipper":
        """Fit clipper bounds to data."""
        sorted_data = sorted(data)
        n = len(sorted_data)
        self._lower_bound = sorted_data[int(n * self.lower_quantile)]
        self._upper_bound = sorted_data[int(n * self.upper_quantile)]
        return self

    def transform(self, data: List[float]) -> List[float]:
        """Clip outliers in data."""
        if self._lower_bound is None:
            raise ValueError("Clipper must be fitted first")
        return [
            max(self._lower_bound, min(self._upper_bound, val))
            for val in data
        ]

    def fit_transform(self, data: List[float]) -> List[float]:
        """Fit and transform in one step."""
        return self.fit(data).transform(data)


class CustomTransformer:
    """Apply custom transformation functions."""

    def __init__(self, func: Callable[[float], float]):
        self.func = func

    def transform(self, data: List[float]) -> List[float]:
        """Apply custom function to data."""
        return [self.func(val) for val in data]

    def fit_transform(self, data: List[float]) -> List[float]:
        """Fit and transform in one step."""
        return self.transform(data)
