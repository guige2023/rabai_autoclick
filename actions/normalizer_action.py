"""normalizer action module for rabai_autoclick.

Provides data normalization and standardization operations:
min-max scaling, z-score normalization, robust scaling,
categorical encoding, and custom transformations.
"""

from __future__ import annotations

import math
import statistics
from collections import Counter
from dataclasses import dataclass
from typing import Any, Callable, Optional, Protocol, Sequence
from enum import Enum, auto

__all__ = [
    "MinMaxScaler",
    "StandardScaler",
    "RobustScaler",
    "MaxAbsScaler",
    "LabelEncoder",
    "OneHotEncoder",
    "BinEncoder",
    "QuantileTransformer",
    "normalize",
    "standardize",
    "robust_scale",
    "scale_to_range",
    "normalize_text",
    "normalize_dict",
    "normalize_vector",
    "l1_normalize",
    "l2_normalize",
    "linf_normalize",
    "ScaleMethod",
]


class ScaleMethod(Enum):
    """Available scaling methods."""
    MIN_MAX = auto()
    Z_SCORE = auto()
    ROBUST = auto()
    MAX_ABS = auto()
    QUANTILE = auto()


@dataclass
class MinMaxScaler:
    """Min-max scaler that transforms features to a given range.

    Transforms each feature to [min_val, max_val] range (default [0, 1]).
    """

    feature_min: float = 0.0
    feature_max: float = 1.0
    _fit: bool = False
    _min: float = 0.0
    _max: float = 0.0
    _scale: float = 1.0

    def fit(self, data: Sequence[float]) -> "MinMaxScaler":
        """Compute min and max from training data.

        Args:
            data: Training data values.

        Returns:
            Self for chaining.
        """
        self._min = min(data)
        self._max = max(data)
        self._scale = self._max - self._min
        if self._scale == 0:
            self._scale = 1.0
        self._fit = True
        return self

    def transform(self, value: float) -> float:
        """Transform a single value.

        Args:
            value: Input value.

        Returns:
            Scaled value.

        Raises:
            RuntimeError: If not fitted.
        """
        if not self._fit:
            raise RuntimeError("Scaler must be fitted before transform")
        if self._scale == 0:
            return self.feature_min
        scaled = (value - self._min) / self._scale
        return self.feature_min + scaled * (self.feature_max - self.feature_min)

    def fit_transform(self, data: Sequence[float]) -> list[float]:
        """Fit and transform in one call."""
        self.fit(data)
        return [self.transform(v) for v in data]

    def inverse_transform(self, value: float) -> float:
        """Reverse the scaling transformation."""
        if not self._fit:
            raise RuntimeError("Scaler must be fitted")
        ratio = (value - self.feature_min) / (self.feature_max - self.feature_min)
        return self._min + ratio * (self._max - self._min)


@dataclass
class StandardScaler:
    """Z-score standardizer: transforms to mean=0, std=1."""

    _fit: bool = False
    _mean: float = 0.0
    _std: float = 1.0

    def fit(self, data: Sequence[float]) -> "StandardScaler":
        """Compute mean and std from training data."""
        self._mean = statistics.mean(data)
        self._std = statistics.stdev(data)
        if self._std == 0:
            self._std = 1.0
        self._fit = True
        return self

    def transform(self, value: float) -> float:
        """Transform using z-score formula."""
        if not self._fit:
            raise RuntimeError("Scaler must be fitted")
        return (value - self._mean) / self._std

    def fit_transform(self, data: Sequence[float]) -> list[float]:
        self.fit(data)
        return [self.transform(v) for v in data]

    def inverse_transform(self, value: float) -> float:
        if not self._fit:
            raise RuntimeError("Scaler must be fitted")
        return value * self._std + self._mean


@dataclass
class RobustScaler:
    """Robust scaler using median and IQR (immune to outliers)."""

    _fit: bool = False
    _median: float = 0.0
    _q1: float = 0.0
    _q3: float = 0.0
    _iqr: float = 1.0

    def fit(self, data: Sequence[float]) -> "RobustScaler":
        """Compute median and IQR from training data."""
        sorted_data = sorted(data)
        n = len(sorted_data)
        self._median = statistics.median(sorted_data)
        self._q1 = sorted_data[n // 4]
        self._q3 = sorted_data[3 * n // 4]
        self._iqr = self._q3 - self._q1
        if self._iqr == 0:
            self._iqr = 1.0
        self._fit = True
        return self

    def transform(self, value: float) -> float:
        if not self._fit:
            raise RuntimeError("Scaler must be fitted")
        return (value - self._median) / self._iqr

    def fit_transform(self, data: Sequence[float]) -> list[float]:
        self.fit(data)
        return [self.transform(v) for v in data]


@dataclass
class MaxAbsScaler:
    """Scale each feature by its maximum absolute value."""

    _fit: bool = False
    _max_abs: float = 1.0

    def fit(self, data: Sequence[float]) -> "MaxAbsScaler":
        self._max_abs = max(abs(v) for v in data)
        if self._max_abs == 0:
            self._max_abs = 1.0
        self._fit = True
        return self

    def transform(self, value: float) -> float:
        if not self._fit:
            raise RuntimeError("Scaler must be fitted")
        return value / self._max_abs

    def fit_transform(self, data: Sequence[float]) -> list[float]:
        self.fit(data)
        return [self.transform(v) for v in data]


class LabelEncoder:
    """Encode categorical labels as integers."""

    def __init__(self) -> None:
        self._classes: list[str] = []
        self._mapping: dict[str, int] = {}

    def fit(self, data: Sequence[str]) -> "LabelEncoder":
        """Build label-to-index mapping."""
        self._classes = sorted(set(str(v) for v in data))
        self._mapping = {label: idx for idx, label in enumerate(self._classes)}
        return self

    def transform(self, value: str) -> int:
        """Encode a single label."""
        return self._mapping.get(str(value), -1)

    def fit_transform(self, data: Sequence[str]) -> list[int]:
        self.fit(data)
        return [self.transform(v) for v in data]

    def inverse_transform(self, index: int) -> Optional[str]:
        """Decode index back to label."""
        if 0 <= index < len(self._classes):
            return self._classes[index]
        return None

    def classes(self) -> list[str]:
        return list(self._classes)


class OneHotEncoder:
    """One-hot encode categorical values."""

    def __init__(self, sparse: bool = False) -> None:
        self.sparse = sparse
        self._classes: list[str] = []
        self._label_encoder = LabelEncoder()

    def fit(self, data: Sequence[str]) -> "OneHotEncoder":
        self._label_encoder.fit(data)
        self._classes = self._label_encoder.classes()
        return self

    def transform(self, value: str) -> list[int]:
        """Return one-hot encoding as list."""
        index = self._label_encoder.transform(value)
        return [1 if i == index else 0 for i in range(len(self._classes))]

    def fit_transform(self, data: Sequence[str]) -> list[list[int]]:
        self.fit(data)
        return [self.transform(v) for v in data]


class BinEncoder:
    """Bin continuous values into discrete intervals."""

    def __init__(self, bins: int = 10, labels: Optional[Sequence[str]] = None) -> None:
        self.bins = bins
        self.labels = labels
        self._bin_edges: list[float] = []
        self._fitted: bool = False

    def fit(self, data: Sequence[float]) -> "BinEncoder":
        """Compute bin edges using equal-width intervals."""
        min_val, max_val = min(data), max(data)
        if min_val == max_val:
            min_val -= 0.5
            max_val += 0.5
        step = (max_val - min_val) / self.bins
        self._bin_edges = [min_val + i * step for i in range(self.bins + 1)]
        self._fitted = True
        return self

    def transform(self, value: float) -> int:
        """Return bin index (0 to bins-1)."""
        if not self._fitted:
            raise RuntimeError("BinEncoder must be fitted")
        for i, edge in enumerate(self._bin_edges[1:]):
            if value <= edge:
                return i
        return self.bins - 1

    def fit_transform(self, data: Sequence[float]) -> list[int]:
        self.fit(data)
        return [self.transform(v) for v in data]


class QuantileTransformer:
    """Transform features to follow a uniform or normal distribution."""

    def __init__(self, n_quantiles: int = 1000, output_distribution: str = "uniform") -> None:
        self.n_quantiles = n_quantiles
        self.output_distribution = output_distribution
        self._quantiles: list[float] = []
        self._references: list[float] = []
        self._fitted: bool = False

    def fit(self, data: Sequence[float]) -> "QuantileTransformer":
        """Compute quantile mapping from training data."""
        sorted_data = sorted(data)
        n = len(sorted_data)
        step = max(1, n // self.n_quantiles)
        self._quantiles = sorted(set(sorted_data[i] for i in range(0, n, step)))[:self.n_quantiles]
        if self.output_distribution == "uniform":
            self._references = [i / len(self._quantiles) for i in range(len(self._quantiles))]
        else:
            from statistics import NormalDist
            nd = NormalDist()
            self._references = [nd.inv_cdf(i / len(self._quantiles)) for i in range(1, len(self._quantiles) + 1)]
        self._fitted = True
        return self

    def transform(self, value: float) -> float:
        if not self._fitted:
            raise RuntimeError("QuantileTransformer must be fitted")
        for i, q in enumerate(self._quantiles):
            if value <= q:
                if i == 0:
                    return self._references[0]
                interp = (value - self._quantiles[i-1]) / (q - self._quantiles[i-1] + 1e-10)
                return self._references[i-1] + interp * (self._references[i] - self._references[i-1])
        return self._references[-1]

    def fit_transform(self, data: Sequence[float]) -> list[float]:
        self.fit(data)
        return [self.transform(v) for v in data]


def normalize(data: Sequence[float], method: ScaleMethod = ScaleMethod.MIN_MAX) -> list[float]:
    """Normalize data using specified method.

    Args:
        data: Input values.
        method: Scaling method to apply.

    Returns:
        Normalized values.
    """
    if method == ScaleMethod.MIN_MAX:
        scaler = MinMaxScaler().fit(list(data))
        return scaler.fit_transform(list(data))
    elif method == ScaleMethod.Z_SCORE:
        scaler = StandardScaler().fit(list(data))
        return scaler.fit_transform(list(data))
    elif method == ScaleMethod.ROBUST:
        scaler = RobustScaler().fit(list(data))
        return scaler.fit_transform(list(data))
    elif method == ScaleMethod.MAX_ABS:
        scaler = MaxAbsScaler().fit(list(data))
        return scaler.fit_transform(list(data))
    elif method == ScaleMethod.QUANTILE:
        scaler = QuantileTransformer().fit(list(data))
        return scaler.fit_transform(list(data))
    return list(data)


def standardize(data: Sequence[float]) -> list[float]:
    """Alias for z-score standardization."""
    return normalize(data, ScaleMethod.Z_SCORE)


def robust_scale(data: Sequence[float]) -> list[float]:
    """Scale using median and IQR."""
    return normalize(data, ScaleMethod.ROBUST)


def scale_to_range(
    data: Sequence[float],
    min_val: float = 0.0,
    max_val: float = 1.0,
) -> list[float]:
    """Scale data to arbitrary range."""
    scaler = MinMaxScaler(feature_min=min_val, feature_max=max_val).fit(list(data))
    return scaler.fit_transform(list(data))


def normalize_text(text: str) -> str:
    """Normalize text: lowercase, collapse whitespace."""
    import re
    text = text.lower().strip()
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_dict(
    d: dict[str, Any],
    keys_lower: bool = True,
    values_strip: bool = True,
) -> dict[str, Any]:
    """Normalize dictionary keys and string values."""
    result = {}
    for k, v in d.items():
        key = k.lower() if keys_lower else k
        if isinstance(v, str) and values_strip:
            v = v.strip()
        result[key] = v
    return result


def normalize_vector(v: list[float], p: float = 2.0) -> list[float]:
    """Normalize vector by Lp norm.

    Args:
        v: Input vector.
        p: Norm order (1=L1, 2=L2, inf=L-inf).

    Returns:
        Normalized vector.
    """
    if p == 1:
        return l1_normalize(v)
    elif p == 2:
        return l2_normalize(v)
    elif p == float("inf"):
        return linf_normalize(v)
    norm = sum(abs(x) ** p for x in v) ** (1.0 / p)
    if norm == 0:
        return v
    return [x / norm for x in v]


def l1_normalize(v: list[float]) -> list[float]:
    """L1 (Manhattan) normalize."""
    norm = sum(abs(x) for x in v)
    if norm == 0:
        return v
    return [x / norm for x in v]


def l2_normalize(v: list[float]) -> list[float]:
    """L2 (Euclidean) normalize."""
    norm = math.sqrt(sum(x * x for x in v))
    if norm == 0:
        return v
    return [x / norm for x in v]


def linf_normalize(v: list[float]) -> list[float]:
    """L-inf (max) normalize."""
    norm = max(abs(x) for x in v)
    if norm == 0:
        return v
    return [x / norm for x in v]
