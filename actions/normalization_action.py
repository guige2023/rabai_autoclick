"""
Normalization module for data scaling and standardization.

Provides multiple normalization strategies including min-max scaling,
z-score standardization, robust scaling, and log transformations
for machine learning and data preprocessing workflows.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional


class NormalizationType(Enum):
    """Supported normalization methods."""
    MIN_MAX = auto()
    Z_SCORE = auto()
    ROBUST = auto()
    LOG = auto()
    LOG10 = auto()
    SQRT = auto()
    ARCSIN = auto()
    TANH = auto()
    MAX_ABS = auto()
    QUANTILE = auto()
    POWER = auto()


@dataclass
class NormalizationParams:
    """Parameters for normalization."""
    type: NormalizationType
    min_val: float = 0.0
    max_val: float = 1.0
    mean: float = 0.0
    std: float = 1.0
    median: float = 0.0
    q1: float = 0.0
    q3: float = 0.0
    power: float = 1.0
    epsilon: float = 1e-10


@dataclass
class NormalizationResult:
    """Result of normalization operation."""
    normalized_data: list[float]
    original_data: list[float]
    params: NormalizationParams
    min_value: float
    max_value: float
    mean: float
    std: float


class DataNormalizer:
    """
    Normalizes data using various scaling and transformation methods.
    
    Example:
        normalizer = DataNormalizer()
        data = [10, 20, 30, 40, 50]
        result = normalizer.normalize(data, NormalizationType.MIN_MAX, target_min=0, target_max=1)
    """

    def __init__(self, epsilon: float = 1e-10) -> None:
        """
        Initialize the normalizer.
        
        Args:
            epsilon: Small constant to prevent division by zero.
        """
        self.epsilon = epsilon
        self._params_cache: dict[str, NormalizationParams] = {}

    def _compute_stats(self, data: list[float]) -> dict[str, float]:
        """Compute basic statistics for the data."""
        n = len(data)
        if n == 0:
            return {'min': 0.0, 'max': 0.0, 'mean': 0.0, 'std': 0.0, 
                    'median': 0.0, 'q1': 0.0, 'q3': 0.0}

        sorted_data = sorted(data)
        mean = sum(data) / n
        variance = sum((x - mean) ** 2 for x in data) / n
        std = math.sqrt(variance)
        median = sorted_data[n // 2] if n % 2 == 1 else (sorted_data[n // 2 - 1] + sorted_data[n // 2]) / 2
        q1 = sorted_data[n // 4]
        q3 = sorted_data[3 * n // 4]

        return {
            'min': min(data),
            'max': max(data),
            'mean': mean,
            'std': std,
            'median': median,
            'q1': q1,
            'q3': q3,
        }

    def normalize(
        self,
        data: list[float],
        method: NormalizationType,
        target_min: float = 0.0,
        target_max: float = 1.0,
        power: float = 1.0
    ) -> NormalizationResult:
        """
        Normalize data using the specified method.
        
        Args:
            data: List of numeric values to normalize.
            method: Normalization method to apply.
            target_min: Target minimum for MIN_MAX scaling.
            target_max: Target maximum for MIN_MAX scaling.
            power: Power parameter for POWER normalization.
            
        Returns:
            NormalizationResult with normalized data and parameters.
            
        Raises:
            ValueError: If data is empty.
        """
        if not data:
            raise ValueError("Data cannot be empty")

        stats = self._compute_stats(data)

        methods: dict[NormalizationType, callable] = {
            NormalizationType.MIN_MAX: self._min_max_scale,
            NormalizationType.Z_SCORE: self._z_score_scale,
            NormalizationType.ROBUST: self._robust_scale,
            NormalizationType.LOG: self._log_transform,
            NormalizationType.LOG10: self._log10_transform,
            NormalizationType.SQRT: self._sqrt_transform,
            NormalizationType.ARCSIN: self._arcsin_transform,
            NormalizationType.TANH: self._tanh_transform,
            NormalizationType.MAX_ABS: self._max_abs_scale,
            NormalizationType.POWER: lambda d, p=power, **_: self._power_transform(d, power=p),
        }

        normalizer = methods.get(method)
        if normalizer is None:
            raise ValueError(f"Unknown normalization method: {method}")

        params = NormalizationParams(
            type=method,
            min_val=stats['min'],
            max_val=stats['max'],
            mean=stats['mean'],
            std=stats['std'],
            median=stats['median'],
            q1=stats['q1'],
            q3=stats['q3'],
            power=power,
            epsilon=self.epsilon
        )

        normalized = normalizer(data, params, target_min, target_max)

        return NormalizationResult(
            normalized_data=normalized,
            original_data=data,
            params=params,
            min_value=min(normalized) if normalized else 0.0,
            max_value=max(normalized) if normalized else 0.0,
            mean=sum(normalized) / len(normalized) if normalized else 0.0,
            std=math.sqrt(sum((x - (sum(normalized) / len(normalized))) ** 2 for x in normalized) / len(normalized)) if normalized else 0.0
        )

    def _min_max_scale(
        self,
        data: list[float],
        params: NormalizationParams,
        target_min: float,
        target_max: float
    ) -> list[float]:
        """Min-Max scaling to target range."""
        data_range = params.max_val - params.min_val
        if data_range < self.epsilon:
            return [target_min] * len(data)
        return [
            target_min + (x - params.min_val) / data_range * (target_max - target_min)
            for x in data
        ]

    def _z_score_scale(
        self,
        data: list[float],
        params: NormalizationParams,
        _: float,
        __: float
    ) -> list[float]:
        """Z-score standardization."""
        if params.std < self.epsilon:
            return [0.0] * len(data)
        return [(x - params.mean) / params.std for x in data]

    def _robust_scale(
        self,
        data: list[float],
        params: NormalizationParams,
        _: float,
        __: float
    ) -> list[float]:
        """Robust scaling using median and IQR."""
        iqr = params.q3 - params.q1
        if iqr < self.epsilon:
            return [0.0] * len(data)
        return [(x - params.median) / iqr for x in data]

    def _log_transform(
        self,
        data: list[float],
        params: NormalizationParams,
        _: float,
        __: float
    ) -> list[float]:
        """Natural log transformation (handles zeros and negatives)."""
        offset = -params.min_val + 1 if params.min_val <= 0 else 0
        return [math.log(x + offset + self.epsilon) for x in data]

    def _log10_transform(
        self,
        data: list[float],
        params: NormalizationParams,
        _: float,
        __: float
    ) -> list[float]:
        """Base-10 log transformation."""
        offset = -params.min_val + 1 if params.min_val <= 0 else 0
        return [math.log10(x + offset + self.epsilon) for x in data]

    def _sqrt_transform(
        self,
        data: list[float],
        params: NormalizationParams,
        _: float,
        __: float
    ) -> list[float]:
        """Square root transformation (handles zeros and negatives)."""
        offset = -params.min_val if params.min_val < 0 else 0
        return [math.sqrt(x + offset) for x in data]

    def _arcsin_transform(
        self,
        data: list[float],
        params: NormalizationParams,
        _: float,
        __: float
    ) -> list[float]:
        """Arcsin (inverse sine) transformation for proportion data."""
        # Clip to [0, 1] range first
        clipped = [max(0.0, min(1.0, x)) for x in data]
        return [math.asin(x) for x in clipped]

    def _tanh_transform(
        self,
        data: list[float],
        params: NormalizationParams,
        _: float,
        __: float
    ) -> list[float]:
        """Tanh normalization (output in [-1, 1])."""
        return [math.tanh(x) for x in data]

    def _max_abs_scale(
        self,
        data: list[float],
        params: NormalizationParams,
        _: float,
        __: float
    ) -> list[float]:
        """Max absolute scaling to [-1, 1] range."""
        max_abs = max(abs(params.min_val), abs(params.max_val))
        if max_abs < self.epsilon:
            return [0.0] * len(data)
        return [x / max_abs for x in data]

    def _power_transform(
        self,
        data: list[float],
        params: NormalizationParams,
        _: float,
        __: float,
        power: float = 1.0
    ) -> list[float]:
        """Power transformation (Yeo-Johnson inspired)."""
        if power == 0:
            return self._log_transform(data, params, _, __)
        elif power == 1:
            return data.copy()
        else:
            return [((x + 1) ** power - 1) / power if x >= 0 else -((-x + 1) ** power - 1) / power for x in data]

    def denormalize(
        self,
        normalized_data: list[float],
        params: NormalizationParams
    ) -> list[float]:
        """
        Reverse the normalization operation.
        
        Args:
            normalized_data: Data that was previously normalized.
            params: Parameters from the original normalization.
            
        Returns:
            Original-scale data.
        """
        method = params.type

        if method == NormalizationType.MIN_MAX:
            data_range = params.max_val - params.min_val
            if data_range < self.epsilon:
                return [params.min_val] * len(normalized_data)
            return [
                params.min_val + (x - 0.0) / 1.0 * data_range
                for x in normalized_data
            ]

        if method == NormalizationType.Z_SCORE:
            return [x * params.std + params.mean for x in normalized_data]

        if method == NormalizationType.ROBUST:
            iqr = params.q3 - params.q1
            return [x * iqr + params.median for x in normalized_data]

        if method == NormalizationType.MAX_ABS:
            max_abs = max(abs(params.min_val), abs(params.max_val))
            return [x * max_abs for x in normalized_data]

        # Most transformations are not easily reversible
        raise ValueError(f"Denormalization not supported for {method}")

    def fit_transform(
        self,
        data: list[float],
        method: NormalizationType,
        target_min: float = 0.0,
        target_max: float = 1.0,
        power: float = 1.0
    ) -> tuple[list[float], NormalizationParams]:
        """
        Fit normalization parameters and transform data.
        
        Args:
            data: Training data to fit parameters on.
            method: Normalization method.
            target_min: Target minimum for MIN_MAX scaling.
            target_max: Target maximum for MIN_MAX scaling.
            power: Power parameter for POWER normalization.
            
        Returns:
            Tuple of (normalized_data, fitted_params).
        """
        result = self.normalize(data, method, target_min, target_max, power)
        return result.normalized_data, result.params

    def transform(
        self,
        data: list[float],
        params: NormalizationParams
    ) -> list[float]:
        """
        Transform new data using pre-fitted parameters.
        
        Args:
            data: New data to transform.
            params: Pre-fitted normalization parameters.
            
        Returns:
            Normalized data using the fitted parameters.
        """
        if params.type == NormalizationType.MIN_MAX:
            target_min = params.min_val
            target_max = params.max_val
            return self._min_max_scale(data, params, target_min, target_max)
        elif params.type == NormalizationType.Z_SCORE:
            return self._z_score_scale(data, params, 0.0, 0.0)
        elif params.type == NormalizationType.ROBUST:
            return self._robust_scale(data, params, 0.0, 0.0)
        else:
            # For non-invertible transforms, just return
            raise ValueError(f"Transform with params not supported for {params.type}")
