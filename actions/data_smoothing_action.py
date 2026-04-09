"""
Data Smoothing Action Module

Provides data smoothing and noise reduction for time series and sequential
data in UI automation workflows. Supports moving average, exponential
smoothing, Gaussian smoothing, and Savitzky-Golay filters.

Author: AI Agent
Version: 1.0.0
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class SmoothingMethod(Enum):
    """Smoothing method types."""
    MOVING_AVERAGE = auto()
    WEIGHTED_MOVING_AVERAGE = auto()
    EXPONENTIAL_WEIGHTED_MOVING_AVERAGE = auto()
    GAUSSIAN_SMOOTHING = auto()
    SAVITZKY_GOLAY = auto()
    MEDIAN_FILTER = auto()
    KALMAN_FILTER = auto()


@dataclass
class SmoothingConfig:
    """Smoothing configuration."""
    method: SmoothingMethod = SmoothingMethod.MOVING_AVERAGE
    window_size: int = 5
    alpha: float = 0.3
    sigma: float = 1.0
    poly_order: int = 2


@dataclass
class SmoothingResult:
    """Smoothing operation result."""
    smoothed_data: list[float]
    original_data: list[float]
    method_used: SmoothingMethod
    metrics: dict[str, float] = field(default_factory=dict)


class MovingAverageSmoother:
    """
    Moving average smoothing.

    Example:
        >>> smoother = MovingAverageSmoother(window_size=5)
        >>> result = smoother.smooth([1.0, 2.0, 3.0, 4.0, 5.0])
    """

    def __init__(self, window_size: int = 5) -> None:
        self.window_size = window_size

    def smooth(self, data: list[float]) -> list[float]:
        """Apply moving average smoothing."""
        if len(data) < self.window_size:
            return self._simple_average(data)

        result = []
        for i in range(len(data)):
            window_start = max(0, i - self.window_size // 2)
            window_end = min(len(data), i + self.window_size // 2 + 1)
            window = data[window_start:window_end]
            result.append(sum(window) / len(window))

        return result

    def _simple_average(self, data: list[float]) -> list[float]:
        """Simple average for small datasets."""
        avg = sum(data) / len(data) if data else 0
        return [avg] * len(data)


class WeightedMovingAverageSmoother:
    """
    Weighted moving average smoothing.

    Example:
        >>> smoother = WeightedMovingAverageSmoother(window_size=5)
        >>> result = smoother.smooth([1.0, 2.0, 3.0, 4.0, 5.0])
    """

    def __init__(self, window_size: int = 5) -> None:
        self.window_size = window_size
        self._weights = self._calculate_weights()

    def _calculate_weights(self) -> list[float]:
        """Calculate triangular weights."""
        weights = []
        for i in range(self.window_size):
            position = i - self.window_size // 2
            weight = self.window_size - abs(position)
            weights.append(weight)
        total = sum(weights)
        return [w / total for w in weights]

    def smooth(self, data: list[float]) -> list[float]:
        """Apply weighted moving average."""
        if len(data) < self.window_size:
            return data

        result = []
        for i in range(len(data)):
            window_start = max(0, i - self.window_size // 2)
            window_end = min(len(data), i + self.window_size // 2 + 1)
            window = data[window_start:window_end]

            weights_offset = window_start - (i - self.window_size // 2)
            weights = self._weights[weights_offset:weights_offset + len(window)]

            while len(weights) < len(window):
                weights.append(1.0 / len(window))

            weighted_sum = sum(w * v for w, v in zip(weights[:len(window)], window))
            result.append(weighted_sum)

        return result


class ExponentialWeightedMovingAverageSmoother:
    """
    Exponential weighted moving average (EWMA) smoothing.

    Example:
        >>> smoother = ExponentialWeightedMovingAverageSmoother(alpha=0.3)
        >>> result = smoother.smooth([1.0, 2.0, 3.0, 4.0, 5.0])
    """

    def __init__(self, alpha: float = 0.3) -> None:
        self.alpha = alpha

    def smooth(self, data: list[float]) -> list[float]:
        """Apply EWMA smoothing."""
        if not data:
            return []

        result = [data[0]]

        for i in range(1, len(data)):
            smoothed = self.alpha * data[i] + (1 - self.alpha) * result[-1]
            result.append(smoothed)

        return result


class GaussianSmoother:
    """
    Gaussian smoothing with configurable sigma.

    Example:
        >>> smoother = GaussianSmoother(window_size=5, sigma=1.0)
        >>> result = smoother.smooth([1.0, 2.0, 3.0, 4.0, 5.0])
    """

    def __init__(self, window_size: int = 5, sigma: float = 1.0) -> None:
        self.window_size = window_size
        self.sigma = sigma
        self._kernel = self._calculate_kernel()

    def _calculate_kernel(self) -> list[float]:
        """Calculate Gaussian kernel."""
        half_window = self.window_size // 2
        kernel = []

        for i in range(-half_window, half_window + 1):
            gaussian = math.exp(-(i ** 2) / (2 * self.sigma ** 2))
            kernel.append(gaussian)

        total = sum(kernel)
        return [k / total for k in kernel]

    def smooth(self, data: list[float]) -> list[float]:
        """Apply Gaussian smoothing."""
        if len(data) < self.window_size:
            return data

        result = []
        half_window = self.window_size // 2

        for i in range(len(data)):
            window_start = max(0, i - half_window)
            window_end = min(len(data), i + half_window + 1)
            window = data[window_start:window_end]

            kernel_offset = window_start - (i - half_window)
            kernel = self._kernel[kernel_offset:kernel_offset + len(window)]

            while len(kernel) < len(window):
                kernel.append(1.0 / len(window))

            smoothed = sum(k * v for k, v in zip(kernel[:len(window)], window))
            result.append(smoothed)

        return result


class SavitzkyGolaySmoother:
    """
    Savitzky-Golay smoothing filter.

    Example:
        >>> smoother = SavitzkyGolaySmoother(window_size=5, poly_order=2)
        >>> result = smoother.smooth([1.0, 2.0, 3.0, 4.0, 5.0])
    """

    def __init__(self, window_size: int = 5, poly_order: int = 2) -> None:
        if window_size % 2 == 0:
            window_size += 1
        self.window_size = window_size
        self.poly_order = poly_order
        self._coefficients = self._calculate_coefficients()

    def _calculate_coefficients(self) -> list[float]:
        """Calculate Savitzky-Golay coefficients."""
        n = self.window_size // 2
        order = self.poly_order

        import numpy as np
        x = np.arange(-n, n + 1)
        A = np.zeros((self.window_size, order + 1))

        for i in range(order + 1):
            A[:, i] = x ** i

        M = np.linalg.inv(A.T @ A) @ A.T
        return M[0].tolist()

    def smooth(self, data: list[float]) -> list[float]:
        """Apply Savitzky-Golay smoothing."""
        if len(data) < self.window_size:
            return data

        result = []
        half_window = self.window_size // 2

        for i in range(len(data)):
            window_start = max(0, i - half_window)
            window_end = min(len(data), i + half_window + 1)
            window = data[window_start:window_end]

            coeff_offset = window_start - (i - half_window)
            coeff = self._coefficients[coeff_offset:coeff_offset + len(window)]

            while len(coeff) < len(window):
                coeff.append(1.0 / len(window))

            smoothed = sum(c * v for c, v in zip(coeff[:len(window)], window))
            result.append(smoothed)

        return result


class MedianFilterSmoother:
    """
    Median filter smoothing.

    Example:
        >>> smoother = MedianFilterSmoother(window_size=3)
        >>> result = smoother.smooth([1.0, 2.0, 3.0, 4.0, 5.0])
    """

    def __init__(self, window_size: int = 3) -> None:
        if window_size % 2 == 0:
            window_size += 1
        self.window_size = window_size

    def smooth(self, data: list[float]) -> list[float]:
        """Apply median filter."""
        if len(data) < self.window_size:
            return data

        result = []
        half_window = self.window_size // 2

        for i in range(len(data)):
            window_start = max(0, i - half_window)
            window_end = min(len(data), i + half_window + 1)
            window = data[window_start:window_end]

            sorted_window = sorted(window)
            median = sorted_window[len(sorted_window) // 2]
            result.append(median)

        return result


class DataSmoother:
    """
    Unified data smoother with multiple methods.

    Example:
        >>> config = SmoothingConfig(method=SmoothingMethod.GAUSSIAN_SMOOTHING, sigma=1.0)
        >>> smoother = DataSmoother(config)
        >>> result = smoother.smooth([1.0, 2.0, 3.0, 4.0, 5.0])
    """

    def __init__(self, config: Optional[SmoothingConfig] = None) -> None:
        self.config = config or SmoothingConfig()
        self._smoothers: dict[SmoothingMethod, Callable] = {
            SmoothingMethod.MOVING_AVERAGE: MovingAverageSmoother(
                window_size=self.config.window_size
            ).smooth,
            SmoothingMethod.WEIGHTED_MOVING_AVERAGE: WeightedMovingAverageSmoother(
                window_size=self.config.window_size
            ).smooth,
            SmoothingMethod.EXPONENTIAL_WEIGHTED_MOVING_AVERAGE: (
                ExponentialWeightedMovingAverageSmoother(alpha=self.config.alpha).smooth
            ),
            SmoothingMethod.GAUSSIAN_SMOOTHING: GaussianSmoother(
                window_size=self.config.window_size,
                sigma=self.config.sigma,
            ).smooth,
            SmoothingMethod.SAVITZKY_GOLAY: SavitzkyGolaySmoother(
                window_size=self.config.window_size,
                poly_order=self.config.poly_order,
            ).smooth,
            SmoothingMethod.MEDIAN_FILTER: MedianFilterSmoother(
                window_size=self.config.window_size
            ).smooth,
        }

    def smooth(self, data: list[float]) -> SmoothingResult:
        """Apply configured smoothing method."""
        smoother = self._smoothers.get(self.config.method)

        if smoother is None:
            smoother = self._smoothers[SmoothingMethod.MOVING_AVERAGE]

        smoothed_data = smoother(data)

        original_variance = self._variance(data)
        smoothed_variance = self._variance(smoothed_data)
        noise_reduction = 1.0 - (smoothed_variance / original_variance) if original_variance > 0 else 0

        metrics = {
            "original_variance": original_variance,
            "smoothed_variance": smoothed_variance,
            "noise_reduction_ratio": max(0, noise_reduction),
            "data_points": len(data),
            "smoothed_points": len(smoothed_data),
        }

        return SmoothingResult(
            smoothed_data=smoothed_data,
            original_data=data,
            method_used=self.config.method,
            metrics=metrics,
        )

    def _variance(self, data: list[float]) -> float:
        """Calculate variance."""
        if len(data) < 2:
            return 0.0
        mean = sum(data) / len(data)
        return sum((x - mean) ** 2 for x in data) / len(data)

    def smooth_dict_list(
        self,
        data: list[dict],
        value_field: str,
        time_field: Optional[str] = None,
    ) -> list[dict]:
        """Smooth values in dictionary list."""
        if not data:
            return data

        values = [d.get(value_field, 0) for d in data]
        result = self.smooth(values)

        result_data = [d.copy() for d in data]
        for i, record in enumerate(result_data):
            record[value_field] = result.smoothed_data[i]

        return result_data
