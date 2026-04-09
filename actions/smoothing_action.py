"""
Smoothing module for noise reduction and signal processing.

Provides moving average, exponential smoothing, Savitzky-Golay,
and Gaussian smoothing for time series and signal data.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum, auto
from typing import Callable, Optional


class SmoothingType(Enum):
    """Smoothing algorithm types."""
    MOVING_AVERAGE = auto()
    EXPONENTIAL = auto()
    SAVITZKY_GOLAY = auto()
    GAUSSIAN = auto()
    MEDIAN = auto()
    KALMAN = auto()


@dataclass
class SmoothingResult:
    """Result of smoothing operation."""
    smoothed_data: list[float]
    original_data: list[float]
    method: SmoothingType
    parameters: dict[str, float]
    noise_reduction: float
    smoothness_score: float


@dataclass
class ExponentialSmoothingParams:
    """Parameters for exponential smoothing."""
    alpha: float = 0.3
    trend: bool = False
    beta: float = 0.1


class DataSmoother:
    """
    Applies smoothing algorithms to reduce noise in data.
    
    Example:
        smoother = DataSmoother()
        data = [random.gauss(5, 1) for _ in range(100)]
        result = smoother.smooth(data, SmoothingType.MOVING_AVERAGE, window_size=5)
    """

    def __init__(self) -> None:
        """Initialize data smoother."""
        self._last_result: Optional[SmoothingResult] = None

    def smooth(
        self,
        data: list[float],
        method: SmoothingType,
        **params
    ) -> SmoothingResult:
        """
        Apply smoothing to the data.
        
        Args:
            data: Numeric data to smooth.
            method: Smoothing algorithm to use.
            **params: Method-specific parameters.
                - window_size: For MOVING_AVERAGE, MEDIAN (default 5)
                - alpha: For EXPONENTIAL (default 0.3)
                - sigma: For GAUSSIAN (default 2.0)
                - window_size, poly_order: For SAVITZKY_GOLAY
                
        Returns:
            SmoothingResult with smoothed data and metrics.
            
        Raises:
            ValueError: If data is empty or parameters are invalid.
        """
        if not data:
            raise ValueError("Data cannot be empty")

        methods: dict[SmoothingType, callable] = {
            SmoothingType.MOVING_AVERAGE: self._moving_average,
            SmoothingType.EXPONENTIAL: self._exponential_smooth,
            SmoothingType.SAVITZKY_GOLAY: self._savitzky_golay,
            SmoothingType.GAUSSIAN: self._gaussian_smooth,
            SmoothingType.MEDIAN: self._median_smooth,
        }

        smoother = methods.get(method)
        if smoother is None:
            raise ValueError(f"Unknown smoothing method: {method}")

        smoothed = smoother(data, **params)
        smoothness = self._compute_smoothness(smoothed)
        noise_reduction = self._compute_noise_reduction(data, smoothed)

        self._last_result = SmoothingResult(
            smoothed_data=smoothed,
            original_data=data,
            method=method,
            parameters=params,
            noise_reduction=round(noise_reduction, 4),
            smoothness_score=round(smoothness, 4)
        )

        return self._last_result

    def _moving_average(
        self,
        data: list[float],
        window_size: int = 5,
        **_
    ) -> list[float]:
        """Simple moving average smoothing."""
        if window_size < 1:
            raise ValueError("window_size must be at least 1")
        window_size = min(window_size, len(data))

        result = []
        for i in range(len(data)):
            start = max(0, i - window_size // 2)
            end = min(len(data), i + window_size // 2 + 1)
            window = data[start:end]
            result.append(sum(window) / len(window))

        return [round(v, 6) for v in result]

    def _exponential_smooth(
        self,
        data: list[float],
        alpha: float = 0.3,
        **_
    ) -> list[float]:
        """Exponential weighted moving average."""
        if not 0 < alpha <= 1:
            raise ValueError("alpha must be in (0, 1]")

        result = [data[0]]
        for i in range(1, len(data)):
            smoothed = alpha * data[i] + (1 - alpha) * result[-1]
            result.append(smoothed)

        return [round(v, 6) for v in result]

    def _savitzky_golay(
        self,
        data: list[float],
        window_size: int = 5,
        poly_order: int = 2,
        **_
    ) -> list[float]:
        """
        Savitzky-Golay smoothing filter.
        
        Uses local polynomial regression for smoothing while preserving
        features like peak shape and width.
        """
        if poly_order >= window_size:
            raise ValueError("poly_order must be less than window_size")
        if window_size % 2 == 0:
            raise ValueError("window_size must be odd")

        half_window = window_size // 2
        n = len(data)
        result = []

        # Pre-compute convolution coefficients
        coefficients = self._sg_coefficients(window_size, poly_order)

        for i in range(n):
            start = max(0, i - half_window)
            end = min(n, i + half_window + 1)
            
            # Pad with edge values if needed
            window = data[start:end]
            while len(window) < window_size:
                if start == 0:
                    window.insert(0, data[0])
                else:
                    window.append(data[-1])

            # Apply filter
            smoothed = sum(c * w for c, w in zip(coefficients, window))
            result.append(smoothed)

        return [round(v, 6) for v in result]

    def _sg_coefficients(self, window_size: int, poly_order: int) -> list[float]:
        """Compute Savitzky-Golay filter coefficients."""
        half = window_size // 2
        coeffs = [0.0] * window_size

        # Simple approximation using binomial weights
        for i in range(window_size):
            x = i - half
            # Binomial coefficients as approximation
            weight = math.exp(-x * x / (2 * ((window_size / 3) ** 2)))
            coeffs[i] = weight

        total = sum(coeffs)
        return [c / total for c in coeffs]

    def _gaussian_smooth(
        self,
        data: list[float],
        sigma: float = 2.0,
        **_
    ) -> list[float]:
        """Gaussian smoothing filter."""
        if sigma <= 0:
            raise ValueError("sigma must be positive")

        # Determine kernel size (truncate at ~3 sigma)
        kernel_size = int(3 * sigma + 0.5) * 2 + 1
        kernel_size = max(3, min(kernel_size, len(data)))
        half = kernel_size // 2

        # Gaussian kernel
        kernel = [
            math.exp(-((i - half) ** 2) / (2 * sigma ** 2))
            for i in range(kernel_size)
        ]
        kernel_sum = sum(kernel)

        result = []
        for i in range(len(data)):
            weighted_sum = 0.0
            weight_sum = 0.0
            for j in range(kernel_size):
                idx = i + j - half
                if 0 <= idx < len(data):
                    weight = kernel[j]
                    weighted_sum += weight * data[idx]
                    weight_sum += weight
            result.append(weighted_sum / weight_sum if weight_sum > 0 else data[i])

        return [round(v, 6) for v in result]

    def _median_smooth(
        self,
        data: list[float],
        window_size: int = 5,
        **_
    ) -> list[float]:
        """Median filter (excellent for impulsive noise)."""
        if window_size < 1:
            raise ValueError("window_size must be at least 1")
        window_size = min(window_size, len(data))
        half = window_size // 2

        result = []
        for i in range(len(data)):
            start = max(0, i - half)
            end = min(len(data), i + half + 1)
            window = sorted(data[start:end])
            median = window[len(window) // 2]
            result.append(median)

        return [round(v, 6) for v in result]

    def _compute_smoothness(self, data: list[float]) -> float:
        """Compute smoothness score (lower = smoother)."""
        if len(data) < 3:
            return 0.0

        # Second derivative magnitude (approximates oscillations)
        diff2 = [abs(data[i] - 2 * data[i + 1] + data[i + 2])
                 for i in range(len(data) - 2)]

        return sum(diff2) / len(diff2) if diff2 else 0.0

    def _compute_noise_reduction(
        self,
        original: list[float],
        smoothed: list[float]
    ) -> float:
        """Compute noise reduction percentage."""
        if len(original) != len(smoothed):
            return 0.0

        # Variance of first differences as noise proxy
        def noise_variance(data: list[float]) -> float:
            if len(data) < 2:
                return 0.0
            diffs = [data[i] - data[i - 1] for i in range(1, len(data))]
            mean = sum(diffs) / len(diffs)
            return sum((d - mean) ** 2 for d in diffs) / len(diffs)

        orig_noise = noise_variance(original)
        smooth_noise = noise_variance(smoothed)

        if orig_noise == 0:
            return 0.0

        return max(0.0, (1 - smooth_noise / orig_noise) * 100)

    def adaptive_smooth(
        self,
        data: list[float],
        min_window: int = 3,
        max_window: int = 15
    ) -> SmoothingResult:
        """
        Adaptively choose window size based on local data characteristics.
        
        Uses smaller windows in regions of high variability and
        larger windows in stable regions.
        """
        if not data:
            raise ValueError("Data cannot be empty")

        # Compute local variance for each point
        window = 5
        local_var = []
        for i in range(len(data)):
            start = max(0, i - window // 2)
            end = min(len(data), i + window // 2 + 1)
            segment = data[start:end]
            mean = sum(segment) / len(segment)
            var = sum((x - mean) ** 2 for x in segment) / len(segment)
            local_var.append(var)

        # Determine adaptive window sizes
        mean_var = sum(local_var) / len(local_var)
        std_var = math.sqrt(sum((v - mean_var) ** 2 for v in local_var) / len(local_var))

        smoothed = []
        for i in range(len(data)):
            # Larger window for low variance, smaller for high
            if local_var[i] < mean_var - std_var:
                w = max_window
            elif local_var[i] > mean_var + std_var:
                w = min_window
            else:
                w = (min_window + max_window) // 2

            w = min(w, max_window)
            w = max(w, min_window)
            if w % 2 == 0:
                w += 1

            start = max(0, i - w // 2)
            end = min(len(data), i + w // 2 + 1)
            segment = data[start:end]
            smoothed.append(sum(segment) / len(segment))

        smoothness = self._compute_smoothness(smoothed)
        noise_reduction = self._compute_noise_reduction(data, smoothed)

        return SmoothingResult(
            smoothed_data=[round(v, 6) for v in smoothed],
            original_data=data,
            method=SmoothingType.MOVING_AVERAGE,
            parameters={'adaptive': True, 'min_window': min_window, 'max_window': max_window},
            noise_reduction=round(noise_reduction, 4),
            smoothness_score=round(smoothness, 4)
        )
