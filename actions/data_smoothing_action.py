"""Data Smoothing Action module.

Provides smoothing and noise reduction for time series
and sequential data using various algorithms including
moving average, exponential smoothing, Savitzky-Golay,
and Gaussian smoothing.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import numpy as np


@dataclass
class SmoothingConfig:
    """Configuration for smoothing operations."""

    window_size: int = 5
    polynomial_order: int = 2
    sigma: float = 1.0


def moving_average_smooth(
    data: list[float],
    window_size: int = 5,
    center: bool = False,
) -> list[float]:
    """Apply simple moving average smoothing.

    Args:
        data: Input data
        window_size: Size of smoothing window
        center: Whether to center the window

    Returns:
        Smoothed data
    """
    if len(data) < window_size:
        return data

    result = []
    half_window = window_size // 2

    for i in range(len(data)):
        if center:
            start = max(0, i - half_window)
            end = min(len(data), i + half_window + 1)
        else:
            start = max(0, i - window_size + 1)
            end = i + 1

        window = data[start:end]
        result.append(sum(window) / len(window))

    return result


def exponential_smooth(
    data: list[float],
    alpha: float = 0.3,
) -> list[float]:
    """Apply exponential smoothing.

    Args:
        data: Input data
        alpha: Smoothing factor (0 < alpha <= 1)

    Returns:
        Smoothed data
    """
    if not data:
        return []

    if not 0 < alpha <= 1:
        raise ValueError("alpha must be in (0, 1]")

    result = [data[0]]

    for i in range(1, len(data)):
        smoothed = alpha * data[i] + (1 - alpha) * result[-1]
        result.append(smoothed)

    return result


def savitzky_golay_smooth(
    data: list[float],
    window_size: int = 5,
    polynomial_order: int = 2,
) -> list[float]:
    """Apply Savitzky-Golay smoothing.

    Args:
        data: Input data
        window_size: Must be odd
        polynomial_order: Order of polynomial

    Returns:
        Smoothed data
    """
    if window_size % 2 == 0:
        raise ValueError("window_size must be odd")

    if window_size < polynomial_order + 2:
        raise ValueError("window_size must be >= polynomial_order + 2")

    n = len(data)
    half_window = window_size // 2

    result = []

    for i in range(n):
        start = max(0, i - half_window)
        end = min(n, i + half_window + 1)

        window_size_actual = end - start
        indices = list(range(start, end))
        j_start = indices[0]

        x = np.array([j - j_start for j in indices])
        y = np.array([data[j] for j in indices])

        try:
            coeffs = np.polyfit(x, y, polynomial_order)
            y_pred = np.polyval(coeffs, [i - j_start])
            result.append(float(y_pred))
        except np.linalg.LinAlgError:
            result.append(data[i])

    return result


def gaussian_smooth(
    data: list[float],
    sigma: float = 1.0,
    truncate: float = 3.0,
) -> list[float]:
    """Apply Gaussian smoothing.

    Args:
        data: Input data
        sigma: Standard deviation for Gaussian
        truncate: Truncate window at this many sigmas

    Returns:
        Smoothed data
    """
    if sigma <= 0:
        raise ValueError("sigma must be positive")

    n = len(data)
    half_window = int(truncate * sigma + 0.5)
    window_size = 2 * half_window + 1

    x = np.arange(-half_window, half_window + 1)
    kernel = np.exp(-(x**2) / (2 * sigma**2))
    kernel = kernel / kernel.sum()

    result = []

    for i in range(n):
        start = max(0, i - half_window)
        end = min(n, i + half_window + 1)

        kernel_start = half_window - (i - start)
        kernel_end = half_window + (end - i)

        local_kernel = kernel[kernel_start:kernel_end]
        local_kernel = local_kernel / local_kernel.sum()

        window = data[start:end]
        smoothed = sum(w * k for w, k in zip(window, local_kernel))
        result.append(smoothed)

    return result


def loess_smooth(
    data: list[float],
    window_size: int = 5,
    degree: int = 1,
    robustness_iterations: int = 2,
) -> list[float]:
    """Apply LOESS (locally estimated scatterplot smoothing).

    Args:
        data: Input data
        window_size: Number of points in local window
        degree: Polynomial degree (0, 1, or 2)
        robustness_iterations: Number of robustness iterations

    Returns:
        Smoothed data
    """
    if not data:
        return []

    n = len(data)
    half_window = window_size // 2
    result = [0.0] * n
    residuals = [0.0] * n

    for iteration in range(robustness_iterations + 1):
        if iteration == 0:
            weights = [1.0] * n
        else:
            max_residual = max(abs(r) for r in residuals) or 1.0
            for i in range(n):
                r = abs(residuals[i]) / max_residual
                weights[i] = (1 - min(r, 1) ** 2) ** 2 if r < 1 else 0

        for i in range(n):
            start = max(0, i - half_window)
            end = min(n, i + half_window + 1)

            x_vals = list(range(start, end))
            y_vals = [data[j] * weights[j] for j in range(start, end)]
            w_vals = [weights[j] for j in range(start, end)]

            if sum(w_vals) == 0:
                result[i] = data[i]
                continue

            x_array = np.array(x_vals)
            y_array = np.array(y_vals)
            w_array = np.diag(w_vals)

            try:
                X = np.vstack([np.ones(len(x_vals)), x_array]).T
                if degree > 1:
                    X = np.column_stack([X, x_array**2])

                W_sqrt = np.sqrt(w_array)
                XW = W_sqrt @ X
                yW = W_sqrt @ y_array

                coeffs, _, _, _ = np.linalg.lstsq(XW, yW, rcond=None)
                result[i] = sum(c * (i ** p) for p, c in enumerate(coeffs))
            except np.linalg.LinAlgError:
                result[i] = data[i]

        if iteration < robustness_iterations:
            for i in range(n):
                residuals[i] = data[i] - result[i]

    return result


class AdaptiveSmoother:
    """Adaptively selects best smoothing method."""

    def __init__(self):
        self._methods = {
            "ma": lambda d, **k: moving_average_smooth(d, **k),
            "exp": lambda d, **k: exponential_smooth(d, **k),
            "sg": lambda d, **k: savitzky_golay_smooth(d, **k),
            "gauss": lambda d, **k: gaussian_smooth(d, **k),
        }

    def smooth(
        self,
        data: list[float],
        method: str = "ma",
        **kwargs: Any,
    ) -> list[float]:
        """Apply smoothing with specified method.

        Args:
            data: Input data
            method: Smoothing method ('ma', 'exp', 'sg', 'gauss')
            **kwargs: Method-specific parameters

        Returns:
            Smoothed data
        """
        if method not in self._methods:
            raise ValueError(f"Unknown method: {method}")

        return self._methods[method](data, **kwargs)

    def smooth_auto(
        self,
        data: list[float],
        noise_level: Optional[float] = None,
    ) -> list[float]:
        """Automatically select best smoothing.

        Args:
            data: Input data
            noise_level: Estimated noise level

        Returns:
            Smoothed data
        """
        if len(data) < 10:
            return exponential_smooth(data, alpha=0.3)

        if noise_level is None:
            diffs = [abs(data[i] - data[i-1]) for i in range(1, len(data))]
            noise_level = sum(diffs) / len(diffs) if diffs else 1.0

        if noise_level < 0.1:
            return exponential_smooth(data, alpha=0.5)
        elif noise_level < 1.0:
            return moving_average_smooth(data, window_size=3)
        else:
            return moving_average_smooth(data, window_size=5)
