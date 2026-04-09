"""
Touch signal denoising utilities.

This module provides denoising filters for raw touch input signals,
including median filtering, bilateral filtering, and Kalman filtering.
"""

from __future__ import annotations

import math
from typing import List, Tuple, Optional
from dataclasses import dataclass


# Type aliases
Signal1D = List[float]
Signal2D = List[Tuple[float, float]]


@dataclass
class MedianFilterConfig:
    """Configuration for median filter."""
    window_size: int = 3


@dataclass
class BilateralFilterConfig:
    """Configuration for bilateral filter (spatial + range)."""
    spatial_sigma: float = 2.0
    range_sigma: float = 10.0


@dataclass
class Kalman1DConfig:
    """Configuration for 1D Kalman filter."""
    process_noise: float = 0.1
    measurement_noise: float = 1.0
    estimate_error: float = 1.0


def median_filter_1d(signal: Signal1D, window_size: int = 3) -> Signal1D:
    """
    Apply median filter to a 1D signal.

    Args:
        signal: Input signal values.
        window_size: Size of the median window (must be odd).

    Returns:
        Filtered signal of the same length.
    """
    if window_size < 1:
        return signal[:]
    if window_size % 2 == 0:
        window_size += 1
    if len(signal) < window_size:
        return signal[:]

    half = window_size // 2
    result: Signal1D = []

    for i in range(len(signal)):
        start = max(0, i - half)
        end = min(len(signal), i + half + 1)
        window = sorted(signal[start:end])
        mid = len(window) // 2
        result.append(window[mid])

    return result


def median_filter_2d(signal: Signal2D, window_size: int = 3) -> Signal2D:
    """
    Apply median filter to a 2D (x, y) signal.

    Args:
        signal: Input signal as list of (x, y) tuples.
        window_size: Size of the median window.

    Returns:
        Filtered 2D signal.
    """
    if not signal:
        return []
    if window_size < 1:
        return signal[:]
    if window_size % 2 == 0:
        window_size += 1
    if len(signal) < window_size:
        return signal[:]

    half = window_size // 2
    result: Signal2D = []

    for i in range(len(signal)):
        start = max(0, i - half)
        end = min(len(signal), i + half + 1)
        window = signal[start:end]
        xs = sorted(p[0] for p in window)
        ys = sorted(p[1] for p in window)
        mid = len(window) // 2
        result.append((xs[mid], ys[mid]))

    return result


def bilateral_filter_1d(signal: Signal1D, config: Optional[BilateralFilterConfig] = None) -> Signal1D:
    """
    Apply bilateral filter to a 1D signal.

    Args:
        signal: Input signal values.
        config: Bilateral filter configuration.

    Returns:
        Filtered signal.
    """
    if config is None:
        config = BilateralFilterConfig()
    if len(signal) < 2:
        return signal[:]

    spatial_sigma = config.spatial_sigma
    range_sigma = config.range_sigma
    sigma2_spatial = 2 * spatial_sigma ** 2
    sigma2_range = 2 * range_sigma ** 2

    result: Signal1D = []
    for i in range(len(signal)):
        total_weight = 0.0
        weighted_sum = 0.0
        for j in range(len(signal)):
            spatial_dist = (i - j) ** 2
            range_dist = (signal[i] - signal[j]) ** 2
            weight = math.exp(-spatial_dist / sigma2_spatial - range_dist / sigma2_range)
            total_weight += weight
            weighted_sum += weight * signal[j]
        result.append(weighted_sum / total_weight if total_weight > 0 else signal[i])

    return result


class Kalman1DFilter:
    """1D Kalman filter for real-time signal smoothing."""

    def __init__(self, config: Optional[Kalman1DConfig] = None):
        if config is None:
            config = Kalman1DConfig()
        self.q = config.process_noise
        self.r = config.measurement_noise
        self.p = config.estimate_error
        self.x = 0.0
        self.initialized = False

    def update(self, measurement: float) -> float:
        """
        Update filter with a new measurement.

        Args:
            measurement: New signal value.

        Returns:
            Filtered estimate.
        """
        if not self.initialized:
            self.x = measurement
            self.initialized = True
            return self.x

        # Prediction
        self.p = self.p + self.q

        # Update
        k = self.p / (self.p + self.r)
        self.x = self.x + k * (measurement - self.x)
        self.p = (1 - k) * self.p

        return self.x

    def reset(self):
        """Reset the filter state."""
        self.x = 0.0
        self.p = 1.0
        self.initialized = False


def kalman_filter_2d(signal: Signal2D) -> Signal2D:
    """
    Apply 1D Kalman filters independently to x and y components.

    Args:
        signal: Input 2D signal.

    Returns:
        Filtered 2D signal.
    """
    if not signal:
        return []
    kf_x = Kalman1DFilter()
    kf_y = Kalman1DFilter()
    result: Signal2D = []
    for x, y in signal:
        result.append((kf_x.update(x), kf_y.update(y)))
    return result


def compute_signal_noise(signal: Signal1D) -> float:
    """
    Estimate noise level in a signal using high-frequency component analysis.

    Args:
        signal: Input signal.

    Returns:
        Estimated noise standard deviation.
    """
    if len(signal) < 3:
        return 0.0

    # High-pass component: second derivative approximation
    hp = []
    for i in range(1, len(signal) - 1):
        hp.append(signal[i] - (signal[i - 1] + signal[i + 1]) / 2)

    if not hp:
        return 0.0
    mean_hp = sum(hp) / len(hp)
    variance = sum((v - mean_hp) ** 2 for v in hp) / len(hp)
    return math.sqrt(variance)


def adaptive_denoise(
    signal: Signal2D,
    noise_estimate: Optional[float] = None,
) -> Signal2D:
    """
    Adaptively denoise a 2D touch signal based on estimated noise level.

    Args:
        signal: Input 2D signal.
        noise_estimate: Pre-computed noise estimate (auto-computed if None).

    Returns:
        Denoised 2D signal.
    """
    if not signal:
        return []

    # Convert to 1D signals for noise estimation
    xs = [p[0] for p in signal]
    ys = [p[1] for p in signal]

    if noise_estimate is None:
        noise_x = compute_signal_noise(xs)
        noise_y = compute_signal_noise(ys)
        noise_estimate = (noise_x + noise_y) / 2.0

    # Choose filter strength based on noise
    if noise_estimate < 1.0:
        # Low noise - light median filter
        return median_filter_2d(signal, window_size=3)
    elif noise_estimate < 5.0:
        # Medium noise - bilateral filter
        config = BilateralFilterConfig(spatial_sigma=noise_estimate, range_sigma=noise_estimate * 2)
        result_x = bilateral_filter_1d(xs, config)
        result_y = bilateral_filter_1d(ys, config)
        return list(zip(result_x, result_y))
    else:
        # High noise - Kalman filter
        return kalman_filter_2d(signal)
