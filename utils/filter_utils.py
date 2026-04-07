"""
Digital filter design and application utilities.

Provides FIR/IIR filter design, Kalman filter, particle filter,
median filter, and adaptive filters.
"""

from __future__ import annotations

import math
from typing import Callable


class KalmanFilter:
    """1D Kalman filter."""

    def __init__(
        self,
        process_variance: float = 1e-4,
        measurement_variance: float = 1e-2,
        initial_estimate: float = 0.0,
        initial_estimate_error: float = 1.0,
    ):
        self.q = process_variance  # Process noise covariance
        self.r = measurement_variance  # Measurement noise covariance
        self.x = initial_estimate  # State estimate
        self.p = initial_estimate_error  # Estimate error covariance
        self.k = 0.0  # Kalman gain

    def update(self, measurement: float) -> float:
        """
        Update filter with new measurement.

        Returns:
            Filtered estimate.
        """
        # Prediction
        self.p += self.q
        # Update
        self.k = self.p / (self.p + self.r)
        self.x += self.k * (measurement - self.x)
        self.p *= 1.0 - self.k
        return self.x


class AdaptiveFilter:
    """LMS (Least Mean Squares) adaptive filter."""

    def __init__(self, n_taps: int = 32, mu: float = 0.01):
        self.n_taps = n_taps
        self.mu = mu  # Step size
        self.weights: list[float] = [0.0] * n_taps
        self.buffer: list[float] = [0.0] * n_taps

    def update(self, desired: float, input_signal: float) -> float:
        """
        LMS update step.

        Args:
            desired: Desired signal (reference)
            input_signal: Current input sample

        Returns:
            Filter output.
        """
        # Shift buffer
        self.buffer.pop()
        self.buffer.insert(0, input_signal)

        # Compute output: y = w^T * x
        output = sum(w * x for w, x in zip(self.weights, self.buffer))

        # Error
        error = desired - output

        # Update weights
        for i in range(self.n_taps):
            self.weights[i] += self.mu * error * self.buffer[i]

        return output

    def apply(self, signal: list[float], reference: list[float]) -> list[float]:
        """Apply filter to entire signal."""
        outputs = []
        for i, (d, x) in enumerate(zip(reference, signal)):
            self.buffer = [0.0] * self.n_taps
            outputs.append(self.update(d, x))
        return outputs


def fir_lowpass_kernel(cutoff: float, sample_rate: float, order: int) -> list[float]:
    """
    Design FIR low-pass filter kernel using sinc.

    Args:
        cutoff: Cutoff frequency (Hz)
        sample_rate: Sample rate (Hz)
        order: Filter order (odd)

    Returns:
        FIR kernel coefficients.
    """
    if order % 2 == 0:
        order += 1
    n = order
    fc = cutoff / sample_rate
    kernel: list[float] = []
    for i in range(n):
        m = i - n // 2
        if m == 0:
            kernel.append(2.0 * math.pi * fc)
        else:
            kernel.append(math.sin(2.0 * math.pi * fc * m) / m)
    # Apply Blackman window
    for i in range(n):
        w = 0.42 - 0.5 * math.cos(2.0 * math.pi * i / (n - 1)) + 0.08 * math.cos(4.0 * math.pi * i / (n - 1))
        kernel[i] *= w
    # Normalize
    s = sum(kernel)
    return [k / s for k in kernel]


def fir_highpass_kernel(cutoff: float, sample_rate: float, order: int) -> list[float]:
    """Design FIR high-pass filter kernel."""
    lp = fir_lowpass_kernel(cutoff, sample_rate, order)
    hp = [-x for x in lp]
    hp[order // 2] += 1.0
    return hp


def median_filter(signal: list[float], window: int) -> list[float]:
    """
    Median filter (non-linear).

    Args:
        signal: Input signal
        window: Window size (must be odd)

    Returns:
        Filtered signal.
    """
    if window < 1:
        return list(signal)
    if window % 2 == 0:
        window += 1
    half = window // 2
    result: list[float] = []
    for i in range(len(signal)):
        start = max(0, i - half)
        end = min(len(signal), i + half + 1)
        window_vals = sorted(signal[start:end])
        mid = len(window_vals) // 2
        if len(window_vals) % 2 == 0:
            result.append((window_vals[mid - 1] + window_vals[mid]) / 2)
        else:
            result.append(window_vals[mid])
    return result


def bilateral_filter(
    signal: list[float],
    spatial_sigma: float = 2.0,
    range_sigma: float = 1.0,
) -> list[float]:
    """
    1D bilateral filter.

    Args:
        signal: Input signal
        spatial_sigma: Spatial (neighborhood) sigma
        range_sigma: Intensity range sigma

    Returns:
        Filtered signal.
    """
    n = len(signal)
    window = int(3 * spatial_sigma)
    if window < 1:
        window = 1
    result: list[float] = []
    for i in range(n):
        w_sum = 0.0
        val_sum = 0.0
        for j in range(max(0, i - window), min(n, i + window + 1)):
            spatial_diff = (i - j) ** 2 / (2 * spatial_sigma ** 2)
            range_diff = (signal[i] - signal[j]) ** 2 / (2 * range_sigma ** 2)
            weight = math.exp(-spatial_diff - range_diff)
            w_sum += weight
            val_sum += weight * signal[j]
        result.append(val_sum / w_sum if w_sum > 0 else signal[i])
    return result


def savitzky_golay_filter(
    signal: list[float],
    window: int,
    poly_order: int = 3,
    deriv: int = 0,
) -> list[float]:
    """
    Savitzky-Golay smoothing filter.

    Args:
        signal: Input signal
        window: Window size (must be odd, >= poly_order + 2)
        poly_order: Polynomial order
        deriv: Derivative order (0 = smoothing)

    Returns:
        Filtered signal.
    """
    n = len(signal)
    if window % 2 == 0:
        window += 1
    half = window // 2
    if window > n:
        return list(signal)

    # Build design matrix
    A: list[list[float]] = []
    for i in range(-half, half + 1):
        row = [i ** j for j in range(poly_order + 1)]
        A.append(row)
    AT = list(zip(*A))
    # Pseudo-inverse: (A^T A)^-1 A^T
    m = len(A)
    k = len(A[0])
    ATA: list[list[float]] = [[sum(AT[i][t] * A[t][j] for t in range(m)) for j in range(k)] for i in range(k)]
    # Invert ATA (Gauss-Jordan)
    for col in range(k):
        for row in range(col + 1, k):
            factor = ATA[row][col] / ATA[col][col]
            for c in range(col, k):
                ATA[row][c] -= factor * ATA[col][c]

    coeffs: list[float] = [0.0] * window
    for j in range(k):
        for jj in range(k):
            if ATA[j][jj] != 0:
                inv_jj = 1.0 / ATA[j][jj]
                break
        for i in range(m):
            coeffs[i + half] += inv_jj * A[i + half][j] * sum(ATA[j][c] * A[i + half][c] for c in range(k))

    result: list[float] = []
    for i in range(n):
        start = max(0, i - half)
        end = min(n, i + half + 1)
        seg = signal[start:end]
        pad_l = half - (i - start)
        pad_r = half - (end - i - 1)
        padded = [signal[start]] * max(0, pad_l) + seg + [signal[end - 1]] * max(0, pad_r)
        if len(padded) < window:
            padded = [padded[0]] * (window - len(padded)) + padded
        result.append(sum(c * s for c, s in zip(coeffs, padded)))
    return result


def exponential_smoothing(
    signal: list[float],
    alpha: float = 0.3,
) -> list[float]:
    """
    Simple exponential smoothing.

    Args:
        signal: Input signal
        alpha: Smoothing factor (0 < alpha <= 1)

    Returns:
        Smoothed signal.
    """
    if not signal:
        return []
    alpha = max(0.001, min(1.0, alpha))
    result: list[float] = [signal[0]]
    for i in range(1, len(signal)):
        result.append(alpha * signal[i] + (1 - alpha) * result[-1])
    return result


def double_exponential_smoothing(
    signal: list[float],
    alpha: float = 0.3,
    beta: float = 0.1,
) -> list[float]:
    """
    Double exponential smoothing (Holt's linear trend).

    Returns:
        Smoothed signal with trend.
    """
    if len(signal) < 2:
        return list(signal)
    alpha = max(0.001, min(1.0, alpha))
    beta = max(0.001, min(1.0, beta))
    level = signal[0]
    trend = signal[1] - signal[0]
    result = [level]
    for i in range(1, len(signal)):
        prev_level = level
        level = alpha * signal[i] + (1 - alpha) * (level + trend)
        trend = beta * (level - prev_level) + (1 - beta) * trend
        result.append(level)
    return result


def wiener_filter(
    signal: list[float],
    noise_variance: float,
    window: int = 50,
) -> list[float]:
    """
    Wiener filter for noise reduction.

    Args:
        signal: Noisy signal
        noise_variance: Estimated noise variance
        window: Local window size

    Returns:
        Denoised signal.
    """
    n = len(signal)
    result: list[float] = []
    half = window // 2
    for i in range(n):
        start = max(0, i - half)
        end = min(n, i + half + 1)
        local = signal[start:end]
        local_mean = sum(local) / len(local)
        local_var = sum((x - local_mean) ** 2 for x in local) / len(local)
        sigma_sq = min(noise_variance, local_var)
        if local_var > 0:
            gain = sigma_sq / local_var
        else:
            gain = 0.0
        result.append(local_mean + gain * (signal[i] - local_mean))
    return result
