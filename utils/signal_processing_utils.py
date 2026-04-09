"""Signal processing utilities for RabAI AutoClick.

Provides:
- Signal filtering operations
- Signal smoothing and differentiation
- Peak and zero-crossing detection
- Signal matching and correlation
"""

from typing import List, Tuple, Optional, Callable
import math


def moving_average(
    signal: List[float],
    window_size: int,
) -> List[float]:
    """Apply moving average filter.

    Args:
        signal: Input signal.
        window_size: Window size (odd).

    Returns:
        Filtered signal.
    """
    if window_size < 1:
        return signal[:]
    half = window_size // 2
    n = len(signal)
    result: List[float] = []

    for i in range(n):
        lo = max(0, i - half)
        hi = min(n, i + half + 1)
        result.append(sum(signal[lo:hi]) / (hi - lo))

    return result


def exponential_moving_average(
    signal: List[float],
    alpha: float = 0.3,
) -> List[float]:
    """Exponential moving average.

    Args:
        signal: Input signal.
        alpha: Smoothing factor (0-1).

    Returns:
        EMA signal.
    """
    if not signal:
        return []
    result: List[float] = [signal[0]]
    for v in signal[1:]:
        result.append(alpha * v + (1 - alpha) * result[-1])
    return result


def savitzky_golay_filter(
    signal: List[float],
    window_size: int,
    order: int = 2,
) -> List[float]:
    """Savitzky-Golay smoothing filter.

    Args:
        signal: Input signal.
        window_size: Window size (odd, >= order+2).
        order: Polynomial order.

    Returns:
        Smoothed signal.
    """
    if window_size % 2 == 0:
        window_size += 1
    if window_size <= order:
        return signal[:]
    half = window_size // 2

    # Compute convolution coefficients
    n = order + 1
    # Simple approach: use polynomial fit at each window
    result: List[float] = []

    for i in range(len(signal)):
        lo = max(0, i - half)
        hi = min(len(signal), i + half + 1)
        window = signal[lo:hi]
        wlen = len(window)
        if wlen < n:
            result.append(sum(window) / wlen)
            continue

        # Simple linear regression for order=1, quadratic for order=2
        center = i - lo
        t_vals = list(range(wlen))
        t_center = t_vals[center]

        if order == 1:
            # Linear fit
            t_mean = sum(t_vals) / wlen
            x_mean = sum(window) / wlen
            num = sum((t - t_mean) * (x - x_mean) for t, x in zip(t_vals, window))
            den = sum((t - t_mean) ** 2 for t in t_vals)
            if abs(den) > 1e-10:
                slope = num / den
                intercept = x_mean - slope * t_mean
                result.append(intercept + slope * t_center)
            else:
                result.append(x_mean)
        else:
            # Use mean for higher orders (simplified)
            result.append(sum(window) / wlen)

    return result


def first_derivative(
    signal: List[float],
    dt: float = 1.0,
) -> List[float]:
    """Compute first derivative using central differences.

    Args:
        signal: Input signal.
        dt: Sample interval.

    Returns:
        Derivative signal.
    """
    n = len(signal)
    if n < 2:
        return [0.0] * n

    result: List[float] = [0.0]
    for i in range(1, n - 1):
        result.append((signal[i + 1] - signal[i - 1]) / (2 * dt))
    result.append(0.0)
    return result


def second_derivative(
    signal: List[float],
    dt: float = 1.0,
) -> List[float]:
    """Compute second derivative."""
    n = len(signal)
    if n < 3:
        return [0.0] * n

    result: List[float] = [0.0]
    for i in range(1, n - 1):
        result.append((signal[i + 1] - 2 * signal[i] + signal[i - 1]) / (dt * dt))
    result.append(0.0)
    return result


def find_peaks(
    signal: List[float],
    threshold: float = 0.0,
    min_distance: int = 1,
) -> List[int]:
    """Find peak indices in signal.

    Args:
        signal: Input signal.
        threshold: Minimum peak height.
        min_distance: Minimum index distance between peaks.

    Returns:
        List of peak indices.
    """
    n = len(signal)
    if n < 3:
        return []

    peaks: List[int] = []
    last_peak = -min_distance

    for i in range(1, n - 1):
        if signal[i] > signal[i - 1] and signal[i] > signal[i + 1]:
            if signal[i] >= threshold and i - last_peak >= min_distance:
                peaks.append(i)
                last_peak = i

    return peaks


def find_valleys(
    signal: List[float],
    threshold: float = 0.0,
    min_distance: int = 1,
) -> List[int]:
    """Find valley (local minimum) indices."""
    n = len(signal)
    if n < 3:
        return []

    valleys: List[int] = []
    last_valley = -min_distance

    for i in range(1, n - 1):
        if signal[i] < signal[i - 1] and signal[i] < signal[i + 1]:
            if signal[i] <= threshold and i - last_valley >= min_distance:
                valleys.append(i)
                last_valley = i

    return valleys


def zero_crossings(signal: List[float]) -> List[int]:
    """Find zero-crossing indices."""
    crossings: List[int] = []
    for i in range(len(signal) - 1):
        if signal[i] >= 0 and signal[i + 1] < 0:
            crossings.append(i)
        elif signal[i] < 0 and signal[i + 1] >= 0:
            crossings.append(i)
    return crossings


def normalize_signal(
    signal: List[float],
    target_min: float = 0.0,
    target_max: float = 1.0,
) -> List[float]:
    """Normalize signal to target range.

    Args:
        signal: Input signal.
        target_min: Desired minimum.
        target_max: Desired maximum.

    Returns:
        Normalized signal.
    """
    if not signal:
        return []
    s_min = min(signal)
    s_max = max(signal)
    if abs(s_max - s_min) < 1e-10:
        return [target_min] * len(signal)

    scale = (target_max - target_min) / (s_max - s_min)
    return [target_min + (v - s_min) * scale for v in signal]


def correlate_signals(
    signal1: List[float],
    signal2: List[float],
    max_lag: int = 50,
) -> List[float]:
    """Cross-correlate two signals.

    Args:
        signal1: First signal.
        signal2: Second signal.
        max_lag: Maximum lag to compute.

    Returns:
        Cross-correlation values for lags 0..max_lag.
    """
    n1, n2 = len(signal1), len(signal2)
    result: List[float] = []

    for lag in range(max_lag + 1):
        if lag < n2:
            corr = sum(signal1[i] * signal2[i + lag] for i in range(n1 - lag))
        else:
            corr = 0.0
        result.append(corr)

    return result


def match_template(
    signal: List[float],
    template: List[float],
) -> List[float]:
    """Match template against signal using sliding dot product.

    Args:
        signal: Input signal.
        template: Template to match.

    Returns:
        Similarity score at each position.
    """
    n, m = len(signal), len(template)
    if m > n or m == 0:
        return []

    scores: List[float] = []
    template_mean = sum(template) / m
    template_std = math.sqrt(sum((t - template_mean) ** 2 for t in template) / m)

    for i in range(n - m + 1):
        window = signal[i:i + m]
        window_mean = sum(window) / m
        window_std = math.sqrt(sum((w - window_mean) ** 2 for w in window) / m)

        if window_std < 1e-10 or template_std < 1e-10:
            scores.append(0.0)
            continue

        corr = sum((window[j] - window_mean) * (template[j] - template_mean)
                   for j in range(m)) / (m * window_std * template_std)
        scores.append(corr)

    return scores


def downsample_signal(
    signal: List[float],
    factor: int,
) -> List[float]:
    """Downsample signal by integer factor (decimation).

    Args:
        signal: Input signal.
        factor: Downsampling factor.

    Returns:
        Downsampled signal.
    """
    if factor < 1:
        return signal[:]
    return [signal[i] for i in range(0, len(signal), factor)]


def upsample_signal(
    signal: List[float],
    factor: int,
) -> List[float]:
    """Upsample signal by integer factor (zero-fill + low-pass)."""
    n = len(signal)
    upsampled: List[float] = [0.0] * (n * factor)
    for i in range(n):
        upsampled[i * factor] = signal[i]
    return moving_average(upsampled, factor)


def signal_envelope(
    signal: List[float],
    window_size: int = 10,
) -> List[float]:
    """Extract signal envelope using peak detection."""
    n = len(signal)
    envelope: List[float] = [0.0] * n
    half = window_size // 2

    for i in range(n):
        lo = max(0, i - half)
        hi = min(n, i + half + 1)
        envelope[i] = max(abs(signal[lo:hi]))

    return envelope
