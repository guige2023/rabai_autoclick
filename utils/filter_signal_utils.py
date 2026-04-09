"""Signal filtering and noise reduction utilities.

Provides various filters for smoothing signals,
removing noise, and extracting trends from data streams.
"""

from __future__ import annotations

from typing import List, Deque, Callable, Optional
from collections import deque
import math
from dataclasses import dataclass, field


@dataclass
class MovingAverageFilter:
    """Simple moving average filter.

    Example:
        filter = MovingAverageFilter(window_size=5)
        values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        for v in values:
            print(filter.filter(v))  # Outputs smoothed values
    """

    def __init__(self, window_size: int = 5) -> None:
        if window_size < 1:
            raise ValueError("Window size must be at least 1")
        self.window_size = window_size
        self._buffer: Deque[float] = deque(maxlen=window_size)

    def filter(self, value: float) -> float:
        """Add value and return filtered result."""
        self._buffer.append(value)
        return sum(self._buffer) / len(self._buffer)

    def reset(self) -> None:
        """Clear filter buffer."""
        self._buffer.clear()

    @property
    def is_ready(self) -> bool:
        """Check if filter has enough data for full window."""
        return len(self._buffer) >= self.window_size


@dataclass
class ExponentialMovingAverageFilter:
    """Exponential moving average filter (EMA).

    Attributes:
        alpha: Smoothing factor between 0 and 1.
               Higher values = more responsive but less smooth.
    """

    alpha: float = 0.3

    def __post_init__(self) -> None:
        if not 0.0 < self.alpha <= 1.0:
            raise ValueError("Alpha must be in (0, 1]")
        self._value: Optional[float] = None

    def filter(self, value: float) -> float:
        """Add value and return exponentially smoothed result."""
        if self._value is None:
            self._value = value
        else:
            self._value = self.alpha * value + (1.0 - self.alpha) * self._value
        return self._value

    def reset(self) -> None:
        """Reset filter state."""
        self._value = None

    @property
    def is_ready(self) -> bool:
        return self._value is not None


@dataclass
class MedianFilter:
    """Median filter for removing outliers.

    Useful for removing spikes from sensor data.
    """

    window_size: int = 5

    def __post_init__(self) -> None:
        if self.window_size < 1 or self.window_size % 2 == 0:
            raise ValueError("Window size must be odd and at least 1")
        self._buffer: Deque[float] = deque(maxlen=self.window_size)

    def filter(self, value: float) -> float:
        """Add value and return median of current window."""
        self._buffer.append(value)
        sorted_vals = sorted(self._buffer)
        n = len(sorted_vals)
        mid = n // 2
        if n % 2 == 1:
            return sorted_vals[mid]
        return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0

    def reset(self) -> None:
        """Clear filter buffer."""
        self._buffer.clear()


@dataclass
class KalmanFilter1D:
    """1D Kalman filter for noise reduction.

    Attributes:
        process_variance: Expected variance of the process noise.
        measurement_variance: Variance of measurement noise.
        estimated_error: Initial estimate error.
    """

    process_variance: float = 0.1
    measurement_variance: float = 1.0
    estimated_error: float = 1.0
    _value: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        self._initialized = False

    def filter(self, measurement: float) -> float:
        """Add measurement and return filtered estimate."""
        if not self._initialized:
            self._value = measurement
            self._initialized = True
            return self._value
        prediction_error = self.estimated_error + self.process_variance
        kalman_gain = prediction_error / (prediction_error + self.measurement_variance)
        self._value = self._value + kalman_gain * (measurement - self._value)
        self.estimated_error = (
            (1.0 - kalman_gain) * prediction_error
        )
        return self._value

    def reset(self) -> None:
        """Reset filter state."""
        self._initialized = False
        self._value = 0.0
        self.estimated_error = 1.0


@dataclass
class OneEuroFilter:
    """One Euro filter for smoothing time-series data.

    Good for motion tracking and real-time filtering with
    adjustable smoothing and responsiveness.
    """

    min_cutoff: float = 1.0
    beta: float = 0.0
    derivative_cutoff: float = 1.0
    _value: Optional[float] = field(default=None, init=False)
    _derivative: float = field(default=0.0, init=False)

    def __post_init__(self) -> None:
        self._initialized = False
        self._prev_time: Optional[float] = None

    def filter(self, value: float, time: Optional[float] = None) -> float:
        """Add value and return filtered result."""
        if self._value is None:
            self._value = value
            self._initialized = True
            return self._value
        if time is not None and self._prev_time is not None:
            dt = time - self._prev_time
            if dt <= 0:
                dt = 1e-6
        else:
            dt = 1e-6
        alpha = self._compute_alpha(self.min_cutoff, dt)
        self._value = alpha * value + (1.0 - alpha) * self._value
        dx = (self._value - value) / dt if dt > 0 else 0.0
        alpha_d = self._compute_alpha(self.derivative_cutoff, dt)
        self._derivative = alpha_d * dx + (1.0 - alpha_d) * self._derivative
        cutoff = self.min_cutoff + self.beta * abs(self._derivative)
        alpha_with_beta = self._compute_alpha(cutoff, dt)
        return alpha_with_beta * value + (1.0 - alpha_with_beta) * self._value

    def _compute_alpha(self, cutoff: float, dt: float) -> float:
        """Compute filter coefficient from cutoff frequency."""
        tau = 1.0 / (2.0 * math.pi * cutoff) if cutoff > 0 else float('inf')
        return 1.0 / (1.0 + tau / dt)

    def reset(self) -> None:
        """Reset filter state."""
        self._value = None
        self._derivative = 0.0
        self._initialized = False
        self._prev_time = None


class SavitzkyGolayFilter:
    """Savitzky-Golay filter for smoothing with minimal distortion.

    Uses convolution to fit successive sub-sets of data with
    a polynomial.
    """

    def __init__(self, window_size: int = 5, poly_order: int = 2) -> None:
        if window_size < 3 or poly_order >= window_size:
            raise ValueError("window_size must be >= 3 and > poly_order")
        if window_size % 2 == 0:
            raise ValueError("window_size must be odd")
        self.window_size = window_size
        self.poly_order = poly_order
        self._coeffs = self._compute_coeffs()

    def _compute_coeffs(self) -> List[float]:
        """Compute Savitzky-Golay coefficients."""
        order = self.poly_order
        half = self.window_size // 2
        indices = list(range(-half, half + 1))
        A = [[i ** k for k in range(order + 1)] for i in indices]
        AT = list(zip(*A))
        ATA = [
            [sum(a * b for a, b in zip(AT[i], AT[j]))
             for j in range(order + 1)]
            for i in range(order + 1)
        ]
        ATA_inv = self._matrix_invert(ATA)
        coeffs = [
            sum(ATA_inv[0][k] * A[i][k] for k in range(order + 1))
            for i in range(self.window_size)
        ]
        return coeffs

    def _matrix_invert(self, A: List[List[float]]) -> List[List[float]]:
        """Simple matrix inversion for small matrices."""
        n = len(A)
        I = [[1.0 if i == j else 0.0 for j in range(n)] for i in range(n)]
        for i in range(n):
            scale = A[i][i]
            if abs(scale) < 1e-10:
                raise ValueError("Matrix is singular")
            for j in range(n):
                A[i][j] /= scale
                I[i][j] /= scale
            for k in range(n):
                if k != i:
                    factor = A[k][i]
                    for j in range(n):
                        A[k][j] -= factor * A[i][j]
                        I[k][j] -= factor * I[i][j]
        return I

    def filter_sequence(self, values: List[float]) -> List[float]:
        """Apply filter to entire sequence."""
        if len(values) < self.window_size:
            return values[:]
        half = self.window_size // 2
        result = []
        for i in range(len(values)):
            start = max(0, i - half)
            end = min(len(values), i + half + 1)
            window = values[start:end]
            if len(window) < self.window_size:
                pad_start = max(0, half - i)
                pad_end = max(0, (i + half + 1) - len(values))
                window = ([window[0]] * pad_start + window
                          + [window[-1]] * pad_end)
            result.append(sum(c * w for c, w in zip(self._coeffs, window)))
        return result


def butterworth_lowpass(
    values: List[float],
    cutoff: float,
    sample_rate: float
) -> List[float]:
    """Apply Butterworth lowpass filter to signal."""
    order = 4
    wc = 2.0 * cutoff / sample_rate
    if wc >= 1.0:
        return values[:]
    k = 1.0 / math.tan(math.pi * wc / 2.0)
    k2 = k * k
    norm = 1.0 / (1.0 + math.sqrt(2.0) * k + k2)
    b0 = norm
    b1 = 2.0 * norm
    b2 = norm
    a1 = 2.0 * (k2 - 1.0) * norm
    a2 = (1.0 - math.sqrt(2.0) * k + k2) * norm
    result = []
    x1, x2, y1, y2 = 0.0, 0.0, 0.0, 0.0
    for x in values:
        y = b0 * x + b1 * x1 + b2 * x2 - a1 * y1 - a2 * y2
        x2, x1 = x1, x
        y2, y1 = y1, y
        result.append(y)
    return result


@dataclass
class BangBangFilter:
    """Bang-bang (hysteresis) filter for digital signals.

    Applies threshold-based filtering with configurable hysteresis.
    """

    high_threshold: float = 1.0
    low_threshold: float = 0.0
    initial_state: bool = False

    def __post_init__(self) -> None:
        self._state = self.initial_state

    def filter(self, value: float) -> bool:
        """Apply filter and return state."""
        if self._state:
            if value < self.low_threshold:
                self._state = False
        else:
            if value > self.high_threshold:
                self._state = True
        return self._state

    def reset(self) -> None:
        """Reset filter state."""
        self._state = self.initial_state
