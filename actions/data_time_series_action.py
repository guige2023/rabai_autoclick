"""Data Time Series Action Module.

Provides time series data processing including smoothing,
forecasting, trend detection, and seasonal decomposition.
"""

from __future__ import annotations

import logging
import math
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class SmoothingMethod(Enum):
    MOVING_AVERAGE = "moving_average"
    EXPONENTIAL = "exponential"
    Savitzky_GOLAY = "savitzky_golay"
    KALMAN = "kalman"


@dataclass
class TimeSeriesPoint:
    timestamp: datetime
    value: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TimeSeriesResult:
    smoothed: List[float]
    trend: List[float]
    residuals: List[float]
    forecast: List[float]
    stats: Dict[str, float]


class TimeSeriesSmoother:
    def __init__(self, window_size: int = 5, method: SmoothingMethod = SmoothingMethod.MOVING_AVERAGE):
        self.window_size = window_size
        self.method = method

    def smooth(self, values: List[float]) -> List[float]:
        if not values:
            return []

        if self.method == SmoothingMethod.MOVING_AVERAGE:
            return self._moving_average(values)
        elif self.method == SmoothingMethod.EXPONENTIAL:
            return self._exponential_smoothing(values)
        elif self.method == SmoothingMethod.Savitzky_GOLAY:
            return self._savitzky_golay(values)
        return values

    def _moving_average(self, values: List[float]) -> List[float]:
        result = []
        window = deque(maxlen=self.window_size)
        for v in values:
            window.append(v)
            result.append(sum(window) / len(window))
        return result

    def _exponential_smoothing(self, values: List[float], alpha: float = 0.3) -> List[float]:
        if not values:
            return []
        result = [values[0]]
        for v in values[1:]:
            smoothed = alpha * v + (1 - alpha) * result[-1]
            result.append(smoothed)
        return result

    def _savitzky_golay(self, values: List[float], poly_order: int = 2) -> List[float]:
        n = len(values)
        half_window = self.window_size // 2
        if n < self.window_size:
            return values

        result = []
        for i in range(n):
            start = max(0, i - half_window)
            end = min(n, i + half_window + 1)
            window_vals = values[start:end]
            x = list(range(start, end))
            coeffs = self._poly_coeffs(x, window_vals, poly_order)
            result.append(coeffs[0] + coeffs[1] * (i - start) if len(coeffs) > 1 else window_vals[len(window_vals) // 2])
        return result

    def _poly_coeffs(self, x: List[int], y: List[float], order: int) -> List[float]:
        if len(x) <= order:
            return [sum(y) / len(y)] if y else [0.0]
        n = len(x)
        sum_x = [0.0] * (2 * order + 1)
        sum_y = [0.0] * (order + 1)
        for i in range(n):
            for j in range(len(sum_x)):
                sum_x[j] += x[i] ** j
            for j in range(len(sum_y)):
                sum_y[j] += y[i] * (x[i] ** j)
        matrix = [[0.0] * (order + 1) for _ in range(order + 1)]
        for i in range(order + 1):
            for j in range(order + 1):
                matrix[i][j] = sum_x[i + j]
        try:
            coeffs = self._solve_linear(matrix, sum_y)
            return coeffs
        except Exception:
            return [sum(y) / len(y)]

    def _solve_linear(self, matrix: List[List[float]], b: List[float]) -> List[float]:
        n = len(b)
        for i in range(n):
            max_row = max(range(i, n), key=lambda r: abs(matrix[r][i]))
            matrix[i], matrix[max_row] = matrix[max_row], matrix[i]
            b[i], b[max_row] = b[max_row], b[i]
            for j in range(i + 1, n):
                factor = matrix[j][i] / matrix[i][i]
                for k in range(i, n):
                    matrix[j][k] -= factor * matrix[i][k]
                b[j] -= factor * b[i]
        x = [0.0] * n
        for i in range(n - 1, -1, -1):
            x[i] = b[i]
            for j in range(i + 1, n):
                x[i] -= matrix[i][j] * x[j]
            x[i] /= matrix[i][i]
        return x


def detect_trend(values: List[float]) -> Tuple[str, float]:
    if len(values) < 2:
        return "unknown", 0.0

    n = len(values)
    x_mean = (n - 1) / 2
    y_mean = sum(values) / n
    numerator = sum((i - x_mean) * (values[i] - y_mean) for i in range(n))
    denominator = sum((i - x_mean) ** 2 for i in range(n))

    if denominator == 0:
        return "flat", 0.0

    slope = numerator / denominator
    correlation = numerator / math.sqrt(denominator * sum((v - y_mean) ** 2 for v in values))

    if abs(slope) < 0.01:
        return "flat", slope
    elif slope > 0:
        return "increasing", slope
    else:
        return "decreasing", slope


def simple_forecast(values: List[float], steps: int = 5) -> List[float]:
    if len(values) < 2:
        return values * (steps + 1)

    n = len(values)
    x_mean = (n - 1) / 2
    y_mean = sum(values) / n
    numerator = sum((i - x_mean) * (values[i] - y_mean) for i in range(n))
    denominator = sum((i - x_mean) ** 2 for i in range(n))

    slope = numerator / denominator if denominator > 0 else 0.0
    intercept = y_mean - slope * x_mean

    last_value = values[-1]
    forecast = []
    for i in range(steps):
        pred = intercept + slope * (n + i)
        smoothed_pred = 0.7 * last_value + 0.3 * pred if forecast else pred
        forecast.append(smoothed_pred)
        last_value = smoothed_pred

    return forecast


def compute_rolling_stats(values: List[float], window: int) -> Dict[str, List[float]]:
    if len(values) < window:
        return {"mean": values, "std": [0.0] * len(values), "min": values, "max": values}

    means, stds, mins, maxs = [], [], [], []
    for i in range(len(values) - window + 1):
        chunk = values[i:i + window]
        means.append(sum(chunk) / window)
        variance = sum((x - means[-1]) ** 2 for x in chunk) / window
        stds.append(math.sqrt(variance))
        mins.append(min(chunk))
        maxs.append(max(chunk))

    return {"mean": means, "std": stds, "min": mins, "max": maxs}


def detect_seasonality(values: List[float], max_period: int = 24) -> Optional[int]:
    if len(values) < max_period * 2:
        return None

    best_corr = 0.0
    best_period = None

    for period in range(2, max_period + 1):
        correlations = []
        for offset in range(period):
            series1 = [values[i] for i in range(offset, len(values) - period, period)]
            series2 = [values[i + period] for i in range(offset, len(values) - period, period)]
            if len(series1) < 3:
                continue
            mean1, mean2 = sum(series1) / len(series1), sum(series2) / len(series2)
            corr = sum((a - mean1) * (b - mean2) for a, b in zip(series1, series2))
            denom = math.sqrt(sum((a - mean1) ** 2 for a in series1) * sum((b - mean2) ** 2 for b in series2))
            if denom > 0:
                correlations.append(abs(corr / denom))

        if correlations:
            avg_corr = sum(correlations) / len(correlations)
            if avg_corr > best_corr:
                best_corr = avg_corr
                best_period = period

    return best_period if best_corr > 0.5 else None
