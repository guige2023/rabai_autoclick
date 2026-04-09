"""Data Time Series Decomposition Action.

Decomposes time series into trend, seasonality, and residual
components using moving average and STL-like methods.
"""
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
import math


@dataclass
class DecompositionResult:
    trend: List[float]
    seasonal: List[float]
    residual: List[float]
    period: int
    method: str

    def as_dict(self) -> Dict[str, List[float]]:
        return {
            "trend": [round(v, 4) for v in self.trend],
            "seasonal": [round(v, 4) for v in self.seasonal],
            "residual": [round(v, 4) for v in self.residual],
        }


class DataTimeSeriesDecompositionAction:
    """Decomposes time series into trend/seasonal/residual."""

    def __init__(self) -> None:
        self._last_result: Optional[DecompositionResult] = None

    def _moving_average(self, values: List[float], window: int) -> List[float]:
        n = len(values)
        result = [0.0] * n
        half = window // 2
        for i in range(n):
            start = max(0, i - half)
            end = min(n, i + half + 1)
            result[i] = sum(values[start:end]) / (end - start)
        return result

    def _centered_ma(self, values: List[float], period: int) -> List[float]:
        """Centered moving average for even periods."""
        if period % 2 == 1:
            return self._moving_average(values, period)
        n = len(values)
        result = [0.0] * n
        half = period // 2
        for i in range(half, n - half):
            window = values[i - half : i + half]
            result[i] = sum(window) / period
        return result

    def decompose(
        self,
        values: List[float],
        period: int,
        method: str = "additive",
    ) -> DecompositionResult:
        if len(values) < 2 * period:
            return DecompositionResult(
                trend=values[:],
                seasonal=[0.0] * len(values),
                residual=[0.0] * len(values),
                period=period,
                method=method,
            )
        # Step 1: Estimate trend using centered moving average
        trend = self._centered_ma(values, period)
        # Extend trend at edges by copying nearest valid value
        half = period // 2
        for i in range(half):
            if i < len(trend) and half < len(trend):
                trend[i] = trend[half]
        for i in range(len(values) - half, len(values)):
            if i < len(trend) and len(values) - half - 1 < len(trend):
                trend[i] = trend[len(values) - half - 1]
        # Step 2: Detrend
        detrended = []
        for i, v in enumerate(values):
            if i < len(trend):
                detrended.append(v - trend[i] if method == "additive" else v / trend[i])
            else:
                detrended.append(0.0)
        # Step 3: Seasonal component (average of detrended values by phase)
        seasonal = [0.0] * len(values)
        seasonal_avg = [0.0] * period
        counts = [0] * period
        for i, v in enumerate(detrended):
            seasonal_avg[i % period] += v
            counts[i % period] += 1
        for j in range(period):
            if counts[j] > 0:
                seasonal_avg[j] /= counts[j]
        # Normalize seasonal to sum to 0 (additive) or geometric mean 1 (multiplicative)
        if method == "additive":
            seasonal_mean = sum(seasonal_avg) / period
            seasonal_avg = [s - seasonal_mean for s in seasonal_avg]
        for i, v in enumerate(values):
            seasonal[i] = seasonal_avg[i % period]
        # Step 4: Residual
        residual = []
        for i, v in enumerate(values):
            t = trend[i] if i < len(trend) else 0.0
            s = seasonal[i]
            if method == "additive":
                residual.append(v - t - s)
            else:
                residual.append(v / (t * s) if t * s != 0 else 0.0)
        result = DecompositionResult(
            trend=trend,
            seasonal=seasonal,
            residual=residual,
            period=period,
            method=method,
        )
        self._last_result = result
        return result

    def detect_period(self, values: List[float], max_period: int = 52) -> int:
        """Auto-detect period using autocorrelation."""
        if len(values) < 4:
            return 7
        mean = sum(values) / len(values)
        deviations = [v - mean for v in values]
        n = len(values)
        best_lag = 1
        best_corr = -1.0
        for lag in range(1, min(max_period, n // 2)):
            numerator = sum(
                deviations[i] * deviations[i + lag]
                for i in range(n - lag)
            )
            denom = sum(d * d for d in deviations)
            if denom == 0:
                continue
            corr = numerator / denom
            if corr > best_corr:
                best_corr = corr
                best_lag = lag
        return best_lag
