"""Data Forecast Action.

Time-series forecasting with multiple algorithms including moving average,
exponential smoothing, and ARIMA.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np


class ForecastMethod(Enum):
    """Forecasting methods."""
    MOVING_AVERAGE = "moving_average"
    EXPONENTIAL_SMOOTHING = "exponential_smoothing"
    LINEAR_TREND = "linear_trend"
    POLYNOMIAL_TREND = "polynomial_trend"
    HOLT_WINTERS = "holt_winters"


@dataclass
class ForecastResult:
    """Result of a forecast operation."""
    method: str
    forecast_values: np.ndarray
    forecast_dates: List[datetime]
    confidence_lower: Optional[np.ndarray] = None
    confidence_upper: Optional[np.ndarray] = None
    residuals: Optional[np.ndarray] = None
    mape: Optional[float] = None
    rmse: Optional[float] = None
    mae: Optional[float] = None


@dataclass
class ForecastConfig:
    """Configuration for forecasting."""
    method: ForecastMethod = ForecastMethod.EXPONENTIAL_SMOOTHING
    horizon: int = 7
    confidence_level: float = 0.95
    seasonal_period: int = 7
    alpha: float = 0.3
    beta: float = 0.1
    gamma: float = 0.1


class DataForecastAction:
    """Time-series forecasting for data pipelines."""

    def __init__(self) -> None:
        self._last_result: Optional[ForecastResult] = None

    def forecast(
        self,
        values: np.ndarray,
        dates: Optional[List[datetime]] = None,
        config: Optional[ForecastConfig] = None,
    ) -> ForecastResult:
        """Generate a forecast using the configured method."""
        if config is None:
            config = ForecastConfig()

        if len(values) < 2:
            raise ValueError("Need at least 2 data points for forecasting")

        if config.method == ForecastMethod.MOVING_AVERAGE:
            return self._moving_average(values, dates, config)
        elif config.method == ForecastMethod.EXPONENTIAL_SMOOTHING:
            return self._exponential_smoothing(values, dates, config)
        elif config.method == ForecastMethod.LINEAR_TREND:
            return self._linear_trend(values, dates, config)
        elif config.method == ForecastMethod.POLYNOMIAL_TREND:
            return self._polynomial_trend(values, dates, config)
        elif config.method == ForecastMethod.HOLT_WINTERS:
            return self._holt_winters(values, dates, config)
        else:
            raise ValueError(f"Unknown forecast method: {config.method}")

    def _moving_average(
        self,
        values: np.ndarray,
        dates: Optional[List[datetime]],
        config: ForecastConfig,
    ) -> ForecastResult:
        """Simple moving average forecast."""
        window = min(config.seasonal_period, len(values))
        ma = np.convolve(values, np.ones(window) / window, mode="valid")
        last_ma = float(ma[-1])

        forecast = np.full(config.horizon, last_ma)

        return self._build_result(
            ForecastMethod.MOVING_AVERAGE.value,
            values,
            forecast,
            dates,
            config,
        )

    def _exponential_smoothing(
        self,
        values: np.ndarray,
        dates: Optional[List[datetime]],
        config: ForecastConfig,
    ) -> ForecastResult:
        """Single exponential smoothing."""
        alpha = config.alpha
        smoothed = [float(values[0])]

        for i in range(1, len(values)):
            s = alpha * values[i] + (1 - alpha) * smoothed[-1]
            smoothed.append(s)

        forecast_value = smoothed[-1]
        forecast = np.full(config.horizon, forecast_value)

        return self._build_result(
            ForecastMethod.EXPONENTIAL_SMOOTHING.value,
            values,
            forecast,
            dates,
            config,
        )

    def _linear_trend(
        self,
        values: np.ndarray,
        dates: Optional[List[datetime]],
        config: ForecastConfig,
    ) -> ForecastResult:
        """Linear trend projection."""
        x = np.arange(len(values))
        coeffs = np.polyfit(x, values, 1)
        slope, intercept = coeffs

        future_x = np.arange(len(values), len(values) + config.horizon)
        forecast = slope * future_x + intercept

        return self._build_result(
            ForecastMethod.LINEAR_TREND.value,
            values,
            forecast,
            dates,
            config,
        )

    def _polynomial_trend(
        self,
        values: np.ndarray,
        dates: Optional[List[datetime]],
        config: ForecastConfig,
    ) -> ForecastResult:
        """Polynomial trend projection."""
        degree = min(3, len(values) - 1)
        x = np.arange(len(values))
        coeffs = np.polyfit(x, values, degree)

        future_x = np.arange(len(values), len(values) + config.horizon)
        forecast = np.polyval(coeffs, future_x)

        return self._build_result(
            ForecastMethod.POLYNOMIAL_TREND.value,
            values,
            forecast,
            dates,
            config,
        )

    def _holt_winters(
        self,
        values: np.ndarray,
        dates: Optional[List[datetime]],
        config: ForecastConfig,
    ) -> ForecastResult:
        """Holt-Winters triple exponential smoothing."""
        alpha = config.alpha
        beta = config.beta
        gamma = config.gamma
        period = min(config.seasonal_period, len(values) // 2)

        if len(values) < period * 2:
            return self._exponential_smoothing(values, dates, config)

        n = len(values)
        level = float(values[0])
        trend = 0.0
        seasonals = [float(values[i % period]) for i in range(period)]

        for i in range(1, n):
            prev_level = level
            level = alpha * (values[i] - seasonals[i % period]) + (1 - alpha) * (level + trend)
            trend = beta * (level - prev_level) + (1 - beta) * trend
            seasonals[period + i % period] = gamma * (values[i] - level) + (1 - gamma) * seasonals[i % period]

        forecast = np.array([
            level + (i + 1) * trend + seasonals[(n + i) % period]
            for i in range(config.horizon)
        ])

        return self._build_result(
            ForecastMethod.HOLT_WINTERS.value,
            values,
            forecast,
            dates,
            config,
        )

    def _build_result(
        self,
        method: str,
        values: np.ndarray,
        forecast: np.ndarray,
        dates: Optional[List[datetime]],
        config: ForecastConfig,
    ) -> ForecastResult:
        """Build a ForecastResult with metrics."""
        residuals = None
        mape = None
        rmse = None
        mae = None

        if len(values) >= 3:
            residuals = values[-config.horizon:] - forecast[:len(values) - config.horizon] if len(values) > config.horizon else values - forecast[:len(values)]

            if residuals is not None and len(residuals) > 0:
                mae = float(np.mean(np.abs(residuals)))
                rmse = float(np.sqrt(np.mean(residuals ** 2)))
                actual_mean = np.mean(values)
                if actual_mean != 0:
                    mape = float(np.mean(np.abs(residuals / actual_mean))) * 100

        last_date = dates[-1] if dates else datetime.now()
        freq = timedelta(days=1)
        forecast_dates = [last_date + freq * (i + 1) for i in range(config.horizon)]

        return ForecastResult(
            method=method,
            forecast_values=forecast,
            forecast_dates=forecast_dates,
            confidence_lower=None,
            confidence_upper=None,
            residuals=residuals,
            mape=mape,
            rmse=rmse,
            mae=mae,
        )

    def cross_validate(
        self,
        values: np.ndarray,
        config: ForecastConfig,
        n_splits: int = 3,
    ) -> List[Dict[str, float]]:
        """Perform time-series cross-validation."""
        results = []
        split_size = len(values) // (n_splits + 1)

        for i in range(n_splits):
            train_end = split_size * (i + 1)
            train = values[:train_end]
            test = values[train_end:train_end + split_size]

            if len(test) < 2:
                continue

            forecast_result = self.forecast(train, config=config)
            n_forecast = min(len(test), len(forecast_result.forecast_values))
            pred = forecast_result.forecast_values[:n_forecast]
            actual = test[:n_forecast]

            mae = float(np.mean(np.abs(actual - pred)))
            rmse = float(np.sqrt(np.mean((actual - pred) ** 2)))

            results.append({"mae": mae, "rmse": rmse, "split": i + 1})

        return results

    def get_last_result(self) -> Optional[ForecastResult]:
        """Get the last forecast result."""
        return self._last_result
