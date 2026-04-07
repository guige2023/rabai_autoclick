"""
Time series analysis utilities.

Provides time series decomposition, smoothing, and
forecasting helper functions.
"""
from __future__ import annotations

from typing import Callable, List, Optional, Tuple

import numpy as np


def moving_average(data: np.ndarray, window: int) -> np.ndarray:
    """
    Compute simple moving average.

    Args:
        data: Input time series
        window: Window size

    Returns:
        Smoothed series

    Example:
        >>> moving_average(np.array([1, 2, 3, 4, 5]), window=3)
        array([2., 3., 4.])
    """
    if window > len(data):
        return np.full_like(data, np.nan)
    result = np.zeros(len(data) - window + 1)
    for i in range(len(result)):
        result[i] = np.mean(data[i : i + window])
    return result


def exponential_smoothing(
    data: np.ndarray, alpha: float = 0.3
) -> np.ndarray:
    """
    Exponential smoothing.

    Args:
        data: Input time series
        alpha: Smoothing factor (0 < alpha < 1)

    Returns:
        Smoothed series

    Example:
        >>> exponential_smoothing(np.array([1, 2, 3, 4, 5]), alpha=0.5)
        array([1.    , 1.5   , 2.25  , 3.125 , 4.0625])
    """
    result = np.zeros(len(data))
    result[0] = data[0]
    for i in range(1, len(data)):
        result[i] = alpha * data[i] + (1 - alpha) * result[i - 1]
    return result


def double_exponential_smoothing(
    data: np.ndarray, alpha: float = 0.3, beta: float = 0.1
) -> np.ndarray:
    """
    Double exponential smoothing (Holt's method).

    Args:
        data: Input time series
        alpha: Level smoothing factor
        beta: Trend smoothing factor

    Returns:
        Smoothed series
    """
    n = len(data)
    result = np.zeros(n)
    level = data[0]
    trend = data[1] - data[0]
    result[0] = level
    for i in range(1, n):
        prev_level = level
        level = alpha * data[i] + (1 - alpha) * (level + trend)
        trend = beta * (level - prev_level) + (1 - beta) * trend
        result[i] = level + trend
    return result


def seasonal_decompose(
    data: np.ndarray, period: int, model: str = "additive"
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Decompose time series into trend, seasonal, and residual.

    Args:
        data: Input time series
        period: Seasonal period
        model: 'additive' or 'multiplicative'

    Returns:
        Tuple of (trend, seasonal, residual, detrended)
    """
    n = len(data)
    trend = moving_average(data, period)
    detrended = data[period // 2 : period // 2 + len(trend)]
    if model == "multiplicative":
        seasonal_raw = detrended / (trend + 1e-10)
    else:
        seasonal_raw = detrended - trend
    seasonal = np.tile(seasonal_raw, (n // period) + 1)[:n]
    if model == "multiplicative":
        residual = data / (trend * seasonal + 1e-10)
    else:
        residual = data - trend - seasonal
    return trend, seasonal[:len(trend)], residual, detrended


def autocorrelation(data: np.ndarray, max_lag: int = None) -> np.ndarray:
    """
    Compute autocorrelation function.

    Args:
        data: Input time series
        max_lag: Maximum lag to compute

    Returns:
        Autocorrelation values
    """
    n = len(data)
    if max_lag is None:
        max_lag = n - 1
    data = data - np.mean(data)
    acf = np.zeros(max_lag + 1)
    acf[0] = 1.0
    var = np.sum(data ** 2)
    for lag in range(1, max_lag + 1):
        acf[lag] = np.sum(data[lag:] * data[: n - lag]) / var
    return acf


def partial_autocorrelation(data: np.ndarray, max_lag: int = None) -> np.ndarray:
    """
    Compute partial autocorrelation function.

    Args:
        data: Input time series
        max_lag: Maximum lag

    Returns:
        PACF values
    """
    from scipy.linalg import solve
    n = len(data)
    if max_lag is None:
        max_lag = n - 1
    acf_vals = autocorrelation(data, max_lag)
    pacf = np.zeros(max_lag + 1)
    pacf[0] = 1.0
    for k in range(1, max_lag + 1):
        r = acf_vals[1 : k + 1]
        R = np.zeros((k, k))
        for i in range(k):
            for j in range(k):
                R[i, j] = acf_vals[np.abs(i - j)]
        try:
            pacf[k] = solve(R, r, assume_a="pos")[k - 1]
        except:
            pacf[k] = 0
    return pacf


def differencing(data: np.ndarray, order: int = 1, seasonal: bool = False, period: int = 12) -> np.ndarray:
    """
    Difference time series to make it stationary.

    Args:
        data: Input time series
        order: Difference order
        seasonal: Apply seasonal differencing
        period: Seasonal period

    Returns:
        Differenced series
    """
    result = data.copy()
    for _ in range(order):
        result = np.diff(result)
    if seasonal:
        result = np.diff(result, n=period)
    return result


def adf_test(data: np.ndarray) -> Tuple[float, float]:
    """
    Augmented Dickey-Fuller test for stationarity.

    Args:
        data: Input time series

    Returns:
        Tuple of (test_statistic, p_value)
    """
    n = len(data)
    diff = differencing(data)
    y = diff[1:]
    X = np.column_stack([diff[:-1], np.arange(1, len(diff))])
    from scipy.linalg import lstsq
    coeffs, _, _, _ = lstsq(X, y)
    residuals = y - X @ coeffs
    s = np.sqrt(np.mean(residuals ** 2))
    if s < 1e-10:
        return 0.0, 1.0
    test_stat = coeffs[0] / s
    p_value = _approx_p_value(len(data), test_stat)
    return float(test_stat), p_value


def _approx_p_value(n: int, t: float) -> float:
    """Approximate p-value for ADF test (simplified)."""
    return max(0.001, min(1.0, 2.0 / (n ** 2)))


def holt_winters_forecast(
    data: np.ndarray, alpha: float, beta: float, gamma: float, period: int, n_forecast: int
) -> np.ndarray:
    """
    Holt-Winters exponential smoothing forecast.

    Args:
        data: Historical time series
        alpha: Level smoothing parameter
        beta: Trend smoothing parameter
        gamma: Seasonal smoothing parameter
        period: Seasonal period
        n_forecast: Number of periods to forecast

    Returns:
        Forecasted values
    """
    n = len(data)
    seasonals = np.zeros(period)
    level = np.mean(data[:period])
    trend = (np.mean(data[period : 2 * period]) - np.mean(data[:period])) / period
    for i in range(period):
        seasonals[i] = data[i] - level
    for i in range(period, n):
        prev_level = level
        level = alpha * (data[i] - seasonals[i % period]) + (1 - alpha) * (level + trend)
        trend = beta * (level - prev_level) + (1 - beta) * trend
        seasonals[i % period] = gamma * (data[i] - level) + (1 - gamma) * seasonals[i % period]
    forecast = np.zeros(n_forecast)
    for i in range(n_forecast):
        forecast[i] = level + (i + 1) * trend + seasonals[i % period]
    return forecast


def arima_forecast(
    data: np.ndarray, p: int, d: int, q: int, n_forecast: int
) -> np.ndarray:
    """
    ARIMA forecast (simplified implementation).

    Args:
        data: Time series data
        p: AR order
        d: Differencing order
        q: MA order
        n_forecast: Forecast horizon

    Returns:
        Forecasted values
    """
    from scipy.signal import lfilter
    if d > 0:
        diff_data = differencing(data, d)
    else:
        diff_data = data.copy()
    ar_coeffs = [1.0] + [-0.5] * p
    ma_coeffs = [1.0] + [0.3] * q
    forecast = lfilter(ma_coeffs, ar_coeffs, diff_data)[-n_forecast:]
    return forecast


def rolling_statistics(
    data: np.ndarray, window: int
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Compute rolling mean and std.

    Args:
        data: Input time series
        window: Window size

    Returns:
        Tuple of (rolling_mean, rolling_std)
    """
    rolling_mean = moving_average(data, window)
    rolling_std = np.zeros(len(data) - window + 1)
    for i in range(len(rolling_std)):
        rolling_std[i] = np.std(data[i : i + window])
    return rolling_mean, rolling_std


def detect_outliers_iqr(data: np.ndarray, factor: float = 1.5) -> np.ndarray:
    """
    Detect outliers using IQR method.

    Args:
        data: Input time series
        factor: IQR multiplier

    Returns:
        Boolean mask where True indicates outlier
    """
    q1 = np.percentile(data, 25)
    q3 = np.percentile(data, 75)
    iqr = q3 - q1
    lower = q1 - factor * iqr
    upper = q3 + factor * iqr
    return (data < lower) | (data > upper)


def detect_outliers_zscore(data: np.ndarray, threshold: float = 3.0) -> np.ndarray:
    """
    Detect outliers using z-score method.

    Args:
        data: Input time series
        threshold: Z-score threshold

    Returns:
        Boolean mask where True indicates outlier
    """
    mean = np.mean(data)
    std = np.std(data)
    if std == 0:
        return np.zeros_like(data, dtype=bool)
    z_scores = np.abs((data - mean) / std)
    return z_scores > threshold


def cross_correlation(
    series1: np.ndarray, series2: np.ndarray, max_lag: int = None
) -> np.ndarray:
    """
    Compute cross-correlation between two series.

    Args:
        series1: First time series
        series2: Second time series
        max_lag: Maximum lag to compute

    Returns:
        Cross-correlation values
    """
    n1, n2 = len(series1), len(series2)
    if max_lag is None:
        max_lag = n1 + n2 - 1
    result = np.zeros(2 * max_lag + 1)
    for lag in range(-max_lag, max_lag + 1):
        if lag < 0:
            s1 = series1[-lag:]
            s2 = series2[:lag]
        elif lag > 0:
            s1 = series1[:-lag]
            s2 = series2[lag:]
        else:
            s1 = series1
            s2 = series2
        if len(s1) > 0 and len(s2) > 0:
            result[lag + max_lag] = np.corrcoef(s1, s2)[0, 1] if np.std(s1) > 0 and np.std(s2) > 0 else 0
    return result


class TimeSeriesFeatureExtractor:
    """Extract statistical features from time series."""

    @staticmethod
    def extract(data: np.ndarray) -> dict:
        """Extract features from time series."""
        features = {
            "mean": float(np.mean(data)),
            "std": float(np.std(data)),
            "min": float(np.min(data)),
            "max": float(np.max(data)),
            "median": float(np.median(data)),
            "q25": float(np.percentile(data, 25)),
            "q75": float(np.percentile(data, 75)),
            "skewness": float(_skewness(data)),
            "kurtosis": float(_kurtosis(data)),
            "acf1": float(autocorrelation(data, 1)[1]),
            "acf5": float(np.mean(autocorrelation(data, 5)[1:])),
        }
        return features


def _skewness(data: np.ndarray) -> float:
    """Calculate skewness."""
    mean, std = np.mean(data), np.std(data)
    if std == 0:
        return 0.0
    n = len(data)
    return np.sum(((data - mean) / std) ** 3) * n / ((n - 1) * (n - 2))


def _kurtosis(data: np.ndarray) -> float:
    """Calculate kurtosis."""
    mean, std = np.mean(data), np.std(data)
    if std == 0:
        return 0.0
    n = len(data)
    return np.sum(((data - mean) / std) ** 4) * n * (n + 1) / ((n - 1) * (n - 2) * (n - 3)) - 3 * (n - 1) ** 2 / ((n - 2) * (n - 3))
