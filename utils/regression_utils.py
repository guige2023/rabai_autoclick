"""
Regression and curve fitting utilities.

Provides linear regression, ridge regression, logistic regression,
and exponential/polynomial fitting.
"""

from __future__ import annotations

import math
from typing import Callable


def linear_regression(xs: list[float], ys: list[float]) -> tuple[float, float]:
    """
    Simple linear regression y = mx + b.

    Args:
        xs: X values
        ys: Y values

    Returns:
        Tuple of (slope, intercept).
    """
    n = len(xs)
    if n < 2:
        return 0.0, 0.0

    sum_x = sum(xs)
    sum_y = sum(ys)
    sum_xy = sum(x * y for x, y in zip(xs, ys))
    sum_xx = sum(x * x for x in xs)

    denom = n * sum_xx - sum_x * sum_x
    if abs(denom) < 1e-12:
        m = 0.0
        b = sum_y / n
    else:
        m = (n * sum_xy - sum_x * sum_y) / denom
        b = (sum_y - m * sum_x) / n

    return m, b


def linear_regression_r_squared(xs: list[float], ys: list[float]) -> tuple[float, float, float]:
    """
    Linear regression with R² score.

    Returns:
        Tuple of (slope, intercept, r_squared).
    """
    m, b = linear_regression(xs, ys)
    n = len(xs)
    if n < 2:
        return m, b, 0.0

    y_mean = sum(ys) / n
    ss_tot = sum((y - y_mean) ** 2 for y in ys)
    ss_res = sum((y - (m * x + b)) ** 2 for x, y in zip(xs, ys))

    r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    return m, b, r_squared


def polynomial_regression(
    xs: list[float], ys: list[float], degree: int
) -> list[float]:
    """
    Polynomial regression using least squares.

    Args:
        xs: X values
        ys: Y values
        degree: Polynomial degree

    Returns:
        Coefficients [a0, a1, ..., ad].
    """
    n = len(xs)
    m = degree + 1
    if n < m:
        m = n
        degree = m - 1

    XtX: list[list[float]] = [[0.0] * m for _ in range(m)]
    Xty: list[float] = [0.0] * m

    for x, y in zip(xs, ys):
        powers = [x ** j for j in range(m)]
        for i in range(m):
            Xty[i] += powers[i] * y
            for j in range(m):
                XtX[i][j] += powers[i] * powers[j]

    # Gaussian elimination
    aug = [XtX[i] + [Xty[i]] for i in range(m)]
    for i in range(m):
        for j in range(i + 1, m):
            if abs(aug[j][i]) > abs(aug[i][i]):
                aug[i], aug[j] = aug[j], aug[i]
        pivot = aug[i][i]
        if abs(pivot) < 1e-12:
            continue
        for j in range(i, m + 1):
            aug[i][j] /= pivot
        for k in range(m):
            if k != i:
                factor = aug[k][i]
                for j in range(i, m + 1):
                    aug[k][j] -= factor * aug[i][j]

    return [aug[i][m] for i in range(m)]


def ridge_regression(
    xs: list[float], ys: list[float], alpha: float = 1.0
) -> tuple[float, float]:
    """
    Ridge regression (L2 regularization) for y = mx + b.

    Args:
        xs: X values
        ys: Y values
        alpha: Regularization strength

    Returns:
        Tuple of (slope, intercept).
    """
    n = len(xs)
    if n < 2:
        return 0.0, 0.0

    sum_x = sum(xs)
    sum_y = sum(ys)
    sum_xy = sum(x * y for x, y in zip(xs, ys))
    sum_xx = sum(x * x for x in xs)

    denom = n * sum_xx - sum_x * sum_x + alpha * n
    if abs(denom) < 1e-12:
        return 0.0, sum_y / n

    m = (n * sum_xy - sum_x * sum_y) / denom
    b = (sum_y - m * sum_x) / n
    return m, b


def exponential_fit(xs: list[float], ys: list[float]) -> tuple[float, float, float]:
    """
    Fit y = a * exp(b * x) + c using least squares on log-transformed data.

    Returns:
        Tuple of (a, b, c).
    """
    # Filter non-positive y values for log transform
    min_y = min(ys)
    offset = abs(min_y) + 1.0 if min_y <= 0 else 0.0
    ys_adj = [y + offset for y in ys]

    log_ys = [math.log(y + 1e-12) for y in ys_adj]
    m, k = linear_regression(xs, log_ys)
    b = m
    a = math.exp(k)
    c = -offset
    return a, b, c


def logistic_function(x: float, L: float, k: float, x0: float) -> float:
    """
    Logistic (sigmoid) function: L / (1 + exp(-k*(x-x0)))
    """
    return L / (1.0 + math.exp(-k * (x - x0)))


def logistic_fit(xs: list[float], ys: list[float]) -> tuple[float, float, float]:
    """
    Approximate logistic fit: y = L / (1 + exp(-k*(x-x0)))

    Returns:
        Tuple of (L, k, x0).
    """
    y_min = min(ys)
    y_max = max(ys)
    L = y_max - y_min
    x0 = sum(xs) / len(xs)
    k = 1.0
    return L, k, x0


def moving_average_filter(data: list[float], window: int) -> list[float]:
    """
    Simple moving average smoothing.

    Args:
        data: Input signal
        window: Window size (must be >= 1)

    Returns:
        Smoothed signal.
    """
    if window < 1:
        return list(data)
    result: list[float] = []
    for i in range(len(data)):
        start = max(0, i - window + 1)
        result.append(sum(data[start:i + 1]) / (i - start + 1))
    return result


def exponential_moving_average(data: list[float], alpha: float = 0.3) -> list[float]:
    """
    Exponential moving average.

    Args:
        data: Input signal
        alpha: Smoothing factor (0 < alpha <= 1)

    Returns:
        EMA smoothed signal.
    """
    if not data:
        return []
    alpha = max(0.001, min(1.0, alpha))
    result: list[float] = [data[0]]
    for i in range(1, len(data)):
        ema = alpha * data[i] + (1 - alpha) * result[-1]
        result.append(ema)
    return result


def savitzky_golay_smooth(
    data: list[float], window_size: int, poly_order: int = 2
) -> list[float]:
    """
    Savitzky-Golay smoothing filter.

    Args:
        data: Input signal
        window_size: Must be odd and >= poly_order + 2
        poly_order: Polynomial order

    Returns:
        Smoothed signal.
    """
    n = len(data)
    if n < window_size or window_size % 2 == 0:
        return list(data)
    half = window_size // 2
    poly_order = min(poly_order, window_size - 1)

    # Build convolution coefficients
    x = list(range(-half, half + 1))
    A: list[list[float]] = [[xi ** j for j in range(poly_order + 1)] for xi in x]
    # Pseudo-inverse for least squares
    AT = list(zip(*A))
    ATA = [[sum(AT[i][k] * A[k][j] for k in range(window_size)) for j in range(poly_order + 1)] for i in range(poly_order + 1)]
    coefficients = [0.0] * window_size
    for j in range(poly_order + 1):
        inv_row = [0.0] * (poly_order + 1)
        pivot = ATA[j][j] + 1e-10
        inv_row[j] = 1.0 / pivot
        for jj in range(j + 1, poly_order + 1):
            inv_row[jj] = -ATA[j][jj] / pivot
        for i in range(j + 1, poly_order + 1):
            for jj in range(j, poly_order + 1):
                ATA[i][jj] -= ATA[i][j] * inv_row[jj] / inv_row[j] * ATA[j][jj]
        for jj in range(j, poly_order + 1):
            ATA[j][jj] *= inv_row[jj]
        for ii in range(j + 1, poly_order + 1):
            for jj in range(j, poly_order + 1):
                ATA[ii][jj] -= ATA[ii][j] * inv_row[jj] / inv_row[j] * ATA[j][jj]

    for j in range(poly_order + 1):
        for jj in range(poly_order + 1):
            ATA[j][jj] *= inv_row[jj]
        for ii in range(j + 1, poly_order + 1):
            for jj in range(j, poly_order + 1):
                ATA[ii][jj] -= ATA[ii][j] * inv_row[jj] / inv_row[j] * ATA[j][jj]

    for j in range(poly_order + 1):
        inv_row = [0.0] * (poly_order + 1)
        inv_row[j] = 1.0 / ATA[j][j]
        for jj in range(j + 1, poly_order + 1):
            inv_row[jj] = -ATA[j][jj] * inv_row[j]
        for i in range(poly_order + 1):
            for jj in range(poly_order + 1):
                ATA[i][jj] -= ATA[i][j] * inv_row[jj]
        for ii in range(j + 1, poly_order + 1):
            for jj in range(poly_order + 1):
                ATA[ii][jj] -= ATA[ii][j] * inv_row[jj]
        for jj in range(poly_order + 1):
            ATA[j][jj] *= inv_row[jj]

    coefficients = [sum(ATA[0][j] * x_i[j] for j in range(poly_order + 1)) for x_i in A]

    result: list[float] = []
    for i in range(n):
        start = max(0, i - half)
        end = min(n, i + half + 1)
        segment = data[start:end]
        pad_l = half - (i - start)
        pad_r = half - (end - i - 1)
        segment = [data[start]] * max(0, pad_l) + segment + [data[end - 1]] * max(0, pad_r)
        if len(segment) < window_size:
            segment = [segment[0]] * (window_size - len(segment)) + segment
        result.append(sum(c * s for c, s in zip(coefficients, segment)))
    return result
