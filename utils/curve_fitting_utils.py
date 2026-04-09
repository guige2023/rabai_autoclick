"""Curve fitting utilities for RabAI AutoClick.

Provides:
- Linear and polynomial regression
- Least squares fitting
- Curve fitting with various models
- Parameter estimation
"""

from typing import List, Tuple, Optional, Callable
import math


def linear_regression(
    points: List[Tuple[float, float]],
) -> Tuple[float, float]:
    """Fit linear regression (y = mx + b).

    Args:
        points: (x, y) data points.

    Returns:
        (slope, intercept).
    """
    n = len(points)
    if n < 2:
        return (0.0, points[0][1] if points else 0.0)

    sum_x = sum(p[0] for p in points)
    sum_y = sum(p[1] for p in points)
    sum_xy = sum(p[0] * p[1] for p in points)
    sum_xx = sum(p[0] * p[0] for p in points)

    denom = n * sum_xx - sum_x * sum_x
    if abs(denom) < 1e-10:
        return (0.0, sum_y / n)

    m = (n * sum_xy - sum_x * sum_y) / denom
    b = (sum_y - m * sum_x) / n

    return (m, b)


def polynomial_regression(
    points: List[Tuple[float, float]],
    degree: int,
) -> List[float]:
    """Fit polynomial regression using least squares.

    Args:
        points: (x, y) data points.
        degree: Polynomial degree.

    Returns:
        Coefficients [c0, c1, c2, ...] for c0 + c1*x + c2*x^2 + ...
    """
    n = len(points)
    if n <= degree:
        # Vandermonde solve
        m = len(points)
        A: List[List[float]] = [[p[0] ** i for i in range(m)] for p in points]
        b = [p[1] for p in points]
        return solve_vandermonde(A, b)

    # Build normal equations A^T A x = A^T b
    size = degree + 1
    ATA: List[List[float]] = [[0.0] * size for _ in range(size)]
    ATb: List[float] = [0.0] * size

    for x, y in points:
        for i in range(size):
            ATb[i] += y * (x ** i)
            for j in range(size):
                ATA[i][j] += x ** (i + j)

    # Solve using Gaussian elimination
    return solve_linear_system(ATA, ATb)


def solve_linear_system(
    A: List[List[float]],
    b: List[float],
) -> List[float]:
    """Solve Ax = b using Gaussian elimination with partial pivoting."""
    n = len(A)
    if n == 0:
        return []
    augmented = [row[:] + [b[i]] for i, row in enumerate(A)]

    # Forward elimination
    for col in range(n):
        # Find pivot
        max_row = col
        for row in range(col + 1, n):
            if abs(augmented[row][col]) > abs(augmented[max_row][col]):
                max_row = row
        augmented[col], augmented[max_row] = augmented[max_row], augmented[col]

        if abs(augmented[col][col]) < 1e-10:
            continue

        for row in range(col + 1, n):
            factor = augmented[row][col] / augmented[col][col]
            for j in range(col, n + 1):
                augmented[row][j] -= factor * augmented[col][j]

    # Back substitution
    x: List[float] = [0.0] * n
    for i in range(n - 1, -1, -1):
        if abs(augmented[i][i]) < 1e-10:
            x[i] = 0.0
            continue
        x[i] = augmented[i][n]
        for j in range(i + 1, n):
            x[i] -= augmented[i][j] * x[j]
        x[i] /= augmented[i][i]

    return x


def solve_vandermonde(
    A: List[List[float]],
    b: List[float],
) -> List[float]:
    """Solve Vandermonde system for polynomial interpolation."""
    n = len(A)
    if n == 0:
        return []
    x: List[float] = [0.0] * n
    for i, row in enumerate(A):
        # row = [1, xi, xi^2, ...]
        xi = row[1] if len(row) > 1 else 0.0
        # Simple evaluation using Horner's method
        val = b[i]
        for j in range(n - 1, -1, -1):
            x[j] += val
            val *= xi
    return x


def fit_polynomial(
    x: float,
    coeffs: List[float],
) -> float:
    """Evaluate polynomial using Horner's method."""
    result = 0.0
    for c in reversed(coeffs):
        result = result * x + c
    return result


def exponential_fit(
    points: List[Tuple[float, float]],
) -> Tuple[float, float, float]:
    """Fit y = a * exp(b * x) + c.

    Returns:
        (a, b, c) parameters.
    """
    n = len(points)
    if n < 3:
        return (1.0, 0.0, 0.0)

    # Linearize: ln(y - c) = ln(a) + b*x
    # Estimate c from min values
    y_vals = [p[1] for p in points]
    c = min(y_vals) - 0.01

    # Linear regression on ln(y - c)
    lin_points = [(p[0], math.log(max(1e-10, p[1] - c))) for p in points]
    m, b = linear_regression(lin_points)

    return (math.exp(b), m, c)


def gaussian_fit(
    points: List[Tuple[float, float]],
) -> Tuple[float, float, float, float]:
    """Fit y = a * exp(-((x - b)^2 / (2*c^2))) + d.

    Returns:
        (amplitude, center, sigma, baseline).
    """
    y_vals = [p[1] for p in points]
    baseline = min(y_vals)
    amplitude = max(y_vals) - baseline

    # Estimate center
    total_y = sum(y_vals) - baseline
    if total_y < 1e-10:
        return (1.0, points[0][0], 1.0, baseline)

    center = sum(p[0] * (p[1] - baseline) for p in points) / total_y

    # Estimate sigma
    variance = sum((p[0] - center) ** 2 * (p[1] - baseline) for p in points) / total_y
    sigma = math.sqrt(variance) if variance > 0 else 1.0

    return (amplitude, center, sigma, baseline)


def moving_window_fit(
    points: List[Tuple[float, float]],
    window_size: int,
    fit_func: Callable[[List[Tuple[float, float]]], List[float]],
) -> List[Tuple[float, float, List[float]]]:
    """Apply a fit function within a moving window.

    Returns:
        List of (x, y, coefficients) tuples.
    """
    n = len(points)
    half = window_size // 2
    results: List[Tuple[float, float, List[float]]] = []

    for i in range(n):
        lo = max(0, i - half)
        hi = min(n, i + half + 1)
        window = points[lo:hi]
        coeffs = fit_func(window)
        x, y = points[i]
        results.append((x, y, coeffs))

    return results


def fit_line_segments(
    points: List[Tuple[float, float]],
    max_error: float,
    min_points: int = 3,
) -> List[List[Tuple[float, float]]]:
    """Fit line segments to points using Ramer-Douglas-Peucker.

    Args:
        points: Input points.
        max_error: Maximum perpendicular distance.
        min_points: Minimum points per segment.

    Returns:
        List of point segments.
    """
    def perp_dist(
        p: Tuple[float, float],
        a: Tuple[float, float],
        b: Tuple[float, float],
    ) -> float:
        ax, ay = p[0] - a[0], p[1] - a[1]
        bx, by = b[0] - a[0], b[1] - a[1]
        len_b = math.sqrt(bx * bx + by * by)
        if len_b < 1e-10:
            return math.sqrt(ax * ax + ay * ay)
        return abs(ax * by - ay * bx) / len_b

    def rdp(
        pts: List[Tuple[float, float]],
        start: int,
        end: int,
    ) -> List[Tuple[float, float]]:
        if end - start + 1 < min_points:
            return pts[start:end + 1]
        max_d = 0.0
        max_i = start
        for i in range(start + 1, end):
            d = perp_dist(pts[i], pts[start], pts[end])
            if d > max_d:
                max_d = d
                max_i = i
        if max_d > max_error:
            left = rdp(pts, start, max_i)
            right = rdp(pts, max_i, end)
            return left[:-1] + right
        return [pts[start], pts[end]]

    if len(points) < min_points:
        return [points[:]]

    return [rdp(points, 0, len(points) - 1)]


def r_squared(
    points: List[Tuple[float, float]],
    predict: Callable[[float], float],
) -> float:
    """Compute R-squared (coefficient of determination).

    Args:
        points: (x, y) data.
        predict: Prediction function.

    Returns:
        R² value (0-1).
    """
    if not points:
        return 0.0
    y_vals = [p[1] for p in points]
    y_mean = sum(y_vals) / len(y_vals)

    ss_tot = sum((y - y_mean) ** 2 for y in y_vals)
    if ss_tot < 1e-10:
        return 1.0

    ss_res = sum((y - predict(x)) ** 2 for x, y in points)
    return 1.0 - ss_res / ss_tot
