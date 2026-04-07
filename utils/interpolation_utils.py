"""
Interpolation and curve fitting utilities.

Provides linear, polynomial, cubic spline, Lagrange, and bilinear interpolation.
"""

from __future__ import annotations

import math
from typing import Callable


def linear_interpolate(x: float, x1: float, y1: float, x2: float, y2: float) -> float:
    """
    Linear interpolation between two points.

    Args:
        x: X value to interpolate at
        x1, y1: First known point
        x2, y2: Second known point

    Returns:
        Interpolated y value.
    """
    if abs(x2 - x1) < 1e-12:
        return y1
    return y1 + (x - x1) * (y2 - y1) / (x2 - x1)


def linear_interpolate_array(
    xs: list[float], ys: list[float], x_query: list[float]
) -> list[float]:
    """
    Interpolate multiple x values against sorted known data points.

    Args:
        xs: Sorted list of known x values
        ys: Known y values (same length as xs)
        x_query: X values to interpolate

    Returns:
        Interpolated y values.
    """
    result: list[float] = []
    n = len(xs)
    for x in x_query:
        if x <= xs[0]:
            result.append(ys[0])
        elif x >= xs[-1]:
            result.append(ys[-1])
        else:
            for i in range(n - 1):
                if xs[i] <= x <= xs[i + 1]:
                    result.append(linear_interpolate(x, xs[i], ys[i], xs[i + 1], ys[i + 1]))
                    break
    return result


def lagrange_interpolate(xs: list[float], ys: list[float], x: float) -> float:
    """
    Lagrange polynomial interpolation.

    Args:
        xs: Known x values
        ys: Known y values
        x: Value to interpolate at

    Returns:
        Interpolated y value.
    """
    n = len(xs)
    result = 0.0
    for i in range(n):
        term = ys[i]
        for j in range(n):
            if i != j:
                term *= (x - xs[j]) / (xs[i] - xs[j])
        result += term
    return result


def cubic_spline_interpolate(
    xs: list[float], ys: list[float], x: float
) -> float:
    """
    Cubic spline interpolation using natural boundary conditions.

    Args:
        xs: Sorted x values
        ys: Corresponding y values
        x: Value to interpolate

    Returns:
        Interpolated y value.
    """
    n = len(xs)
    if n < 4:
        # Fall back to linear for small datasets
        return linear_interpolate_array(xs, ys, [x])[0]

    # Compute second derivatives
    h = [xs[i + 1] - xs[i] for i in range(n - 1)]

    # Tridiagonal system
    A = [0.0] * (n - 2)
    B = [0.0] * (n - 2)
    C = [0.0] * (n - 2)
    D = [0.0] * (n - 2)

    for i in range(n - 2):
        A[i] = h[i] if i > 0 else 0.0
        B[i] = 2.0 * (h[i] + h[i + 1])
        C[i] = h[i + 1]
        D[i] = 3.0 * ((ys[i + 2] - ys[i + 1]) / h[i + 1] - (ys[i + 1] - ys[i]) / h[i])

    # Thomas algorithm
    c_prime = [0.0] * (n - 2)
    d_prime = [0.0] * (n - 2)
    c_prime[0] = C[0] / B[0]
    d_prime[0] = D[0] / B[0]
    for i in range(1, n - 2):
        denom = B[i] - A[i] * c_prime[i - 1]
        c_prime[i] = C[i] / denom if i < n - 3 else 0.0
        d_prime[i] = (D[i] - A[i] * d_prime[i - 1]) / denom

    M = [0.0] * n
    M[0] = 0.0
    for i in range(n - 2):
        idx = n - 3 - i
        M[idx + 1] = d_prime[idx] - c_prime[idx] * M[idx + 2]
    M[n - 1] = 0.0

    # Find the interval
    for i in range(n - 1):
        if xs[i] <= x <= xs[i + 1]:
            t = (x - xs[i]) / h[i]
            a = ys[i]
            b = (ys[i + 1] - ys[i]) / h[i] - h[i] * (2 * M[i] + M[i + 1]) / 6
            c = M[i] / 2
            d = (M[i + 1] - M[i]) / (6 * h[i])
            return a + b * t + c * t * t + d * t * t * t

    return ys[-1]


def polynomial_fit(xs: list[float], ys: list[float], degree: int) -> list[float]:
    """
    Fit a polynomial of given degree using least squares.

    Args:
        xs: X values
        ys: Y values
        degree: Polynomial degree

    Returns:
        Coefficients [a0, a1, ..., ad] for polynomial a0 + a1*x + ... + ad*x^d.
    """
    n = len(xs)
    m = degree + 1
    if n < m:
        m = n
        degree = m - 1

    # Build normal equations: X^T X c = X^T y
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
        pivot = aug[i][i]
        if abs(pivot) < 1e-12:
            for j in range(i + 1, m):
                if abs(aug[j][i]) > 1e-12:
                    aug[i], aug[j] = aug[j], aug[i]
                    pivot = aug[i][i]
                    break
        if abs(pivot) < 1e-12:
            aug[i][i] = 1.0
            continue
        for j in range(i, m + 1):
            aug[i][j] /= pivot
        for k in range(m):
            if k != i:
                factor = aug[k][i]
                for j in range(i, m + 1):
                    aug[k][j] -= factor * aug[i][j]

    return [aug[i][m] for i in range(m)]


def polynomial_eval(coeffs: list[float], x: float) -> float:
    """Evaluate a polynomial at x using Horner's method."""
    result = 0.0
    for c in reversed(coeffs):
        result = result * x + c
    return result


class BilinearInterpolator:
    """2D bilinear interpolation on a rectangular grid."""

    def __init__(self, x_coords: list[float], y_coords: list[float], values: list[list[float]]):
        """
        Args:
            x_coords: Sorted x grid values
            y_coords: Sorted y grid values
            values: 2D array of z values (len(y_coords) x len(x_coords))
        """
        self.xs = x_coords
        self.ys = y_coords
        self.values = values

    def interpolate(self, x: float, y: float) -> float:
        """Interpolate z value at (x, y)."""
        xs, ys, vals = self.xs, self.ys, self.values
        nx, ny = len(xs), len(ys)

        if x <= xs[0]:
            xi, xf = 0, 0.0
        elif x >= xs[-1]:
            xi, xf = nx - 2, 1.0
        else:
            for i in range(nx - 1):
                if xs[i] <= x <= xs[i + 1]:
                    xi = i
                    xf = (x - xs[i]) / (xs[i + 1] - xs[i])
                    break

        if y <= ys[0]:
            yi, yf = 0, 0.0
        elif y >= ys[-1]:
            yi, yf = ny - 2, 1.0
        else:
            for j in range(ny - 1):
                if ys[j] <= y <= ys[j + 1]:
                    yi = j
                    yf = (y - ys[j]) / (ys[j + 1] - ys[j])
                    break

        v00 = vals[yi][xi]
        v10 = vals[yi][xi + 1]
        v01 = vals[yi + 1][xi]
        v11 = vals[yi + 1][xi + 1]

        v0 = v00 + (v10 - v00) * xf
        v1 = v01 + (v11 - v01) * xf
        return v0 + (v1 - v0) * yf
