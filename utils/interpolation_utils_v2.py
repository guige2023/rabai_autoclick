"""
Advanced interpolation and curve fitting v2.

Extends interpolation_utils.py with B-splines, Hermite interpolation,
Akima spline, and barycentric interpolation.
"""

from __future__ import annotations

import math
from typing import Callable


def hermite_interpolate(
    points: list[tuple[float, float, float]],
    x: float,
) -> float:
    """
    Hermite cubic interpolation given points with derivatives.

    Args:
        points: List of (x, y, derivative) tuples
        x: Value to interpolate at

    Returns:
        Interpolated y value.
    """
    n = len(points)
    if n == 0:
        return 0.0
    if n == 1:
        return points[0][1]

    for i in range(n - 1):
        x0, y0, d0 = points[i]
        x1, y1, d1 = points[i + 1]
        if x0 <= x <= x1:
            t = (x - x0) / (x1 - x0)
            t2 = t * t
            t3 = t2 * t
            h00 = 2 * t3 - 3 * t2 + 1
            h10 = t3 - 2 * t2 + t
            h01 = -2 * t3 + 3 * t2
            h11 = t3 - t2
            return h00 * y0 + h10 * (x1 - x0) * d0 + h01 * y1 + h11 * (x1 - x0) * d1
    return points[-1][1]


def akima_interpolate(xs: list[float], ys: list[float], x: float) -> float:
    """
    Akima spline interpolation (smooth, handles uneven spacing).

    Args:
        xs: Sorted x values
        ys: Corresponding y values
        x: Value to interpolate

    Returns:
        Interpolated y value.
    """
    n = len(xs)
    if n < 5:
        # Fall back to linear
        for i in range(n - 1):
            if xs[i] <= x <= xs[i + 1]:
                t = (x - xs[i]) / (xs[i + 1] - xs[i])
                return ys[i] + t * (ys[i + 1] - ys[i])
        return ys[-1]

    # Compute slopes
    m: list[float] = []
    for i in range(n - 1):
        m.append((ys[i + 1] - ys[i]) / (xs[i + 1] - xs[i]))

    # Compute weights
    def w(i: int) -> float:
        if i < 2 or i >= n - 2:
            return abs(m[i + 1] - m[i])
        return abs(m[i + 1] - m[i]) + abs(m[i - 1] - m[i - 2])

    weights = [w(i) for i in range(n - 2)]
    # Pad
    m_padded = [m[0], m[0]] + m + [m[-1], m[-1]]

    def akima_slope(i: int) -> float:
        w0 = weights[i - 2] if i >= 2 else 0.0
        w1 = weights[i - 1] if i >= 1 and i - 1 < len(weights) else 0.0
        w2 = weights[i] if i < len(weights) else 0.0
        w3 = weights[i + 1] if i + 1 < len(weights) else 0.0
        total = w0 + w1 + w2 + w3
        if total < 1e-12:
            return (m_padded[i + 1] + m_padded[i + 2]) / 2.0
        return (w0 * m_padded[i + 1] + w1 * m_padded[i + 2] + w2 * m_padded[i + 3] + w3 * m_padded[i + 4]) / total

    # Find interval
    for i in range(n - 1):
        if xs[i] <= x <= xs[i + 1]:
            h = xs[i + 1] - xs[i]
            t = (x - xs[i]) / h
            # Hermite form with Akima slopes
            m0 = akima_slope(i)
            m1 = akima_slope(i + 1)
            y0, y1 = ys[i], ys[i + 1]
            # Cardinal basis
            h00 = 2 * t**3 - 3 * t**2 + 1
            h10 = t**3 - 2 * t**2 + t
            h01 = -2 * t**3 + 3 * t**2
            h11 = t**3 - t**2
            return h00 * y0 + h10 * h * m0 + h01 * y1 + h11 * h * m1
    return ys[-1]


def barycentric_interpolate(
    xs: list[float], ys: list[float], x: float
) -> float:
    """
    Barycentric interpolation (fast, O(n) evaluation after O(n) setup).

    Args:
        xs: X values
        ys: Y values
        x: Value to interpolate

    Returns:
        Interpolated y value.
    """
    n = len(xs)
    if n == 0:
        return 0.0
    if n == 1:
        return ys[0]

    # Compute weights
    weights: list[float] = []
    for i in range(n):
        w = 1.0
        for j in range(n):
            if i != j:
                w /= (xs[i] - xs[j])
        weights.append(w)

    numerator = 0.0
    denominator = 0.0
    for i in range(n):
        if abs(x - xs[i]) < 1e-12:
            return ys[i]
        term = weights[i] / (x - xs[i])
        numerator += term * ys[i]
        denominator += term
    return numerator / denominator if abs(denominator) > 1e-12 else 0.0


def neville_interpolate(xs: list[float], ys: list[float], x: float) -> float:
    """
    Neville's algorithm for polynomial interpolation.

    Recursive algorithm, O(n^2).
    """
    n = len(xs)
    if n == 0:
        return 0.0
    if n == 1:
        return ys[0]

    # Build table
    p: list[list[float]] = [[0.0] * n for _ in range(n)]
    for i in range(n):
        p[i][i] = ys[i]

    for step in range(1, n):
        for i in range(n - step):
            factor = (x - xs[i + step]) / (xs[i] - xs[i + step])
            p[i][i + step] = p[i][i + step - 1] + factor * (p[i][i + step - 1] - p[i + 1][i + step])
    return p[0][n - 1]


class BSpline:
    """B-spline curve."""

    def __init__(self, xs: list[float], ys: list[float], degree: int = 3):
        self.xs = xs
        self.ys = ys
        self.degree = min(degree, len(xs) - 1)
        self.n = len(xs)

    def basis(self, i: int, k: int, t: float) -> float:
        """Compute B-spline basis function."""
        if k == 0:
            return 1.0 if self.xs[i] <= t < self.xs[i + 1] else 0.0
        denom1 = self.xs[i + k] - self.xs[i] if self.xs[i + k] != self.xs[i] else 1.0
        denom2 = self.xs[i + k + 1] - self.xs[i + 1] if self.xs[i + k + 1] != self.xs[i + 1] else 1.0
        result = 0.0
        if denom1 != 0:
            result += (t - self.xs[i]) / denom1 * self.basis(i, k - 1, t)
        if denom2 != 0:
            result += (self.xs[i + k + 1] - t) / denom2 * self.basis(i + 1, k - 1, t)
        return result

    def evaluate(self, x: float) -> float:
        """Evaluate B-spline at x."""
        result = 0.0
        for i in range(self.n - self.degree):
            b = self.basis(i, self.degree, x)
            result += self.ys[i] * b
        return result


class BilinearInterpolator2D:
    """Bilinear interpolation on a 2D grid."""

    def __init__(self, x_coords: list[float], y_coords: list[float], values: list[list[float]]):
        self.xs = x_coords
        self.ys = y_coords
        self.values = values

    def __call__(self, x: float, y: float) -> float:
        """Interpolate at (x, y)."""
        xs, ys, vals = self.xs, self.ys, self.values
        nx, ny = len(xs), len(ys)

        # Find x interval
        xi = 0
        for i in range(nx - 1):
            if xs[i] <= x <= xs[i + 1]:
                xi = i
                break
        else:
            xi = nx - 2 if x > xs[-1] else 0

        yi = 0
        for j in range(ny - 1):
            if ys[j] <= y <= ys[j + 1]:
                yi = j
                break
        else:
            yi = ny - 2 if y > ys[-1] else 0

        x0, x1 = xs[xi], xs[xi + 1]
        y0, y1 = ys[yi], ys[yi + 1]
        dx = (x - x0) / (x1 - x0) if x1 != x0 else 0.0
        dy = (y - y0) / (y1 - y0) if y1 != y0 else 0.0

        v00 = vals[yi][xi]
        v10 = vals[yi][xi + 1]
        v01 = vals[yi + 1][xi]
        v11 = vals[yi + 1][xi + 1]

        v0 = v00 * (1 - dx) + v10 * dx
        v1 = v01 * (1 - dx) + v11 * dx
        return v0 * (1 - dy) + v1 * dy


class TricubicInterpolator:
    """Tricubic interpolation on a 3D grid (simplified)."""

    def __init__(self, grid: list[list[list[float]]], step: float = 1.0):
        self.grid = grid
        self.step = step

    def evaluate(self, x: float, y: float, z: float) -> float:
        """Evaluate at (x, y, z) in grid units."""
        ix = int(x / self.step)
        iy = int(y / self.step)
        iz = int(z / self.step)
        # Clamp
        nz, ny, nx = len(self.grid), len(self.grid[0]), len(self.grid[0][0])
        ix = max(0, min(ix, nx - 2))
        iy = max(0, min(iy, ny - 2))
        iz = max(0, min(iz, nz - 2))
        tx = (x / self.step) - ix
        ty = (y / self.step) - iy
        tz = (z / self.step) - iz
        # Tri-linear (simplified)
        v000 = self.grid[iz][iy][ix]
        v100 = self.grid[iz][iy][ix + 1]
        v010 = self.grid[iz][iy + 1][ix]
        v110 = self.grid[iz][iy + 1][ix + 1]
        v001 = self.grid[iz + 1][iy][ix]
        v101 = self.grid[iz + 1][iy][ix + 1]
        v011 = self.grid[iz + 1][iy + 1][ix]
        v111 = self.grid[iz + 1][iy + 1][ix + 1]
        x0 = v000 * (1 - tx) + v100 * tx
        x1 = v010 * (1 - tx) + v110 * tx
        x2 = v001 * (1 - tx) + v101 * tx
        x3 = v011 * (1 - tx) + v111 * tx
        y0 = x0 * (1 - ty) + x1 * ty
        y1 = x2 * (1 - ty) + x3 * ty
        return y0 * (1 - tz) + y1 * tz


def piecewise_linear_fit(xs: list[float], ys: list[float], n_segments: int) -> list[tuple[float, float, float]]:
    """
    Fit piecewise linear segments to data.

    Returns:
        List of (x_start, slope, intercept) tuples for each segment.
    """
    n = len(xs)
    if n_segments >= n:
        return [(xs[i], 0.0, ys[i]) for i in range(n)]
    segment_size = n // n_segments
    segments: list[tuple[float, float, float]] = []
    for seg in range(n_segments):
        start = seg * segment_size
        end = (seg + 1) * segment_size if seg < n_segments - 1 else n
        seg_xs = xs[start:end]
        seg_ys = ys[start:end]
        if len(seg_xs) < 2:
            continue
        sum_x = sum(seg_xs)
        sum_y = sum(seg_ys)
        sum_xy = sum(x * y for x, y in zip(seg_xs, seg_ys))
        sum_xx = sum(x * x for x in seg_xs)
        m = (sum_xy - sum_x * sum_y / len(seg_xs)) / (sum_xx - sum_x ** 2 / len(seg_xs))
        b = (sum_y - m * sum_x) / len(seg_xs)
        segments.append((xs[start], m, b))
    return segments
