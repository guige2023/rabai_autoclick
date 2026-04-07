"""
Approximation utilities v2 — numerical approximation and function fitting.

Companion to approximation_utils.py. Adds spline interpolation,
Pade approximation, least squares fitting, and special function approximations.
"""

from __future__ import annotations

import math
from typing import Callable, NamedTuple


class LeastSquaresResult(NamedTuple):
    """Result of least squares fitting."""
    coefficients: list[float]
    residuals: float
    rank: int


def chebyshev_nodes(n: int, a: float = -1.0, b: float = 1.0) -> list[float]:
    """
    Generate Chebyshev nodes for optimal polynomial interpolation.

    Args:
        n: Number of nodes
        a: Left interval endpoint
        b: Right interval endpoint

    Returns:
        List of n Chebyshev nodes in [a, b]

    Example:
        >>> chebyshev_nodes(5, -1, 1)
        [0.309..., -0.809..., 0.809..., -0.309...]
    """
    return [(a + b) / 2 + (b - a) / 2 * math.cos((2 * k + 1) * math.pi / (2 * n + 2)) for k in range(n)]


def lagrange_interpolate(x_vals: list[float], y_vals: list[float], x: float) -> float:
    """
    Evaluate Lagrange interpolation polynomial at x.

    Args:
        x_vals: x-coordinates of data points
        y_vals: y-coordinates of data points
        x: Point at which to evaluate

    Returns:
        Interpolated y value
    """
    n = len(x_vals)
    result = 0.0
    for i in range(n):
        term = y_vals[i]
        for j in range(n):
            if i != j:
                term *= (x - x_vals[j]) / (x_vals[i] - x_vals[j])
        result += term
    return result


def cubic_spline_interpolate(x_vals: list[float], y_vals: list[float], x: float) -> float:
    """
    Interpolate using cubic spline with natural boundary conditions.

    Args:
        x_vals: Sorted x-coordinates
        y_vals: y-coordinates
        x: Point at which to evaluate

    Returns:
        Interpolated value
    """
    n = len(x_vals) - 1
    h = [x_vals[i + 1] - x_vals[i] for i in range(n)]

    alpha = [0.0] * (n + 1)
    for i in range(1, n):
        alpha[i] = (3 / h[i]) * (y_vals[i + 1] - y_vals[i]) - (3 / h[i - 1]) * (y_vals[i] - y_vals[i - 1])

    l = [1.0] + [0.0] * n
    mu = [0.0] * (n + 1)
    z = [0.0] * (n + 1)
    for i in range(1, n):
        l[i] = 2 * (x_vals[i + 1] - x_vals[i - 1]) - h[i - 1] * mu[i - 1]
        mu[i] = h[i] / l[i] if l[i] != 0 else 0
        z[i] = (alpha[i] - h[i - 1] * z[i - 1]) / l[i] if l[i] != 0 else 0

    c = [0.0] * (n + 1)
    b = [0.0] * n
    d = [0.0] * n
    for j in range(n - 1, -1, -1):
        c[j] = z[j] - mu[j] * c[j + 1] if j < n else 0
        b[j] = (y_vals[j + 1] - y_vals[j]) / h[j] - h[j] * (c[j + 1] + 2 * c[j]) / 3
        d[j] = (c[j + 1] - c[j]) / (3 * h[j])

    for i in range(n):
        if x_vals[i] <= x <= x_vals[i + 1]:
            dx = x - x_vals[i]
            return y_vals[i] + b[i] * dx + c[i] * dx**2 + d[i] * dx**3
    return y_vals[-1]


def least_squares_fit(
    xs: list[float],
    ys: list[float],
    degree: int,
) -> LeastSquaresResult:
    """
    Fit a polynomial of given degree using least squares.

    Args:
        xs: x-coordinates
        ys: y-coordinates
        degree: Polynomial degree

    Returns:
        LeastSquaresResult with coefficients
    """
    n = len(xs)
    k = degree + 1

    A = [[0.0] * k for _ in range(k)]
    B = [0.0] * k
    for i in range(n):
        pow_x = 1.0
        for j in range(k):
            B[j] += pow_x * ys[i]
            pow_xi = pow_x
            for l in range(k):
                A[j][l] += pow_xi
                pow_xi *= xs[i]
            pow_x *= xs[i]

    coeffs = gaussian_elimination(A, B)
    residuals = sum((eval_poly(coeffs, xs[i]) - ys[i]) ** 2 for i in range(n))
    return LeastSquaresResult(coefficients=coeffs, residuals=residuals, rank=k)


def gaussian_elimination(A: list[list[float]], B: list[float]) -> list[float]:
    """Solve linear system Ax = B using Gaussian elimination with partial pivoting."""
    n = len(B)
    aug = [row[:] + [B[i]] for i, row in enumerate(A)]
    for i in range(n):
        max_row = max(range(i, n), key=lambda r: abs(aug[r][i]))
        aug[i], aug[max_row] = aug[max_row], aug[i]
        pivot = aug[i][i]
        if abs(pivot) < 1e-12:
            continue
        for j in range(i, n + 1):
            aug[i][j] /= pivot
        for k in range(n):
            if k != i:
                factor = aug[k][i]
                for j in range(i, n + 1):
                    aug[k][j] -= factor * aug[i][j]
    return [aug[i][n] for i in range(n)]


def eval_poly(coeffs: list[float], x: float) -> float:
    """Evaluate polynomial at x using Horner's method."""
    result = 0.0
    for c in coeffs:
        result = result * x + c
    return result


def rational_pade_approximant(
    f: Callable[[float], float],
    a: float,
    n: int,
    m: int,
) -> Callable[[float], float]:
    """
    Compute Pade approximation of order (n, m) at point a.

    Returns a rational function approximation R(x) = P(x) / Q(x).

    Args:
        f: Function to approximate
        a: Expansion point
        n: Degree of numerator
        m: Degree of denominator
    """
    h = [f(a + i * 1e-4) for i in range(n + m + 1)]
    c = [0.0] * (n + m + 1)
    for i in range(n + m + 1):
        c[i] = h[i]

    for k in range(1, n + 1):
        for j in range(1, k + 1):
            if j <= m:
                c[k] = (c[k] - c[k - j]) / (j * 1e-4)

    A = [[0.0] * (m + 1) for _ in range(m + 1)]
    for i in range(m + 1):
        for j in range(m + 1):
            idx = n + i - j
            if idx < 0:
                A[i][j] = 0
            else:
                A[i][j] = c[idx] if idx <= n else 0
    B = [c[i] for i in range(n + 1)]
    q = gaussian_elimination(A, B[: m + 1]) if m > 0 else [1.0]
    p = B[: n + 1]
    for i in range(1, m + 1):
        for j in range(i):
            if j < len(p):
                p[j] -= q[i] * c[i - j - 1]
    return lambda x: eval_poly(p, x - a) / eval_poly(q, x - a) if q[0] != 0 else 0.0


def runge_function(x: float) -> float:
    """Runge function: 1 / (1 + 25*x^2) — problematic for polynomial interpolation."""
    return 1.0 / (1.0 + 25.0 * x * x)


def chebyshev_approximate(
    f: Callable[[float], float],
    degree: int,
    a: float = -1.0,
    b: float = 1.0,
) -> list[float]:
    """
    Compute Chebyshev polynomial approximation of f.

    Args:
        f: Function to approximate
        degree: Polynomial degree
        a: Left endpoint
        b: Right endpoint

    Returns:
        Chebyshev coefficients
    """
    n = degree + 1
    coeffs = [0.0] * n
    for k in range(n):
        sum_val = 0.0
        for i in range(n):
            x_i = (a + b) / 2 + (b - a) / 2 * math.cos((2 * i + 1) * math.pi / (2 * n))
            T_k = math.cos(k * math.acos(max(-1, min(1, (2 * x_i - a - b) / (b - a)))))
            sum_val += f(x_i) * T_k
        coeffs[k] = (2 / n) * sum_val if k > 0 else sum_val / n
    return coeffs
