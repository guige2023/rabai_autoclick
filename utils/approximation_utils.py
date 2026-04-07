"""Approximation algorithms: numeric approximation, curve fitting, and heuristics."""

from __future__ import annotations

import math
import random
from typing import Callable, List, Tuple, Optional, Any


# ---------------------------------------------------------------------------
# Numeric approximation
# ---------------------------------------------------------------------------


def binary_search(
    f: Callable[[float], float],
    lo: float,
    hi: float,
    target: float = 0.0,
    tol: float = 1e-12,
    max_iter: int = 100,
) -> float:
    """Find x in [lo, hi] where f(x) ≈ target using binary search."""
    for _ in range(max_iter):
        mid = (lo + hi) / 2.0
        val = f(mid) - target
        if abs(val) < tol or (hi - lo) / 2 < tol:
            return mid
        if val > 0:
            hi = mid
        else:
            lo = mid
    return (lo + hi) / 2.0


def newton_raphson(
    f: Callable[[float], float],
    df: Callable[[float], float],
    x0: float,
    tol: float = 1e-12,
    max_iter: int = 100,
) -> float:
    """Find root using Newton-Raphson method."""
    x = x0
    for _ in range(max_iter):
        fx = f(x)
        dfx = df(x)
        if abs(dfx) < 1e-30:
            break
        delta = fx / dfx
        x -= delta
        if abs(delta) < tol:
            break
    return x


def secant_method(
    f: Callable[[float], float],
    x0: float,
    x1: float,
    tol: float = 1e-12,
    max_iter: int = 100,
) -> float:
    """Find root using secant method (derivative-free)."""
    x_prev = x0
    x_curr = x1
    for _ in range(max_iter):
        fx_prev = f(x_prev)
        fx_curr = f(x_curr)
        denom = fx_curr - fx_prev
        if abs(denom) < 1e-30:
            break
        x_next = x_curr - fx_curr * (x_curr - x_prev) / denom
        x_prev, x_curr = x_curr, x_next
        if abs(fx_curr) < tol:
            break
    return x_curr


def fixed_point_iteration(
    g: Callable[[float], float],
    x0: float,
    tol: float = 1e-12,
    max_iter: int = 100,
) -> float:
    """Find fixed point x = g(x) via simple iteration."""
    x = x0
    for _ in range(max_iter):
        x_next = g(x)
        if abs(x_next - x) < tol:
            return x_next
        x = x_next
    return x


def trapezoidal_rule(
    f: Callable[[float], float],
    a: float,
    b: float,
    n: int = 1000,
) -> float:
    """Approximate ∫_a^b f(x) dx using trapezoidal rule."""
    h = (b - a) / n
    result = (f(a) + f(b)) / 2.0
    for i in range(1, n):
        result += f(a + i * h)
    return result * h


def simpson_rule(
    f: Callable[[float], float],
    a: float,
    b: float,
    n: int = 1000,
) -> float:
    """Approximate ∫_a^b f(x) dx using Simpson's rule (n must be even)."""
    if n % 2 == 1:
        n += 1
    h = (b - a) / n
    result = f(a) + f(b)
    for i in range(1, n):
        coeff = 4 if i % 2 == 1 else 2
        result += coeff * f(a + i * h)
    return result * h / 3.0


def midpoint_rule(
    f: Callable[[float], float],
    a: float,
    b: float,
    n: int = 1000,
) -> float:
    """Approximate ∫_a^b f(x) dx using midpoint rule."""
    h = (b - a) / n
    result = 0.0
    for i in range(n):
        mid = a + (i + 0.5) * h
        result += f(mid)
    return result * h


def richardson_extrapolation(
    f: Callable[[float], float],
    h: float,
    order: int = 2,
) -> float:
    """Richardson extrapolation for better integral approximation."""
    if order == 2:
        T1 = trapezoidal_rule(f, 0, 1, int(1 / h))
        T2 = trapezoidal_rule(f, 0, 1, int(2 / h))
        return (4 * T2 - T1) / 3.0
    return trapezoidal_rule(f, 0, 1, int(1 / h))


# ---------------------------------------------------------------------------
# Curve fitting (linear least squares)
# ---------------------------------------------------------------------------


def linear_regression(points: List[Tuple[float, float]]) -> Tuple[float, float]:
    """Fit y = mx + b via least squares. Returns (m, b)."""
    n = len(points)
    if n < 2:
        return (0.0, 0.0)
    sum_x = sum_y = sum_xy = sum_x2 = 0.0
    for x, y in points:
        sum_x += x
        sum_y += y
        sum_xy += x * y
        sum_x2 += x * x
    denom = n * sum_x2 - sum_x * sum_x
    if abs(denom) < 1e-14:
        m = 0.0
    else:
        m = (n * sum_xy - sum_x * sum_y) / denom
    b = (sum_y - m * sum_x) / n
    return m, b


def polynomial_regression(
    points: List[Tuple[float, float]],
    degree: int,
) -> List[float]:
    """Least-squares polynomial fit. Returns coefficients lowest-to-highest."""
    n = len(points)
    m = degree + 1
    A = [[xi ** j for j in range(m)] for xi, _ in points]
    ATy = [sum(A[i][j] * points[i][1] for i in range(n)) for j in range(m)]
    ATA = [[sum(A[i][j] * A[i][k] for i in range(n)) for k in range(m)] for j in range(m)]
    return _solve_linear(ATA, ATy)


def _solve_linear(A: List[List[float]], b: List[float]) -> List[float]:
    """Gaussian elimination with partial pivoting."""
    n = len(b)
    aug = [row[:] + [b[i]] for i, row in enumerate(A)]
    for col in range(n):
        max_row = max(range(col, n), key=lambda r: abs(aug[r][col]))
        aug[col], aug[max_row] = aug[max_row], aug[col]
        pivot = aug[col][col]
        if abs(pivot) < 1e-14:
            continue
        for row in range(col + 1, n):
            factor = aug[row][col] / pivot
            for j in range(col, n + 1):
                aug[row][j] -= factor * aug[col][j]
    x = [0.0] * n
    for i in reversed(range(n)):
        x[i] = aug[i][n]
        for j in range(i + 1, n):
            x[i] -= aug[i][j] * x[j]
        x[i] /= aug[i][i] if abs(aug[i][i]) > 1e-14 else 1.0
    return x


# ---------------------------------------------------------------------------
# Heuristic approximations
# ---------------------------------------------------------------------------


def golden_section_search(
    f: Callable[[float], float],
    lo: float,
    hi: float,
    tol: float = 1e-12,
    max_iter: int = 100,
) -> float:
    """Find minimum of f on [lo, hi] using golden-section search."""
    phi = (1 + math.sqrt(5)) / 2
    a, b = lo, hi
    c = b - (b - a) / phi
    d = a + (b - a) / phi
    for _ in range(max_iter):
        if abs(b - a) < tol:
            return (a + b) / 2
        if f(c) < f(d):
            b = d
        else:
            a = c
        c = b - (b - a) / phi
        d = a + (b - a) / phi
    return (a + b) / 2


def hill_climbing(
    f: Callable[[List[float]], float],
    x0: List[float],
    step: float = 0.01,
    max_iter: int = 1000,
    dims: int = 2,
) -> List[float]:
    """Simple hill climbing for multi-dimensional optimization."""
    x = list(x0)
    best = f(x)
    for _ in range(max_iter):
        improved = False
        for i in range(dims):
            for delta in [-step, step]:
                trial = list(x)
                trial[i] += delta
                val = f(trial)
                if val > best:
                    best = val
                    x = trial
                    improved = True
        if not improved:
            step /= 2.0
            if step < 1e-10:
                break
    return x


def simulated_annealing(
    f: Callable[[List[float]], float],
    x0: List[float],
    T0: float = 1000.0,
    cooling_rate: float = 0.995,
    min_T: float = 1e-8,
    dims: int = 2,
    step: float = 0.1,
) -> Tuple[List[float], float]:
    """Simulated annealing. Returns (best solution, best score)."""
    x = list(x0)
    best = list(x)
    best_score = f(x)
    T = T0
    while T > min_T:
        for _ in range(100):
            i = random.randint(0, dims - 1)
            trial = list(x)
            trial[i] += random.uniform(-step, step)
            delta = f(trial) - f(x)
            if delta > 0 or random.random() < math.exp(delta / T):
                x = trial
                if f(x) > best_score:
                    best_score = f(x)
                    best = list(x)
        T *= cooling_rate
    return best, best_score
