"""
Numerical integration utilities.

Provides trapezoidal rule, Simpson's rule, Romberg integration,
Gaussian quadrature, and Monte Carlo integration.
"""

from __future__ import annotations

import math
import random
from typing import Callable


def trapezoidal_rule(
    f: Callable[[float], float],
    a: float,
    b: float,
    n: int = 100,
) -> float:
    """
    Composite trapezoidal rule.

    Args:
        f: Function to integrate
        a: Lower bound
        b: Upper bound
        n: Number of subintervals (must be even for Simpson)

    Returns:
        Approximate integral value.
    """
    if n < 1:
        return 0.0
    h = (b - a) / n
    result = f(a) + f(b)
    for i in range(1, n):
        result += 2 * f(a + i * h)
    return result * h / 2


def simpson_rule(
    f: Callable[[float], float],
    a: float,
    b: float,
    n: int = 100,
) -> float:
    """
    Composite Simpson's 1/3 rule (n must be even).

    Args:
        f: Function to integrate
        a: Lower bound
        b: Upper bound
        n: Number of subintervals (even)

    Returns:
        Approximate integral value.
    """
    if n < 2:
        n = 2
    if n % 2 == 1:
        n += 1
    h = (b - a) / n
    result = f(a) + f(b)
    for i in range(1, n):
        coeff = 4 if i % 2 == 1 else 2
        result += coeff * f(a + i * h)
    return result * h / 3


def simpson_3_8_rule(
    f: Callable[[float], float],
    a: float,
    b: float,
    n: int = 99,
) -> float:
    """
    Composite Simpson's 3/8 rule (n must be multiple of 3).

    Args:
        f: Function to integrate
        a: Lower bound
        b: Upper bound
        n: Number of subintervals (multiple of 3)

    Returns:
        Approximate integral value.
    """
    if n < 3:
        n = 3
    while n % 3 != 0:
        n += 1
    h = (b - a) / n
    result = f(a) + f(b)
    for i in range(1, n):
        coeff = 3 if i % 3 != 0 else 2
        result += coeff * f(a + i * h)
    return result * 3 * h / 8


def boole_rule(
    f: Callable[[float], float],
    a: float,
    b: float,
) -> float:
    """
    Boole's rule (5-point Newton-Cotes, 4th order).

    For a single panel [a, b].
    """
    fa = f(a)
    fb = f(b)
    x1 = a + (b - a) / 4
    x2 = a + (b - a) / 2
    x3 = a + 3 * (b - a) / 4
    return (b - a) * (7 * fa + 32 * f(x1) + 12 * f(x2) + 32 * f(x3) + 7 * fb) / 45


def midpoint_rule(
    f: Callable[[float], float],
    a: float,
    b: float,
    n: int = 100,
) -> float:
    """
    Composite midpoint rule.

    Args:
        f: Function to integrate
        a: Lower bound
        b: Upper bound
        n: Number of subintervals

    Returns:
        Approximate integral value.
    """
    h = (b - a) / n
    result = 0.0
    for i in range(n):
        x_mid = a + (i + 0.5) * h
        result += f(x_mid)
    return result * h


def romberg_integration(
    f: Callable[[float], float],
    a: float,
    b: float,
    max_iter: int = 10,
    tol: float = 1e-8,
) -> float:
    """
    Romberg integration ( Richardson extrapolation on trapezoidal rule).

    Args:
        f: Function to integrate
        a: Lower bound
        b: Upper bound
        max_iter: Maximum number of iterations
        tol: Convergence tolerance

    Returns:
        Highly accurate integral approximation.
    """
    R: list[list[float]] = [[0.0] * max_iter for _ in range(max_iter)]

    # First estimate: n=1
    R[0][0] = (b - a) * (f(a) + f(b)) / 2

    for i in range(1, max_iter):
        n = 2 ** i
        h = (b - a) / n
        # Composite trapezoidal
        total = sum(f(a + k * h) for k in range(1, n))
        R[i][0] = h * (f(a) + 2 * total + f(b)) / 2

        # Richardson extrapolation
        for j in range(1, i + 1):
            factor = 4 ** j
            R[i][j] = (factor * R[i][j - 1] - R[i - 1][j - 1]) / (factor - 1)

        # Check convergence
        if i > 0 and abs(R[i][i] - R[i - 1][i - 1]) < tol:
            return R[i][i]

    return R[max_iter - 1][max_iter - 1]


def gaussian_quadrature(
    f: Callable[[float], float],
    a: float,
    b: float,
    order: int = 5,
) -> float:
    """
    Gauss-Legendre quadrature.

    Args:
        f: Function to integrate
        a: Lower bound
        b: Upper bound
        order: Number of quadrature points (1-7)

    Returns:
        Approximate integral value.
    """
    # Gauss-Legendre nodes and weights (precomputed for orders 1-7)
    nodes_weights: dict[int, list[tuple[float, float]]] = {
        1: [(0.0, 2.0)],
        2: [(-0.5773502691896257, 1.0), (0.5773502691896257, 1.0)],
        3: [(-0.7745966692414834, 0.5555555555555556),
            (0.0, 0.8888888888888888),
            (0.7745966692414834, 0.5555555555555556)],
        4: [(-0.8611363115940526, 0.3478548451374538),
            (-0.3399810435848563, 0.6521451548625461),
            (0.3399810435848563, 0.6521451548625461),
            (0.8611363115940526, 0.3478548451374538)],
        5: [(-0.906179845938664, 0.2369268850561891),
            (-0.5384693101056831, 0.4786286704993665),
            (0.0, 0.5688888888888889),
            (0.5384693101056831, 0.4786286704993665),
            (0.906179845938664, 0.2369268850561891)],
        6: [(-0.9324695142031521, 0.1713244923791704),
            (-0.6612093864662645, 0.3607615730481386),
            (-0.2386191860831969, 0.467913934572691),
            (0.2386191860831969, 0.467913934572691),
            (0.6612093864662645, 0.3607615730481386),
            (0.9324695142031521, 0.1713244923791704)],
        7: [(-0.9491079123427585, 0.1294849661688697),
            (-0.7415311855993945, 0.2797053914891767),
            (-0.4058451513773972, 0.381830050505118),
            (0.0, 0.4179591836734694),
            (0.4058451513773972, 0.381830050505118),
            (0.7415311855993945, 0.2797053914891767),
            (0.9491079123427585, 0.1294849661688697)],
    }

    if order not in nodes_weights:
        order = max(1, min(7, order))

    # Map from [-1, 1] to [a, b]
    nodes, weights = zip(*nodes_weights[order])
    result = 0.0
    for node, weight in zip(nodes, weights):
        x = ((b - a) * node + (b + a)) / 2
        result += weight * f(x)
    return result * (b - a) / 2


def monte_carlo_integration(
    f: Callable[[float], float],
    a: float,
    b: float,
    n: int = 10000,
) -> float:
    """
    Monte Carlo integration.

    Args:
        f: Function to integrate
        a: Lower bound
        b: Upper bound
        n: Number of random samples

    Returns:
        Approximate integral value.
    """
    if n < 1:
        return 0.0
    samples = [f(a + (b - a) * random.random()) for _ in range(n)]
    return (b - a) * sum(samples) / n


def monte_carlo_2d(
    f: Callable[[float, float], float],
    x_range: tuple[float, float],
    y_range: tuple[float, float],
    n: int = 10000,
) -> float:
    """
    2D Monte Carlo integration.

    Args:
        f: 2D function to integrate
        x_range: (x_min, x_max)
        y_range: (y_min, y_max)
        n: Number of samples

    Returns:
        Approximate double integral.
    """
    x_min, x_max = x_range
    y_min, y_max = y_range
    area = (x_max - x_min) * (y_max - y_min)
    samples = [f(x_min + (x_max - x_min) * random.random(),
                 y_min + (y_max - y_min) * random.random()) for _ in range(n)]
    return area * sum(samples) / n


def adaptive_quadrature(
    f: Callable[[float], float],
    a: float,
    b: float,
    tol: float = 1e-8,
    max_depth: int = 50,
) -> tuple[float, int]:
    """
    Adaptive Simpson's rule.

    Args:
        f: Function to integrate
        a: Lower bound
        b: Upper bound
        tol: Tolerance
        max_depth: Maximum recursion depth

    Returns:
        Tuple of (integral, num_evaluations).
    """
    def asr(a: float, b: float, tol: float, depth: int) -> tuple[float, int]:
        if depth == 0:
            return 0.0, 0
        c = (a + b) / 2
        fa, fb, fc = f(a), f(b), f(c)
        left = (b - a) * (fa + 4 * fc + fb) / 6
        d, e = (a + c) / 2, (c + b) / 2
        fd, fe = f(d), f(e)
        right = (c - a) * (fa + 4 * fd + fc) / 6 + (b - c) * (fc + 4 * fe + fb) / 6
        if abs(right - left) <= 15 * tol or depth <= 1:
            return right + (right - left) / 15, 6
        left_res, left_cnt = asr(a, c, tol / 2, depth - 1)
        right_res, right_cnt = asr(c, b, tol / 2, depth - 1)
        return left_res + right_res, left_cnt + right_cnt + 2

    return asr(a, b, tol, max_depth)


def integrate_oscillatory(
    f: Callable[[float], float],
    a: float,
    b: float,
    omega: float,
    n: int = 100,
) -> float:
    """
    Filon's method for oscillatory integrals ∫ f(x) * sin(omega*x) dx.

    Args:
        f: Amplitude function
        omega: Angular frequency
        a: Lower bound
        b: Upper bound
        n: Number of subintervals (even)

    Returns:
        Approximate integral.
    """
    if n % 2 == 1:
        n += 1
    h = (b - a) / n
    sin_sum = 0.0
    cos_sum = 0.0
    for i in range(n + 1):
        x_i = a + i * h
        theta = omega * x_i
        if i == 0 or i == n:
            coeff = 1.0
        elif i % 2 == 1:
            coeff = 4.0
        else:
            coeff = 2.0
        sin_sum += coeff * f(x_i) * math.sin(theta)
        cos_sum += coeff * f(x_i) * math.cos(theta)
    alpha = omega * h
    if abs(alpha) < 1e-12:
        theta_L = omega * a
        theta_R = omega * b
        return (f(b) * math.cos(theta_R) - f(a) * math.cos(theta_L)) / omega
    sinc = math.sin(alpha) / alpha
    sin_sq = math.sin(alpha) ** 2
    S = (alpha * (math.sin(alpha) * math.cos(alpha) - alpha) / sin_sq
         if abs(sin_sq) > 1e-12 else 1.0 / 3.0)
    C = (alpha * (alpha - math.sin(alpha) * math.cos(alpha)) / sin_sq
         if abs(sin_sq) > 1e-12 else 1.0 / 3.0)
    return h * (S * sin_sum + C * cos_sum)
