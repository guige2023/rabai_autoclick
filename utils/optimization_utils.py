"""
Optimization utilities for numerical optimization and search algorithms.

Provides implementations of common optimization algorithms including
golden section search, gradient descent, and simulated annealing.
"""

from __future__ import annotations

import math
import random
from typing import Callable, NamedTuple


class OptimizationResult(NamedTuple):
    """Result of an optimization run."""
    minimum: float
    x_min: float
    iterations: int
    evaluations: int
    converged: bool


class MaximizationResult(NamedTuple):
    """Result of a maximization run."""
    maximum: float
    x_max: float
    iterations: int
    evaluations: int
    converged: bool


def golden_section_search(
    f: Callable[[float], float],
    a: float,
    b: float,
    tol: float = 1e-8,
    max_iter: int = 100,
) -> OptimizationResult:
    """
    Find the minimum of a unimodal function using golden section search.

    Args:
        f: Unimodal function to minimize
        a: Left bound
        b: Right bound
        tol: Convergence tolerance
        max_iter: Maximum iterations

    Returns:
        OptimizationResult with minimum value and location

    Example:
        >>> result = golden_section_search(lambda x: x**2, -5.0, 5.0)
        >>> round(result.minimum, 6)
        0.0
    """
    phi = (1 + math.sqrt(5)) / 2  # Golden ratio ≈ 1.618
    inv_phi = 1 / phi

    x1 = b - inv_phi * (b - a)
    x2 = a + inv_phi * (b - a)
    f1, f2 = f(x1), f(x2)
    evaluations = 2
    iterations = 0

    for _ in range(max_iter):
        iterations += 1
        if abs(b - a) < tol:
            return OptimizationResult(min(f1, f2), x1 if f1 < f2 else x2, iterations, evaluations, True)

        if f1 < f2:
            b, x2, f2 = x1, x1, f1
            x1 = b - inv_phi * (b - a)
            f1 = f(x1)
        else:
            a, x1, f1 = x2, x2, f2
            x2 = a + inv_phi * (b - a)
            f2 = f(x2)
        evaluations += 1

    x_min = x1 if f1 < f2 else x2
    return OptimizationResult(f(x_min), x_min, iterations, evaluations, False)


def gradient_descent(
    f: Callable[[float], float],
    df: Callable[[float], float],
    x0: float,
    lr: float = 0.01,
    tol: float = 1e-8,
    max_iter: int = 1000,
) -> OptimizationResult:
    """
    Find minimum using gradient descent with fixed learning rate.

    Args:
        f: Objective function
        df: Gradient of objective function
        x0: Initial guess
        lr: Learning rate (step size)
        tol: Convergence tolerance on gradient
        max_iter: Maximum iterations

    Returns:
        OptimizationResult with minimum value and location
    """
    x = x0
    evaluations = 0

    for i in range(max_iter):
        grad = df(x)
        evaluations += 2
        if abs(grad) < tol:
            return OptimizationResult(f(x), x, i + 1, evaluations, True)
        x = x - lr * grad

    return OptimizationResult(f(x), x, max_iter, evaluations, False)


def simulated_annealing(
    f: Callable[[float], float],
    bounds: tuple[float, float],
    temp: float = 1000.0,
    cooling_rate: float = 0.995,
    min_temp: float = 1e-8,
    max_iter: int = 2000,
    seed: int | None = None,
) -> MaximizationResult:
    """
    Global optimization using simulated annealing algorithm.

    Args:
        f: Objective function to minimize
        bounds: (lower, upper) bounds for search space
        temp: Initial temperature
        cooling_rate: Temperature multiplier per step (0 < rate < 1)
        min_temp: Stopping temperature
        max_iter: Maximum iterations
        seed: Random seed for reproducibility

    Returns:
        MaximizationResult (negates to show minimum found)

    Example:
        >>> result = simulated_annealing(lambda x: (x - 2)**2, (-10, 10), seed=42)
        >>> round(result.maximum, 4)
        0.0
    """
    if seed is not None:
        random.seed(seed)

    lower, upper = bounds
    x = random.uniform(lower, upper)
    current_energy = f(x)
    best_x, best_energy = x, current_energy
    iterations = 0
    evaluations = 0

    while temp > min_temp and iterations < max_iter:
        iterations += 1
        x_candidate = random.uniform(lower, upper)
        new_energy = f(x_candidate)
        evaluations += 1

        delta = new_energy - current_energy
        if delta < 0 or random.random() < math.exp(-delta / temp):
            x, current_energy = x_candidate, new_energy
            if current_energy < best_energy:
                best_x, best_energy = x, current_energy

        temp *= cooling_rate

    return MaximizationResult(-best_energy, best_x, iterations, evaluations, temp <= min_temp)


def bisection_minimize(
    f: Callable[[float], float],
    a: float,
    b: float,
    tol: float = 1e-8,
    max_iter: int = 100,
) -> OptimizationResult:
    """
    Find minimum of a convex function using bisection method on derivative.

    Args:
        f: Convex objective function
        a: Left bound
        b: Right bound
        tol: Convergence tolerance
        max_iter: Maximum iterations

    Returns:
        OptimizationResult with minimum value and location
    """
    from typing import Sequence

    def has_root(g: Callable[[float], float], lo: float, hi: float, eps: float = 1e-12) -> bool:
        return g(lo) * g(hi) <= 0

    def find_root(g: Callable[[float], float], lo: float, hi: float, eps: float, max_it: int) -> float:
        for _ in range(max_it):
            mid = (lo + hi) / 2
            if abs(g(mid)) < eps or (hi - lo) / 2 < eps:
                return mid
            if g(lo) * g(mid) <= 0:
                hi = mid
            else:
                lo = mid
        return mid

    def dfdx(f: Callable[[float], float], x: float, h: float = 1e-6) -> float:
        return (f(x + h) - f(x - h)) / (2 * h)

    evaluations = 0
    g = lambda x: dfdx(f, x)
    for i in range(max_iter):
        if not has_root(g, a, b):
            break
        x_mid = (a + b) / 2
        evaluations += 3  # f(x+h), f(x-h), plus has_root check
        if abs(b - a) < tol:
            return OptimizationResult(f(x_mid), x_mid, i + 1, evaluations, True)
        if g(a) * g(x_mid) <= 0:
            b = x_mid
        else:
            a = x_mid

    x_min = (a + b) / 2
    return OptimizationResult(f(x_min), x_min, max_iter, evaluations, False)


def line_search_armijo(
    f: Callable[[float], float],
    df: Callable[[float], float],
    x: float,
    direction: float,
    alpha: float = 1.0,
    beta: float = 0.5,
    sigma: float = 0.1,
    max_iter: int = 20,
) -> tuple[float, int]:
    """
    Backtracking line search using Armijo condition.

    Args:
        f: Objective function
        df: Gradient function
        x: Current position
        direction: Search direction (typically negative gradient)
        alpha: Initial step size
        beta: Step size reduction factor (0 < beta < 1)
        sigma: Acceptable decrease fraction (0 < sigma < 0.5)
        max_iter: Maximum backtracking iterations

    Returns:
        Tuple of (step_size, evaluations)
    """
    f0 = f(x)
    df0 = df(x)
    grad_dot_dir = df0 * direction
    evaluations = 1

    for _ in range(max_iter):
        step = alpha * direction
        f_new = f(x + step)
        evaluations += 1
        if f_new <= f0 + sigma * alpha * grad_dot_dir:
            return alpha, evaluations
        alpha *= beta

    return alpha, evaluations
