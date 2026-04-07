"""
Optimization utilities v2 — constrained and multi-objective optimization.

Companion to optimization_utils.py. Adds constrained optimization,
conjugate gradient, and Newton's method.
"""

from __future__ import annotations

import math


def conjugate_gradient(
    A: list[list[float]],
    b: list[float],
    x0: list[float] | None = None,
    tol: float = 1e-8,
    max_iter: int = 100,
) -> list[float]:
    """
    Solve Ax = b using Conjugate Gradient method (A must be symmetric positive-definite).

    Args:
        A: Symmetric positive-definite matrix (n×n)
        b: Right-hand side vector
        x0: Initial guess
        tol: Convergence tolerance
        max_iter: Maximum iterations

    Returns:
        Solution vector x
    """
    n = len(b)
    x = x0[:] if x0 else [0.0] * n
    r = [b[i] - sum(A[i][j] * x[j] for j in range(n)) for i in range(n)]
    p = r[:]
    rsold = sum(r[i] * r[i] for i in range(n))

    for _ in range(max_iter):
        Ap = [sum(A[i][j] * p[j] for j in range(n)) for i in range(n)]
        pAp = sum(p[i] * Ap[i] for i in range(n))
        if abs(pAp) < 1e-12:
            break
        alpha = rsold / pAp
        for i in range(n):
            x[i] += alpha * p[i]
            r[i] -= alpha * Ap[i]
        rsnew = sum(r[i] * r[i] for i in range(n))
        if math.sqrt(rsnew) < tol:
            break
        p = [r[i] + (rsnew / rsold) * p[i] for i in range(n)]
        rsold = rsnew
    return x


def newton_raphson(
    f: callable,
    df: callable,
    ddf: callable,
    x0: float,
    tol: float = 1e-8,
    max_iter: int = 50,
) -> tuple[float, int, bool]:
    """
    Find root using Newton-Raphson with second-order convergence.

    Args:
        f: Function
        df: First derivative
        ddf: Second derivative
        x0: Initial guess
        tol: Convergence tolerance
        max_iter: Maximum iterations

    Returns:
        Tuple of (root, iterations, converged)
    """
    x = x0
    for i in range(max_iter):
        fx = f(x)
        dfx = df(x)
        if abs(dfx) < 1e-12:
            return x, i, False
        ddfx = ddf(x)
        denom = dfx * dfx - fx * ddfx
        if abs(denom) < 1e-12:
            x = x - fx / dfx
        else:
            x = x - (2 * fx * dfx) / denom
        if abs(fx) < tol:
            return x, i + 1, True
    return x, max_iter, False


def successive_approximation(
    g: callable,
    x0: float,
    tol: float = 1e-8,
    max_iter: int = 100,
) -> tuple[float, int, bool]:
    """
    Fixed-point iteration (successive approximation) for solving x = g(x).

    Args:
        g: Iteration function
        x0: Initial guess
        tol: Convergence tolerance
        max_iter: Maximum iterations

    Returns:
        Tuple of (fixed_point, iterations, converged)
    """
    x = x0
    for i in range(max_iter):
        x_new = g(x)
        if abs(x_new - x) < tol:
            return x_new, i + 1, True
        x = x_new
    return x, max_iter, False


def constrained_gradient_descent(
    f: callable,
    grad_f: callable,
    x0: list[float],
    constraints: list[callable],
    grad_constraints: list[callable],
    lr: float = 0.01,
    tol: float = 1e-6,
    max_iter: int = 1000,
) -> tuple[list[float], int, bool]:
    """
    Projected gradient descent for constrained optimization.

    Args:
        f: Objective function
        grad_f: Gradient of objective
        x0: Initial feasible point
        constraints: List of constraint functions (g(x) <= 0)
        grad_constraints: List of constraint gradients
        lr: Learning rate
        tol: Convergence tolerance
        max_iter: Maximum iterations

    Returns:
        Tuple of (optimal point, iterations, converged)
    """
    x = x0[:]
    for i in range(max_iter):
        g = grad_f(x)
        penalty = 0.0
        grad_penalty = [0.0] * len(x)
        for constr, grad_c in zip(constraints, grad_constraints):
            ci = constr(x)
            if ci > 0:
                penalty += ci ** 2
                gc = grad_c(x)
                for k in range(len(x)):
                    grad_penalty[k] += 2 * ci * gc[k]
        grad_total = [g[k] + grad_penalty[k] for k in range(len(x))]
        grad_norm = math.sqrt(sum(g[k] ** 2 for k in range(len(x))))
        if grad_norm < tol:
            return x, i + 1, True
        for k in range(len(x)):
            x[k] -= lr * grad_total[k]
    return x, max_iter, False


def nelder_mead(
    f: callable,
    x0: list[float],
    alpha: float = 1.0,
    gamma: float = 2.0,
    rho: float = 0.5,
    sigma: float = 0.5,
    tol: float = 1e-8,
    max_iter: int = 1000,
) -> tuple[list[float], float, int]:
    """
    Nelder-Mead simplex method for derivative-free optimization.

    Args:
        f: Objective function
        x0: Initial point
        alpha: Reflection coefficient
        gamma: Expansion coefficient
        rho: Contraction coefficient
        sigma: Shrink coefficient
        tol: Convergence tolerance
        max_iter: Maximum iterations

    Returns:
        Tuple of (optimal point, minimum value, iterations)
    """
    n = len(x0)
    simplex = [x0[:]]
    for i in range(n):
        point = x0[:]
        point[i] += 0.5 if x0[i] != 0 else 0.25
        simplex.append(point)
    values = [f(p) for p in simplex]

    for iteration in range(max_iter):
        simplex = sorted(zip(values, simplex), key=lambda x: x[0])
        values = [s[0] for s in simplex]
        centroid = [sum(simplex[i][1][j] for i in range(n)) / n for j in range(n)]
        xr = [centroid[j] + alpha * (centroid[j] - simplex[n][1][j]) for j in range(n)]
        xr_val = f(xr)
        if xr_val < values[0]:
            xe = [centroid[j] + gamma * (xr[j] - centroid[j]) for j in range(n)]
            xe_val = f(xe)
            if xe_val < xr_val:
                simplex[n] = (xe_val, xe)
            else:
                simplex[n] = (xr_val, xr)
        elif xr_val < values[n - 1]:
            simplex[n] = (xr_val, xr)
        else:
            xc = [centroid[j] + rho * (simplex[n][1][j] - centroid[j]) for j in range(n)]
            xc_val = f(xc)
            if xc_val < values[n]:
                simplex[n] = (xc_val, xc)
            else:
                for i in range(1, n + 1):
                    simplex[i] = (f([simplex[0][1][j] + sigma * (simplex[i][1][j] - simplex[0][1][j]) for j in range(n)]),
                                  [simplex[0][1][j] + sigma * (simplex[i][1][j] - simplex[0][1][j]) for j in range(n)])
        values = [s[0] for s in simplex]
        if values[-1] - values[0] < tol:
            break

    best = simplex[0]
    return best[1], best[0], iteration + 1
