"""Decimal action v4 - optimization and numerical methods.

Decimal utilities for optimization, interpolation,
and numerical analysis.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Callable, Sequence

__all__ = [
    "bisection_method",
    "newton_raphson",
    "secant_method",
    "fixed_point_iteration",
    "horner_eval",
    "lagrange_interpolate",
    "newton_interpolate",
    "cubic_spline_coeffs",
    "simpson_rule",
    "trapezoid_rule",
    "monte_carlo_integral",
    "gradient_descent",
    "newtons_method_2d",
    "jacobian",
    "hessian",
    "golden_section_search",
    "FibonacciSearch",
    "DecimalOptimizer",
    "DecimalInterpolator",
]


def bisection_method(f: Callable[[Decimal], Decimal], a: Decimal, b: Decimal, tol: Decimal = Decimal("1e-10"), max_iter: int = 100) -> Decimal | None:
    """Bisection method for finding root.

    Args:
        f: Function returning Decimal.
        a: Left bound.
        b: Right bound.
        tol: Tolerance.
        max_iter: Maximum iterations.

    Returns:
        Root approximation or None.
    """
    fa, fb = f(a), f(b)
    if fa * fb > 0:
        return None
    for _ in range(max_iter):
        c = (a + b) / 2
        fc = f(c)
        if abs(fc) < tol or (b - a) / 2 < tol:
            return c
        if fa * fc < 0:
            b, fb = c, fc
        else:
            a, fa = c, fc
    return (a + b) / 2


def newton_raphson(f: Callable[[Decimal], Decimal], df: Callable[[Decimal], Decimal], x0: Decimal, tol: Decimal = Decimal("1e-10"), max_iter: int = 50) -> Decimal | None:
    """Newton-Raphson method.

    Args:
        f: Function.
        df: Derivative function.
        x0: Initial guess.
        tol: Tolerance.
        max_iter: Maximum iterations.

    Returns:
        Root approximation or None.
    """
    x = x0
    for _ in range(max_iter):
        fx = f(x)
        if abs(fx) < tol:
            return x
        dfx = df(x)
        if dfx == 0:
            return None
        x_new = x - fx / dfx
        if abs(x_new - x) < tol:
            return x_new
        x = x_new
    return x


def secant_method(f: Callable[[Decimal], Decimal], x0: Decimal, x1: Decimal, tol: Decimal = Decimal("1e-10"), max_iter: int = 50) -> Decimal | None:
    """Secant method.

    Args:
        f: Function.
        x0: First initial guess.
        x1: Second initial guess.
        tol: Tolerance.
        max_iter: Maximum iterations.

    Returns:
        Root approximation or None.
    """
    x_prev, x = x0, x1
    f_prev = f(x_prev)
    for _ in range(max_iter):
        fx = f(x)
        if abs(fx) < tol:
            return x
        if fx == f_prev:
            return None
        x_new = x - fx * (x - x_prev) / (fx - f_prev)
        if abs(x_new - x) < tol:
            return x_new
        x_prev, f_prev = x, fx
        x = x_new
    return x


def fixed_point_iteration(g: Callable[[Decimal], Decimal], x0: Decimal, tol: Decimal = Decimal("1e-10"), max_iter: int = 50) -> Decimal | None:
    """Fixed point iteration.

    Args:
        g: Fixed point function (x = g(x)).
        x0: Initial guess.
        tol: Tolerance.
        max_iter: Maximum iterations.

    Returns:
        Fixed point approximation or None.
    """
    x = x0
    for _ in range(max_iter):
        x_new = g(x)
        if abs(x_new - x) < tol:
            return x_new
        x = x_new
    return x


def horner_eval(coeffs: Sequence[Decimal], x: Decimal) -> Decimal:
    """Horner's method polynomial evaluation.

    Args:
        coeffs: Polynomial coefficients.
        x: Value to evaluate at.

    Returns:
        Polynomial value.
    """
    result = Decimal(0)
    for c in coeffs:
        result = result * x + c
    return result


def lagrange_interpolate(x_vals: Sequence[Decimal], y_vals: Sequence[Decimal], x: Decimal) -> Decimal:
    """Lagrange interpolation.

    Args:
        x_vals: Known x values.
        y_vals: Known y values.
        x: Value to interpolate at.

    Returns:
        Interpolated y value.
    """
    if len(x_vals) != len(y_vals):
        raise ValueError("Length mismatch")
    n = len(x_vals)
    result = Decimal(0)
    for i in range(n):
        term = y_vals[i]
        for j in range(n):
            if i != j:
                denom = x_vals[i] - x_vals[j]
                if denom == 0:
                    raise ValueError("Duplicate x value")
                term *= (x - x_vals[j]) / denom
        result += term
    return result


def newton_interpolate(x_vals: Sequence[Decimal], y_vals: Sequence[Decimal], x: Decimal) -> Decimal:
    """Newton's divided differences interpolation.

    Args:
        x_vals: Known x values.
        y_vals: Known y values.
        x: Value to interpolate.

    Returns:
        Interpolated value.
    """
    n = len(x_vals)
    div_diff: list[list[Decimal]] = [[y_vals[i]] for i in range(n)]
    for j in range(1, n):
        for i in range(n - j):
            denom = x_vals[i + j] - x_vals[i]
            if denom == 0:
                raise ValueError("Duplicate x")
            div_diff[i].append((div_diff[i + 1][j - 1] - div_diff[i][j - 1]) / denom)
    result = Decimal(0)
    product = Decimal(1)
    for i in range(n):
        result += div_diff[0][i] * product
        product *= (x - x_vals[i])
    return result


def cubic_spline_coeffs(x_vals: Sequence[Decimal], y_vals: Sequence[Decimal]) -> list[tuple[Decimal, Decimal, Decimal, Decimal]]:
    """Compute cubic spline coefficients.

    Args:
        x_vals: X values.
        y_vals: Y values.

    Returns:
        List of (a, b, c, d) coefficient tuples.
    """
    n = len(x_vals) - 1
    h = [x_vals[i + 1] - x_vals[i] for i in range(n)]
    alpha = [Decimal(0)] * (n + 1)
    for i in range(1, n):
        alpha[i] = Decimal(3) * (y_vals[i + 1] - y_vals[i]) / h[i] - Decimal(3) * (y_vals[i] - y_vals[i - 1]) / h[i - 1]
    l = [Decimal(1)] * (n + 1)
    mu = [Decimal(0)] * (n + 1)
    z = [Decimal(0)] * (n + 1)
    for i in range(1, n):
        l[i] = Decimal(2) * (x_vals[i + 1] - x_vals[i - 1]) - h[i - 1] * mu[i - 1]
        mu[i] = h[i] / l[i]
        z[i] = (alpha[i] - h[i - 1] * z[i - 1]) / l[i]
    c = [Decimal(0)] * (n + 1)
    b = [Decimal(0)] * n
    d = [Decimal(0)] * n
    for j in range(n - 1, -1, -1):
        c[j] = z[j] - mu[j] * c[j + 1]
        b[j] = (y_vals[j + 1] - y_vals[j]) / h[j] - h[j] * (c[j + 1] + Decimal(2) * c[j]) / Decimal(3)
        d[j] = (c[j + 1] - c[j]) / (Decimal(3) * h[j])
        a = y_vals[j]
    return [(y_vals[i], b[i], c[i], d[i]) for i in range(n)]


def simpson_rule(f: Callable[[Decimal], Decimal], a: Decimal, b: Decimal, n: int = 100) -> Decimal:
    """Simpson's rule for numerical integration.

    Args:
        f: Function to integrate.
        a: Lower bound.
        b: Upper bound.
        n: Number of subintervals (must be even).

    Returns:
        Approximate integral.
    """
    if n % 2 == 1:
        n += 1
    h = (b - a) / n
    result = f(a) + f(b)
    for i in range(1, n):
        x = a + i * h
        result += f(x) * (Decimal(4) if i % 2 == 1 else Decimal(2))
    return result * h / Decimal(3)


def trapezoid_rule(f: Callable[[Decimal], Decimal], a: Decimal, b: Decimal, n: int = 100) -> Decimal:
    """Trapezoid rule for numerical integration.

    Args:
        f: Function to integrate.
        a: Lower bound.
        b: Upper bound.
        n: Number of subintervals.

    Returns:
        Approximate integral.
    """
    h = (b - a) / n
    result = f(a) + f(b)
    for i in range(1, n):
        result += Decimal(2) * f(a + i * h)
    return result * h / Decimal(2)


def monte_carlo_integral(f: Callable[[Decimal], Decimal], a: Decimal, b: Decimal, n: int = 10000) -> Decimal:
    """Monte Carlo integration.

    Args:
        f: Function to integrate.
        a: Lower bound.
        b: Upper bound.
        n: Number of samples.

    Returns:
        Approximate integral.
    """
    import random
    total = Decimal(0)
    for _ in range(n):
        x = Decimal(str(random.random())) * (b - a) + a
        total += f(x)
    return total * (b - a) / Decimal(n)


def gradient_descent(f: Callable[[Decimal], Decimal], df: Callable[[Decimal], Decimal], x0: Decimal, alpha: Decimal = Decimal("0.1"), tol: Decimal = Decimal("1e-6"), max_iter: int = 1000) -> Decimal:
    """Gradient descent optimization.

    Args:
        f: Objective function.
        df: Gradient function.
        x0: Initial point.
        alpha: Learning rate.
        tol: Tolerance.
        max_iter: Maximum iterations.

    Returns:
        Optimal point.
    """
    x = x0
    for _ in range(max_iter):
        grad = df(x)
        x_new = x - alpha * grad
        if abs(x_new - x) < tol:
            return x_new
        x = x_new
    return x


def newtons_method_2d(f: Callable[[tuple[Decimal, Decimal]], Decimal], x0: tuple[Decimal, Decimal], tol: Decimal = Decimal("1e-8"), max_iter: int = 50) -> tuple[Decimal, Decimal]:
    """Newton's method in 2D.

    Args:
        f: Scalar function.
        x0: Initial point.
        tol: Tolerance.
        max_iter: Maximum iterations.

    Returns:
        (x, y) optimum.
    """
    x, y = x0
    h = Decimal("1e-5")
    for _ in range(max_iter):
        fx = f((x, y))
        fx_h = f((x + h, y))
        fy_h = f((x, y + h))
        grad_x = (fx_h - fx) / h
        grad_y = (fy_h - fx) / h
        x_new = x - fx / grad_x if grad_x != 0 else x
        y_new = y - fx / grad_y if grad_y != 0 else y
        if abs(x_new - x) < tol and abs(y_new - y) < tol:
            return (x_new, y_new)
        x, y = x_new, y_new
    return (x, y)


def jacobian(f: Callable[[Sequence[Decimal]], Sequence[Decimal]], x: Sequence[Decimal], h: Decimal = Decimal("1e-5")) -> list[list[Decimal]]:
    """Compute Jacobian matrix numerically.

    Args:
        f: Vector function.
        x: Point.
        h: Step size.

    Returns:
        Jacobian matrix.
    """
    n = len(x)
    m = len(f(x))
    jac = [[Decimal(0)] * n for _ in range(m)]
    for j in range(n):
        x_plus = list(x)
        x_plus[j] += h
        f_plus = f(x_plus)
        f_minus = f(x)
        for i in range(m):
            jac[i][j] = (f_plus[i] - f_minus[i]) / h
    return jac


def hessian(f: Callable[[Sequence[Decimal]], Decimal], x: Sequence[Decimal], h: Decimal = Decimal("1e-5")) -> list[list[Decimal]]:
    """Compute Hessian matrix numerically.

    Args:
        f: Scalar function.
        x: Point.
        h: Step size.

    Returns:
        Hessian matrix.
    """
    n = len(x)
    hess = [[Decimal(0)] * n for _ in range(n)]
    for i in range(n):
        for j in range(i, n):
            x_ij_plus = list(x)
            x_ij_plus[i] += h
            x_ij_plus[j] += h
            f_ij_plus = f(x_ij_plus)
            x_i_plus = list(x)
            x_i_plus[i] += h
            f_i_plus = f(x_i_plus)
            x_j_plus = list(x)
            x_j_plus[j] += h
            f_j_plus = f(x_j_plus)
            f_base = f(x)
            hess[i][j] = (f_ij_plus - f_i_plus - f_j_plus + f_base) / (h * h)
            hess[j][i] = hess[i][j]
    return hess


def golden_section_search(f: Callable[[Decimal], Decimal], a: Decimal, b: Decimal, tol: Decimal = Decimal("1e-8"), max_iter: int = 100) -> tuple[Decimal, Decimal]:
    """Golden section search for unimodal optimization.

    Args:
        f: Unimodal function to minimize.
        a: Left bound.
        b: Right bound.
        tol: Tolerance.
        max_iter: Maximum iterations.

    Returns:
        (minimum_x, minimum_fx).
    """
    phi = (1 + Decimal(5) ** Decimal(0.5)) / 2
    inv_phi = Decimal(1) / Decimal(phi)
    x1 = a + (b - a) * inv_phi ** 2
    x2 = a + (b - a) * inv_phi
    f1, f2 = f(x1), f(x2)
    for _ in range(max_iter):
        if b - a < tol:
            break
        if f1 < f2:
            b, x2, f2 = x2, x1, f1
            x1 = a + (b - a) * inv_phi ** 2
            f1 = f(x1)
        else:
            a, x1, f1 = x1, x2, f2
            x2 = a + (b - a) * inv_phi
            f2 = f(x2)
    return ((a + b) / 2, f((a + b) / 2))


class FibonacciSearch:
    """Fibonacci search for unimodal optimization."""

    def __init__(self) -> None:
        self._fib = [1, 1]

    def _fib_sequence(self, n: int) -> list[int]:
        """Generate Fibonacci sequence up to n."""
        while self._fib[-1] < n:
            self._fib.append(self._fib[-1] + self._fib[-2])
        return self._fib

    def search(self, f: Callable[[Decimal], Decimal], a: Decimal, b: Decimal, tol: Decimal = Decimal("1e-8"), max_iter: int = 100) -> tuple[Decimal, Decimal]:
        """Fibonacci search.

        Args:
            f: Unimodal function to minimize.
            a: Left bound.
            b: Right bound.
            tol: Tolerance.
            max_iter: Maximum iterations.

        Returns:
            (minimum_x, minimum_fx).
        """
        L = b - a
        fibs = self._fib_sequence(int(L / tol))
        n = len(fibs) - 1
        rho = Decimal(1) - Decimal(fibs[n - 1]) / Decimal(fibs[n])
        x1 = a + rho * (b - a)
        x2 = b - rho * (b - a)
        f1, f2 = f(x1), f(x2)
        for _ in range(max_iter):
            if abs(b - a) < tol:
                break
            if f1 > f2:
                a = x1
                x1, f1 = x2, f2
                x2 = b - rho * (b - a)
                f2 = f(x2)
            else:
                b = x2
                x2, f2 = x1, f1
                x1 = a + rho * (b - a)
                f1 = f(x1)
        return ((a + b) / 2, f((a + b) / 2))


class DecimalOptimizer:
    """Optimization utilities with decimal precision."""

    @staticmethod
    def gradient_descent(f: Callable[[Decimal], Decimal], df: Callable[[Decimal], Decimal], x0: Decimal, alpha: Decimal = Decimal("0.1"), tol: Decimal = Decimal("1e-6"), max_iter: int = 1000) -> Decimal:
        return gradient_descent(f, df, x0, alpha, tol, max_iter)

    @staticmethod
    def newton_raphson(f: Callable[[Decimal], Decimal], df: Callable[[Decimal], Decimal], x0: Decimal, tol: Decimal = Decimal("1e-10"), max_iter: int = 50) -> Decimal | None:
        return newton_raphson(f, df, x0, tol, max_iter)

    @staticmethod
    def bisection(f: Callable[[Decimal], Decimal], a: Decimal, b: Decimal, tol: Decimal = Decimal("1e-10"), max_iter: int = 100) -> Decimal | None:
        return bisection_method(f, a, b, tol, max_iter)

    @staticmethod
    def golden_section(f: Callable[[Decimal], Decimal], a: Decimal, b: Decimal, tol: Decimal = Decimal("1e-8"), max_iter: int = 100) -> tuple[Decimal, Decimal]:
        return golden_section_search(f, a, b, tol, max_iter)


class DecimalInterpolator:
    """Interpolation utilities."""

    @staticmethod
    def lagrange(x_vals: Sequence[Decimal], y_vals: Sequence[Decimal], x: Decimal) -> Decimal:
        return lagrange_interpolate(x_vals, y_vals, x)

    @staticmethod
    def newton(x_vals: Sequence[Decimal], y_vals: Sequence[Decimal], x: Decimal) -> Decimal:
        return newton_interpolate(x_vals, y_vals, x)

    @staticmethod
    def cubic_spline(x_vals: Sequence[Decimal], y_vals: Sequence[Decimal]) -> list:
        return cubic_spline_coeffs(x_vals, y_vals)
