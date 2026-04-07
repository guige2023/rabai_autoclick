"""Polynomial operations: evaluation, arithmetic, root finding, and interpolation."""

from __future__ import annotations

import math
from typing import Callable, List, Tuple, Optional, Any


class Polynomial:
    """Immutable polynomial with coefficient array (highest degree first)."""

    __slots__ = ("_coeffs",)

    def __init__(self, coeffs: List[float]) -> None:
        """Create polynomial from coefficient list (c[0] + c[1]*x + c[2]*x^2 + ...).

        Args:
            coeffs: Coefficient list, lowest to highest degree.
        """
        # Remove trailing zeros for a canonical form
        self._coeffs = _trim_coeffs(list(coeffs))

    @classmethod
    def from_roots(cls, roots: List[float], scale: float = 1.0) -> Polynomial:
        """Build monic polynomial from roots: scale * prod(x - r_i)."""
        result: List[float] = [1.0]
        for r in roots:
            result = _poly_mul(result, [-r, 1.0])
        if scale != 1.0:
            result = [c * scale for c in result]
        return cls(result)

    @property
    def degree(self) -> int:
        return len(self._coeffs) - 1

    @property
    def coeffs(self) -> List[float]:
        return list(self._coeffs)

    def __repr__(self) -> str:
        terms = []
        for i, c in enumerate(self._coeffs):
            if abs(c) < 1e-12:
                continue
            if i == 0:
                terms.append(f"{c:g}")
            elif i == 1:
                terms.append(f"{c:g}*x")
            else:
                terms.append(f"{c:g}*x^{i}")
        return " + ".join(terms) if terms else "0"

    def __call__(self, x: float) -> float:
        """Evaluate polynomial at x using Horner's method."""
        result = 0.0
        for c in reversed(self._coeffs):
            result = result * x + c
        return result

    def __add__(self, other: Polynomial) -> Polynomial:
        return Polynomial(_poly_add(self._coeffs, other._coeffs))

    def __sub__(self, other: Polynomial) -> Polynomial:
        return Polynomial(_poly_sub(self._coeffs, other._coeffs))

    def __mul__(self, other: Polynomial) -> Polynomial:
        return Polynomial(_poly_mul(self._coeffs, other._coeffs))

    def derivative(self) -> Polynomial:
        """Return the first derivative."""
        if self.degree == 0:
            return Polynomial([0.0])
        new_coeffs = [c * i for i, c in enumerate(self._coeffs) if i > 0]
        return Polynomial(new_coeffs)

    def integral(self, const: float = 0.0) -> Polynomial:
        """Return the indefinite integral with given constant term."""
        new_coeffs = [const] + [c / (i + 1) for i, c in enumerate(self._coeffs)]
        return Polynomial(new_coeffs)

    def roots(self, eps: float = 1e-12) -> List[float]:
        """Find all real roots using Newton-Raphson on factored form or Durand-Kerner."""
        if self.degree == 0:
            return []
        if self.degree == 1:
            return [-self._coeffs[0] / self._coeffs[1]]
        return _durand_kerner(self._coeffs, eps)

    def definite_integral(self, a: float, b: float) -> float:
        """Compute ∫_a^b P(x) dx."""
        antideriv = self.integral()
        return antideriv(b) - antideriv(a)


def _trim_coeffs(coeffs: List[float]) -> List[float]:
    while len(coeffs) > 1 and abs(coeffs[-1]) < 1e-14:
        coeffs.pop()
    return coeffs


def _poly_add(a: List[float], b: List[float]) -> List[float]:
    n = max(len(a), len(b))
    result = [0.0] * n
    for i in range(n):
        if i < len(a):
            result[i] += a[i]
        if i < len(b):
            result[i] += b[i]
    return _trim_coeffs(result)


def _poly_sub(a: List[float], b: List[float]) -> List[float]:
    n = max(len(a), len(b))
    result = [0.0] * n
    for i in range(n):
        if i < len(a):
            result[i] += a[i]
        if i < len(b):
            result[i] -= b[i]
    return _trim_coeffs(result)


def _poly_mul(a: List[float], b: List[float]) -> List[float]:
    result = [0.0] * (len(a) + len(b) - 1)
    for i, ca in enumerate(a):
        for j, cb in enumerate(b):
            result[i + j] += ca * cb
    return _trim_coeffs(result)


def _durand_kerner(coeffs: List[float], eps: float, max_iter: int = 100) -> List[float]:
    """Durand-Kerner method for finding polynomial roots."""
    n = len(coeffs) - 1
    if n <= 0:
        return []
    # Initial guesses: roots of unity scaled
    roots = [complex(math.cos(2 * math.pi * k / n), math.sin(2 * math.pi * k / n)) for k in range(n)]
    for _ in range(max_iter):
        max_delta = 0.0
        for i in range(n):
            p_val = _poly_eval_complex(coeffs, roots[i])
            denom = 1.0
            for j in range(n):
                if i != j:
                    denom *= roots[i] - roots[j]
            if abs(denom) < 1e-30:
                continue
            delta = p_val / denom
            roots[i] -= delta
            max_delta = max(max_delta, abs(delta))
        if max_delta < eps:
            break
    # Return real roots only (imaginary part < eps)
    real_roots = []
    seen = set()
    for r in roots:
        if abs(r.imag) < eps and r.real not in seen:
            seen.add(r.real)
            real_roots.append(r.real)
    return real_roots


def _poly_eval_complex(coeffs: List[float], x: complex) -> complex:
    result = 0.0 + 0.0j
    for c in reversed(coeffs):
        result = result * x + c
    return result


def lagrange_interpolate(xs: List[float], ys: List[float]) -> Callable[[float], float]:
    """Return a Lagrange interpolation polynomial as a callable."""
    n = len(xs)

    def _interp(x: float) -> float:
        total = 0.0
        for i in range(n):
            term = ys[i]
            for j in range(n):
                if i != j:
                    term *= (x - xs[j]) / (xs[i] - xs[j])
            total += term
        return total

    return _interp


def polynomial_fit(xs: List[float], ys: List[float], degree: int) -> Polynomial:
    """Least-squares fit of a polynomial of given degree to data points."""
    n = len(xs)
    m = degree + 1
    # Build Vandermonde-like matrix
    A = [[xi ** j for j in range(m)] for xi in xs]
    # Normal equations: A^T A c = A^T y
    ATA = [[0.0] * m for _ in range(m)]
    ATy = [0.0] * m
    for i in range(n):
        for j in range(m):
            ATy[j] += A[i][j] * ys[i]
            for k in range(m):
                ATA[j][k] += A[i][j] * A[i][k]
    # Gaussian elimination
    coeffs = _solve_linear(ATA, ATy)
    return Polynomial(coeffs)


def _solve_linear(A: List[List[float]], b: List[float]) -> List[float]:
    """Solve Ax = b via Gaussian elimination with partial pivoting."""
    n = len(b)
    aug = [row[:] + [b[i]] for i, row in enumerate(A)]
    for col in range(n):
        # Find pivot
        max_row = max(range(col, n), key=lambda r: abs(aug[r][col]))
        aug[col], aug[max_row] = aug[max_row], aug[col]
        pivot = aug[col][col]
        if abs(pivot) < 1e-14:
            continue
        for row in range(col + 1, n):
            factor = aug[row][col] / pivot
            for j in range(col, n + 1):
                aug[row][j] -= factor * aug[col][j]
    # Back substitution
    x = [0.0] * n
    for i in reversed(range(n)):
        x[i] = aug[i][n]
        for j in range(i + 1, n):
            x[i] -= aug[i][j] * x[j]
        x[i] /= aug[i][i] if abs(aug[i][i]) > 1e-14 else 1.0
    return x
