"""
Polynomial utilities v2 — extended operations and advanced algorithms.

Companion to polynomial_utils.py. Adds polynomial arithmetic, root finding,
interpolation, and special polynomials.
"""

from __future__ import annotations

import math
from typing import Iterator, NamedTuple


Coefficients = list[float | int]


class Polynomial:
    """Represents a polynomial with coefficient array."""

    def __init__(self, coeffs: Coefficients) -> None:
        """Initialize with coefficient array (highest degree first)."""
        self.coeffs = list(coeffs)
        while len(self.coeffs) > 1 and self.coeffs[0] == 0:
            self.coeffs.pop(0)

    def degree(self) -> int:
        """Return the degree of the polynomial."""
        return len(self.coeffs) - 1

    def evaluate(self, x: float) -> float:
        """Evaluate polynomial at x using Horner's method."""
        result = 0.0
        for c in self.coeffs:
            result = result * x + c
        return result

    def __add__(self, other: Polynomial) -> Polynomial:
        """Add two polynomials."""
        n, m = len(self.coeffs), len(other.coeffs)
        res = [0.0] * max(n, m)
        for i in range(n):
            res[i + m - n] += self.coeffs[i]
        for i in range(m):
            res[i + n - m] += other.coeffs[i]
        return Polynomial([int(x) if x == int(x) else x for x in res])

    def __mul__(self, other: Polynomial) -> Polynomial:
        """Multiply two polynomials."""
        if not self.coeffs or not other.coeffs:
            return Polynomial([0])
        res = [0.0] * (len(self.coeffs) + len(other.coeffs) - 1)
        for i, a in enumerate(self.coeffs):
            for j, b in enumerate(other.coeffs):
                res[i + j] += a * b
        return Polynomial([int(x) if x == int(x) else x for x in res])

    def __repr__(self) -> str:
        return f"Polynomial({self.coeffs})"


def poly_add(a: Coefficients, b: Coefficients) -> Coefficients:
    """Add two polynomials (coeff arrays)."""
    n, m = len(a), len(b)
    res = [0.0] * max(n, m)
    for i in range(n):
        res[i + m - n] += a[i]
    for i in range(m):
        res[i + n - m] += b[i]
    return [int(x) if x == int(x) else x for x in res]


def poly_sub(a: Coefficients, b: Coefficients) -> Coefficients:
    """Subtract polynomial b from a."""
    n, m = len(a), len(b)
    res = [0.0] * max(n, m)
    for i in range(n):
        res[i + m - n] += a[i]
    for i in range(m):
        res[i + n - m] -= b[i]
    return [int(x) if x == int(x) else x for x in res]


def poly_mul(a: Coefficients, b: Coefficients) -> Coefficients:
    """Multiply two polynomials."""
    if not a or not b:
        return [0]
    res = [0.0] * (len(a) + len(b) - 1)
    for i, ca in enumerate(a):
        for j, cb in enumerate(b):
            res[i + j] += ca * cb
    return [int(x) if x == int(x) else x for x in res]


def poly_divmod(num: Coefficients, den: Coefficients) -> tuple[Coefficients, Coefficients]:
    """Polynomial long division. Returns (quotient, remainder)."""
    if len(num) < len(den):
        return [0], num[:]
    quotient = [0.0] * (len(num) - len(den) + 1)
    remainder = num[:]
    for i in range(len(quotient) - 1, -1, -1):
        if remainder[i + len(den) - 1] != 0:
            coeff = remainder[i + len(den) - 1] / den[0]
            quotient[i] = coeff
            for j in range(len(den)):
                remainder[i + j] -= coeff * den[j]
    return [int(x) if x == int(x) else x for x in quotient], [int(x) if x == int(x) else x for x in remainder]


def poly_gcd(a: Coefficients, b: Coefficients) -> Coefficients:
    """Compute GCD of two polynomials using Euclidean algorithm."""
    while b:
        _, rem = poly_divmod(a, b)
        a, b = b, [x for x in rem if abs(x) > 1e-12]
    if a and a[0] != 0:
        lead = a[0]
        a = [x / lead for x in a]
    return [int(x) if abs(x - int(x)) < 1e-12 else x for x in a]


def horner_eval(coeffs: Coefficients, x: float) -> float:
    """Evaluate polynomial at x using Horner's method."""
    result = 0.0
    for c in coeffs:
        result = result * x + c
    return result


def poly_derivative(coeffs: Coefficients) -> Coefficients:
    """Compute the derivative of a polynomial."""
    if len(coeffs) < 2:
        return [0]
    return [c * (len(coeffs) - 1 - i) for i, c in enumerate(coeffs[:-1])]


def poly_integrate(coeffs: Coefficients, const: float = 0.0) -> Coefficients:
    """Compute indefinite integral of a polynomial."""
    return [const] + [c / (len(coeffs) - i) for i, c in enumerate(coeffs)]


def lagrange_interpolate(x_vals: list[float], y_vals: list[float]) -> Polynomial:
    """Build Lagrange interpolation polynomial."""
    n = len(x_vals)
    result_coeffs = [0.0] * n
    for i in range(n):
        numerator = [1.0]
        denominator = 1.0
        for j in range(n):
            if i != j:
                numerator = poly_mul(numerator, [-x_vals[j], 1.0])
                denominator *= x_vals[i] - x_vals[j]
        term = [y_vals[i] * c / denominator for c in numerator]
        for j, c in enumerate(term):
            result_coeffs[j] += c
    return Polynomial([int(x) if abs(x - int(x)) < 1e-12 else x for x in result_coeffs])


def newton_interpolate(x_vals: list[float], y_vals: list[float]) -> Polynomial:
    """Newton's divided differences interpolation."""
    n = len(x_vals)
    divided = y_vals[:]
    for i in range(1, n):
        for j in range(n - 1, i - 1, -1):
            divided[j] = (divided[j] - divided[j - 1]) / (x_vals[j] - x_vals[j - i])
    coeffs = [divided[0]]
    basis = [1.0]
    for i in range(1, n):
        basis = poly_mul(basis, [-x_vals[i - 1], 1.0])
        coeffs = poly_add(coeffs, [divided[i] * c for c in basis])
    return Polynomial([int(x) if abs(x - int(x)) < 1e-12 else x for x in coeffs])


def laguerre_poly(n: int) -> Polynomial:
    """Generate Laguerre polynomial L_n(x)."""
    if n == 0:
        return Polynomial([1])
    if n == 1:
        return Polynomial([1, -1])
    l_prev = Polynomial([1])
    l_curr = Polynomial([1, -1])
    for _ in range(2, n + 1):
        l_next = Polynomial(poly_sub(
            poly_mul([1, -1], l_curr.coeffs),
            poly_mul([(n - 1) * (n - 1), 0, 1], l_prev.coeffs)[:n + 1]
        ))
        l_prev, l_curr = l_curr, l_next
    return l_curr


def hermite_poly(n: int) -> Polynomial:
    """Generate Hermite polynomial H_n(x)."""
    if n == 0:
        return Polynomial([1])
    if n == 1:
        return Polynomial([2, 0])
    h_prev = Polynomial([1])
    h_curr = Polynomial([2, 0])
    for _ in range(2, n + 1):
        h_next = Polynomial(poly_sub(
            poly_mul([2, 0], h_curr.coeffs),
            poly_mul([2 * (_ - 1), 0, 0], h_prev.coeffs)[:n + 1]
        ))
        h_prev, h_curr = h_curr, h_next
    return h_curr
