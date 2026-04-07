"""
Fraction utilities for rational number arithmetic.

Provides Fraction class with arithmetic operations,
comparison, and conversion utilities.
"""

from __future__ import annotations

import math
from typing import NamedTuple


class Fraction:
    """
    Immutable rational number representation.

    Attributes:
        numerator: Integer numerator
        denominator: Positive integer denominator
    """

    __slots__ = ("numerator", "denominator")

    def __init__(self, numerator: int = 0, denominator: int = 1) -> None:
        if denominator == 0:
            raise ZeroDivisionError("Denominator cannot be zero")
        if denominator < 0:
            numerator = -numerator
            denominator = -denominator
        g = math.gcd(abs(numerator), denominator)
        self.numerator = numerator // g
        self.denominator = denominator // g

    @classmethod
    def from_float(cls, x: float, tol: float = 1e-12) -> Fraction:
        """
        Convert float to Fraction by continued fraction approximation.

        Args:
            x: Float to convert
            tol: Convergence tolerance
        """
        if x == int(x):
            return cls(int(x))
        sign = -1 if x < 0 else 1
        x = abs(x)
        a = int(x)
        frac = Fraction(sign * a)
        h1, h2 = a + 1, 1
        k1, k2 = 1, 0
        x -= a
        for _ in range(100):
            if x < tol:
                break
            x = 1 / x
            a = int(x)
            x -= a
            h1, h2 = a * h1 + h2, h1
            k1, k2 = a * k1 + k2, k1
        return cls(sign * (a * h1 + h2), k1)

    def __repr__(self) -> str:
        if self.denominator == 1:
            return str(self.numerator)
        return f"{self.numerator}/{self.denominator}"

    def __float__(self) -> float:
        return self.numerator / self.denominator

    def __int__(self) -> int:
        return int(self.numerator / self.denominator)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Fraction):
            return self.numerator == other.numerator and self.denominator == other.denominator
        if isinstance(other, int):
            return self.denominator == 1 and self.numerator == other
        if isinstance(other, float):
            return abs(float(self) - other) < 1e-12
        return False

    def __lt__(self, other: object) -> bool:
        if isinstance(other, Fraction):
            return self.numerator * other.denominator < other.numerator * self.denominator
        if isinstance(other, (int, float)):
            return float(self) < other
        return NotImplemented

    def __le__(self, other: object) -> bool:
        return self == other or self < other

    def __gt__(self, other: object) -> bool:
        if isinstance(other, Fraction):
            return self.numerator * other.denominator > other.numerator * self.denominator
        if isinstance(other, (int, float)):
            return float(self) > other
        return NotImplemented

    def __ge__(self, other: object) -> bool:
        return self == other or self > other

    def __hash__(self) -> int:
        return hash((self.numerator, self.denominator))

    def __add__(self, other: Fraction | int) -> Fraction:
        if isinstance(other, int):
            return Fraction(self.numerator + other * self.denominator, self.denominator)
        if isinstance(other, Fraction):
            return Fraction(
                self.numerator * other.denominator + other.numerator * self.denominator,
                self.denominator * other.denominator,
            )
        return NotImplemented

    def __radd__(self, other: int) -> Fraction:
        return self + other

    def __sub__(self, other: Fraction | int) -> Fraction:
        if isinstance(other, int):
            return Fraction(self.numerator - other * self.denominator, self.denominator)
        if isinstance(other, Fraction):
            return Fraction(
                self.numerator * other.denominator - other.numerator * self.denominator,
                self.denominator * other.denominator,
            )
        return NotImplemented

    def __rsub__(self, other: int) -> Fraction:
        return Fraction(other * self.denominator - self.numerator, self.denominator)

    def __mul__(self, other: Fraction | int) -> Fraction:
        if isinstance(other, int):
            return Fraction(self.numerator * other, self.denominator)
        if isinstance(other, Fraction):
            return Fraction(self.numerator * other.numerator, self.denominator * other.denominator)
        return NotImplemented

    def __rmul__(self, other: int) -> Fraction:
        return self * other

    def __truediv__(self, other: Fraction | int) -> Fraction:
        if isinstance(other, int):
            return Fraction(self.numerator, self.denominator * other)
        if isinstance(other, Fraction):
            return Fraction(self.numerator * other.denominator, self.denominator * other.numerator)
        return NotImplemented

    def __rtruediv__(self, other: int) -> Fraction:
        return Fraction(other * self.denominator, self.numerator)

    def __neg__(self) -> Fraction:
        return Fraction(-self.numerator, self.denominator)

    def __abs__(self) -> Fraction:
        return Fraction(abs(self.numerator), self.denominator)

    def __pow__(self, n: int) -> Fraction:
        if n < 0:
            return Fraction(self.denominator ** -n, self.numerator ** -n)
        return Fraction(self.numerator ** n, self.denominator ** n)

    def is_integer(self) -> bool:
        return self.denominator == 1

    def is_proper(self) -> bool:
        return abs(self.numerator) < self.denominator

    def mixed(self) -> tuple[int, Fraction]:
        """Return as mixed number (whole, fraction)."""
        whole = self.numerator // self.denominator
        frac = Fraction(self.numerator % self.denominator, self.denominator)
        return whole, frac

    def limit_denominator(self, max_d: int) -> Fraction:
        """
        Approximate with fraction having denominator <= max_d.

        Uses continued fraction approximation.
        """
        if self.denominator <= max_d:
            return self
        p0, p1 = 0, 1
        q0, q1 = 1, 0
        a0 = self.numerator // self.denominator
        frac = Fraction(self.numerator - a0 * self.denominator, self.denominator)
        for _ in range(max_d):
            if frac.denominator == 0:
                break
            a = frac.numerator // frac.denominator
            p2 = a * p1 + p0
            q2 = a * q1 + q0
            if q2 > max_d:
                break
            p0, p1 = p1, p2
            q0, q1 = q1, q2
            frac = Fraction(frac.numerator - a * frac.denominator, frac.denominator)
        return Fraction(a0 * q1 + p0, q1)
