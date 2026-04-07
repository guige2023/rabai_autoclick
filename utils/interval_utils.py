"""
Interval arithmetic utilities.

Provides interval operations, interval intersection,
and interval-based root finding.
"""

from __future__ import annotations


class Interval:
    """Represents a closed interval [lower, upper]."""

    def __init__(self, lower: float, upper: float) -> None:
        if lower > upper:
            raise ValueError(f"Invalid interval: [{lower}, {upper}]")
        self.lower = lower
        self.upper = upper

    def __repr__(self) -> str:
        return f"[{self.lower}, {self.upper}]"

    def __contains__(self, x: float) -> bool:
        return self.lower <= x <= self.upper

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Interval):
            return False
        return self.lower == other.lower and self.upper == other.upper

    def __add__(self, other: Interval | float) -> Interval:
        if isinstance(other, Interval):
            return Interval(self.lower + other.lower, self.upper + other.upper)
        return Interval(self.lower + other, self.upper + other)

    def __sub__(self, other: Interval | float) -> Interval:
        if isinstance(other, Interval):
            return Interval(self.lower - other.upper, self.upper - other.lower)
        return Interval(self.lower - other, self.upper - other)

    def __mul__(self, other: Interval | float) -> Interval:
        if isinstance(other, Interval):
            candidates = [
                self.lower * other.lower, self.lower * other.upper,
                self.upper * other.lower, self.upper * other.upper,
            ]
            return Interval(min(candidates), max(candidates))
        return Interval(self.lower * other, self.upper * other)

    def __truediv__(self, other: Interval | float) -> Interval:
        if isinstance(other, Interval):
            if 0 in other:
                raise ZeroDivisionError("Interval contains zero")
            candidates = [
                self.lower / other.lower, self.lower / other.upper,
                self.upper / other.lower, self.upper / other.upper,
            ]
            return Interval(min(candidates), max(candidates))
        return Interval(self.lower / other, self.upper / other)

    def width(self) -> float:
        """Return the width of the interval."""
        return self.upper - self.lower

    def midpoint(self) -> float:
        """Return the midpoint of the interval."""
        return (self.lower + self.upper) / 2

    def overlaps(self, other: Interval) -> bool:
        """Check if two intervals overlap."""
        return self.lower <= other.upper and other.lower <= self.upper

    def intersection(self, other: Interval) -> Interval | None:
        """Return intersection of two intervals, or None if no overlap."""
        if not self.overlaps(other):
            return None
        return Interval(max(self.lower, other.lower), min(self.upper, other.upper))

    def union(self, other: Interval) -> list[Interval]:
        """Return union of two intervals (may be one or two intervals)."""
        if self.overlaps(other) or self.upper == other.lower or other.upper == self.lower:
            return [Interval(min(self.lower, other.lower), max(self.upper, other.upper))]
        return [self, other] if self.lower < other.lower else [other, self]


def bisect(interval: Interval) -> tuple[Interval, Interval]:
    """Split interval into two halves."""
    mid = interval.midpoint()
    return Interval(interval.lower, mid), Interval(mid, interval.upper)


def interval_function_eval(
    f: callable,
    interval: Interval,
) -> Interval:
    """
    Evaluate a function over an interval using simple bounds.

    For monotonic functions, this gives exact bounds.
    For non-monotonic, it over-approximates.
    """
    try:
        return Interval(f(interval.lower), f(interval.upper))
    except Exception:
        mid = interval.midpoint()
        vals = [f(interval.lower), f(mid), f(interval.upper)]
        return Interval(min(vals), max(vals))


def find_roots_bisection(
    f: callable,
    a: float,
    b: float,
    tol: float = 1e-8,
    max_iter: int = 100,
) -> list[float]:
    """
    Find roots using bisection method.

    Args:
        f: Function to find roots of
        a: Left endpoint
        b: Right endpoint
        tol: Convergence tolerance
        max_iter: Maximum iterations

    Returns:
        List of root values
    """
    roots = []
    stack = [(a, b)]
    while stack:
        lo, hi = stack.pop()
        f_lo, f_hi = f(lo), f(hi)
        if f_lo * f_hi > 0:
            continue
        if hi - lo < tol:
            roots.append((lo + hi) / 2)
            continue
        mid = (lo + hi) / 2
        f_mid = f(mid)
        if abs(f_mid) < tol:
            roots.append(mid)
        else:
            if f_lo * f_mid < 0:
                stack.append((lo, mid))
            if f_mid * f_hi < 0:
                stack.append((mid, hi))
    return roots
