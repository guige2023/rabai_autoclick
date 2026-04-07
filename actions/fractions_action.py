"""Fractions action for rabai_autoclick.

Provides utilities for working with Python fractions module,
including arithmetic operations, conversions, and formatting.
"""

from __future__ import annotations

from fractions import Fraction
from typing import Iterable, Optional, Sequence, Union

__all__ = [
    "Fraction",
    "make_fraction",
    "fraction_from_float",
    "fraction_from_decimal",
    "fraction_simplify",
    "fraction_add",
    "fraction_sub",
    "fraction_mul",
    "fraction_div",
    "fraction_power",
    "fraction_to_float",
    "fraction_to_int",
    "fraction_to_string",
    "fraction_to_latex",
    "fraction_to_mixed",
    "fraction_from_string",
    "fraction_range",
    "fraction_approx",
    "continued_fraction",
    "best_fraction",
    "fraction_list",
    "sum_fractions",
    "product_fractions",
    "fraction_gcd",
    "fraction_lcm",
    "is_fraction",
    "FractionStats",
]


def make_fraction(numerator: int | Fraction, denominator: int | None = None) -> Fraction:
    """Create a Fraction from numerator/denominator or from another value.

    Args:
        numerator: Numerator or a value to convert.
        denominator: Denominator if numerator is int.

    Returns:
        Simplified Fraction.

    Raises:
        ZeroDivisionError: If denominator is 0.
        TypeError: If inputs are invalid types.
    """
    if denominator is None:
        if isinstance(numerator, Fraction):
            return Fraction(numerator.numerator, numerator.denominator)
        return Fraction(numerator)
    return Fraction(int(numerator), int(denominator))


def fraction_from_float(value: float, tolerance: float = 1e-10, max_denominator: int = 1000000) -> Fraction:
    """Convert a float to the nearest Fraction within tolerance.

    Args:
        value: Float value to convert.
        tolerance: Maximum difference allowed.
        max_denominator: Maximum denominator to consider.

    Returns:
        Approximate Fraction.
    """
    if not isinstance(value, float):
        raise TypeError(f"Expected float, got {type(value).__name__}")
    result = Fraction(value).limit_denominator(max_denominator)
    if abs(result - value) > tolerance:
        raise ValueError(f"Cannot approximate {value} within tolerance {tolerance}")
    return result


def fraction_from_decimal(value: Union[Decimal, float], precision: int = 28) -> Fraction:
    """Convert a Decimal or float to Fraction.

    Args:
        value: Decimal or float value.
        precision: Precision for Decimal conversion.

    Returns:
        Exact Fraction representation.
    """
    from decimal import Decimal
    if isinstance(value, Decimal):
        return Fraction(value)
    return Fraction(str(value))


def fraction_simplify(frac: Fraction | tuple[int, int]) -> Fraction:
    """Simplify a fraction to lowest terms.

    Args:
        frac: Fraction or (numerator, denominator) tuple.

    Returns:
        Simplified Fraction.
    """
    if isinstance(frac, tuple):
        frac = Fraction(*frac)
    return Fraction(frac.numerator, frac.denominator)


def fraction_add(a: Fraction | int, b: Fraction | int) -> Fraction:
    """Add two fractions or a fraction and an integer.

    Args:
        a: First fraction or integer.
        b: Second fraction or integer.

    Returns:
        Sum as simplified Fraction.
    """
    return Fraction(a) + Fraction(b)


def fraction_sub(a: Fraction | int, b: Fraction | int) -> Fraction:
    """Subtract second fraction from first.

    Args:
        a: First fraction or integer.
        b: Second fraction or integer.

    Returns:
        Difference as simplified Fraction.
    """
    return Fraction(a) - Fraction(b)


def fraction_mul(a: Fraction | int, b: Fraction | int) -> Fraction:
    """Multiply two fractions or a fraction and an integer.

    Args:
        a: First fraction or integer.
        b: Second fraction or integer.

    Returns:
        Product as simplified Fraction.
    """
    return Fraction(a) * Fraction(b)


def fraction_div(a: Fraction | int, b: Fraction | int) -> Fraction:
    """Divide first fraction by second.

    Args:
        a: First fraction or integer (dividend).
        b: Second fraction or integer (divisor).

    Returns:
        Quotient as simplified Fraction.

    Raises:
        ZeroDivisionError: If b is 0.
    """
    return Fraction(a) / Fraction(b)


def fraction_power(frac: Fraction | int, exponent: int | float) -> Fraction | float:
    """Raise a fraction to a power.

    Args:
        frac: Fraction or integer base.
        exponent: Integer or float exponent.

    Returns:
        Fraction if exponent is integer, float if negative or float.
    """
    result = Fraction(frac) ** exponent
    if isinstance(result, float):
        return result
    return result


def fraction_to_float(frac: Fraction | int) -> float:
    """Convert fraction to float.

    Args:
        frac: Fraction or integer.

    Returns:
        Float representation.
    """
    return float(Fraction(frac))


def fraction_to_int(frac: Fraction) -> int:
    """Convert fraction to integer (truncating toward zero).

    Args:
        frac: Fraction to convert.

    Returns:
        Integer part of fraction.
    """
    if not isinstance(frac, Fraction):
        raise TypeError(f"Expected Fraction, got {type(frac).__name__}")
    return int(frac)


def fraction_to_string(frac: Fraction | int, simplify: bool = True) -> str:
    """Convert fraction to string representation.

    Args:
        frac: Fraction or integer.
        simplify: Whether to simplify before converting.

    Returns:
        String like '3/4' or '-5/7'.
    """
    f = Fraction(frac)
    if simplify:
        f = Fraction(f.numerator, f.denominator)
    return f"{f.numerator}/{f.denominator}"


def fraction_to_latex(frac: Fraction | int) -> str:
    """Convert fraction to LaTeX representation.

    Args:
        frac: Fraction or integer.

    Returns:
        LaTeX string like '\\frac{3}{4}'.
    """
    f = Fraction(frac)
    return rf"\frac{{{f.numerator}}}{{{f.denominator}}}"


def fraction_to_mixed(frac: Fraction | int) -> tuple[int, Fraction]:
    """Convert fraction to mixed number (whole part + proper fraction).

    Args:
        frac: Fraction or integer.

    Returns:
        Tuple of (whole_part, proper_fraction).
    """
    f = Fraction(frac)
    if f.numerator >= f.denominator:
        whole = f.numerator // f.denominator
        remainder = f.numerator % f.denominator
        return (whole, Fraction(remainder, f.denominator) if remainder else Fraction(0))
    return (0, f)


def fraction_from_string(s: str) -> Fraction:
    """Parse a string into a Fraction.

    Args:
        s: String like '3/4', '-5/7', '1.5', '2', or '1/2 + 3/4'.

    Returns:
        Parsed Fraction.

    Raises:
        ValueError: If string cannot be parsed.
    """
    s = s.strip()
    if "/" in s:
        parts = s.split("/")
        if len(parts) != 2:
            raise ValueError(f"Invalid fraction format: {s}")
        return Fraction(int(parts[0].strip()), int(parts[1].strip()))
    if "." in s:
        return Fraction(s)
    return Fraction(int(s))


def fraction_range(start: Fraction | float, stop: Fraction | float, step: Fraction | float | None = None) -> list[Fraction]:
    """Generate a range of fractions.

    Args:
        start: Start value (inclusive).
        stop: Stop value (exclusive for positive step).
        step: Step size (default 1).

    Returns:
        List of Fractions from start to stop.
    """
    if step is None:
        step = Fraction(1)
    start_f = Fraction(start)
    stop_f = Fraction(stop)
    step_f = Fraction(step)
    result = []
    current = start_f
    if step_f > 0:
        while current < stop_f:
            result.append(current)
            current += step_f
    else:
        while current > stop_f:
            result.append(current)
            current += step_f
    return result


def fraction_approx(value: float, max_denominator: int = 100) -> Fraction:
    """Find best fraction approximation of a float.

    Args:
        value: Float to approximate.
        max_denominator: Maximum denominator allowed.

    Returns:
        Best approximating Fraction.
    """
    if not isinstance(value, float):
        raise TypeError(f"Expected float, got {type(value).__name__}")
    return Fraction(value).limit_denominator(max_denominator)


def continued_fraction(value: float | Fraction, max_terms: int = 20) -> list[int]:
    """Compute continued fraction expansion of a value.

    Args:
        value: Float or Fraction to expand.
        max_terms: Maximum number of terms.

    Returns:
        List of continued fraction terms.
    """
    from math import floor, isnan, isinf
    f = Fraction(value) if isinstance(value, float) else value
    terms = []
    for _ in range(max_terms):
        if f.numerator == 1 and f.denominator == 1:
            break
        terms.append(int(floor(float(f))))
        f = f - floor(float(f))
        if f == 0:
            break
        f = Fraction(f.denominator, f.numerator)
        if isnan(float(f)) or isinf(float(f)):
            break
    return terms


def best_fraction(value: float, max_denominator: int = 1000) -> Fraction:
    """Find the best fraction approximation using continued fractions.

    Args:
        value: Float to approximate.
        max_denominator: Maximum denominator allowed.

    Returns:
        Best approximating Fraction.
    """
    terms = continued_fraction(value)
    frac = Fraction(0)
    for term in reversed(terms):
        frac = Fraction(term) + Fraction(1, frac) if frac else Fraction(term)
        if frac.denominator > max_denominator:
            frac = Fraction(value).limit_denominator(max_denominator)
            break
    return frac


def fraction_list(numerator: int, denominator: int) -> list[Fraction]:
    """Generate all fractions between 0 and 1 with given denominator.

    Args:
        numerator: Maximum numerator.
        denominator: Common denominator.

    Returns:
        Sorted list of Fractions.
    """
    return sorted(Fraction(i, denominator) for i in range(int(numerator) + 1))


def sum_fractions(fractions: Iterable[Fraction | int]) -> Fraction:
    """Sum an iterable of fractions.

    Args:
        fractions: Iterable of fractions or integers.

    Returns:
        Sum as simplified Fraction.
    """
    total = Fraction(0)
    for f in fractions:
        total += Fraction(f)
    return total


def product_fractions(fractions: Iterable[Fraction | int]) -> Fraction:
    """Multiply an iterable of fractions.

    Args:
        fractions: Iterable of fractions or integers.

    Returns:
        Product as simplified Fraction.
    """
    total = Fraction(1)
    for f in fractions:
        total *= Fraction(f)
    return total


def fraction_gcd(a: Fraction | int, b: Fraction | int) -> Fraction:
    """Compute GCD of two fractions.

    Args:
        a: First fraction.
        b: Second fraction.

    Returns:
        GCD as Fraction.
    """
    from math import gcd
    a_f = Fraction(a)
    b_f = Fraction(b)
    return Fraction(gcd(a_f.numerator, b_f.numerator), max(a_f.denominator, b_f.denominator))


def fraction_lcm(a: Fraction | int, b: Fraction | int) -> Fraction:
    """Compute LCM of two fractions.

    Args:
        a: First fraction.
        b: Second fraction.

    Returns:
        LCM as Fraction.
    """
    return Fraction(a) * Fraction(b) / fraction_gcd(a, b)


def is_fraction(value: Any) -> bool:
    """Check if value is a Fraction.

    Args:
        value: Value to check.

    Returns:
        True if value is a Fraction instance.
    """
    return isinstance(value, Fraction)


class FractionStats:
    """Statistics collector for Fraction values."""

    def __init__(self) -> None:
        self._values: list[Fraction] = []
        self._sum = Fraction(0)
        self._sum_sq = Fraction(0)

    def add(self, value: Fraction | int | float) -> None:
        """Add a value to the statistics.

        Args:
            value: Fraction or numeric value to add.
        """
        f = Fraction(value)
        self._values.append(f)
        self._sum += f
        self._sum_sq += f * f

    def mean(self) -> Fraction:
        """Get the mean of all values."""
        if not self._values:
            raise ValueError("No values added")
        return self._sum / len(self._values)

    def total(self) -> Fraction:
        """Get the sum of all values."""
        return self._sum

    def count(self) -> int:
        """Get the count of values."""
        return len(self._values)
