"""Fractions action v3 - music, physics, and engineering utilities.

Fraction utilities for music intervals, physical units,
and engineering approximations.
"""

from __future__ import annotations

from fractions import Fraction
from typing import Sequence

__all__ = [
    "equal_temperament_ratio",
    "just_intonation_ratio",
    "pythagorean_ratio",
    "cents_to_ratio",
    "ratio_to_cents",
    "interval_name",
    "complement_interval",
    "fraction_pythagorean_triple",
    "primitive_triple",
    "fraction_approximation_error",
    "best_approximation_chain",
    "medial_fraction",
    "subtract_unit_fraction",
    "split_fraction",
    "FractionMusic",
    "FractionPhysics",
    "FractionEngineering",
]


def equal_temperament_ratio(semitones: int | float, base: float = 440.0) -> tuple[Fraction, float]:
    """Equal temperament ratio and frequency.

    Args:
        semitones: Number of semitones from base.
        base: Base frequency in Hz.

    Returns:
        Tuple of (ratio, frequency).
    """
    ratio = Fraction(2 ** (semitones / 12)).limit_denominator(10000)
    freq = base * float(ratio)
    return (ratio, freq)


def just_intonation_ratio(interval_name: str) -> Fraction | None:
    """Get just intonation ratio for named interval.

    Args:
        interval_name: e.g., 'octave', 'fifth', 'third', 'fourth'.

    Returns:
        Just intonation ratio.
    """
    ratios = {
        "unison": Fraction(1, 1),
        "second": Fraction(9, 8),
        "third": Fraction(5, 4),
        "fourth": Fraction(4, 3),
        "fifth": Fraction(3, 2),
        "sixth": Fraction(5, 3),
        "seventh": Fraction(15, 8),
        "octave": Fraction(2, 1),
    }
    return ratios.get(interval_name.lower())


def pythagorean_ratio(interval_name: str) -> Fraction | None:
    """Get Pythagorean tuning ratio.

    Args:
        interval_name: Interval name.

    Returns:
        Pythagorean ratio.
    """
    pyth = {
        "unison": Fraction(1, 1),
        "second": Fraction(9, 8),
        "third": Fraction(81, 64),
        "fourth": Fraction(4, 3),
        "fifth": Fraction(3, 2),
        "sixth": Fraction(27, 16),
        "seventh": Fraction(243, 128),
        "octave": Fraction(2, 1),
    }
    return pyth.get(interval_name.lower())


def cents_to_ratio(cents: float) -> Fraction:
    """Convert cents to frequency ratio.

    Args:
        cents: Cents value.

    Returns:
        Frequency ratio as Fraction.
    """
    ratio = 2 ** (cents / 1200)
    return Fraction(ratio).limit_denominator(10000)


def ratio_to_cents(ratio: Fraction | float) -> float:
    """Convert frequency ratio to cents.

    Args:
        ratio: Frequency ratio.

    Returns:
        Cents value.
    """
    import math
    return 1200 * math.log2(float(ratio))


def interval_name(ratio: Fraction) -> str:
    """Approximate interval name from ratio.

    Args:
        ratio: Frequency ratio.

    Returns:
        Interval name.
    """
    ratio = Fraction(ratio.numerator, ratio.denominator)
    intervals = [
        (Fraction(2, 1), "octave"),
        (Fraction(3, 2), "perfect fifth"),
        (Fraction(4, 3), "perfect fourth"),
        (Fraction(5, 4), "major third"),
        (Fraction(6, 5), "minor third"),
        (Fraction(9, 8), "major second"),
        (Fraction(15, 8), "major seventh"),
        (Fraction(1, 1), "unison"),
    ]
    for r, name in intervals:
        if abs(float(ratio) - float(r)) < 0.01:
            return name
    return f"{ratio.numerator}/{ratio.denominator}"


def complement_interval(frac: Fraction) -> Fraction:
    """Get complement interval (to octave).

    Args:
        frac: Interval ratio.

    Returns:
        Complement ratio.
    """
    return Fraction(2, 1) / frac


def fraction_pythagorean_triple(a: int, b: int, c: int) -> tuple[int, int, int]:
    """Check if (a, b, c) is a Pythagorean triple.

    Args:
        a, b, c: Triple to check.

    Returns:
        Normalized triple or raises ValueError.
    """
    if a * a + b * b != c * c:
        raise ValueError(f"({a}, {b}, {c}) is not a Pythagorean triple")
    return (a, b, c)


def primitive_triple(m: int, n: int) -> tuple[int, int, int]:
    """Generate primitive Pythagorean triple from m > n > 0.

    Args:
        m: Parameter m.
        n: Parameter n.

    Returns:
        (a, b, c) primitive triple.
    """
    if m <= n or m < 1 or n < 1:
        raise ValueError("Require m > n > 0")
    a = m * m - n * n
    b = 2 * m * n
    c = m * m + n * n
    return (a, b, c)


def fraction_approximation_error(value: float, frac: Fraction) -> float:
    """Compute relative error of approximation.

    Args:
        value: True value.
        frac: Approximation.

    Returns:
        Relative error.
    """
    return abs(float(frac) - value) / abs(value)


def best_approximation_chain(value: float, max_denominator: int = 1000) -> list[tuple[Fraction, float]]:
    """Generate chain of best approximations.

    Args:
        value: Target value.
        max_denominator: Maximum denominator.

    Returns:
        List of (fraction, error) tuples.
    """
    results = []
    for d in range(1, max_denominator + 1):
        n = round(value * d)
        frac = Fraction(n, d)
        error = fraction_approximation_error(value, frac)
        results.append((frac, error))
    results.sort(key=lambda x: x[1])
    return results[:10]


def medial_fraction(a: Fraction, b: Fraction) -> Fraction:
    """Compute medial fraction (mediant of two fractions).

    Args:
        a: First fraction.
        b: Second fraction.

    Returns:
        Medial fraction (a+b)/(c+d).
    """
    return Fraction(a.numerator + b.numerator, a.denominator + b.denominator)


def subtract_unit_fraction(frac: Fraction) -> tuple[Fraction, Fraction]:
    """Egyptian fraction subtraction: split into unit fraction and remainder.

    Args:
        frac: Fraction to split.

    Returns:
        (unit_fraction, remainder).
    """
    import math
    denom = math.ceil(frac.denominator / frac.numerator)
    unit = Fraction(1, denom)
    remainder = frac - unit
    return (unit, remainder)


def split_fraction(frac: Fraction, n: int) -> list[Fraction]:
    """Split fraction into n equal parts.

    Args:
        frac: Fraction to split.
        n: Number of parts.

    Returns:
        List of n fractions summing to original.
    """
    if n < 1:
        raise ValueError("n must be >= 1")
    unit = Fraction(frac.numerator, frac.denominator * n)
    return [unit] * n


class FractionMusic:
    """Music interval utilities using fractions."""

    @staticmethod
    def semitone_ratio(semitones: int) -> Fraction:
        """Get ratio for N semitones."""
        ratio, _ = equal_temperament_ratio(semitones)
        return ratio

    @staticmethod
    def frequency(f0: float, ratio: Fraction) -> float:
        """Calculate frequency from base and ratio."""
        return f0 * float(ratio)

    @staticmethod
    def consonance_score(ratio: Fraction) -> float:
        """Score how consonant a ratio is (0-1)."""
        from math import gcd
        g = gcd(ratio.numerator, ratio.denominator)
        simplicity = g / max(ratio.numerator, ratio.denominator)
        return float(simplicity)


class FractionPhysics:
    """Physics unit conversions using fractions."""

    @staticmethod
    def wavelength(frequency: float, speed: float = 343.0) -> Fraction:
        """Calculate wavelength from frequency."""
        ratio = speed / frequency
        return Fraction(ratio).limit_denominator(100)

    @staticmethod
    def period(frequency: Fraction) -> float:
        """Calculate period from frequency ratio."""
        return 1.0 / float(frequency)

    @staticmethod
    def de_broglie(mass: float, velocity: float, h: float = 6.626e-34) -> float:
        """Calculate de Broglie wavelength."""
        return h / (mass * velocity)


class FractionEngineering:
    """Engineering approximations using fractions."""

    @staticmethod
    def pi_approximations(max_denom: int = 100) -> list[tuple[Fraction, float]]:
        """Get fraction approximations of pi."""
        results = []
        for d in range(1, max_denom + 1):
            n = round(3.14159265 * d)
            frac = Fraction(n, d)
            error = abs(float(frac) - 3.14159265)
            results.append((frac, error))
        results.sort(key=lambda x: x[1])
        return results[:10]

    @staticmethod
    def golden_ratio_approximations(max_denom: int = 100) -> list[tuple[Fraction, float]]:
        """Get fraction approximations of golden ratio."""
        phi = 1.618033988749895
        results = []
        for d in range(1, max_denom + 1):
            n = round(phi * d)
            frac = Fraction(n, d)
            error = abs(float(frac) - phi)
            results.append((frac, error))
        results.sort(key=lambda x: x[1])
        return results[:10]

    @staticmethod
    def sqrt2_approximations(max_denom: int = 100) -> list[tuple[Fraction, float]]:
        """Get fraction approximations of sqrt(2)."""
        sqrt2 = 1.41421356
        results = []
        for d in range(1, max_denom + 1):
            n = round(sqrt2 * d)
            frac = Fraction(n, d)
            error = abs(float(frac) - sqrt2)
            results.append((frac, error))
        results.sort(key=lambda x: x[1])
        return results[:10]
