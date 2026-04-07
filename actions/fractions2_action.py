"""Fractions action v2 - advanced fraction operations.

Extended fraction utilities including Egyptian fractions,
 Farey sequences, Stern-Brocot tree, and rational approximation.
"""

from __future__ import annotations

from fractions import Fraction
from typing import Generator, Iterable, Sequence
import math

__all__ = [
    "egyptian_fraction",
    "egyptian_decomposition",
    "farey_sequence",
    "farey_neighbors",
    "farey_between",
    "stern_brocot_tree",
    "stern_brocot_path",
    "mediant",
    "best_rational_approx",
    "convergents",
    "semitones_to_ratio",
    "ratio_to_semitones",
    "fraction_compare",
    "fraction_normalize",
    "fraction_sign",
    "fraction_is_proper",
    "fraction_is_unit",
    "fraction_components",
    "FractionTree",
    "RationalApproximator",
    "FractionGrid",
]


def egyptian_fraction(frac: Fraction | tuple[int, int]) -> list[Fraction]:
    """Decompose fraction into sum of unit fractions (Egyptian fraction).

    Args:
        frac: Fraction to decompose.

    Returns:
        List of unit fractions that sum to the original.

    Example:
        3/4 -> [1/2, 1/4] or [1/3, 1/4, 1/6, 1/12] etc.
    """
    f = Fraction(*frac) if isinstance(frac, tuple) else Fraction(frac)
    result = []
    while f.numerator != 1:
        denom = math.ceil(f.denominator / f.numerator)
        unit = Fraction(1, denom)
        result.append(unit)
        f = f - unit
    result.append(f)
    return result


def egyptian_decomposition(frac: Fraction, method: str = "greedy") -> list[Fraction]:
    """Decompose fraction using specified method.

    Args:
        frac: Fraction to decompose.
        method: 'greedy' or 'splitting'.

    Returns:
        List of unit fractions.
    """
    if method == "greedy":
        return egyptian_fraction(frac)
    f = Fraction(frac)
    result = []
    if f.numerator > f.denominator:
        whole = f.numerator // f.denominator
        result.extend([Fraction(1, 1)] * whole)
        f = Fraction(f.numerator % f.denominator, f.denominator)
    while f.numerator > 0:
        d = math.ceil(f.denominator / f.numerator)
        result.append(Fraction(1, d))
        f = Fraction(f.numerator * d - f.denominator, f.denominator * d)
        if f.numerator == 0:
            break
        g = math.gcd(f.numerator, f.denominator)
        f = Fraction(f.numerator // g, f.denominator // g)
    return result


def farey_sequence(order: int) -> list[Fraction]:
    """Generate Farey sequence of given order.

    Args:
        order: Maximum denominator.

    Returns:
        Farey sequence as list of Fractions.
    """
    if order < 1:
        raise ValueError(f"Order must be >= 1, got {order}")
    a, b, c, d = 0, 1, 1, order
    result = [Fraction(a, b), Fraction(c, d)]
    while c < order:
        k = (order + b) // d
        a, b, c, d = c, d, k * c - a, k * d - b
        result.append(Fraction(a, b))
    return result


def farey_neighbors(frac: Fraction, order: int) -> tuple[Fraction, Fraction] | tuple[None, None]:
    """Find neighbors of fraction in Farey sequence of given order.

    Args:
        frac: Fraction to find neighbors for.
        order: Maximum denominator.

    Returns:
        Tuple of (left_neighbor, right_neighbor) or (None, None).
    """
    seq = farey_sequence(order)
    if frac not in seq:
        raise ValueError(f"{frac} not in Farey sequence of order {order}")
    idx = seq.index(frac)
    left = seq[idx - 1] if idx > 0 else None
    right = seq[idx + 1] if idx < len(seq) - 1 else None
    return (left, right)


def farey_between(a: Fraction, b: Fraction, order: int) -> list[Fraction]:
    """Find all fractions between a and b in Farey sequence.

    Args:
        a: Left fraction.
        b: Right fraction.
        order: Maximum denominator.

    Returns:
        List of fractions between a and b.
    """
    if a >= b:
        raise ValueError("a must be less than b")
    seq = farey_sequence(order)
    result = []
    in_range = False
    for f in seq:
        if f == a:
            in_range = True
            continue
        if f == b:
            break
        if in_range:
            result.append(f)
    return result


def stern_brocot_tree() -> Generator[tuple[Fraction, Fraction], None, None]:
    """Generate Stern-Brocot tree as (left, right) fraction pairs.

    Yields:
        Tuples of (left_limit, right_limit) for each level.
    """
    yield (Fraction(0, 1), Fraction(1, 0))
    left, right = Fraction(0, 1), Fraction(1, 0)
    while True:
        med = mediant(left, right)
        yield (left, right)
        if left.numerator * 2 < right.numerator:
            left = med
        else:
            right = med


def stern_brocot_path(target: Fraction, max_iterations: int = 100) -> str:
    """Find path from root to target fraction in Stern-Brocot tree.

    Args:
        target: Fraction to find.
        max_iterations: Maximum iterations.

    Returns:
        String of L and R moves (left/right mediants).
    """
    if target <= Fraction(0, 1) or target >= Fraction(1, 0):
        raise ValueError("Target must be between 0 and 1")
    left, right = Fraction(0, 1), Fraction(1, 0)
    path = []
    for _ in range(max_iterations):
        med = mediant(left, right)
        if med == target:
            return "".join(path)
        elif med < target:
            path.append("R")
            left = med
        else:
            path.append("L")
            right = med
    raise ValueError(f"Could not find path to {target} within {max_iterations} iterations")


def mediant(a: Fraction, b: Fraction) -> Fraction:
    """Compute mediant (special fraction between two fractions).

    Args:
        a: First fraction.
        b: Second fraction.

    Returns:
        Mediant fraction (a+b)/(c+d) if a=m/n and b=p/q.
    """
    return Fraction(a.numerator + b.numerator, a.denominator + b.denominator)


def best_rational_approx(value: float, max_denominator: int = 1000) -> Fraction:
    """Find best rational approximation using continued fractions.

    Args:
        value: Float to approximate.
        max_denominator: Maximum denominator.

    Returns:
        Best approximating Fraction.
    """
    frac = Fraction(value).limit_denominator(max_denominator)
    return frac


def convergents(continued_fraction: list[int]) -> list[Fraction]:
    """Compute convergents from continued fraction terms.

    Args:
        continued_fraction: List of continued fraction terms.

    Returns:
        List of convergent Fractions.
    """
    convergents_list = []
    p_prev, p_curr = 0, 1
    q_prev, q_curr = 1, 0
    for i, a in enumerate(continued_fraction):
        if i == 0:
            p = a
            q = 1
        elif i == 1:
            p = a * continued_fraction[0] + 1
            q = a
        else:
            p = a * p_curr + p_prev
            q = a * q_curr + q_prev
            p_prev, p_curr = p_curr, p
            q_prev, q_curr = q_curr, q
        convergents_list.append(Fraction(int(p), int(q)))
    return convergents_list


def semitones_to_ratio(semitones: int | float) -> Fraction:
    """Convert equal-temperament semitones to frequency ratio.

    Args:
        semitones: Number of semitones (can be fractional).

    Returns:
        Frequency ratio as Fraction.
    """
    ratio = 2 ** (semitones / 12)
    return Fraction(ratio).limit_denominator(10000)


def ratio_to_semitones(ratio: Fraction | float) -> float:
    """Convert frequency ratio to semitones.

    Args:
        ratio: Frequency ratio.

    Returns:
        Number of semitones.
    """
    r = float(ratio)
    return 12 * math.log2(r)


def fraction_compare(a: Fraction, b: Fraction) -> int:
    """Compare two fractions.

    Args:
        a: First fraction.
        b: Second fraction.

    Returns:
        -1 if a < b, 0 if equal, 1 if a > b.
    """
    diff = a - b
    if diff.numerator < 0:
        return -1
    elif diff.numerator == 0:
        return 0
    return 1


def fraction_normalize(frac: Fraction) -> Fraction:
    """Normalize fraction to lowest terms.

    Args:
        frac: Fraction to normalize.

    Returns:
        Normalized Fraction.
    """
    return Fraction(frac.numerator, frac.denominator)


def fraction_sign(frac: Fraction) -> int:
    """Get sign of fraction.

    Args:
        frac: Fraction.

    Returns:
        -1, 0, or 1.
    """
    if frac.numerator < 0:
        return -1
    elif frac.numerator == 0:
        return 0
    return 1


def fraction_is_proper(frac: Fraction) -> bool:
    """Check if fraction is proper (|numerator| < denominator).

    Args:
        frac: Fraction to check.

    Returns:
        True if proper.
    """
    return abs(frac.numerator) < frac.denominator


def fraction_is_unit(frac: Fraction) -> bool:
    """Check if fraction is a unit fraction (numerator = 1).

    Args:
        frac: Fraction to check.

    Returns:
        True if unit fraction.
    """
    return frac.numerator == 1 and frac.denominator > 1


def fraction_components(frac: Fraction) -> tuple[int, Fraction]:
    """Get whole number and proper fraction parts.

    Args:
        frac: Fraction.

    Returns:
        Tuple of (whole_part, proper_fraction).
    """
    whole = frac.numerator // frac.denominator
    remainder = frac.numerator % frac.denominator
    return (whole, Fraction(remainder, frac.denominator) if remainder else Fraction(0))


class FractionTree:
    """Stern-Brocot fraction tree with navigation."""

    def __init__(self) -> None:
        self._root_left = Fraction(0, 1)
        self._root_right = Fraction(1, 0)

    def children(self, frac: Fraction) -> tuple[Fraction, Fraction]:
        """Get left and right children of fraction.

        Args:
            frac: Parent fraction.

        Returns:
            Tuple of (left_child, right_child).
        """
        left = Fraction(frac.numerator, frac.numerator + frac.denominator)
        right = Fraction(frac.numerator + frac.denominator, frac.denominator)
        return (left, right)

    def parent(self, frac: Fraction) -> Fraction | None:
        """Find parent of fraction in tree.

        Args:
            frac: Child fraction.

        Returns:
            Parent fraction or None.
        """
        if frac.numerator > frac.denominator:
            p = frac.numerator - frac.denominator
            return Fraction(p, frac.denominator)
        elif frac.denominator > frac.numerator:
            p = frac.denominator - frac.numerator
            return Fraction(frac.numerator, p)
        return None

    def depth(self, frac: Fraction) -> int:
        """Get depth of fraction in tree (root depth = 0)."""
        depth = 0
        while frac.numerator > 0 and frac.denominator > 0:
            frac = self.parent(frac)
            if frac is None:
                break
            depth += 1
        return depth


class RationalApproximator:
    """Find rational approximations for real numbers."""

    def __init__(self, max_denominator: int = 1000) -> None:
        self._max_denominator = max_denominator

    def approximate(self, value: float) -> Fraction:
        """Find best approximation."""
        return Fraction(value).limit_denominator(self._max_denominator)

    def approximations(self, value: float, count: int = 5) -> list[Fraction]:
        """Get multiple approximations with increasing accuracy."""
        results = []
        for d in range(1, self._max_denominator + 1):
            n = round(value * d)
            f = Fraction(n, d)
            if f not in results:
                results.append(f)
            if len(results) >= count:
                break
        return sorted(results, key=lambda x: abs(float(x) - value))[:count]

    def is_best_approx(self, value: float, frac: Fraction) -> bool:
        """Check if fraction is the best approximation at its denominator."""
        n, d = frac.numerator, frac.denominator
        if d > self._max_denominator:
            return False
        best = Fraction(value).limit_denominator(d)
        return best == frac


class FractionGrid:
    """2D grid of fractions for visualization."""

    def __init__(self, max_denominator: int = 10) -> None:
        self._max_denominator = max_denominator
        self._grid: list[list[Fraction | None]] = []
        self._build_grid()

    def _build_grid(self) -> None:
        """Build grid of fractions."""
        for denom in range(1, self._max_denominator + 1):
            row = []
            for num in range(denom + 1):
                if math.gcd(num, denom) == 1:
                    row.append(Fraction(num, denom))
                else:
                    row.append(None)
            self._grid.append(row)

    def fractions(self) -> list[Fraction]:
        """Get all fractions in grid."""
        result = []
        for row in self._grid:
            for f in row:
                if f is not None:
                    result.append(f)
        return sorted(result)

    def between(self, a: Fraction, b: Fraction) -> list[Fraction]:
        """Get fractions between a and b."""
        all_fracs = self.fractions()
        return [f for f in all_fracs if a < f < b]
