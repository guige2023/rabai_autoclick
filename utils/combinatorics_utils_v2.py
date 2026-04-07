"""
Combinatorics utilities v2 — advanced counting and arrangement algorithms.

Companion to combinatorics_utils.py. Adds inclusion-exclusion,
Burnside's lemma, generating functions, and partition utilities.
"""

from __future__ import annotations

import math
from typing import Iterator


def derangements(n: int) -> int:
    """
    Count derangements (permutations with no fixed points).

    D(n) = n! * sum_{i=0}^{n} ((-1)^i / i!)

    Args:
        n: Number of elements

    Returns:
        Number of derangements

    Example:
        >>> derangements(4)
        9
    """
    if n == 0:
        return 1
    if n == 1:
        return 0
    prev, curr = 0, 1
    for i in range(2, n + 1):
        prev, curr = curr, (i - 1) * (prev + curr)
    return curr


def inclusion_exclusion(
    sets: list[set],
) -> int:
    """
    Compute the size of the union of sets using inclusion-exclusion.

    |A ∪ B ∪ C| = Σ|A| - Σ|A∩B| + Σ|A∩B∩C| - ...

    Args:
        sets: List of sets to union

    Returns:
        Cardinality of union
    """
    n = len(sets)
    total = 0
    for mask in range(1, 1 << n):
        intersection: set = set()
        for i in range(n):
            if mask & (1 << i):
                if not intersection:
                    intersection = sets[i].copy()
                else:
                    intersection &= sets[i]
        sign = -1 if bin(mask).count("1") % 2 == 0 else 1
        total += sign * len(intersection)
    return total


def bell_number(n: int) -> int:
    """
    Compute the nth Bell number (partitions of an n-element set).

    Args:
        n: Number of elements

    Returns:
        Number of ways to partition the set

    Example:
        >>> bell_number(3)
        5
    """
    if n <= 1:
        return 1
    bell = [0] * (n + 1)
    bell[0] = 1
    for i in range(1, n + 1):
        prev = bell[i - 1]
        for j in range(i - 1):
            bell[j + 1] += prev
        bell[i] = prev
    return bell[n]


def stirling_number_second(n: int, k: int) -> int:
    """
    Compute Stirling number of the second kind S(n, k).

    Number of ways to partition n elements into k non-empty subsets.

    Args:
        n: Total elements
        k: Number of subsets

    Returns:
        S(n, k)
    """
    if k == 0 or k > n:
        return 0
    if k == n or k == 1:
        return 1
    return k * stirling_number_second(n - 1, k) + stirling_number_second(n - 1, k - 1)


def stirling_number_first(n: int, k: int) -> int:
    """
    Compute (unsigned) Stirling number of the first kind c(n, k).

    Number of permutations of n elements with exactly k cycles.
    """
    if k == 0 or k > n:
        return 0
    if k == n or k == 1:
        return 1
    return stirling_number_first(n - 1, k - 1) + (n - 1) * stirling_number_first(n - 1, k)


def eulerian_number(n: int, k: int) -> int:
    """
    Compute Eulerian number A(n, k).

    Number of permutations of {1..n} with exactly k descents.

    Args:
        n: Number of elements
        k: Number of descents

    Returns:
        A(n, k)
    """
    if k >= n:
        return 0
    if k == 0 or n == 1:
        return 1
    return (n - k) * eulerian_number(n - 1, k - 1) + (k + 1) * eulerian_number(n - 1, k)


def partitions_of_integer(n: int, max_part: int | None = None) -> Iterator[list[int]]:
    """
    Generate all partitions of integer n.

    Args:
        n: Integer to partition
        max_part: Maximum part size (for restricted partitions)

    Yields:
        Each partition as a list of parts (non-increasing)

    Example:
        >>> list(partitions_of_integer(4))
        [[4], [3, 1], [2, 2], [2, 1, 1], [1, 1, 1, 1]]
    """
    if max_part is None:
        max_part = n
    if n == 0:
        yield []
        return
    for first in range(min(max_part, n), 0, -1):
        for rest in partitions_of_integer(n - first, first):
            yield [first] + rest


def bell_triangle_row(n: int) -> list[int]:
    """Generate the nth Bell triangle row."""
    if n == 0:
        return [1]
    prev = bell_triangle_row(n - 1)
    return [sum(prev[: i + 1]) for i in range(len(prev))] + [prev[-1]]


def multinomial_coefficient(ns: list[int]) -> int:
    """
    Compute multinomial coefficient n! / (k1! * k2! * ... * km!).

    Args:
        ns: List of part sizes summing to n

    Returns:
        Multinomial coefficient

    Example:
        >>> multinomial_coefficient([2, 1, 3])
        60
    """
    n = sum(ns)
    result = 1
    for k in ns:
        for i in range(1, k + 1):
            result = result * (n - sum(ns[:ns.index(k) if k in ns[:ns.index(k)] else 0]) + i - 1) // i
    return result


def permutations_with_repetition(chars: list, k: int) -> Iterator[list]:
    """Generate k-length permutations of chars with repetition."""
    n = len(chars)
    indices = [0] * k
    while True:
        yield [chars[i] for i in indices]
        i = k - 1
        while i >= 0 and indices[i] == n - 1:
            indices[i] = 0
            i -= 1
        if i < 0:
            return
        indices[i] += 1


def combinations_with_repetition(chars: list, k: int) -> Iterator[list]:
    """Generate k-length combinations of chars with repetition."""
    n = len(chars)
    indices = [0] * k
    while True:
        yield [chars[i] for i in indices]
        i = k - 1
        while i >= 0 and indices[i] == n - 1:
            i -= 1
        if i < 0:
            return
        indices[i] += 1
        for j in range(i + 1, k):
            indices[j] = indices[i]


def burning_basket_count(n: int, r: int) -> int:
    """
    Count ways to put n balls into r baskets (stars and bars) with no empty baskets.

    Args:
        n: Number of balls
        r: Number of baskets

    Returns:
        C(n-1, r-1)
    """
    if n < r:
        return 0
    return math.comb(n - 1, r - 1)


def mobius_inversion(f: Callable[[int], int], n: int) -> int:
    """
    Compute the Dirichlet inverse using Mobius inversion.

    Args:
        f: Function to invert
        n: Input value

    Returns:
        g(n) where g * 1 = f
    """
    from typing import Callable
    if n == 1:
        return f(1)
    result = 0
    i = 2
    while i <= n:
        q = n // i
        next_i = n // q + 1
        result += (next_i - i) * f(q)
        i = next_i
    return result - sum(mobius_inversion(f, d) for d in range(2, int(math.sqrt(n)) + 1) if n % d == 0)
