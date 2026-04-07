"""
Combinatorics utilities for automation actions.

Provides permutations, combinations, cartesian product,
and partition utilities.
"""

from __future__ import annotations

import itertools
import math
from typing import Callable, Iterator, TypeVar


T = TypeVar("T")


def factorial(n: int) -> int:
    """Compute n! (factorial)."""
    if n < 0:
        raise ValueError("Factorial of negative number")
    if n == 0:
        return 1
    result = 1
    for i in range(2, n + 1):
        result *= i
    return result


def permutations_count(n: int, r: int) -> int:
    """Compute number of permutations P(n, r) = n! / (n-r)!."""
    if r < 0 or r > n:
        return 0
    return factorial(n) // factorial(n - r)


def combinations_count(n: int, r: int) -> int:
    """Compute number of combinations C(n, r) = n! / (r! * (n-r)!)."""
    if r < 0 or r > n:
        return 0
    if r == 0 or r == n:
        return 1
    return factorial(n) // (factorial(r) * factorial(n - r))


def permutations(items: list[T], r: int | None = None) -> Iterator[tuple[T, ...]]:
    """Generate all permutations of items taken r at a time."""
    yield from itertools.permutations(items, r)


def combinations(items: list[T], r: int) -> Iterator[tuple[T, ...]]:
    """Generate all combinations of items taken r at a time."""
    yield from itertools.combinations(items, r)


def combinations_with_replacement(items: list[T], r: int) -> Iterator[tuple[T, ...]]:
    """Generate combinations with replacement."""
    yield from itertools.combinations_with_replacement(items, r)


def cartesian_product(*iterables: list[T]) -> Iterator[tuple[T, ...]]:
    """Generate cartesian product of iterables."""
    yield from itertools.product(*iterables)


def power_set(items: list[T]) -> Iterator[tuple[T, ...]]:
    """Generate all subsets of items."""
    n = len(items)
    for mask in range(1 << n):
        yield tuple(items[i] for i in range(n) if mask & (1 << i))


def derangements(n: int) -> int:
    """Count number of derangements (permutations with no fixed points)."""
    if n < 0:
        raise ValueError("Derangements not defined for negative n")
    if n == 0:
        return 1
    if n == 1:
        return 0
    a, b = 0, 1
    for i in range(2, n + 1):
        a, b = b, (i - 1) * (a + b)
    return b


def partial_derangements(items: list[T], fixed: set[int]) -> Iterator[tuple[T, ...]]:
    """Generate derangements where certain positions are fixed."""
    n = len(items)
    free_indices = [i for i in range(n) if i not in fixed]
    free_items = [items[i] for i in free_indices]
    fixed_items = [items[i] for i in range(n) if i in fixed]
    for perm in _derangement_iter(free_items):
        result = fixed_items.copy()
        for idx, pos in enumerate(free_indices):
            while len(result) <= pos:
                result.append(None)
            result[pos] = perm[idx]
        yield tuple(result)


def _derangement_iter(items: list[T]) -> Iterator[tuple[T, ...]]:
    """Internal generator for derangements."""
    n = len(items)
    if n == 0:
        yield ()
        return
    if n == 1:
        return
    for perm in permutations(items):
        if all(perm[i] != items[i] for i in range(n)):
            yield perm


def partitions(n: int, max_part: int | None = None) -> Iterator[tuple[int, ...]]:
    """Generate all integer partitions of n."""
    if n <= 0:
        yield ()
        return
    if max_part is None:
        max_part = n
    yield from _partition_helper(n, max_part, [])


def _partition_helper(n: int, max_part: int, current: list[int]) -> Iterator[tuple[int, ...]]:
    """Helper for integer partitions."""
    if n == 0:
        yield tuple(current)
        return
    for i in range(min(n, max_part), 0, -1):
        yield from _partition_helper(n - i, i, current + [i])


def set_partitions(items: list[T]) -> Iterator[list[list[T]]]:
    """Generate all ways to partition a set into non-empty subsets."""
    if not items:
        yield []
        return
    if len(items) == 1:
        yield [[items[0]]]
        return
    first = items[0]
    rest = items[1:]
    for partition in set_partitions(rest):
        for i, subset in enumerate(partition):
            yield partition[:i] + [[first] + subset] + partition[i + 1 :]
        yield [[first]] + partition


def bell_numbers(n: int) -> int:
    """Compute nth Bell number (number of set partitions)."""
    if n < 0:
        raise ValueError("Bell number not defined for negative n")
    if n == 0:
        return 1
    bell = [0] * (n + 1)
    bell[0] = 1
    for i in range(1, n + 1):
        bell[i] = 0
        for k in range(i):
            bell[i] += math.comb(i - 1, k) * bell[k]
    return bell[n]


def catalan_number(n: int) -> int:
    """Compute nth Catalan number C_n = (2n)! / ((n+1)! * n!)."""
    if n < 0:
        raise ValueError("Catalan number not defined for negative n")
    return math.comb(2 * n, n) // (n + 1)


def multinomial(n: int, parts: list[int]) -> int:
    """Compute multinomial coefficient n! / (k1! * k2! * ...)."""
    if sum(parts) != n:
        raise ValueError("Parts must sum to n")
    result = factorial(n)
    for k in parts:
        result //= factorial(k)
    return result


def stirling_number_second(n: int, k: int) -> int:
    """Compute Stirling number of the second kind S(n, k)."""
    if n < 0 or k < 0 or k > n:
        return 0
    if k == 0 or k == n:
        return 1
    return k * stirling_number_second(n - 1, k) + stirling_number_second(n - 1, k - 1)


def arrangement_number(n: int, k: int, /, *, distinct: bool = True) -> int:
    """Compute number of arrangements (permutations if distinct, combinations otherwise)."""
    if distinct:
        return permutations_count(n, k)
    return combinations_count(n, k)


def subset_with_sum(items: list[int], target: int) -> Iterator[tuple[int, ...]]:
    """Find all subsets whose sum equals target."""
    n = len(items)
    for mask in range(1 << n):
        total = 0
        subset = []
        for i in range(n):
            if mask & (1 << i):
                total += items[i]
                subset.append(i)
        if total == target:
            yield tuple(subset)


def permutations_r(permutations_list: list[T]) -> dict[T, int]:
    """Count occurrences of each unique element in a list."""
    counts: dict[T, int] = {}
    for item in permutations_list:
        counts[item] = counts.get(item, 0) + 1
    return counts


def permutations_of_multiset(items: list[T]) -> Iterator[tuple[T, ...]]:
    """Generate all permutations of a multiset."""
    counts = permutations_r(items)
    keys = list(counts.keys())
    total = len(items)
    yield from _multiset_permutations(keys, counts, total, [])


def _multiset_permutations(
    keys: list[T], counts: dict[T, int], remaining: int, current: list[T]
) -> Iterator[tuple[T, ...]]:
    """Helper for multiset permutations."""
    if remaining == 0:
        yield tuple(current)
        return
    for key in keys:
        if counts[key] > 0:
            counts[key] -= 1
            current.append(key)
            yield from _multiset_permutations(keys, counts, remaining - 1, current)
            current.pop()
            counts[key] += 1
