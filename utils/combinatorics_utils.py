"""Combinatorics operations: permutations, combinations, partitions, andCartesian product."""

from __future__ import annotations

from typing import List, Iterator, Tuple, Callable, Any, TypeVar
from functools import reduce
import math

T = TypeVar("T")


def permutations(items: List[T], r: int = -1) -> Iterator[Tuple[T, ...]]:
    """Generate all r-length permutations of items (order matters, no repeat)."""
    n = len(items)
    if r == -1:
        r = n
    if r > n:
        return
    indices = list(range(n))
    cycles = list(range(n, n - r, -1))
    yield tuple(items[i] for i in indices[:r])
    while n:
        for i in reversed(range(r)):
            cycles[i] -= 1
            if cycles[i] == 0:
                indices[i:] = indices[i + 1:] + indices[i:i + 1]
                cycles[i] = n - i
            else:
                j = cycles[i]
                indices[i], indices[-j] = indices[-j], indices[i]
                yield tuple(items[i] for i in indices[:r])
                break
        else:
            return


def combinations(items: List[T], r: int) -> Iterator[Tuple[T, ...]]:
    """Generate all r-length combinations of items (order doesn't matter, no repeat)."""
    n = len(items)
    if r > n or r <= 0:
        return
    indices = list(range(r))

    def yield_combo():
        yield tuple(items[i] for i in indices)
        while True:
            for i in reversed(range(r)):
                if indices[i] != i + n - r:
                    break
            else:
                return
            indices[i] += 1
            for j in range(i + 1, r):
                indices[j] = indices[j - 1] + 1
            yield tuple(items[i] for i in indices)

    yield from yield_combo()


def combinations_with_replacement(items: List[T], r: int) -> Iterator[Tuple[T, ...]]:
    """Generate r-length combinations with replacement."""
    n = len(items)
    if r <= 0:
        return
    indices = [0] * r

    def yield_combo():
        yield tuple(items[i] for i in indices)
        while True:
            for i in reversed(range(r)):
                if indices[i] != n - 1:
                    break
            else:
                return
            indices[i] += 1
            for j in range(i + 1, r):
                indices[j] = indices[i]
            yield tuple(items[i] for i in indices)

    yield from yield_combo()


def permutations_with_replacement(items: List[T], r: int) -> Iterator[Tuple[T, ...]]:
    """Generate r-length permutations with replacement (product)."""
    n = len(items)
    if r <= 0 or n == 0:
        return
    for combo in combinations_with_replacement(list(range(n)), r):
        yield combo  # indices are in sorted order; need all permutations of each


def cartesian_product(*sequences: List[T]) -> Iterator[Tuple[T, ...]]:
    """Generate Cartesian product of one or more sequences."""
    if not sequences:
        return
    result: List[List[T]] = [[]]
    for seq in sequences:
        result = [r + [item] for r in result for item in seq]
    for r in result:
        yield tuple(r)


def derangements(n: int) -> Iterator[Tuple[int, ...]]:
    """Generate all derangements of {0..n-1} (no element stays in place)."""
    if n == 0:
        yield ()
        return
    if n == 1:
        return
    if n == 2:
        yield (1, 0)
        return

    def _derange(a: List[int]) -> Iterator[List[int]]:
        if not a:
            yield []
        for i, x in enumerate(a):
            for j, y in enumerate(a):
                if i == j:
                    continue
                rest = a[:i] + a[i + 1:]
                if i < j:
                    rest = a[:i] + a[i + 1:j] + a[j + 1:]
                else:
                    rest = a[:j] + a[j + 1:i] + a[i + 1:]
                for result in _derange(rest):
                    yield [x] + result

    for d in _derange(list(range(n))):
        yield tuple(d)


def partitions(n: int, min_part: int = 1) -> Iterator[Tuple[int, ...]]:
    """Generate integer partitions of n (order-independent)."""
    if n <= 0:
        return
    if min_part > n:
        return
    yield (n,)
    for k in range(min_part, n // 2 + 1):
        for rest in partitions(n - k, k):
            yield (k,) + rest


def subsets(items: List[T]) -> Iterator[Tuple[T, ...]]:
    """Generate all subsets (power set elements) as tuples."""
    n = len(items)
    for mask in range(1 << n):
        yield tuple(items[i] for i in range(n) if mask & (1 << i))


def binomial_coefficient(n: int, k: int) -> int:
    """Compute C(n, k) = n! / (k! * (n-k)!)."""
    if k < 0 or k > n:
        return 0
    k = min(k, n - k)
    result = 1
    for i in range(k):
        result = result * (n - i) // (i + 1)
    return result


def multinomial_coefficient(counts: List[int]) -> int:
    """Compute multinomial coefficient: n! / (c1! * c2! * ...)."""
    n = sum(counts)
    result = math.factorial(n)
    for c in counts:
        result //= math.factorial(c)
    return result


def catalan_number(n: int) -> int:
    """Return the nth Catalan number: C_n = binomial(2n, n) / (n+1)."""
    return binomial_coefficient(2 * n, n) // (n + 1)


def fibonacci_index(f: int) -> int | None:
    """Return the index n such that F_n = f, or None if not a Fibonacci number."""
    if f < 0:
        return None
    phi = (1 + math.sqrt(5)) / 2
    index = int(round(math.log(f * math.sqrt(5)) / math.log(phi)))
    a, b = 0, 1
    for i in range(index + 2):
        a, b = b, a + b
    if a == f:
        return index
    return None


class CombinationLock:
    """Iterator over all combinations of a lock's digits."""

    def __init__(self, num_digits: int, digit_range: int) -> None:
        self.num_digits = num_digits
        self.digit_range = digit_range

    def __iter__(self) -> Iterator[Tuple[int, ...]]:
        indices = [0] * self.num_digits
        while True:
            yield tuple(indices)
            for i in range(self.num_digits - 1, -1, -1):
                indices[i] += 1
                if indices[i] < self.digit_range:
                    break
                indices[i] = 0
            else:
                return


def sliding_window(items: List[T], size: int, step: int = 1) -> Iterator[Tuple[T, ...]]:
    """Yield sliding windows of given size over items."""
    if size <= 0 or step <= 0:
        return
    n = len(items)
    for start in range(0, n - size + 1, step):
        yield tuple(items[start:start + size])


def batch_items(items: List[T], batch_size: int) -> Iterator[List[T]]:
    """Batch items into chunks of given size."""
    if batch_size <= 0:
        return
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]
