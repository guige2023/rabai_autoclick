"""
Set utilities for advanced set operations.

Provides power sets, set partitions, and set algebra operations.
"""

from __future__ import annotations

from typing import TypeVar


T = TypeVar("T")


def powerset(iterable: list[T]) -> list[list[T]]:
    """
    Generate all subsets of a set.

    Args:
        iterable: Input list

    Returns:
        List of all subsets

    Example:
        >>> powerset([1, 2, 3])
        [[], [1], [2], [3], [1, 2], [1, 3], [2, 3], [1, 2, 3]]
    """
    items = list(iterable)
    result = [[]]
    for item in items:
        result += [subset + [item] for subset in result]
    return result


def set_partitions(collection: list[T]) -> list[list[list[T]]]:
    """
    Generate all ways to partition a set.

    Args:
        collection: Input elements

    Returns:
        List of partitions (each partition is a list of subsets)
    """
    if len(collection) == 0:
        return [[]]
    if len(collection) == 1:
        return [[[collection[0]]]]
    result = []
    first = collection[0]
    rest = collection[1:]
    for partition in set_partitions(rest):
        for i in range(len(partition)):
            result.append(partition[:i] + [[first] + partition[i]] + partition[i + 1 :])
        result.append([[first]] + partition)
    return result


def symmetric_difference(s1: set, s2: set) -> set:
    """Elements in either set but not both."""
    return s1 ^ s2


def set_union(*sets: set) -> set:
    """Union of multiple sets."""
    result = set()
    for s in sets:
        result |= s
    return result


def set_intersection(*sets: set) -> set:
    """Intersection of multiple sets."""
    if not sets:
        return set()
    result = sets[0]
    for s in sets[1:]:
        result &= s
    return result


def disjoint(*sets: set) -> bool:
    """Check if sets are pairwise disjoint (no common elements)."""
    seen = set()
    for s in sets:
        intersection = seen & s
        if intersection:
            return False
        seen |= s
    return True


def superset_of(s1: set, s2: set) -> bool:
    """Check if s1 is a strict superset of s2."""
    return s1 > s2


def subset_of(s1: set, s2: set) -> bool:
    """Check if s1 is a strict subset of s2."""
    return s1 < s2
