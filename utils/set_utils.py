"""Set utilities for RabAI AutoClick.

Provides:
- Set operations
- Set algebra
- Frozen set utilities
- Set partitions
"""

from __future__ import annotations

from typing import (
    Any,
    Callable,
    FrozenSet,
    Iterator,
    List,
    Set,
    Tuple,
)


def set_union(*sets: Set[Any]) -> Set[Any]:
    """Union of multiple sets.

    Args:
        *sets: Sets to union.

    Returns:
        Union set.
    """
    result: Set[Any] = set()
    for s in sets:
        result |= s
    return result


def set_intersection(*sets: Set[Any]) -> Set[Any]:
    """Intersection of multiple sets.

    Args:
        *sets: Sets to intersect.

    Returns:
        Intersection set.
    """
    if not sets:
        return set()
    result = sets[0].copy()
    for s in sets[1:]:
        result &= s
    return result


def set_difference(base: Set[Any], *others: Set[Any]) -> Set[Any]:
    """Set difference.

    Args:
        base: Base set.
        *others: Sets to subtract.

    Returns:
        Difference set.
    """
    result = base.copy()
    for s in others:
        result -= s
    return result


def set_symmetric_difference(a: Set[Any], b: Set[Any]) -> Set[Any]:
    """Symmetric difference.

    Args:
        a: First set.
        b: Second set.

    Returns:
        Symmetric difference.
    """
    return a ^ b


def powerset(s: Set[T]) -> List[Set[T]]:
    """Generate all subsets of a set.

    Args:
        s: Input set.

    Returns:
        List of all subsets.
    """
    items = list(s)
    result: List[Set[T]] = []
    for mask in range(1 << len(items)):
        subset = {items[i] for i in range(len(items)) if mask & (1 << i)}
        result.append(subset)
    return result


def partitions(s: Set[T]) -> List[List[Set[T]]]:
    """Generate all partitions of a set.

    Args:
        s: Input set.

    Returns:
        List of partitions (each partition is a list of non-empty subsets).
    """
    if not s:
        return [[]]
    if len(s) == 1:
        return [[[next(iter(s))]]]

    result: List[List[Set[T]]] = []
    first = next(iter(s))
    rest = s - {first}

    for partition in partitions(rest):
        result.append([{first}] + partition)

        for subset in partition:
            new_subset = subset | {first}
            new_partition = [new_subset if s == subset else s for s in partition]
            result.append(new_partition)

    return result


def disjoint(*sets: Set[Any]) -> bool:
    """Check if sets are pairwise disjoint.

    Args:
        *sets: Sets to check.

    Returns:
        True if all sets are pairwise disjoint.
    """
    seen: Set[Any] = set()
    for s in sets:
        if seen & s:
            return False
        seen |= s
    return True


def subset(a: Set[Any], b: Set[Any]) -> bool:
    """Check if a is subset of b."""
    return a <= b


def proper_subset(a: Set[Any], b: Set[Any]) -> bool:
    """Check if a is proper subset of b."""
    return a < b


def superset(a: Set[Any], b: Set[Any]) -> bool:
    """Check if a is superset of b."""
    return a >= b


def set_map(func: Callable[[T], U], s: Set[T]) -> Set[U]:
    """Apply function to each element of set.

    Args:
        func: Function to apply.
        s: Input set.

    Returns:
        New set with transformed values.
    """
    return {func(x) for x in s}


def set_filter(predicate: Callable[[T], bool], s: Set[T]) -> Set[T]:
    """Filter set by predicate.

    Args:
        predicate: Filter function.
        s: Input set.

    Returns:
        Filtered set.
    """
    return {x for x in s if predicate(x)}


def cartesian_product(*sets: Set[T]) -> Set[Tuple[T, ...]]:
    """Cartesian product of sets.

    Args:
        *sets: Sets to product.

    Returns:
        Set of tuples.
    """
    if not sets:
        return {()}
    result: Set[Tuple[T, ...]] = {()}
    for s in sets:
        result = {x + (item,) for x in result for item in s}
    return result


def set_zip(a: Set[T], b: Set[U]) -> List[Tuple[T, U]]:
    """Zip two sets into list of tuples.

    Args:
        a: First set.
        b: Second set.

    Returns:
        List of tuples.
    """
    return list(zip(sorted(a), sorted(b)))


def frozen(s: Set[T]) -> FrozenSet[T]:
    """Convert set to frozenset."""
    return frozenset(s)


def to_sorted_list(s: Set[T]) -> List[T]:
    """Convert set to sorted list."""
    return sorted(s)


def intersection_cardinality(*sets: Set[Any]) -> int:
    """Get cardinality of intersection."""
    if not sets:
        return 0
    return len(set_intersection(*sets))


def union_cardinality(*sets: Set[Any]) -> int:
    """Get cardinality of union."""
    seen: Set[Any] = set()
    total = 0
    for s in sets:
        for x in s:
            if x not in seen:
                total += 1
                seen.add(x)
    return total
