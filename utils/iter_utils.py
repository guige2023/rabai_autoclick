"""Iterator utilities for RabAI AutoClick.

Provides:
- Advanced iterator operations
- Cartesian product and combinations
- Grouping and windowing
- Iterator mathematics
"""

from __future__ import annotations

import itertools
from typing import (
    Any,
    Callable,
    Generic,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
)


T = TypeVar("T")
K = TypeVar("K")
U = TypeVar("U")


def product(*iterables: List[T]) -> Iterator[Tuple[T, ...]]:
    """Cartesian product of input iterables.

    Args:
        *iterables: Input lists.

    Yields:
        Tuples of one item from each input.
    """
    return itertools.product(*iterables)


def combinations(
    items: List[T],
    r: int,
) -> Iterator[Tuple[T, ...]]:
    """Return r-length combinations of items.

    Args:
        items: Source list.
        r: Combination length.

    Yields:
        Combinations.
    """
    return itertools.combinations(items, r)


def permutations(
    items: List[T],
    r: Optional[int] = None,
) -> Iterator[Tuple[T, ...]]:
    """Return r-length permutations of items.

    Args:
        items: Source list.
        r: Permutation length. None means len(items).

    Yields:
        Permutations.
    """
    return itertools.permutations(items, r)


def powerset(items: List[T]) -> Iterator[Tuple[T, ...]]:
    """Yield all possible subsets of items.

    Args:
        items: Source list.

    Yields:
        Tuples representing each subset.
    """
    return itertools.chain.from_iterable(
        itertools.combinations(items, r)
        for r in range(len(items) + 1)
    )


def grouper(
    iterable: Iterator[T],
    n: int,
    fillvalue: Optional[T] = None,
) -> Iterator[List[T]]:
    """Group items into tuples of size n.

    Args:
        iterable: Source iterator.
        n: Group size.
        fillvalue: Value to fill incomplete groups.

    Yields:
        Groups of n items.
    """
    return itertools.zip_longest(iterable, *([iter(iterable)] * (n - 1)), fillvalue=fillvalue)  # type: ignore


def first(
    iterable: Iterator[T],
    default: Optional[T] = None,
) -> Optional[T]:
    """Get first item from iterator.

    Args:
        iterable: Source iterator.
        default: Default if empty.

    Returns:
        First item or default.
    """
    return next(iter(iterable), default)


def last(
    iterable: Iterator[T],
    default: Optional[T] = None,
) -> Optional[T]:
    """Get last item from iterator.

    Args:
        iterable: Source iterator.
        default: Default if empty.

    Returns:
        Last item or default.
    """
    item = default
    for item in iterable:
        pass
    return item


def nth(
    iterable: Iterator[T],
    n: int,
    default: Optional[T] = None,
) -> Optional[T]:
    """Get the nth item from iterator.

    Args:
        iterable: Source iterator.
        n: Index (0-based).
        default: Default if out of range.

    Returns:
        nth item or default.
    """
    return next(itertools.islice(iterable, n, None), default)


def quantify(
    iterable: Iterator[Any],
    pred: Callable[[Any], bool] = bool,
) -> int:
    """Count how many items satisfy a predicate.

    Args:
        iterable: Source iterator.
        pred: Predicate function.

    Returns:
        Count of items where pred(item) is True.
    """
    return sum(1 for item in iterable if pred(item))


def all_equal(iterable: Iterator[Any]) -> bool:
    """Check if all items in iterator are equal.

    Args:
        iterable: Source iterator.

    Returns:
        True if all items are equal.
    """
    iterator = iter(iterable)
    try:
        first_item = next(iterator)
    except StopIteration:
        return True
    return all(item == first_item for item in iterator)


def pairwise(iterable: Iterator[T]) -> Iterator[Tuple[T, T]]:
    """Yield consecutive pairs from iterator.

    Args:
        iterable: Source iterator.

    Yields:
        Pairs of consecutive items.
    """
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def roundrobin(*iterables: Iterator[T]) -> Iterator[T]:
    """Round-robin merge of iterables.

    Args:
        *iterables: Iterables to merge round-robin.

    Yields:
        Items from iterables in round-robin order.
    """
    return itertools.chain.from_iterable(
        itertools.zip_longest(*iterables)
    )


def iterate(func: Callable[[T], T], start: T) -> Iterator[T]:
    """Generate sequence: start, func(start), func(func(start)), ...

    Args:
        func: Function to apply iteratively.
        start: Starting value.

    Yields:
        Infinite sequence of applied function.
    """
    while True:
        yield start
        start = func(start)


def consume(iterator: Iterator[Any], n: Optional[int] = None) -> None:
    """Consume an iterator, optionally up to n items.

    Args:
        iterator: Source iterator.
        n: Number of items to consume. None means all.
    """
    if n is None:
        itertools.consume(iterator)
    else:
        next(itertools.islice(iterator, n, n), None)


__all__ = [
    "product",
    "combinations",
    "permutations",
    "powerset",
    "grouper",
    "first",
    "last",
    "nth",
    "quantify",
    "all_equal",
    "pairwise",
    "roundrobin",
    "iterate",
    "consume",
]
