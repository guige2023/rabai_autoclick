"""
Advanced itertools utilities and generator helpers.

Provides generator-based utilities extending itertools with
new iterators, transformations, and infinite sequences.
"""

from __future__ import annotations

import itertools
import math
from typing import Callable, Generator, Iterator, TypeVar


T = TypeVar("T")
U = TypeVar("U")


def chunked(iterable: Iterator[T], n: int) -> Generator[list[T], None, None]:
    """
    Yield items in chunks of size n.

    Example:
        >>> list(chunked(iter([1,2,3,4,5]), 2))
        [[1, 2], [3, 4], [5]]
    """
    chunk: list[T] = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) == n:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def windowed(iterable: Iterator[T], n: int) -> Generator[list[T], None, None]:
    """
    Yield sliding windows of size n.

    Example:
        >>> list(windowed(iter([1,2,3,4]), 3))
        [[1, 2, 3], [2, 3, 4]]
    """
    hist = []
    for item in iterable:
        hist.append(item)
        if len(hist) == n:
            yield hist[:]
            hist.pop(0)


def unique_everseen(
    iterable: Iterator[T],
    key: Callable[[T], U] | None = None,
) -> Generator[T, None, None]:
    """
    Yield unique elements in order, preserving first-seen order.
    Uses O(n) memory for seen set.

    Example:
        >>> list(unique_everseen([1, 2, 2, 3, 1]))
        [1, 2, 3]
    """
    seen: set = set()
    for item in iterable:
        val = key(item) if key else item
        if val not in seen:
            seen.add(val)
            yield item


def take(n: int, iterable: Iterator[T]) -> list[T]:
    """Take first n items from iterable."""
    return list(itertools.islice(iterable, n))


def drop(n: int, iterable: Iterator[T]) -> Iterator[T]:
    """Skip first n items."""
    return itertools.islice(iterable, n, None)


def first(iterable: Iterator[T], default: U = None) -> T | U:
    """Return first item or default."""
    return next(iter(iterable), default)


def last(iterable: Iterator[T], default: U = None) -> T | U:
    """Return last item or default."""
    item = default
    for item in iterable:
        pass
    return item


def nth(n: int, iterable: Iterator[T], default: U = None) -> T | U:
    """Return nth item (0-indexed) or default."""
    return next(itertools.islice(iterable, n, n + 1), default)


def interleave(*iterables: Iterator[T]) -> Generator[T, None, None]:
    """
    Interleave elements from multiple iterables.

    Example:
        >>> list(interleave([1, 4], [2, 5], [3, 6]))
        [1, 2, 3, 4, 5, 6]
    """
    for items in itertools.zip_longest(*iterables):
        for item in items:
            if item is not None:
                yield item


def interpose(
    sep: T,
    iterable: Iterator[U],
) -> Generator[T | U, None, None]:
    """
    Insert separator between items.

    Example:
        >>> list(interpose(0, [1, 2, 3]))
        [1, 0, 2, 0, 3]
    """
    it = iter(iterable)
    yield next(it)
    for item in it:
        yield sep
        yield item


def partition(
    pred: Callable[[T], bool],
    iterable: Iterator[T],
) -> tuple[list[T], list[T]]:
    """
    Partition iterable into (true_items, false_items).

    Example:
        >>> partition(lambda x: x % 2, range(5))
        ([1, 3], [0, 2, 4])
    """
    true_vals, false_vals = [], []
    for item in iterable:
        if pred(item):
            true_vals.append(item)
        else:
            false_vals.append(item)
    return true_vals, false_vals


def split_at(
    n: int,
    iterable: Iterator[T],
) -> tuple[list[T], list[T]]:
    """Split at index n."""
    items = list(iterable)
    return items[:n], items[n:]


def flatten(nested: Iterator[Iterator[T]]) -> Generator[T, None, None]:
    """Flatten one level of nesting."""
    for item in nested:
        yield from item


def powerset(iterable: Iterator[T]) -> Generator[tuple[T, ...], None, None]:
    """
    Yield all possible subsets.

    Example:
        >>> list(powerset([1, 2, 3]))
        [(), (1,), (2,), (3,), (1, 2), (1, 3), (2, 3), (1, 2, 3)]
    """
    items = list(iterable)
    for r in range(len(items) + 1):
        for combo in itertools.combinations(items, r):
            yield combo


def iterate(func: Callable[[T], T], start: T) -> Generator[T, None, None]:
    """
    Generate infinite sequence: start, f(start), f(f(start)), ...

    Example:
        >>> take(5, iterate(lambda x: x*2, 1))
        [1, 2, 4, 8, 16]
    """
    cur = start
    while True:
        yield cur
        cur = func(cur)


def repeat_func(func: Callable[[], T]) -> Generator[T, None, None]:
    """Yield repeated calls to func."""
    while True:
        yield func()


def consume(iterator: Iterator, n: int | None = None) -> None:
    """Advance iterator by n steps, discarding values."""
    if n is None:
        collections.deque(iterator, maxlen=0)
    else:
        next(itertools.islice(iterator, n, n), None)


def nth_combination(
    elements: list[T],
    n: int,
    k: int,
) -> tuple[T, ...]:
    """
    Get k-th combination (0-indexed) without generating all.

    Example:
        >>> nth_combination([1,2,3,4], 4, 2)
        (3, 4)
    """
    return tuple(itertools.combinations(elements, n)[k])
