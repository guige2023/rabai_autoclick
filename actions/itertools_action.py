"""itertools action extensions for rabai_autoclick.

Provides utilities for working with iterators and itertools,
including advanced iteration patterns and combinations.
"""

from __future__ import annotations

import itertools
from typing import (
    Any,
    Callable,
    Iterable,
    Iterator,
    TypeVar,
)

__all__ = [
    "chain",
    "islice",
    "count",
    "cycle",
    "repeat",
    "accumulate",
    "compress",
    "filterfalse",
    "takewhile",
    "dropwhile",
    "zip_longest",
    "pairwise",
    "groupby",
    "tee",
    "product",
    "permutations",
    "combinations",
    "combinations_with_replacement",
    "chunked",
    "windowed",
    "sliding_window",
    "grouper",
    "flatten_iterable",
    "unique_iter",
    "unique_everseen",
    "intersperse",
    "collage",
    "partition",
    "first",
    "last",
    "nth",
    "take",
    "skip",
    "head",
    "tail",
    "consume",
    "padnone",
    "ncycles",
    "all_equal",
    "consecutive_groups",
    "duplicate_elements",
    "iterate",
    "flatten",
    "broader",
    "interleave",
    "flatten_dict",
    "IteratorCache",
]


T = TypeVar("T")
T2 = TypeVar("T2")


def chain(*iterables: Iterable) -> Iterator:
    """Chain iterables together.

    Args:
        *iterables: Iterables to chain.

    Returns:
        Iterator over all items.
    """
    return itertools.chain(*iterables)


def chunked(iterable: Iterable[T], n: int) -> Iterator[list[T]]:
    """Split iterable into chunks of size n.

    Args:
        iterable: Items to chunk.
        n: Chunk size.

    Yields:
        Lists of n items.
    """
    return iter(lambda: list(itertools.islice(iterable, n)), [])


def windowed(iterable: Iterable, n: int, step: int = 1) -> Iterator[tuple]:
    """Create sliding window.

    Args:
        iterable: Items to window.
        n: Window size.
        step: Step between windows.

    Yields:
        Tuples of window size.
    """
    iters = itertools.tee(iterable, n)
    for i, it in enumerate(iters):
        next(itertools.islice(it, i, i), None)
    return itertools.starmap(zip, zip(*[islide(it, n) for it, islide in zip(iters, [itertools.islice]*n)]))


def islide(iterable: Iterable, n: int) -> Iterator:
    """islice with step 1."""
    return itertools.islice(iterable, n, None)


def sliding_window(iterable: Iterable[T], size: int) -> Iterator[list[T]]:
    """Sliding window of size.

    Args:
        iterable: Items to window.
        size: Window size.

    Yields:
        Lists of window size.
    """
    it = iter(iterable)
    window = list(itertools.islice(it, size))
    if len(window) == size:
        yield window
    for item in it:
        window = window[1:] + [item]
        yield window


def grouper(iterable: Iterable, n: int, fillvalue: Any = None) -> Iterator:
    """Group items in tuples of n.

    Args:
        iterable: Items to group.
        n: Group size.
        fillvalue: Value for incomplete groups.

    Yields:
        Tuples of n items.
    """
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)


def flatten_iterable(nested: Iterable[Iterable[T]]) -> Iterator[T]:
    """Flatten nested iterables.

    Args:
        nested: Nested iterable structure.

    Yields:
        Flattened items.
    """
    for item in nested:
        if isinstance(item, (list, tuple, set)):
            yield from flatten_iterable(item)
        else:
            yield item


def unique_iter(iterable: Iterable, /) -> Iterator:
    """Yield unique items in order.

    Args:
        iterable: Items to dedupe.

    Yields:
        Unique items.
    """
    seen = set()
    for item in iterable:
        if item not in seen:
            seen.add(item)
            yield item


def unique_everseen(iterable: Iterable, /, key: Callable | None = None) -> Iterator:
    """Yield unique unseen items.

    Args:
        iterable: Items to dedupe.
        key: Optional key function.

    Yields:
        Unique items.
    """
    seen = set()
    for item in iterable:
        k = key(item) if key else item
        if k not in seen:
            seen.add(k)
            yield item


def intersperse(iterable: Iterable[T], element: T) -> Iterator[T]:
    """Intersperse element between items.

    Args:
        iterable: Items to intersperse.
        element: Element to insert.

    Yields:
        Items with interspersed element.
    """
    it = iter(iterable)
    yield next(it)
    for item in it:
        yield element
        yield item


def collage(iterable: Iterable[Iterable[T]]) -> Iterator[T]:
    """Interleave multiple iterables.

    Args:
        iterable: Iterable of iterables.

    Yields:
        Items interleaved round-robin.
    """
    iterators = [iter(it) for it in iterable]
    for it in itertools.cycle(iterators):
        try:
            yield next(it)
        except StopIteration:
            break


def take(n: int, iterable: Iterable[T]) -> list[T]:
    """Take first n items.

    Args:
        n: Number of items.
        iterable: Items to take.

    Returns:
        List of first n items.
    """
    return list(itertools.islice(iterable, n))


def skip(n: int, iterable: Iterable[T]) -> Iterator[T]:
    """Skip first n items.

    Args:
        n: Number to skip.
        iterable: Items to process.

    Returns:
        Iterator starting after skip.
    """
    return itertools.islice(iterable, n, None)


def head(iterable: Iterable, n: int = 5) -> list[T]:
    """Get head of iterable.

    Args:
        iterable: Items to take.
        n: Head size.

    Returns:
        First n items.
    """
    return take(n, iterable)


def tail(iterable: Iterable, n: int = 5) -> list[T]:
    """Get tail of iterable.

    Args:
        iterable: Items to process.
        n: Tail size.

    Returns:
        Last n items.
    """
    return list(itertools.islice(iterable, n))


def consume(iterator: Iterator, n: int | None = None) -> None:
    """Consume iterator.

    Args:
        iterator: Iterator to consume.
        n: Number of items (None = all).
    """
    if n is None:
        collections.deque(iterator, maxlen=0)
    else:
        next(itertools.islice(iterator, n, n), None)


def padnone(iterable: Iterable[T]) -> Iterator[T | None]:
    """Pad iterable with None.

    Args:
        iterable: Items to pad.

    Yields:
        Items then None indefinitely.
    """
    for item in iterable:
        yield item
    while True:
        yield None


def ncycles(iterable: Iterable[T], n: int) -> Iterator[T]:
    """Repeat iterable n times.

    Args:
        iterable: Items to repeat.
        n: Number of cycles.

    Yields:
        Items repeated n times.
    """
    for _ in range(n):
        yield from iterable


def all_equal(iterable: Iterable) -> bool:
    """Check if all items are equal.

    Args:
        iterable: Items to check.

    Returns:
        True if all equal.
    """
    it = iter(iterable)
    try:
        first = next(it)
    except StopIteration:
        return True
    return all(first == item for item in it)


def consecutive_groups(iterable: Iterable[int]) -> Iterator[list[int]]:
    """Group consecutive integers.

    Args:
        iterable: Integers to group.

    Yields:
        Lists of consecutive integers.
    """
    for k, g in itertools.groupby(enumerate(iterable), lambda x: x[0] - x[1]):
        yield [item for _, item in g]


def duplicate_elements(iterable: Iterable[T]) -> Iterator[T]:
    """Yield elements that appear more than once.

    Args:
        iterable: Items to check.

    Yields:
        Duplicate items.
    """
    seen = set()
    duplicates = set()
    for item in iterable:
        if item in seen:
            duplicates.add(item)
        seen.add(item)
    for item in duplicates:
        yield item


def iterate(func: Callable[[T], T], start: T) -> Iterator[T]:
    """Iterate applying func repeatedly.

    Args:
        func: Function to apply.
        start: Starting value.

    Yields:
        start, func(start), func(func(start)), ...
    """
    while True:
        yield start
        start = func(start)


def flatten(nested: Iterable) -> Iterator:
    """Recursively flatten nested structures.

    Args:
        nested: Nested iterable.

    Yields:
        Flattened items.
    """
    for item in nested:
        if isinstance(item, (list, tuple, set)):
            yield from flatten(item)
        else:
            yield item


def interleave(*iterables: Iterable[T]) -> Iterator[T]:
    """Round-robin interleave iterables.

    Args:
        *iterables: Iterables to interleave.

    Yields:
        Items interleaved.
    """
    iterators = [iter(it) for it in iterables]
    for it in itertools.cycle(iterators):
        try:
            yield next(it)
        except StopIteration:
            pass


def flatten_dict(d: dict, parent_key: str = "", sep: str = ".") -> dict:
    """Flatten nested dict.

    Args:
        d: Dict to flatten.
        parent_key: Prefix for keys.
        sep: Separator between levels.

    Returns:
        Flattened dict.
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def pairwise(iterable: Iterable[T]) -> Iterator[tuple[T, T]]:
    """Yield consecutive pairs.

    Args:
        iterable: Items to pair.

    Yields:
        Tuples of (item, next_item).
    """
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def first(iterable: Iterable[T], default: T | None = None) -> T | None:
    """Get first item or default.

    Args:
        iterable: Items to process.
        default: Default if empty.

    Returns:
        First item or default.
    """
    return next(iter(iterable), default)


def last(iterable: Iterable[T], default: T | None = None) -> T | None:
    """Get last item or default.

    Args:
        iterable: Items to process.
        default: Default if empty.

    Returns:
        Last item or default.
    """
    item = default
    for item in iterable:
        pass
    return item


def nth(iterable: Iterable[T], n: int, default: T | None = None) -> T | None:
    """Get nth item or default.

    Args:
        iterable: Items to process.
        n: Index to get.
        default: Default if out of range.

    Returns:
        nth item or default.
    """
    return next(itertools.islice(iterable, n, n + 1), default)


def partition(
    iterable: Iterable,
    predicate: Callable[[Any], bool],
) -> tuple[Iterator, Iterator]:
    """Partition iterable by predicate.

    Args:
        iterable: Items to partition.
        predicate: Function returning True for first partition.

    Returns:
        Tuple of (matching, non-matching) iterators.
    """
    t1, t2 = itertools.tee(iterable)
    return filter(predicate, t1), filterfalse(predicate, t2)


class IteratorCache(Generic[T]):
    """Cache iterator results."""

    def __init__(self, iterable: Iterable[T]) -> None:
        self._iter = iter(iterable)
        self._cache: list[T] = []

    def __iter__(self) -> Iterator[T]:
        for item in self._cache:
            yield item
        for item in self._iter:
            self._cache.append(item)
            yield item

    def peek(self, n: int = 1) -> list[T]:
        """Peek at next n items without consuming.

        Args:
            n: Number of items.

        Returns:
            List of next n items.
        """
        while len(self._cache) < n:
            try:
                self._cache.append(next(self._iter))
            except StopIteration:
                break
        return self._cache[:n]

    def get(self) -> T | None:
        """Get next item.

        Returns:
            Next item or None.
        """
        if self._cache:
            return self._cache.pop(0)
        try:
            return next(self._iter)
        except StopIteration:
            return None


import collections
