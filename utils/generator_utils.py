"""Generator utilities for RabAI AutoClick.

Provides:
- Generator helpers and transformations
- Batching and chunking
- Pipeline utilities
- Infinite generator constructors
"""

from __future__ import annotations

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

import itertools
import random


T = TypeVar("T")
U = TypeVar("U")
K = TypeVar("K")


def batched(
    iterable: Iterator[T],
    batch_size: int,
) -> Iterator[List[T]]:
    """Yield successive batches from an iterable.

    Args:
        iterable: Source iterator.
        batch_size: Size of each batch.

    Yields:
        Lists of up to batch_size items.
    """
    iterator = iter(iterable)
    while True:
        batch = list(itertools.islice(iterator, batch_size))
        if not batch:
            return
        yield batch


def chunked(
    items: List[T],
    size: int,
) -> Iterator[List[T]]:
    """Split a list into chunks of specified size.

    Args:
        items: Source list.
        size: Chunk size.

    Yields:
        Chunks.
    """
    for i in range(0, len(items), size):
        yield items[i : i + size]


def take(
    iterable: Iterator[T],
    n: int,
) -> List[T]:
    """Take the first n items from an iterable.

    Args:
        iterable: Source iterator.
        n: Number of items.

    Returns:
        List of first n items.
    """
    return list(itertools.islice(iterable, n))


def drop(
    iterable: Iterator[T],
    n: int,
) -> Iterator[T]:
    """Skip the first n items from an iterable.

    Args:
        iterable: Source iterator.
        n: Number of items to skip.

    Yields:
        Remaining items after skipping.
    """
    return itertools.islice(iterable, n, None)


def flatten(
    nested: Iterator[Iterator[T]],
) -> Iterator[T]:
    """Flatten a nested iterable of iterables.

    Args:
        nested: Iterable of iterables.

    Yields:
        Flattened items.
    """
    for inner in nested:
        yield from inner


def map_gen(
    func: Callable[[T], U],
    iterable: Iterator[T],
) -> Iterator[U]:
    """Map a function over an iterable (generator version).

    Args:
        func: Transformation function.
        iterable: Source iterator.

    Yields:
        Transformed items.
    """
    for item in iterable:
        yield func(item)


def filter_gen(
    predicate: Callable[[T], bool],
    iterable: Iterator[T],
) -> Iterator[T]:
    """Filter an iterable by predicate (generator version).

    Args:
        predicate: Keep items that return True.
        iterable: Source iterator.

    Yields:
        Filtered items.
    """
    for item in iterable:
        if predicate(item):
            yield item


def unique_gen(
    iterable: Iterator[T],
) -> Iterator[T]:
    """Yield unique items from an iterable preserving order.

    Args:
        iterable: Source iterator.

    Yields:
        Unique items.
    """
    seen: set = set()
    for item in iterable:
        if item not in seen:
            seen.add(item)
            yield item


def interleave(
    *iterables: Iterator[T],
) -> Iterator[T]:
    """Interleave multiple iterables round-robin.

    Args:
        *iterables: Iterables to interleave.

    Yields:
        Items interleaved.
    """
    iterators = [iter(it) for it in iterables]
    while iterators:
        next_iterators: List[Iterator[T]] = []
        for it in iterators:
            try:
                yield next(it)
                next_iterators.append(it)
            except StopIteration:
                pass
        iterators = next_iterators


def interpose(
    separator: T,
    iterable: Iterator[T],
) -> Iterator[T]:
    """Yield items with a separator between each.

    Args:
        separator: Value to insert between items.
        iterable: Source iterator.

    Yields:
        Items with separators.
    """
    iterator = iter(iterable)
    try:
        yield next(iterator)
    except StopIteration:
        return
    for item in iterator:
        yield separator
        yield item


def partition_gen(
    iterable: Iterator[T],
    predicate: Callable[[T], bool],
) -> Tuple[Iterator[T], Iterator[T]]:
    """Partition an iterable into (matching, non-matching) iterators.

    Args:
        iterable: Source iterator.
        predicate: Function that returns True for first group.

    Returns:
        Tuple of (yes_iterator, no_iterator).
    """
    def yes_gen() -> Iterator[T]:
        for item in iterable:
            if predicate(item):
                yield item

    def no_gen() -> Iterator[T]:
        for item in iterable:
            if not predicate(item):
                yield item

    return yes_gen(), no_gen()


def sliding_window_gen(
    iterable: Iterator[T],
    n: int,
) -> Iterator[List[T]]:
    """Yield sliding windows of size n.

    Args:
        iterable: Source iterator.
        n: Window size.

    Yields:
        Lists of n consecutive items.
    """
    window: List[T] = []
    for item in iterable:
        window.append(item)
        if len(window) == n:
            yield window.copy()
            window.pop(0)


def repeat_gen(
    value: T,
    times: Optional[int] = None,
) -> Iterator[T]:
    """Generate a value repeatedly.

    Args:
        value: Value to repeat.
        times: Number of times. None for infinite.

    Yields:
        Repeated values.
    """
    if times is None:
        while True:
            yield value
    else:
        for _ in range(times):
            yield value


def cycle_gen(
    iterable: Iterator[T],
) -> Iterator[T]:
    """Cycle through an iterable indefinitely.

    Args:
        iterable: Source iterator.

    Yields:
        Cycled items.
    """
    for item in itertools.cycle(iterable):
        yield item


def random_sample_gen(
    population: List[T],
    k: int,
) -> Iterator[T]:
    """Yield a random sample of k items from population.

    Args:
        population: Source list.
        k: Sample size.

    Yields:
        Randomly sampled items.
    """
    pool = random.sample(population, min(k, len(population)))
    for item in pool:
        yield item


__all__ = [
    "batched",
    "chunked",
    "take",
    "drop",
    "flatten",
    "map_gen",
    "filter_gen",
    "unique_gen",
    "interleave",
    "interpose",
    "partition_gen",
    "sliding_window_gen",
    "repeat_gen",
    "cycle_gen",
    "random_sample_gen",
]
