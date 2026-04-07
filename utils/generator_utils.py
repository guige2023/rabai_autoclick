"""Generator and iterator utilities for RabAI AutoClick.

Provides:
- Infinite sequence generators
- Batched/chunked iteration
- Windowed iteration (sliding window)
- Filtered iteration
- Generator combinators
- Async generators
"""

from __future__ import annotations

import asyncio
import itertools
from typing import (
    Any,
    Awaitable,
    Callable,
    Generic,
    Iterator,
    List,
    Optional,
    TypeVar,
    Union,
)


T = TypeVar("T")
U = TypeVar("U")


def chunk(iterable: Iterator[T], size: int) -> Iterator[List[T]]:
    """Split iterable into chunks of specified size.

    Args:
        iterable: Input iterable.
        size: Chunk size (must be > 0).

    Yields:
        Lists of up to size elements.

    Example:
        list(chunk(range(10), 3))
        # [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    """
    if size <= 0:
        raise ValueError(f"Chunk size must be positive, got {size}")

    iterator = iter(iterable)
    while True:
        chunk_list = list(itertools.islice(iterator, size))
        if not chunk_list:
            break
        yield chunk_list


def window(
    iterable: Iterator[T],
    size: int,
    fill: Optional[T] = None,
) -> Iterator[List[T]]:
    """Sliding window over iterable.

    Args:
        iterable: Input iterable.
        size: Window size (must be > 0).
        fill: Optional value to fill incomplete windows at the end.

    Yields:
        Lists of size elements (last may be shorter with fill=None).

    Example:
        list(window(range(5), 3))
        # [[0, 1, 2], [1, 2, 3], [2, 3, 4]]
    """
    if size <= 0:
        raise ValueError(f"Window size must be positive, got {size}")

    window_list: List[T] = []

    for item in iterable:
        window_list.append(item)
        if len(window_list) == size:
            yield window_list[:]
            window_list.pop(0)
        elif len(window_list) < size and fill is None:
            pass
        elif len(window_list) < size and fill is not None:
            while len(window_list) < size:
                window_list.append(fill)

    if window_list and fill is not None:
        while len(window_list) < size:
            window_list.append(fill)
        if len(window_list) == size:
            yield window_list


def batch(
    iterable: Iterator[T],
    batch_size: int,
    allow_partial: bool = True,
) -> Iterator[List[T]]:
    """Batch items into groups.

    Args:
        iterable: Input iterable.
        batch_size: Number of items per batch.
        allow_partial: If True, yield partial last batch.

    Yields:
        Lists of batch_size elements.

    Example:
        list(batch(range(7), 3))
        # [[0, 1, 2], [3, 4, 5], [6]]
    """
    batch_list: List[T] = []
    for item in iterable:
        batch_list.append(item)
        if len(batch_list) == batch_size:
            yield batch_list
            batch_list = []
    if allow_partial and batch_list:
        yield batch_list


def filter_none(iterable: Iterator[Optional[T]]) -> Iterator[T]:
    """Filter out None values from iterable.

    Args:
        iterable: Input iterable that may contain None.

    Yields:
        Non-None values.

    Example:
        list(filter_none([1, None, 2, None, 3]))
        # [1, 2, 3]
    """
    for item in iterable:
        if item is not None:
            yield item


def filter_falsy(iterable: Iterator[T]) -> Iterator[T]:
    """Filter out falsy values from iterable.

    Args:
        iterable: Input iterable.

    Yields:
        Truthy values.

    Example:
        list(filter_falsy([0, 1, False, 2, '', 3]))
        # [1, 2, 3]
    """
    for item in iterable:
        if item:
            yield item


def map_partial(
    func: Callable[[T], U],
    iterable: Iterator[T],
    error_handler: Optional[Callable[[T, Exception], U]] = None,
) -> Iterator[U]:
    """Map function over iterable, handling errors gracefully.

    Args:
        func: Function to apply.
        iterable: Input iterable.
        error_handler: Optional (item, error) -> replacement.

    Yields:
        Transformed values.
    """
    for item in iterable:
        try:
            yield func(item)
        except Exception as e:
            if error_handler is not None:
                yield error_handler(item, e)
            else:
                raise


def interleave(*iterables: Iterator[T]) -> Iterator[T]:
    """Interleave elements from multiple iterables.

    Args:
        *iterables: Iterables to interleave.

    Yields:
        Elements in round-robin fashion.

    Example:
        list(interleave([1, 2, 3], [4, 5, 6]))
        # [1, 4, 2, 5, 3, 6]
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


def flatten(nested: Iterator[Iterator[T]]) -> Iterator[T]:
    """Flatten nested iterables.

    Args:
        nested: Iterable of iterables.

    Yields:
        Individual elements.

    Example:
        list(flatten([[1, 2], [3, 4], [5]]))
        # [1, 2, 3, 4, 5]
    """
    for item in nested:
        for sub_item in item:
            yield sub_item


def take(n: int, iterable: Iterator[T]) -> List[T]:
    """Take first n elements from iterable.

    Args:
        n: Number of elements to take.
        iterable: Input iterable.

    Returns:
        List of up to n elements.
    """
    return list(itertools.islice(iterable, n))


def drop(n: int, iterable: Iterator[T]) -> Iterator[T]:
    """Skip first n elements of iterable.

    Args:
        n: Number of elements to skip.
        iterable: Input iterable.

    Yields:
        Elements after first n.
    """
    return itertools.islice(iterable, n, None)


def unique(
    iterable: Iterator[T],
    key: Optional[Callable[[T], Any]] = None,
) -> Iterator[T]:
    """Yield unique elements preserving order.

    Args:
        iterable: Input iterable.
        key: Optional function to extract comparison key.

    Yields:
        Unique elements.
    """
    seen: set = set()
    seen_add = seen.add
    for item in iterable:
        k = key(item) if key else item
        if k not in seen:
            seen_add(k)
            yield item


def unique_by(
    iterable: Iterator[T],
    key: Callable[[T], Any],
) -> Iterator[T]:
    """Yield elements unique by some key, preserving order.

    Args:
        iterable: Input iterable.
        key: Function to extract key.

    Yields:
        First occurrence of each unique key.
    """
    seen: set = set()
    for item in iterable:
        k = key(item)
        if k not in seen:
            seen.add(k)
            yield item


def frequencies(iterable: Iterator[T]) -> dict[T, int]:
    """Count frequency of each element.

    Args:
        iterable: Input iterable.

    Returns:
        Dictionary mapping element to count.
    """
    freq: dict[T, int] = {}
    for item in iterable:
        freq[item] = freq.get(item, 0) + 1
    return freq


def partition(
    iterable: Iterator[T],
    predicate: Callable[[T], bool],
) -> tuple[List[T], List[T]]:
    """Partition iterable into two lists by predicate.

    Args:
        iterable: Input iterable.
        predicate: Function that returns True for kept elements.

    Returns:
        Tuple of (matching, non-matching).
    """
    matching: List[T] = []
    non_matching: List[T] = []
    for item in iterable:
        if predicate(item):
            matching.append(item)
        else:
            non_matching.append(item)
    return matching, non_matching


def first(
    iterable: Iterator[T],
    default: Optional[T] = None,
) -> Optional[T]:
    """Get first element from iterable.

    Args:
        iterable: Input iterable.
        default: Value to return if iterable is empty.

    Returns:
        First element or default.
    """
    return next(iter(iterable), default)


def last(
    iterable: Iterator[T],
    default: Optional[T] = None,
) -> Optional[T]:
    """Get last element from iterable.

    Args:
        iterable: Input iterable.
        default: Value to return if iterable is empty.

    Returns:
        Last element or default.
    """
    item = default
    for item in iterable:
        pass
    return item


def nth(n: int, iterable: Iterator[T], default: Optional[T] = None) -> Optional[T]:
    """Get nth element from iterable.

    Args:
        n: Zero-based index.
        iterable: Input iterable.
        default: Value to return if index out of bounds.

    Returns:
        Nth element or default.
    """
    return next(itertools.islice(iterable, n, n + 1), default)


def grouper(
    iterable: Iterator[T],
    n: int,
    fillvalue: Optional[T] = None,
) -> Iterator[List[T]]:
    """Group items into tuples of size n.

    Args:
        iterable: Input iterable.
        n: Tuple size.
        fillvalue: Value to fill incomplete tuples.

    Yields:
        Tuples of n elements.
    """
    return itertools.zip_longest(
        *[iter(iterable)] * n,
        fillvalue=fillvalue,  # type: ignore
    )


def sliding_window(
    iterable: Iterator[T],
    size: int,
) -> Iterator[tuple[T, ...]]:
    """Sliding window as tuples.

    Args:
        iterable: Input iterable.
        size: Window size.

    Yields:
        Tuples of size elements.
    """
    it = iter(iterable)
    window = tuple(itertools.islice(it, size))
    if len(window) == size:
        yield window
    for item in it:
        window = window[1:] + (item,)
        yield window


def iterate(func: Callable[[T], T], initial: T) -> Iterator[T]:
    """Generate infinite sequence by repeated application.

    Args:
        func: Function to apply repeatedly.
        initial: Starting value.

    Yields:
        initial, func(initial), func(func(initial)), ...

    Example:
        list(iterate(lambda x: x * 2, 1))[:5]
        # [1, 2, 4, 8, 16]
    """
    current = initial
    while True:
        yield current
        current = func(current)


def cycle(iterable: Iterator[T], count: Optional[int] = None) -> Iterator[T]:
    """Cycle through iterable infinitely or count times.

    Args:
        iterable: Input iterable.
        count: Optional number of cycles.

    Yields:
        Elements in cycles.
    """
    if count is None:
        yield from itertools.cycle(iterable)
    else:
        for _ in range(count):
            yield from iterable


async def async_map(
    func: Callable[[T], Awaitable[U]],
    items: List[T],
    max_concurrency: int = 10,
) -> List[U]:
    """Map async function over items with concurrency limit.

    Args:
        func: Async function to apply.
        items: Items to process.
        max_concurrency: Maximum concurrent tasks.

    Returns:
        List of results in same order as items.
    """
    semaphore = asyncio.Semaphore(max_concurrency)

    async def process(item: T) -> U:
        async with semaphore:
            return await func(item)

    return await asyncio.gather(*(process(item) for item in items))


def range_infinite(
    start: float = 0,
    step: float = 1,
) -> Iterator[float]:
    """Infinite range-like sequence.

    Args:
        start: Starting value.
        step: Step size.

    Yields:
        start, start+step, start+2*step, ...
    """
    current = start
    while True:
        yield current
        current += step
