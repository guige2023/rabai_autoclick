"""Iterator utilities for RabAI AutoClick.

Provides:
- Iterator helpers and transformations
- Infinite sequence generators
- Accumulation utilities
"""

import itertools
import random
from typing import (
    Any,
    Callable,
    Iterable,
    Iterator,
    List,
    Optional,
    TypeVar,
    Generic,
)


T = TypeVar("T")
U = TypeVar("U")


def first(iterable: Iterable[T]) -> Optional[T]:
    """Get first item from iterable.

    Args:
        iterable: Iterable to get first from.

    Returns:
        First item or None.
    """
    return next(iter(iterable), None)


def second(iterable: Iterable[T]) -> Optional[T]:
    """Get second item from iterable.

    Args:
        iterable: Iterable to get second from.

    Returns:
        Second item or None.
    """
    items = list(iter(iterable))
    return items[1] if len(items) > 1 else None


def last(iterable: Iterable[T]) -> Optional[T]:
    """Get last item from iterable.

    Args:
        iterable: Iterable to get last from.

    Returns:
        Last item or None.
    """
    item = None
    for item in iterable:
        pass
    return item


def nth(iterable: Iterable[T], n: int) -> Optional[T]:
    """Get nth item from iterable.

    Args:
        iterable: Iterable to get from.
        n: Index (0-based).

    Returns:
        Nth item or None.
    """
    return next(itertools.islice(iterable, n, n + 1), None)


def take(iterable: Iterable[T], n: int) -> List[T]:
    """Take first n items.

    Args:
        iterable: Iterable to take from.
        n: Number of items.

    Returns:
        List of first n items.
    """
    return list(itertools.islice(iterable, n))


def drop(iterable: Iterable[T], n: int) -> Iterator[T]:
    """Drop first n items.

    Args:
        iterable: Iterable to drop from.
        n: Number of items.

    Returns:
        Iterator starting after n items.
    """
    return itertools.islice(iterable, n, None)


def chunked(iterable: Iterable[T], size: int) -> Iterator[List[T]]:
    """Chunk iterable into batches.

    Args:
        iterable: Iterable to chunk.
        size: Chunk size.

    Yields:
        Chunks of items.
    """
    it = iter(iterable)
    while True:
        batch = list(itertools.islice(it, size))
        if not batch:
            break
        yield batch


def windowed(iterable: Iterable[T], size: int) -> Iterator[List[T]]:
    """Create sliding windows.

    Args:
        iterable: Iterable to window.
        size: Window size.

    Yields:
        Windows of items.
    """
    return windowed_(iterable, size)


def windowed_(iterable: Iterable[T], size: int) -> Iterator[List[T]]:
    """Create sliding windows (alternative implementation).

    Args:
        iterable: Iterable to window.
        size: Window size.

    Yields:
        Windows of items.
    """
    iters = itertools.tee(iterable, size)
    for i, it in enumerate(iters):
        next(itertools.islice(it, i, i), None)
    return zip(*iters)


def flatten(iterable: Iterable[Iterable[T]]) -> Iterator[T]:
    """Flatten nested iterables.

    Args:
        iterable: Nested iterables.

    Yields:
        Flattened items.
    """
    for item in iterable:
        for subitem in item:
            yield subitem


def flatten_once(iterable: Iterable[Iterable[T]]) -> Iterator[T]:
    """Flatten one level.

    Args:
        iterable: Nested iterables.

    Yields:
        Flattened items.
    """
    return itertools.chain.from_iterable(iterable)


def map_filter(func: Callable[[T], Optional[U]], iterable: Iterable[T]) -> Iterator[U]:
    """Map and filter None values.

    Args:
        func: Function returning Optional.
        iterable: Iterable to process.

    Yields:
        Non-None results.
    """
    for item in iterable:
        result = func(item)
        if result is not None:
            yield result


def interleave(*iterables: Iterable[T]) -> Iterator[T]:
    """Interleave iterables.

    Args:
        *iterables: Iterables to interleave.

    Yields:
        Interleaved items.
    """
    return flatten(zip(*iterables))


def intersperse(iterable: Iterable[T], value: T) -> Iterator[T]:
    """Insert value between items.

    Args:
        iterable: Iterable to intersperse.
        value: Value to insert.

    Yields:
        Items with value interspersed.
    """
    it = iter(iterable)
    yield next(it, None)
    for item in it:
        yield value
        yield item


def iterate(func: Callable[[T], T], initial: T) -> Iterator[T]:
    """Generate sequence by applying function iteratively.

    Args:
        func: Function to apply.
        initial: Initial value.

    Yields:
        Infinite sequence.
    """
    current = initial
    while True:
        yield current
        current = func(current)


def repeat(value: T, times: Optional[int] = None) -> Iterator[T]:
    """Repeat value.

    Args:
        value: Value to repeat.
        times: Optional number of times.

    Yields:
        Repeated values.
    """
    if times is None:
        return itertools.repeat(value)
    return itertools.repeat(value, times)


def cycle(iterable: Iterable[T]) -> Iterator[T]:
    """Cycle through iterable.

    Args:
        iterable: Iterable to cycle.

    Yields:
        Cycled items.
    """
    return itertools.cycle(iterable)


def count(start: int = 0, step: int = 1) -> Iterator[int]:
    """Generate counting sequence.

    Args:
        start: Starting number.
        step: Step size.

    Yields:
        Counting numbers.
    """
    return itertools.count(start, step)


def range_(start: int, stop: Optional[int] = None, step: int = 1) -> Iterator[int]:
    """Generate range sequence.

    Args:
        start: Start value.
        stop: Stop value (exclusive).
        step: Step size.

    Yields:
        Range values.
    """
    if stop is None:
        return itertools.count(start, step)
    return itertools.islice(itertools.count(start, step), stop - start)


def every_other(iterable: Iterable[T]) -> Iterator[T]:
    """Get every other item.

    Args:
        iterable: Iterable to process.

    Yields:
        Every other item.
    """
    it = iter(iterable)
    yield next(it, None)
    for item in it:
        next(it, None)
        yield item


def distinct(iterable: Iterable[T]) -> Iterator[T]:
    """Get distinct items preserving order.

    Args:
        iterable: Iterable to process.

    Yields:
        Distinct items.
    """
    seen = set()
    for item in iterable:
        if item not in seen:
            seen.add(item)
            yield item


def duplicates(iterable: Iterable[T]) -> Iterator[T]:
    """Find duplicate items.

    Args:
        iterable: Iterable to process.

    Yields:
        Items that appear more than once.
    """
    seen = set()
    seen_again = set()
    for item in iterable:
        if item in seen:
            seen_again.add(item)
        seen.add(item)
    for item in seen_again:
        yield item


def partition(
    iterable: Iterable[T],
    predicate: Callable[[T], bool]
) -> tuple:
    """Partition iterable by predicate.

    Args:
        iterable: Iterable to partition.
        predicate: Function returning True/False.

    Returns:
        Tuple of (matching, non-matching).
    """
    matching = []
    non_matching = []
    for item in iterable:
        if predicate(item):
            matching.append(item)
        else:
            non_matching.append(item)
    return matching, non_matching


def group_by(
    iterable: Iterable[T],
    key_func: Callable[[T], Any]
) -> dict:
    """Group items by key.

    Args:
        iterable: Iterable to group.
        key_func: Function to extract key.

    Returns:
        Dict mapping keys to item lists.
    """
    result = {}
    for item in iterable:
        key = key_func(item)
        if key not in result:
            result[key] = []
        result[key].append(item)
    return result


def reduce_(
    func: Callable[[T, T], T],
    iterable: Iterable[T],
    initial: Optional[T] = None
) -> T:
    """Reduce iterable to single value.

    Args:
        func: Reduction function.
        iterable: Iterable to reduce.
        initial: Initial value.

    Returns:
        Reduced value.
    """
    it = iter(iterable)
    if initial is None:
        try:
            result = next(it)
        except StopIteration:
            raise ValueError("Cannot reduce empty iterable without initial")
    else:
        result = initial
    for item in it:
        result = func(result, item)
    return result


def accumulate(
    iterable: Iterable[T],
    func: Callable[[T, T], T] = None
) -> Iterator[T]:
    """Accumulate values.

    Args:
        iterable: Iterable to accumulate.
        func: Optional accumulation function.

    Yields:
        Accumulated values.
    """
    if func is None:
        return itertools.accumulate(iterable)
    return itertools.accumulate(iterable, func)


def sum_(iterable: Iterable[T]) -> T:
    """Sum all items.

    Args:
        iterable: Iterable to sum.

    Returns:
        Sum of items.
    """
    return sum(iterable)


def product(*iterables: Iterable[T]) -> Iterator[tuple]:
    """Cartesian product.

    Args:
        *iterables: Iterables to product.

    Yields:
        Tuples of cartesian product.
    """
    return itertools.product(*iterables)


def permutations(iterable: Iterable[T], r: int = None) -> Iterator[tuple]:
    """Generate permutations.

    Args:
        iterable: Iterable to permute.
        r: Length of permutations.

    Yields:
        Permutations.
    """
    items = list(iterable)
    return itertools.permutations(items, r)


def combinations(iterable: Iterable[T], r: int) -> Iterator[tuple]:
    """Generate combinations.

    Args:
        iterable: Iterable to combine.
        r: Length of combinations.

    Yields:
        Combinations.
    """
    items = list(iterable)
    return itertools.combinations(items, r)


def combinations_with_replacement(iterable: Iterable[T], r: int) -> Iterator[tuple]:
    """Generate combinations with replacement.

    Args:
        iterable: Iterable to combine.
        r: Length of combinations.

    Yields:
        Combinations.
    """
    items = list(iterable)
    return itertools.combinations_with_replacement(items, r)


def powerset(iterable: Iterable[T]) -> Iterator[tuple]:
    """Generate all subsets.

    Args:
        iterable: Iterable to process.

    Yields:
        All subsets.
    """
    items = list(iterable)
    for r in range(len(items) + 1):
        yield from itertools.combinations(items, r)


def random_choice(iterable: Iterable[T]) -> T:
    """Choose random item.

    Args:
        iterable: Iterable to choose from.

    Returns:
        Random item.
    """
    items = list(iterable)
    return random.choice(items)


def random_sample(iterable: Iterable[T], n: int) -> List[T]:
    """Sample n items randomly.

    Args:
        iterable: Iterable to sample.
        n: Number of samples.

    Returns:
        List of samples.
    """
    items = list(iterable)
    return random.sample(items, min(n, len(items)))


def shuffled(iterable: Iterable[T]) -> List[T]:
    """Return shuffled list.

    Args:
        iterable: Iterable to shuffle.

    Returns:
        Shuffled list.
    """
    items = list(iterable)
    random.shuffle(items)
    return items


def reversed_(iterable: Iterable[T]) -> List[T]:
    """Return reversed list.

    Args:
        iterable: Iterable to reverse.

    Returns:
        Reversed list.
    """
    return list(iterable)[::-1]


def sorted_by(
    iterable: Iterable[T],
    key: Callable[[T], Any],
    reverse: bool = False
) -> List[T]:
    """Sort by key function.

    Args:
        iterable: Iterable to sort.
        key: Sort key function.
        reverse: Sort descending.

    Returns:
        Sorted list.
    """
    return sorted(iterable, key=key, reverse=reverse)


def unique_justseen(iterable: Iterable[T]) -> Iterator[T]:
    """Remove consecutive duplicates.

    Args:
        iterable: Iterable to process.

    Yields:
        Items with no consecutive duplicates.
    """
    return itertools.groupby(iterable, key=lambda x: x)


def head(iterable: Iterable[T], n: int = 5) -> List[T]:
    """Get first n items (alias for take).

    Args:
        iterable: Iterable to process.
        n: Number of items.

    Returns:
        First n items.
    """
    return take(iterable, n)


def tail(iterable: Iterable[T], n: int = 5) -> List[T]:
    """Get last n items.

    Args:
        iterable: Iterable to process.
        n: Number of items.

    Returns:
        Last n items.
    """
    items = list(iterable)
    return items[-n:] if len(items) > n else items