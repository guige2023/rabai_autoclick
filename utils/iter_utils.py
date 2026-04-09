"""
Iterator and iterable utilities for UI automation.

Provides functions for working with iterables, generators,
batches, windows, and iteration patterns.
"""

from __future__ import annotations

import itertools
from typing import (
    TypeVar, Callable, Iterable, Iterator, 
    Optional, List, Any, Generic, Union,
    Sequence, Protocol, overload
)
from collections import deque
from functools import reduce


T = TypeVar('T')
U = TypeVar('U')
R = TypeVar('R')
K = TypeVar('K')
V = TypeVar('V')


def chunk(iterable: Iterable[T], size: int) -> Iterator[List[T]]:
    """Split iterable into chunks of specified size.
    
    Args:
        iterable: Source iterable
        size: Chunk size
    
    Yields:
        Chunks of size up to size
    
    Example:
        list(chunk([1, 2, 3, 4, 5], 2))  # [[1, 2], [3, 4], [5]]
    """
    it = iter(iterable)
    while True:
        batch = list(itertools.islice(it, size))
        if not batch:
            break
        yield batch


def window(
    iterable: Iterable[T], 
    size: int, 
    fill: Optional[T] = None
) -> Iterator[List[T]]:
    """Create sliding window over iterable.
    
    Args:
        iterable: Source iterable
        size: Window size
        fill: Value to pad incomplete windows
    
    Yields:
        Windows of size
    
    Example:
        list(window([1, 2, 3, 4], 3))  # [[1, 2, 3], [2, 3, 4]]
    """
    it = iter(iterable)
    win = deque(itertools.islice(it, size), maxlen=size)
    
    if len(win) < size and fill is not None:
        win.extend([fill] * (size - len(win)))
    
    yield list(win)
    
    for item in it:
        win.append(item)
        yield list(win)


def batch(iterable: Iterable[T], n: int) -> Iterator[List[T]]:
    """Alias for chunk with clearer name."""
    return chunk(iterable, n)


def partition(
    iterable: Iterable[T],
    predicate: Callable[[T], bool],
) -> tuple[List[T], List[T]]:
    """Partition iterable by predicate.
    
    Args:
        iterable: Source iterable
        predicate: Function returning True/False
    
    Returns:
        (matching, non_matching)
    
    Example:
        partition([1, 2, 3, 4], lambda x: x % 2 == 0)  # ([2, 4], [1, 3])
    """
    true_list: List[T] = []
    false_list: List[T] = []
    
    for item in iterable:
        if predicate(item):
            true_list.append(item)
        else:
            false_list.append(item)
    
    return (true_list, false_list)


def flatten(nested: Iterable[Iterable[T]]) -> Iterator[T]:
    """Flatten nested iterables.
    
    Args:
        nested: Iterable of iterables
    
    Yields:
        Flattened items
    
    Example:
        list(flatten([[1, 2], [3, 4]]))  # [1, 2, 3, 4]
    """
    for item in nested:
        for sub in item:
            yield sub


def flatten_depth(
    nested: Any,
    depth: int = 1,
) -> Iterator[Any]:
    """Flatten nested structure to specified depth.
    
    Args:
        nested: Nested structure
        depth: Depth to flatten (1 = shallow, -1 = full)
    
    Yields:
        Flattened items
    """
    for item in nested:
        if isinstance(item, (list, tuple)) and depth != 0:
            yield from flatten_depth(item, depth - 1)
        else:
            yield item


def groupby_key(
    iterable: Iterable[T],
    key: Callable[[T], K],
) -> Iterator[tuple[K, List[T]]]:
    """Group items by key function.
    
    Args:
        iterable: Source iterable
        key: Function to extract group key
    
    Yields:
        (key, items_in_group)
    
    Note:
        Requires iterable to be sorted by key for correct grouping
    """
    for k, group in itertools.groupby(iterable, key):
        yield (k, list(group))


def group_by(
    iterable: Iterable[T],
    key: Callable[[T], K],
) -> dict[K, List[T]]:
    """Group items by key into dict.
    
    Args:
        iterable: Source iterable
        key: Function to extract group key
    
    Returns:
        Dict mapping key -> list of items
    """
    result: dict[K, List[T]] = {}
    
    for item in iterable:
        k = key(item)
        if k not in result:
            result[k] = []
        result[k].append(item)
    
    return result


def unique(
    iterable: Iterable[T],
    key: Optional[Callable[[T], Any]] = None,
) -> Iterator[T]:
    """Yield unique items preserving order.
    
    Args:
        iterable: Source iterable
        key: Optional function to extract comparison key
    
    Yields:
        Unique items
    
    Example:
        list(unique([1, 2, 1, 3, 2]))  # [1, 2, 3]
    """
    seen: set[Any] = set()
    seen_add = seen.add
    
    for item in iterable:
        k = key(item) if key else item
        if k not in seen:
            seen_add(k)
            yield item


def unique_by(
    iterable: Iterable[T],
    key: Callable[[T], Any],
) -> Iterator[T]:
    """Yield unique items by key function."""
    return unique(iterable, key)


def deduplicate(
    iterable: Iterable[T],
    key: Optional[Callable[[T], Any]] = None,
) -> List[T]:
    """Remove duplicates preserving order.
    
    Args:
        iterable: Source iterable
        key: Optional comparison key function
    
    Returns:
        List with duplicates removed
    """
    return list(unique(iterable, key))


def interleave(*iterables: Iterable[T]) -> Iterator[T]:
    """Interleave multiple iterables.
    
    Args:
        *iterables: Iterables to interleave
    
    Yields:
        Items from each iterable in round-robin
    
    Example:
        list(interleave([1, 2], ['a', 'b']))  # [1, 'a', 2, 'b']
    """
    queues = [iter(it) for it in iterables]
    
    while queues:
        next_queue: List[Iterator[T]] = []
        
        for queue in queues:
            try:
                yield next(queue)
                next_queue.append(queue)
            except StopIteration:
                pass
        
        queues = next_queue


def intersperse(
    iterable: Iterable[T],
    value: T,
) -> Iterator[T]:
    """Insert value between items.
    
    Args:
        iterable: Source iterable
        value: Value to insert
    
    Yields:
        Items with value interspersed
    
    Example:
        list(intersperse([1, 2, 3], 0))  # [1, 0, 2, 0, 3]
    """
    it = iter(iterable)
    
    try:
        yield next(it)
    except StopIteration:
        return
    
    for item in it:
        yield value
        yield item


def take(n: int, iterable: Iterable[T]) -> List[T]:
    """Take first n items.
    
    Args:
        n: Number of items
        iterable: Source iterable
    
    Returns:
        List of first n items
    """
    return list(itertools.islice(iterable, n))


def take_while(
    predicate: Callable[[T], bool],
    iterable: Iterable[T],
) -> Iterator[T]:
    """Take items while predicate is True.
    
    Args:
        predicate: Function returning True to continue
        iterable: Source iterable
    
    Yields:
        Items until predicate fails
    """
    for item in iterable:
        if not predicate(item):
            break
        yield item


def drop(n: int, iterable: Iterable[T]) -> Iterator[T]:
    """Skip first n items.
    
    Args:
        n: Number of items to skip
        iterable: Source iterable
    
    Yields:
        Items after first n
    """
    return itertools.islice(iterable, n, None)


def drop_while(
    predicate: Callable[[T], bool],
    iterable: Iterable[T],
) -> Iterator[T]:
    """Skip items while predicate is True.
    
    Args:
        predicate: Function returning True to skip
        iterable: Source iterable
    
    Yields:
        Items after predicate fails
    """
    it = iter(iterable)
    
    for item in it:
        if not predicate(item):
            yield item
            break
    
    yield from it


def nth(n: int, iterable: Iterable[T], default: T = None) -> T:  # type: ignore
    """Get nth item or default.
    
    Args:
        n: Index (0-based)
        iterable: Source iterable
        default: Default if not found
    
    Returns:
        Item at index or default
    """
    return next(itertools.islice(iterable, n, n + 1), default)


def first(
    iterable: Iterable[T],
    default: T = None,  # type: ignore
) -> T:
    """Get first item or default."""
    return nth(0, iterable, default)


def second(
    iterable: Iterable[T],
    default: T = None,  # type: ignore
) -> T:
    """Get second item or default."""
    return nth(1, iterable, default)


def last(
    iterable: Iterable[T],
    default: T = None,  # type: ignore
) -> T:
    """Get last item or default."""
    item = default
    for item in iterable:
        pass
    return item


def collect(
    iterable: Iterable[T],
    factory: Callable[[], List[T]] = list,
) -> Union[List[T], set[T], Any]:
    """Collect iterable into collection.
    
    Args:
        iterable: Source iterable
        factory: Collection factory (default: list)
    
    Returns:
        Collection of items
    """
    return factory(iterable)


def for_each(func: Callable[[T], Any], iterable: Iterable[T]) -> None:
    """Execute function for each item.
    
    Args:
        func: Function to execute
        iterable: Source iterable
    """
    for item in iterable:
        func(item)


def map_filter(
    func: Callable[[T], Optional[U]],
    iterable: Iterable[T],
) -> Iterator[U]:
    """Map then filter None values.
    
    Args:
        func: Map function returning Optional
        iterable: Source iterable
    
    Yields:
        Non-None results
    
    Example:
        list(map_filter(lambda x: x * 2 if x > 0 else None, [1, -1, 2]))  # [2, 4]
    """
    for item in iterable:
        result = func(item)
        if result is not None:
            yield result


def reduce_with(
    func: Callable[[U, T], U],
    iterable: Iterable[T],
    initial: U,
) -> U:
    """Reduce with initial value."""
    return reduce(func, iterable, initial)


def scan(
    func: Callable[[T, T], T],
    iterable: Iterable[T],
    initial: Optional[T] = None,
) -> Iterator[T]:
    """Cumulative reduction.
    
    Args:
        func: Reduction function
        iterable: Source iterable
        initial: Initial value
    
    Yields:
        Cumulative results
    
    Example:
        list(scan(lambda x, y: x + y, [1, 2, 3, 4]))  # [1, 3, 6, 10]
    """
    it = iter(iterable)
    
    if initial is None:
        try:
            value = next(it)
        except StopIteration:
            return
        yield value
    else:
        value = initial
        yield value
    
    for item in it:
        value = func(value, item)
        yield value


def sliding_window(
    iterable: Iterable[T],
    size: int,
) -> Iterator[List[T]]:
    """Alias for window function."""
    return window(iterable, size)


def cyclic(iterable: Iterable[T], n: Optional[int] = None) -> Iterator[T]:
    """Repeat iterable cyclically.
    
    Args:
        iterable: Source iterable
        n: Optional max repetitions
    
    Yields:
        Cycled items
    """
    if n is None:
        while True:
            for item in iterable:
                yield item
    else:
        for _ in range(n):
            for item in iterable:
                yield item


def iterate(func: Callable[[T], T], initial: T, n: int) -> Iterator[T]:
    """Iterate function n times.
    
    Args:
        func: Function to apply
        initial: Initial value
        n: Number of iterations
    
    Yields:
        Sequence of values
    """
    value = initial
    for _ in range(n):
        yield value
        value = func(value)


def powerset(iterable: Iterable[T]) -> Iterator[tuple[T, ...]]:
    """Generate powerset of iterable.
    
    Args:
        iterable: Source iterable
    
    Yields:
        All subsets as tuples
    
    Example:
        list(powerset([1, 2, 3]))  # [(), (1,), (2,), (3,), (1,2), (1,3), (2,3), (1,2,3)]
    """
    items = list(iterable)
    n = len(items)
    
    for r in range(n + 1):
        yield from itertools.combinations(items, r)


def combinations_n(
    iterable: Iterable[T],
    n: int,
) -> Iterator[tuple[T, ...]]:
    """Generate n-length combinations."""
    return itertools.combinations(iterable, n)


def permutations_n(
    iterable: Iterable[T],
    n: int,
) -> Iterator[tuple[T, ...]]:
    """Generate n-length permutations."""
    return itertools.permutations(iterable, n)


def product(*iterables: Iterable[T]) -> Iterator[tuple[T, ...]]:
    """Cartesian product of iterables."""
    return itertools.product(*iterables)


def zip_longest_fill(
    *iterables: Iterable[T],
    fill: Any = None,
) -> Iterator[tuple]:
    """Zip with fill for unequal lengths."""
    return itertools.zip_longest(*iterables, fillvalue=fill)


def chain_from_iterable(iterable: Iterable[Iterable[T]]) -> Iterator[T]:
    """Chain items from nested iterables."""
    return itertools.chain.from_iterable(iterable)
