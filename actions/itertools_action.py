"""
itertools extensions - chunk, window, flatten, unique, and more.
"""

from __future__ import annotations

import itertools
import math
from typing import (
    Any,
    Callable,
    Iterable,
    Iterator,
    List,
    Optional,
    TypeVar,
    Union,
)

T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


def chunk(iterable: Iterable[T], size: int) -> Iterator[List[T]]:
    """
    Split an iterable into chunks of specified size.
    
    Args:
        iterable: Input iterable to chunk
        size: Maximum size of each chunk (must be > 0)
    
    Yields:
        Lists of up to `size` elements from the iterable
    
    Raises:
        ValueError: If size is not a positive integer
    
    Example:
        >>> list(chunk([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]
    """
    if size <= 0:
        raise ValueError(f"Chunk size must be positive, got {size}")
    iterator = iter(iterable)
    while True:
        batch = list(itertools.islice(iterator, size))
        if not batch:
            break
        yield batch


def window(
    iterable: Iterable[T],
    size: int,
    step: int = 1,
    fillvalue: Optional[T] = None,
) -> Iterator[List[T]]:
    """
    Sliding window over an iterable.
    
    Args:
        iterable: Input iterable
        size: Window size (must be > 0)
        step: Step size between windows (default 1)
        fillvalue: Value to use for missing elements if specified
    
    Yields:
        Lists representing each window position
    
    Raises:
        ValueError: If size is not positive
    
    Example:
        >>> list(window([1, 2, 3, 4], 3))
        [[1, 2, 3], [2, 3, 4]]
    """
    if size <= 0:
        raise ValueError(f"Window size must be positive, got {size}")
    if step <= 0:
        raise ValueError(f"Step must be positive, got {step}")
    
    iterator = iter(iterable)
    window_list = list(itertools.islice(iterator, size))
    
    if len(window_list) < size:
        if fillvalue is None:
            return
        while len(window_list) < size:
            window_list.append(fillvalue)
    
    yield window_list
    
    for item in iterator:
        window_list = window_list[step:] + [item]
        if fillvalue is None and len(window_list) < size:
            continue
        yield window_list[:size]


def flatten(
    nested: Iterable[Any],
    depth: Optional[int] = None,
    types: Optional[tuple] = (list, tuple),
) -> Iterator[Any]:
    """
    Recursively flatten a nested iterable.
    
    Args:
        nested: Nested iterable to flatten
        depth: Maximum depth to flatten (None for unlimited)
        types: Tuple of types to consider as iterable
    
    Yields:
        Single flattened elements
    
    Example:
        >>> list(flatten([[1, 2], [3, [4, 5]]]))
        [1, 2, 3, 4, 5]
    """
    for item in nested:
        if depth is None or depth > 0:
            if isinstance(item, types):
                depth_arg = None if depth is None else depth - 1
                yield from flatten(item, depth=depth_arg, types=types)
            else:
                yield item
        else:
            yield item


def unique(
    iterable: Iterable[T],
    key: Optional[Callable[[T], Any]] = None,
    preserve_order: bool = True,
) -> Iterator[T]:
    """
    Get unique elements from an iterable.
    
    Args:
        iterable: Input iterable
        key: Optional function to compute key for uniqueness
        preserve_order: If True (default), preserves insertion order
    
    Yields:
        Unique elements in order first seen
    
    Example:
        >>> list(unique([1, 2, 2, 3, 1, 3]))
        [1, 2, 3]
    """
    if preserve_order:
        seen_keys: set = set()
        seen_list: List[T] = []
        for item in iterable:
            k = key(item) if key else item
            if k not in seen_keys:
                seen_keys.add(k)
                seen_list.append(item)
        yield from seen_list
    else:
        seen: set = set()
        for item in iterable:
            k = key(item) if key else item
            if k not in seen:
                seen.add(k)
                yield item


def batched(
    iterable: Iterable[T],
    n: int,
    fillvalue: Optional[T] = None,
) -> Iterator[tuple]:
    """
    Backport of itertools.batched for older Python versions.
    
    Batch data into tuples of length n. The last batch may be shorter.
    
    Args:
        iterable: Input iterable
        n: Batch size
        fillvalue: Value to fill last batch if provided
    
    Yields:
        Tuples of up to n elements
    
    Example:
        >>> list(batched([1, 2, 3, 4, 5], 2))
        [(1, 2), (3, 4), (5,)]
    """
    if n < 1:
        raise ValueError(f"Batch size must be >= 1, got {n}")
    iterator = iter(iterable)
    while True:
        batch = tuple(itertools.islice(iterator, n))
        if not batch:
            break
        if fillvalue is not None and len(batch) < n:
            batch = batch + (fillvalue,) * (n - len(batch))
        yield batch


def distribute(n: int, iterable: Iterable[T]) -> Iterator[Iterator[T]]:
    """
    Distribute items from iterable across n rounds.
    
    Args:
        n: Number of rounds (must be > 0)
        iterable: Input iterable
    
    Yields:
        Iterator for each round
    
    Example:
        >>> rounds = list(distribute(3, range(10)))
        >>> len(rounds)
        3
    """
    if n < 1:
        raise ValueError(f"Number of rounds must be >= 1, got {n}")
    
    items = list(iterable)
    if not items:
        return
    
    n = min(n, len(items))
    queue: List[List[T]] = [[] for _ in range(n)]
    
    for i, item in enumerate(items):
        queue[i % n].append(item)
    
    for round_items in queue:
        if round_items:
            yield iter(round_items)


def divide(n: int, iterable: Iterable[T]) -> Iterator[List[T]]:
    """
    Divide an iterable into n roughly equal parts.
    
    Args:
        n: Number of parts (must be > 0)
        iterable: Input iterable
    
    Yields:
        List for each part
    
    Example:
        >>> parts = list(divide(3, range(10)))
        >>> [len(p) for p in parts]
        [4, 3, 3]
    """
    if n < 1:
        raise ValueError(f"Number of parts must be >= 1, got {n}")
    
    items = list(iterable)
    if not items:
        return
    
    n = min(n, len(items))
    base_size, remainder = divmod(len(items), n)
    
    start = 0
    for i in range(n):
        size = base_size + (1 if i < remainder else 0)
        yield items[start:start + size]
        start += size


def unique_in_order(sequence: Iterable[T]) -> Iterator[T]:
    """
    Yield unique elements in order, removing consecutive duplicates.
    
    Args:
        sequence: Input sequence (string, list, etc.)
    
    Yields:
        Elements with no consecutive duplicates
    
    Example:
        >>> list(unique_in_order("AAAABBBCCDAABBB"))
        ['A', 'B', 'C', 'D', 'A', 'B']
    """
    prev: Optional[T] = None
    for item in sequence:
        if item != prev:
            yield item
            prev = item


def grouper(
    iterable: Iterable[T],
    n: int,
    fillvalue: Optional[T] = None,
) -> Iterator[tuple]:
    """
    Collect data into fixed-length chunks.
    
    Args:
        iterable: Input iterable
        n: Chunk size
        fillvalue: Value to fill incomplete chunk
    
    Yields:
        Tuples of exactly n elements (last may have fillvalue)
    
    Example:
        >>> list(grouper('ABCDEFG', 3, 'x'))
        [('A', 'B', 'C'), ('D', 'E', 'F'), ('G', 'x', 'x')]
    """
    yield from batched(iterable, n, fillvalue=fillvalue)


def iterate(start: T) -> Iterator[T]:
    """
    Generate infinite sequence of value, f(value), f(f(value)), etc.
    
    Args:
        start: Starting value
    
    Yields:
        Infinite sequence of transformed values
    
    Example:
        >>> import operator
        >>> it = iterate(1)
        >>> next(it), next(it), next(it)
        (1, 2, 3)
    """
    while True:
        yield start
        start = start + 1


def flatten_levels(
    nested: Iterable[Any],
    levels: int,
    types: Optional[tuple] = (list, tuple),
) -> Iterator[Any]:
    """
    Flatten a nested iterable by a specific number of levels.
    
    Args:
        nested: Nested iterable to flatten
        levels: Number of levels to flatten
        types: Tuple of types to consider as iterable
    
    Yields:
        Elements flattened by specified levels
    
    Example:
        >>> list(flatten_levels([[[1, 2], [3]], [[4]]], 1))
        [[1, 2], [3], [4]]
    """
    return flatten(nested, depth=levels, types=types)


def sliding_window_2d(
    matrix: List[List[T]],
    window_size: int,
    step: int = 1,
) -> Iterator[List[List[T]]]:
    """
    Extract sliding windows from a 2D matrix.
    
    Args:
        matrix: 2D matrix (list of lists)
        window_size: Size of square window
        step: Step size between windows
    
    Yields:
        2D windows extracted from the matrix
    
    Example:
        >>> m = [[1,2,3],[4,5,6],[7,8,9]]
        >>> for win in sliding_window_2d(m, 2):
        ...     print(win)
        [[1, 2], [4, 5]]
        [[2, 3], [5, 6]]
        [[4, 5], [7, 8]]
        [[5, 6], [8, 9]]
    """
    if not matrix:
        return
    
    rows = len(matrix)
    cols = len(matrix[0]) if matrix else 0
    
    if window_size > rows or window_size > cols:
        return
    
    for r in range(0, rows - window_size + 1, step):
        for c in range(0, cols - window_size + 1, step):
            window = [row[c:c + window_size] for row in matrix[r:r + window_size]]
            yield window


def intersperse(
    iterable: Iterable[T],
    delimiter: T,
) -> Iterator[T]:
    """
    Yield items from iterable with delimiter between each.
    
    Args:
        iterable: Input iterable
        delimiter: Value to insert between items
    
    Yields:
        Items with delimiter interspersed
    
    Example:
        >>> list(intersperse([1, 2, 3], 0))
        [1, 0, 2, 0, 3]
    """
    iterator = iter(iterable)
    try:
        yield next(iterator)
    except StopIteration:
        return
    for item in iterator:
        yield delimiter
        yield item


def quantify(iterable: Iterable[Any], pred: Optional[Callable[[Any], bool]] = None) -> int:
    """
    Count the number of items where pred(item) is True.
    
    Args:
        iterable: Input iterable
        pred: Predicate function (default: identity/bool)
    
    Returns:
        Count of items where pred returns True
    
    Example:
        >>> quantify([True, False, True, True])
        3
    """
    if pred is None:
        return sum(1 for item in iterable if item)
    return sum(1 for item in iterable if pred(item))


def list_compare(l1: List[T], l2: List[T]) -> int:
    """
    Lexicographic comparison of two lists.
    
    Args:
        l1: First list
        l2: Second list
    
    Returns:
        -1 if l1 < l2, 0 if equal, 1 if l1 > l2
    
    Example:
        >>> list_compare([1, 2, 3], [1, 2, 4])
        -1
    """
    for a, b in zip(l1, l2):
        if a < b:
            return -1
        if a > b:
            return 1
    if len(l1) < len(l2):
        return -1
    if len(l1) > len(l2):
        return 1
    return 0
