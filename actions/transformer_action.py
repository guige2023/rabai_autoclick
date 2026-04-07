"""transformer action module for rabai_autoclick.

Provides data transformation operations: mapping, filtering, reducing,
flatmapping, partitioning, and function composition utilities.
"""

from __future__ import annotations

from collections import deque
from functools import reduce
from typing import Any, Callable, Generic, Iterable, Iterator, List, Optional, Sequence, TypeVar, Union

__all__ = [
    "pipe",
    "compose",
    "map",
    "filter",
    "flatmap",
    "fold",
    "reduce",
    "scan",
    "take",
    "drop",
    "take_while",
    "drop_while",
    "partition_by",
    "group_by",
    "unique",
    "unique_by",
    "flatten",
    "chunk",
    "interleave",
    "zip_with",
    "enumerate_items",
    "compact",
    "pipe_transform",
]


T = TypeVar("T")
U = TypeVar("U")
K = TypeVar("K")


def pipe(*funcs: Callable) -> Callable[[T], Any]:
    """Compose functions left-to-right.

    Args:
        *funcs: Functions to compose.

    Returns:
        Composed function that passes output of each to next.

    Example:
        >>> pipe(str.upper, str.strip)("  hello  ")
        'HELLO'
    """
    def composed(x: T) -> Any:
        result = x
        for fn in funcs:
            result = fn(result)
        return result
    return composed


def compose(*funcs: Callable) -> Callable[[T], Any]:
    """Compose functions right-to-left (mathematical composition).

    Args:
        *funcs: Functions to compose (rightmost first).

    Returns:
        Composed function f(g(x)) style.

    Example:
        >>> compose(str.upper, str.strip)("  hello  ")
        'HELLO'
    """
    def composed(x: T) -> Any:
        result = x
        for fn in reversed(funcs):
            result = fn(result)
        return result
    return composed


def map(fn: Callable[[T], U]) -> Callable[[Iterable[T]], Iterator[U]]:
    """Transform each item with function.

    Args:
        fn: Transformation function.

    Returns:
        Iterator of transformed items.
    """
    def mapper(items: Iterable[T]) -> Iterator[U]:
        return (fn(item) for item in items)
    return mapper


def filter(pred: Callable[[T], bool]) -> Callable[[Iterable[T]], Iterator[T]]:
    """Keep only items where predicate is True.

    Args:
        pred: Filter predicate.

    Returns:
        Iterator of filtered items.
    """
    def filterer(items: Iterable[T]) -> Iterator[T]:
        return (item for item in items if pred(item))
    return filterer


def flatmap(fn: Callable[[T], Iterable[U]]) -> Callable[[Iterable[T]], Iterator[U]]:
    """Map and flatten results.

    Args:
        fn: Function returning iterable.

    Returns:
        Iterator of flattened results.
    """
    def flatmapper(items: Iterable[T]) -> Iterator[U]:
        for item in items:
            for result in fn(item):
                yield result
    return flatmapper


def fold(initial: U, reducer: Callable[[U, T], U]) -> Callable[[Iterable[T]], U]:
    """Fold items into single value with initial accumulator.

    Args:
        initial: Starting accumulator value.
        reducer: (accumulator, item) -> new_accumulator.

    Returns:
        Accumulated result.
    """
    def folder(items: Iterable[T]) -> U:
        return reduce(lambda acc, item: reducer(acc, item), items, initial)
    return folder


def reduce(reducer: Callable[[U, T], U]) -> Callable[[Iterable[T]], Optional[U]]:
    """Reduce items without initial value (first item is initial).

    Args:
        reducer: (accumulator, item) -> new_accumulator.

    Returns:
        Reduced value or None if empty.
    """
    def reducer_fn(items: Iterable[T]) -> Optional[U]:
        iterator = iter(items)
        try:
            initial = next(iterator)
        except StopIteration:
            return None
        return reduce(lambda acc, item: reducer(acc, item), iterator, initial)
    return reducer_fn


def scan(
    initial: Optional[U],
    reducer: Callable[[U, T], U],
) -> Callable[[Iterable[T]], Iterator[U]]:
    """Scan: like fold but yields all intermediate results.

    Args:
        initial: Starting accumulator (or None to use first item).
        reducer: (accumulator, item) -> new_accumulator.

    Returns:
        Iterator of accumulated values.
    """
    def scanner(items: Iterable[T]) -> Iterator[U]:
        iterator = iter(items)
        if initial is None:
            try:
                acc = next(iterator)
            except StopIteration:
                return
            yield acc
        else:
            acc = initial
        for item in iterator:
            acc = reducer(acc, item)
            yield acc
    return scanner


def take(n: int) -> Callable[[Iterable[T]], List[T]]:
    """Take first n items.

    Args:
        n: Number of items to take.

    Returns:
        List of first n items.
    """
    def taker(items: Iterable[T]) -> List[T]:
        results: List[T] = []
        for i, item in enumerate(items):
            if i >= n:
                break
            results.append(item)
        return results
    return taker


def drop(n: int) -> Callable[[Iterable[T]], Iterator[T]]:
    """Skip first n items.

    Args:
        n: Number of items to skip.

    Returns:
        Iterator starting after first n items.
    """
    def dropper(items: Iterable[T]) -> Iterator[T]:
        iterator = iter(items)
        for _ in range(n):
            try:
                next(iterator)
            except StopIteration:
                return
        yield from iterator
    return dropper


def take_while(pred: Callable[[T], bool]) -> Callable[[Iterable[T]], List[T]]:
    """Take items while predicate is True, then stop.

    Args:
        pred: Predicate function.

    Returns:
        List of items from start while pred is True.
    """
    def taker(items: Iterable[T]) -> List[T]:
        results: List[T] = []
        for item in items:
            if not pred(item):
                break
            results.append(item)
        return results
    return taker


def drop_while(pred: Callable[[T], bool]) -> Callable[[Iterable[T]], Iterator[T]]:
    """Drop items while predicate is True, then yield rest.

    Args:
        pred: Predicate function.

    Returns:
        Iterator starting after first non-matching item.
    """
    def dropper(items: Iterable[T]) -> Iterator[T]:
        iterator = iter(items)
        dropping = True
        for item in iterator:
            if dropping and pred(item):
                continue
            dropping = False
            yield item
    return dropper


def partition_by(pred: Callable[[T], bool]) -> Callable[[Iterable[T]], Tuple[List[T], List[T]]]:
    """Partition items into two lists by predicate.

    Args:
        pred: Function returning True for first partition.

    Returns:
        Tuple of (matching, non-matching).
    """
    def partitioner(items: Iterable[T]) -> Tuple[List[T], List[T]]:
        true_list: List[T] = []
        false_list: List[T] = []
        for item in items:
            if pred(item):
                true_list.append(item)
            else:
                false_list.append(item)
        return true_list, false_list
    return partitioner


def group_by(key_fn: Callable[[T], K]) -> Callable[[Iterable[T]], Dict[K, List[T]]]:
    """Group items by key function.

    Args:
        key_fn: Function to extract group key.

    Returns:
        Dict mapping key to list of items.
    """
    def grouper(items: Iterable[T]) -> Dict[K, List[T]]:
        groups: Dict[K, List[T]] = {}
        for item in items:
            key = key_fn(item)
            if key not in groups:
                groups[key] = []
            groups[key].append(item)
        return groups
    return grouper


def unique() -> Callable[[Iterable[T]], Iterator[T]]:
    """Remove duplicate items (preserves order).

    Returns:
        Iterator of unique items.
    """
    def uniquer(items: Iterable[T]) -> Iterator[T]:
        seen: set = set()
        for item in items:
            if item not in seen:
                seen.add(item)
                yield item
    return uniquer


def unique_by(key_fn: Callable[[T], Any]) -> Callable[[Iterable[T]], Iterator[T]]:
    """Remove duplicates by key (preserves first occurrence order).

    Args:
        key_fn: Function to extract comparison key.

    Returns:
        Iterator of unique-by-key items.
    """
    def uniquer(items: Iterable[T]) -> Iterator[T]:
        seen: set = set()
        for item in items:
            key = key_fn(item)
            if key not in seen:
                seen.add(key)
                yield item
    return uniquer


def flatten() -> Callable[[Iterable[Iterable[T]]], Iterator[T]]:
    """Flatten one level of nesting.

    Returns:
        Iterator of items from nested iterables.
    """
    def flatter(nested: Iterable[Iterable[T]]) -> Iterator[T]:
        for outer in nested:
            for inner in outer:
                yield inner
    return flatter


def chunk(size: int) -> Callable[[Iterable[T]], Iterator[List[T]]]:
    """Split into chunks of size n.

    Args:
        size: Chunk size.

    Returns:
        Iterator of chunks.
    """
    def chunker(items: Iterable[T]) -> Iterator[List[T]]:
        batch: List[T] = []
        for item in items:
            batch.append(item)
            if len(batch) == size:
                yield batch
                batch = []
        if batch:
            yield batch
    return chunker


def interleave(*sequences: Sequence[T]) -> Iterator[T]:
    """Interleave multiple sequences round-robin.

    Args:
        *sequences: Sequences to interleave.

    Yields:
        Items from each sequence in round-robin order.
    """
    iterators = [iter(seq) for seq in sequences]
    while iterators:
        next_iters = []
        for it in iterators:
            try:
                yield next(it)
                next_iters.append(it)
            except StopIteration:
                pass
        iterators = next_iters


def zip_with(fn: Callable[..., U]) -> Callable[[Iterable[Iterable[T]]], Iterator[U]]:
    """Zip sequences with transformation function.

    Args:
        fn: Function to combine corresponding items.

    Returns:
        Iterator of transformed tuples.
    """
    def zipper(multiple: Iterable[Iterable[T]]) -> Iterator[U]:
        iterators = [iter(seq) for seq in multiple]
        sentinel = object()
        while True:
            items = []
            for it in iterators:
                val = next(it, sentinel)
                if val is sentinel:
                    return
                items.append(val)
            yield fn(*items)
    return zipper


def enumerate_items(start: int = 0) -> Callable[[Iterable[T]], Iterator[Tuple[int, T]]]:
    """Add index to each item.

    Args:
        start: Starting index.

    Returns:
        Iterator of (index, item) tuples.
    """
    def enumerator(items: Iterable[T]) -> Iterator[Tuple[int, T]]:
        for i, item in enumerate(items, start=start):
            yield i, item
    return enumerator


def compact() -> Callable[[Iterable[Optional[T]]], Iterator[T]]:
    """Remove None and falsy values.

    Returns:
        Iterator of truthy items.
    """
    def compactor(items: Iterable[Optional[T]]) -> Iterator[T]:
        for item in items:
            if item is not None and item:
                yield item
    return compactor


def pipe_transform(*operations: Callable) -> Callable[[Iterable], Iterator]:
    """Chain multiple transform operations.

    Args:
        *operations: Functions that transform iterables to iterables.

    Returns:
        Composed transform function.
    """
    def pipeline(items: Iterable) -> Iterator:
        result: Any = items
        for op in operations:
            result = op(result)
            if not hasattr(result, "__iter__") or isinstance(result, (str, bytes)):
                result = iter([result])
        return result
    return pipeline
