"""splitter_action module for rabai_autoclick.

Provides data splitting operations: split by delimiter, chunking,
partitioning, train/test splits, and stratified sampling.
"""

from __future__ import annotations

import math
import random
from collections import deque
from typing import Any, Callable, Iterable, Iterator, List, Optional, Sequence, Tuple, TypeVar

__all__ = [
    "split",
    "split_at",
    "split_at_predicate",
    "split_into_n",
    "split_by_size",
    "split_train_test",
    "split_stratified",
    "chunk",
    "chunk_by_size",
    "partition",
    "partition_n",
    "head",
    "tail",
    "split_lines",
    "split_chunks",
    "split_every",
]


T = TypeVar("T")


def split(
    items: Sequence[T],
    delimiter: Optional[T] = None,
    maxsplit: int = -1,
) -> List[List[T]]:
    """Split sequence by delimiter or count.

    Args:
        items: Sequence to split.
        delimiter: Delimiter element (None = split by count).
        maxsplit: Maximum splits (-1 = unlimited).

    Returns:
        List of splits.
    """
    if delimiter is None:
        count = maxsplit if maxsplit > 0 else len(items)
        chunk_size = max(1, len(items) // count)
        return [list(items[i:i+chunk_size]) for i in range(0, len(items), chunk_size)]

    result: List[List[T]] = []
    current: List[T] = []
    splits = 0

    for item in items:
        if item == delimiter and (maxsplit < 0 or splits < maxsplit):
            result.append(current)
            current = []
            splits += 1
        else:
            current.append(item)

    result.append(current)
    return result


def split_at(
    items: Sequence[T],
    index: int,
) -> Tuple[Sequence[T], Sequence[T]]:
    """Split sequence at index.

    Args:
        items: Sequence to split.
        index: Index where split occurs.

    Returns:
        (before, after) tuple.
    """
    return items[:index], items[index:]


def split_at_predicate(
    items: Sequence[T],
    pred: Callable[[T], bool],
) -> List[List[T]]:
    """Split at every item where predicate is True.

    Args:
        items: Sequence to split.
        pred: Function returning True at split points.

    Returns:
        List of splits.
    """
    result: List[List[T]] = []
    current: List[T] = []

    for item in items:
        if pred(item):
            if current:
                result.append(current)
                current = []
        else:
            current.append(item)

    if current:
        result.append(current)
    return result


def split_into_n(
    items: Sequence[T],
    n: int,
) -> List[List[T]]:
    """Split sequence into n approximately equal parts.

    Args:
        items: Sequence to split.
        n: Number of parts.

    Returns:
        List of n splits.
    """
    if n <= 0:
        return []
    if n == 1:
        return [list(items)]

    length = len(items)
    base_size = length // n
    remainder = length % n

    result: List[List[T]] = []
    start = 0
    for i in range(n):
        size = base_size + (1 if i < remainder else 0)
        result.append(list(items[start:start + size]))
        start += size

    return result


def split_by_size(
    items: Sequence[T],
    chunk_size: int,
) -> List[List[T]]:
    """Split into chunks of specified size.

    Args:
        items: Sequence to split.
        chunk_size: Maximum items per chunk.

    Returns:
        List of chunks.
    """
    return [list(items[i:i + chunk_size]) for i in range(0, len(items), chunk_size)]


def split_train_test(
    items: Sequence[T],
    test_size: float = 0.2,
    shuffle: bool = True,
    seed: Optional[int] = None,
) -> Tuple[List[T], List[T]]:
    """Split items into train and test sets.

    Args:
        items: Items to split.
        test_size: Fraction for test set (0.0-1.0).
        shuffle: Whether to shuffle before splitting.
        seed: Random seed for reproducibility.

    Returns:
        (train, test) tuple.
    """
    if seed is not None:
        random.seed(seed)

    data = list(items)
    if shuffle:
        random.shuffle(data)

    split_idx = int(len(data) * (1 - test_size))
    return data[:split_idx], data[split_idx:]


def split_stratified(
    items: Sequence[T],
    stratify_fn: Callable[[T], str],
    test_size: float = 0.2,
    seed: Optional[int] = None,
) -> Tuple[List[T], List[T]]:
    """Stratified split maintaining class distribution.

    Args:
        items: Items to split.
        stratify_fn: Function returning class label.
        test_size: Fraction for test set.
        seed: Random seed.

    Returns:
        (train, test) tuple with same class ratios.
    """
    if seed is not None:
        random.seed(seed)

    groups: dict = {}
    for item in items:
        label = stratify_fn(item)
        if label not in groups:
            groups[label] = []
        groups[label].append(item)

    train: List[T] = []
    test: List[T] = []

    for label, group_items in groups.items():
        shuffled = list(group_items)
        random.shuffle(shuffled)
        split_idx = int(len(shuffled) * (1 - test_size))
        train.extend(shuffled[:split_idx])
        test.extend(shuffled[split_idx:])

    random.shuffle(train)
    random.shuffle(test)

    return train, test


def chunk(
    items: Sequence[T],
    size: int,
) -> Iterator[List[T]]:
    """Yield successive chunks of specified size.

    Args:
        items: Sequence to chunk.
        size: Chunk size.

    Yields:
        Successive chunks.
    """
    for i in range(0, len(items), size):
        yield list(items[i:i + size])


def chunk_by_size(
    items: Sequence[T],
    max_bytes: int,
    size_fn: Optional[Callable[[T], int]] = None,
) -> Iterator[List[T]]:
    """Split into chunks not exceeding max byte size.

    Args:
        items: Sequence to chunk.
        max_bytes: Maximum bytes per chunk.
        size_fn: Function to get item size in bytes.

    Returns:
        Iterator of chunks.
    """
    current: List[T] = []
    current_size = 0

    for item in items:
        item_size = size_fn(item) if size_fn else len(str(item))
        if current_size + item_size > max_bytes and current:
            yield current
            current = []
            current_size = 0
        current.append(item)
        current_size += item_size

    if current:
        yield current


def partition(
    items: Sequence[T],
    pred: Callable[[T], bool],
) -> Tuple[List[T], List[T]]:
    """Partition items into two lists by predicate.

    Args:
        items: Items to partition.
        pred: Partition predicate.

    Returns:
        (matching, non-matching) tuple.
    """
    matching: List[T] = []
    non_matching: List[T] = []
    for item in items:
        if pred(item):
            matching.append(item)
        else:
            non_matching.append(item)
    return matching, non_matching


def partition_n(
    items: Sequence[T],
    n: int,
    key_fn: Optional[Callable[[T], Any]] = None,
) -> List[List[T]]:
    """Partition items into n groups by key function.

    Args:
        items: Items to partition.
        n: Number of partitions.
        key_fn: Function to extract partition key.

    Returns:
        List of n partitions.
    """
    if key_fn is None:
        key_fn = lambda x: hash(x) % n

    groups: dict = {i: [] for i in range(n)}
    for item in items:
        idx = key_fn(item) % n
        groups[idx].append(item)

    return [groups[i] for i in range(n)]


def head(items: Sequence[T], n: int = 10) -> List[T]:
    """Get first n items.

    Args:
        items: Sequence.
        n: Number of items.

    Returns:
        First n items.
    """
    return list(items)[:n]


def tail(items: Sequence[T], n: int = 10) -> List[T]:
    """Get last n items.

    Args:
        items: Sequence.
        n: Number of items.

    Returns:
        Last n items.
    """
    return list(items)[-n:]


def split_lines(
    text: str,
    keep_empty: bool = False,
) -> List[str]:
    """Split text into lines.

    Args:
        text: Text to split.
        keep_empty: Include empty lines if True.

    Returns:
        List of lines.
    """
    lines = text.splitlines()
    if keep_empty:
        return lines
    return [line for line in lines if line.strip()]


def split_chunks(
    items: Sequence[T],
    chunk_count: int,
) -> List[List[T]]:
    """Split into chunk_count chunks (approximately equal).

    Args:
        items: Sequence to split.
        chunk_count: Number of chunks.

    Returns:
        List of chunks.
    """
    return split_into_n(items, chunk_count)


def split_every(
    items: Sequence[T],
    n: int,
) -> Iterator[List[T]]:
    """Split yielding every n items.

    Args:
        items: Sequence.
        n: Items per split.

    Yields:
        Successive groups of n items.
    """
    buffer: List[T] = []
    for item in items:
        buffer.append(item)
        if len(buffer) == n:
            yield buffer
            buffer = []
    if buffer:
        yield buffer
