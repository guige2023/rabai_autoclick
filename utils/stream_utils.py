"""Stream utilities for RabAI AutoClick.

Provides:
- Stream processing helpers
- Buffered readers/writers
- Line-by-line processing
"""

import io
from typing import (
    Callable,
    Generator,
    Iterable,
    Iterator,
    List,
    Optional,
    TypeVar,
)


T = TypeVar("T")
U = TypeVar("U")


def chunked(
    iterable: Iterable[T],
    chunk_size: int,
) -> Generator[List[T], None, None]:
    """Split an iterable into chunks.

    Args:
        iterable: Input iterable.
        chunk_size: Size of each chunk.

    Yields:
        Lists of items of up to chunk_size.
    """
    chunk: List[T] = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def chunked_iter(
    iterator: Iterator[T],
    chunk_size: int,
) -> Generator[List[T], None, None]:
    """Split an iterator into chunks.

    Args:
        iterator: Input iterator.
        chunk_size: Size of each chunk.

    Yields:
        Lists of items of up to chunk_size.
    """
    return chunked(iterator, chunk_size)


def take(
    iterable: Iterable[T],
    n: int,
) -> List[T]:
    """Take first n items from iterable.

    Args:
        iterable: Input iterable.
        n: Number of items to take.

    Returns:
        List of first n items.
    """
    result = []
    for i, item in enumerate(iterable):
        if i >= n:
            break
        result.append(item)
    return result


def drop(
    iterable: Iterable[T],
    n: int,
) -> Generator[T, None, None]:
    """Skip first n items from iterable.

    Args:
        iterable: Input iterable.
        n: Number of items to skip.

    Yields:
        Items after skipping first n.
    """
    for i, item in enumerate(iterable):
        if i >= n:
            yield item


def first(iterable: Iterable[T], default: Optional[T] = None) -> Optional[T]:
    """Get first item from iterable.

    Args:
        iterable: Input iterable.
        default: Default value if empty.

    Returns:
        First item or default.
    """
    for item in iterable:
        return item
    return default


def last(iterable: Iterable[T], default: Optional[T] = None) -> Optional[T]:
    """Get last item from iterable.

    Args:
        iterable: Input iterable.
        default: Default value if empty.

    Returns:
        Last item or default.
    """
    item = default
    for item in iterable:
        pass
    return item


def unique(
    iterable: Iterable[T],
    key: Optional[Callable[[T], Any]] = None,
) -> Generator[T, None, None]:
    """Yield unique items from iterable.

    Args:
        iterable: Input iterable.
        key: Optional key function for comparison.

    Yields:
        Unique items.
    """
    seen: set = set()
    for item in iterable:
        k = key(item) if key else item
        if k not in seen:
            seen.add(k)
            yield item


def unique_preserve_order(
    iterable: Iterable[T],
) -> Generator[T, None, None]:
    """Yield unique items preserving original order.

    Args:
        iterable: Input iterable.

    Yields:
        Unique items in order.
    """
    seen: set = set()
    for item in iterable:
        if item not in seen:
            seen.add(item)
            yield item


def flatten(
    nested: Iterable[Iterable[T]],
) -> Generator[T, None, None]:
    """Flatten a nested iterable.

    Args:
        nested: Nested iterable of iterables.

    Yields:
        Flattened items.
    """
    for inner in nested:
        for item in inner:
            yield item


def interleave(
    *iterables: Iterable[T],
) -> Generator[T, None, None]:
    """Interleave items from multiple iterables.

    Args:
        *iterables: Iterables to interleave.

    Yields:
        Interleaved items.
    """
    iterators = [iter(it) for it in iterables]
    while iterators:
        for it in iterators[:]:
            try:
                yield next(it)
            except StopIteration:
                iterators.remove(it)


def partition(
    iterable: Iterable[T],
    predicate: Callable[[T], bool],
) -> tuple[List[T], List[T]]:
    """Partition iterable by predicate.

    Args:
        iterable: Input iterable.
        predicate: Function that returns True for "truthy" group.

    Returns:
        Tuple of (truthy_list, falsy_list).
    """
    truthy: List[T] = []
    falsy: List[T] = []
    for item in iterable:
        if predicate(item):
            truthy.append(item)
        else:
            falsy.append(item)
    return truthy, falsy


def group_by(
    iterable: Iterable[T],
    key: Callable[[T], U],
) -> dict[U, List[T]]:
    """Group items by key function.

    Args:
        iterable: Input iterable.
        key: Function to extract group key.

    Returns:
        Dict mapping key to list of items.
    """
    groups: dict[U, List[T]] = {}
    for item in iterable:
        k = key(item)
        if k not in groups:
            groups[k] = []
        groups[k].append(item)
    return groups


def batch_process(
    items: List[T],
    batch_size: int,
    processor: Callable[[List[T]], List[U]],
) -> List[U]:
    """Process items in batches.

    Args:
        items: Items to process.
        batch_size: Batch size.
        processor: Function to process each batch.

    Returns:
        List of all results.
    """
    results: List[U] = []
    for chunk in chunked(items, batch_size):
        results.extend(processor(chunk))
    return results


def sliding_window(
    iterable: Iterable[T],
    window_size: int,
) -> Generator[tuple[T, ...], None, None]:
    """Yield sliding windows over iterable.

    Args:
        iterable: Input iterable.
        window_size: Size of each window.

    Yields:
        Tuples of window_size items.
    """
    window: List[T] = []
    for item in iterable:
        window.append(item)
        if len(window) >= window_size:
            yield tuple(window)
            window.pop(0)


def stream_lines(
    file_like: io.TextIOBase,
) -> Generator[str, None, None]:
    """Stream lines from a text file-like object.

    Args:
        file_like: Text file-like object.

    Yields:
        Lines without trailing newline.
    """
    for line in file_like:
        yield line.rstrip("\n\r")


def stream_csv_rows(
    file_like: io.TextIOBase,
    delimiter: str = ",",
) -> Generator[List[str], None, None]:
    """Stream CSV rows from a file-like object.

    Args:
        file_like: Text file-like object.
        delimiter: CSV delimiter.

    Yields:
        Lists of column values.
    """
    for line in stream_lines(file_like):
        yield line.split(delimiter)


class BufferedWriter:
    """Buffered writer with flush control."""

    def __init__(
        self,
        write_func: Callable[[str], None],
        buffer_size: int = 4096,
    ) -> None:
        self._write_func = write_func
        self._buffer_size = buffer_size
        self._buffer: List[str] = []

    def write(self, text: str) -> None:
        """Write text to buffer."""
        self._buffer.append(text)
        if sum(len(s) for s in self._buffer) >= self._buffer_size:
            self.flush()

    def flush(self) -> None:
        """Flush buffer to output."""
        if self._buffer:
            self._write_func("".join(self._buffer))
            self._buffer.clear()

    def close(self) -> None:
        """Close and flush."""
        self.flush()


def consume(
    iterable: Iterable[T],
    count: Optional[int] = None,
) -> None:
    """Consume an iterable without storing results.

    Args:
        iterable: Input iterable.
        count: Optional max items to consume.
    """
    if count is None:
        for _ in iterable:
            pass
    else:
        for i, item in enumerate(iterable):
            if i >= count:
                break


def iterator_from_file(filepath: str) -> Generator[str, None, None]:
    """Create a line iterator from a file.

    Args:
        filepath: Path to file.

    Yields:
        Lines from file.
    """
    with open(filepath, "r") as f:
        for line in f:
            yield line.rstrip("\n\r")
