"""
Data Chunk Action Module.

Provides chunking and batching utilities for processing large
datasets in manageable pieces.
"""

from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar, Generic
from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")
U = TypeVar("U")


@dataclass
class Chunk:
    """A chunk of data."""
    index: int
    items: List[Any]
    start_index: int
    end_index: int
    is_first: bool = False
    is_last: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "index": self.index,
            "start_index": self.start_index,
            "end_index": self.end_index,
            "size": len(self.items),
            "is_first": self.is_first,
            "is_last": self.is_last,
        }


@dataclass
class ChunkStats:
    """Statistics for chunking operations."""
    total_items: int
    chunk_count: int
    chunk_size: int
    smallest_chunk: int
    largest_chunk: int
    overhead_bytes: int = 0

    def avg_chunk_size(self) -> float:
        """Calculate average chunk size."""
        if self.chunk_count == 0:
            return 0.0
        return self.total_items / self.chunk_count

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_items": self.total_items,
            "chunk_count": self.chunk_count,
            "chunk_size": self.chunk_size,
            "smallest_chunk": self.smallest_chunk,
            "largest_chunk": self.largest_chunk,
            "avg_chunk_size": self.avg_chunk_size(),
            "overhead_bytes": self.overhead_bytes,
        }


class DataChunkAction(Generic[T]):
    """
    Provides chunking utilities for large datasets.

    This action implements various chunking strategies for processing
    large datasets in manageable pieces, supporting both fixed-size
    and adaptive chunking.

    Example:
        >>> chunker = DataChunkAction(chunk_size=100)
        >>> chunks = chunker.chunk([1, 2, 3, 4, 5])
        >>> for chunk in chunks:
        ...     process(chunk.items)
    """

    def __init__(
        self,
        chunk_size: int = 100,
        overlap: int = 0,
        min_chunk_size: int = 1,
        max_chunk_size: Optional[int] = None,
    ):
        """
        Initialize the Data Chunk Action.

        Args:
            chunk_size: Target size for each chunk.
            overlap: Number of overlapping items between chunks.
            min_chunk_size: Minimum chunk size.
            max_chunk_size: Maximum chunk size.
        """
        self.chunk_size = chunk_size
        self.overlap = overlap
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size or chunk_size * 10

        if self.overlap >= self.chunk_size:
            raise ValueError("Overlap must be smaller than chunk_size")

    def chunk(self, items: List[T]) -> List[Chunk]:
        """
        Split items into chunks.

        Args:
            items: Items to chunk.

        Returns:
            List of Chunks.
        """
        if not items:
            return []

        chunks: List[Chunk] = []
        total_items = len(items)
        num_chunks = (total_items + self.chunk_size - 1) // self.chunk_size

        for i in range(num_chunks):
            start = i * self.chunk_size
            end = min(start + self.chunk_size, total_items)
            chunk_items = items[start:end]

            if len(chunk_items) < self.min_chunk_size and chunks:
                chunks[-1].items.extend(chunk_items)
                chunks[-1].end_index = end
            else:
                chunk = Chunk(
                    index=i,
                    items=chunk_items,
                    start_index=start,
                    end_index=end,
                    is_first=(i == 0),
                    is_last=(i == num_chunks - 1),
                )
                chunks.append(chunk)

        return self._apply_overlap(chunks)

    def _apply_overlap(self, chunks: List[Chunk]) -> List[Chunk]:
        """Apply overlap between chunks."""
        if self.overlap == 0 or len(chunks) < 2:
            return chunks

        result = [chunks[0]]

        for i in range(1, len(chunks)):
            prev = result[-1]
            curr = chunks[i]

            prev.items = prev.items + curr.items[:self.overlap]
            result.append(curr)

        return result

    def chunk_with_index(
        self,
        items: List[T],
    ) -> Iterator[Tuple[int, Chunk]]:
        """
        Iterate over chunks with their indices.

        Args:
            items: Items to chunk.

        Yields:
            Tuples of (index, Chunk).
        """
        chunks = self.chunk(items)
        for chunk in chunks:
            yield chunk.index, chunk

    def unchunk(self, chunks: List[Chunk]) -> List[T]:
        """
        Reconstruct items from chunks.

        Args:
            chunks: Chunks to reconstruct.

        Returns:
            Reconstructed list of items.
        """
        if not chunks:
            return []

        result = []
        for chunk in chunks:
            result.extend(chunk.items)

        return result

    def adaptive_chunk(
        self,
        items: List[T],
        size_func: Callable[[T], int],
    ) -> List[Chunk]:
        """
        Chunk items with adaptive sizing based on item sizes.

        Args:
            items: Items to chunk.
            size_func: Function to get size of each item.

        Returns:
            List of Chunks with estimated sizes.
        """
        if not items:
            return []

        chunks: List[Chunk] = []
        current_chunk: List[T] = []
        current_size = 0

        for i, item in enumerate(items):
            item_size = size_func(item)

            if current_size + item_size > self.max_chunk_size and current_chunk:
                chunks.append(self._create_chunk(chunks, current_chunk, len(chunks)))
                current_chunk = []
                current_size = 0

            if item_size > self.max_chunk_size:
                if current_chunk:
                    chunks.append(self._create_chunk(chunks, current_chunk, len(chunks)))
                    current_chunk = []

                chunks.append(Chunk(
                    index=len(chunks),
                    items=[item],
                    start_index=i,
                    end_index=i + 1,
                    is_first=len(chunks) == 0,
                    is_last=False,
                ))
            else:
                current_chunk.append(item)
                current_size += item_size

        if current_chunk:
            chunks.append(self._create_chunk(chunks, current_chunk, len(chunks)))

        if chunks:
            chunks[-1].is_last = True

        return chunks

    def _create_chunk(
        self,
        existing_chunks: List[Chunk],
        items: List[T],
        index: int,
    ) -> Chunk:
        """Create a chunk with proper indices."""
        start = 0
        if existing_chunks:
            start = existing_chunks[-1].end_index

        return Chunk(
            index=index,
            items=items,
            start_index=start,
            end_index=start + len(items),
            is_first=len(existing_chunks) == 0,
            is_last=False,
        )

    def window(
        self,
        items: List[T],
        window_size: Optional[int] = None,
    ) -> List[Chunk]:
        """
        Create sliding window chunks.

        Args:
            items: Items to window.
            window_size: Window size (defaults to chunk_size).

        Returns:
            List of window Chunks.
        """
        size = window_size or self.chunk_size
        if size < 1:
            raise ValueError("Window size must be at least 1")

        if not items:
            return []

        chunks: List[Chunk] = []

        for i in range(len(items)):
            start = i
            end = min(i + size, len(items))
            chunk_items = items[start:end]

            chunk = Chunk(
                index=i,
                items=chunk_items,
                start_index=start,
                end_index=end,
                is_first=(i == 0),
                is_last=(end == len(items)),
            )
            chunks.append(chunk)

        return chunks

    def get_stats(self, chunks: List[Chunk]) -> ChunkStats:
        """
        Get statistics for chunks.

        Args:
            chunks: Chunks to analyze.

        Returns:
            ChunkStats.
        """
        if not chunks:
            return ChunkStats(
                total_items=0,
                chunk_count=0,
                chunk_size=self.chunk_size,
                smallest_chunk=0,
                largest_chunk=0,
            )

        sizes = [len(c.items) for c in chunks]

        return ChunkStats(
            total_items=sum(sizes),
            chunk_count=len(chunks),
            chunk_size=self.chunk_size,
            smallest_chunk=min(sizes),
            largest_chunk=max(sizes),
        )

    def partition(
        self,
        items: List[T],
        num_partitions: int,
    ) -> List[Chunk]:
        """
        Partition items into a specific number of chunks.

        Args:
            items: Items to partition.
            num_partitions: Number of partitions.

        Returns:
            List of Chunks.
        """
        if num_partitions < 1:
            raise ValueError("Number of partitions must be at least 1")

        if not items:
            return []

        import math

        partition_size = math.ceil(len(items) / num_partitions)
        chunks: List[Chunk] = []

        for i in range(num_partitions):
            start = i * partition_size
            end = min(start + partition_size, len(items))
            chunk_items = items[start:end]

            if chunk_items:
                chunks.append(Chunk(
                    index=i,
                    items=chunk_items,
                    start_index=start,
                    end_index=end,
                    is_first=(i == 0),
                    is_last=(i == num_partitions - 1 or end == len(items)),
                ))

        return chunks


def create_chunker(
    chunk_size: int = 100,
    **kwargs,
) -> DataChunkAction:
    """Factory function to create a DataChunkAction."""
    return DataChunkAction(chunk_size=chunk_size, **kwargs)
