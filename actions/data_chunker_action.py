"""
Data Chunker Action Module.

Provides data chunking with overlap support
for batch processing and streaming.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar, Optional

T = TypeVar("T")


@dataclass
class ChunkConfig:
    """Chunking configuration."""
    chunk_size: int = 100
    overlap: int = 0
    strategy: str = "fixed"
    max_chunks: Optional[int] = None


@dataclass
class Chunk(Generic[T]):
    """Data chunk."""
    index: int
    data: list[T]
    start_index: int
    end_index: int
    is_first: bool = False
    is_last: bool = False


class DataChunkerAction:
    """
    Data chunking for batch processing.

    Example:
        chunker = DataChunkerAction(
            chunk_size=100,
            overlap=10
        )

        chunks = chunker.chunk(data)
        for chunk in chunks:
            await process_batch(chunk.data)
    """

    def __init__(
        self,
        chunk_size: int = 100,
        overlap: int = 0,
        strategy: str = "fixed"
    ):
        self.config = ChunkConfig(
            chunk_size=chunk_size,
            overlap=overlap,
            strategy=strategy
        )

    def chunk(self, data: list[T]) -> list[Chunk[T]]:
        """Split data into chunks."""
        if not data:
            return []

        chunks = []
        start = 0
        index = 0

        while start < len(data):
            end = min(start + self.config.chunk_size, len(data))

            chunk = Chunk(
                index=index,
                data=data[start:end],
                start_index=start,
                end_index=end,
                is_first=(index == 0),
                is_last=(end >= len(data))
            )
            chunks.append(chunk)

            if self.config.max_chunks and index >= self.config.max_chunks - 1:
                break

            start = end - self.config.overlap if self.config.overlap > 0 else end
            index += 1

        return chunks

    def chunk_by_count(self, data: list[T], num_chunks: int) -> list[Chunk[T]]:
        """Split data into fixed number of chunks."""
        if not data or num_chunks <= 0:
            return []

        chunk_size = len(data) // num_chunks
        if chunk_size == 0:
            chunk_size = 1

        self.config.chunk_size = chunk_size
        return self.chunk(data)

    async def chunk_async(
        self,
        data: list[T],
        process_func: Callable[[Chunk[T]], Any]
    ) -> list[Any]:
        """Process data in chunks asynchronously."""
        chunks = self.chunk(data)
        results = []

        for chunk in chunks:
            result = await process_func(chunk)
            results.append(result)

        return results

    async def chunk_parallel(
        self,
        data: list[T],
        process_func: Callable[[Chunk[T]], Any],
        max_concurrent: int = 5
    ) -> list[Any]:
        """Process chunks in parallel."""
        chunks = self.chunk(data)
        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_with_semaphore(chunk: Chunk[T]) -> Any:
            async with semaphore:
                return await process_func(chunk)

        tasks = [process_with_semaphore(chunk) for chunk in chunks]
        return await asyncio.gather(*tasks)

    def get_chunk_stats(self, data: list[T]) -> dict:
        """Get chunking statistics."""
        chunks = self.chunk(data)
        return {
            "total_items": len(data),
            "chunk_count": len(chunks),
            "chunk_size": self.config.chunk_size,
            "overlap": self.config.overlap,
            "avg_chunk_size": len(data) / len(chunks) if chunks else 0
        }
