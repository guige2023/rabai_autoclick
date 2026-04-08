"""
Data Chunking Action Module.

Provides data chunking capabilities for
processing large datasets in batches.
"""

from typing import Any, Callable, Dict, Iterator, List, Optional
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


class ChunkStrategy(Enum):
    """Chunk strategies."""
    FIXED_SIZE = "fixed_size"
    BY_KEY = "by_key"
    ADAPTIVE = "adaptive"


class Enum:
    pass


@dataclass
class Chunk:
    """Data chunk."""
    chunk_id: int
    data: List[Any]
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ChunkConfig:
    """Chunk configuration."""
    chunk_size: int = 100
    strategy: ChunkStrategy = ChunkStrategy.FIXED_SIZE
    overlap: int = 0
    key_field: Optional[str] = None


class DataChunker:
    """Chunks data for processing."""

    def __init__(self, config: ChunkConfig):
        self.config = config

    def chunk_list(self, data: List[Any]) -> List[Chunk]:
        """Chunk a list into smaller pieces."""
        chunks = []

        if self.config.strategy == ChunkStrategy.FIXED_SIZE:
            chunks = self._chunk_fixed_size(data)

        elif self.config.strategy == ChunkStrategy.BY_KEY:
            chunks = self._chunk_by_key(data)

        elif self.config.strategy == ChunkStrategy.ADAPTIVE:
            chunks = self._chunk_adaptive(data)

        return chunks

    def _chunk_fixed_size(self, data: List[Any]) -> List[Chunk]:
        """Chunk by fixed size."""
        chunks = []
        chunk_id = 0

        for i in range(0, len(data), self.config.chunk_size):
            chunk_data = data[i:i + self.config.chunk_size]
            chunk = Chunk(
                chunk_id=chunk_id,
                data=chunk_data,
                metadata={
                    "start_index": i,
                    "end_index": i + len(chunk_data),
                    "size": len(chunk_data)
                }
            )
            chunks.append(chunk)
            chunk_id += 1

        return chunks

    def _chunk_by_key(self, data: List[Dict[str, Any]]) -> List[Chunk]:
        """Chunk by key field."""
        if not self.config.key_field:
            return self._chunk_fixed_size(data)

        chunks = []
        chunk_id = 0
        current_group: List[Any] = []
        current_key = None

        for item in data:
            if isinstance(item, dict):
                key = item.get(self.config.key_field)
            else:
                key = None

            if current_key is None:
                current_key = key

            if key == current_key:
                current_group.append(item)
            else:
                if current_group:
                    chunks.append(Chunk(
                        chunk_id=chunk_id,
                        data=current_group,
                        metadata={"key": current_key, "size": len(current_group)}
                    ))
                    chunk_id += 1

                current_group = [item]
                current_key = key

        if current_group:
            chunks.append(Chunk(
                chunk_id=chunk_id,
                data=current_group,
                metadata={"key": current_key, "size": len(current_group)}
            ))

        return chunks

    def _chunk_adaptive(self, data: List[Dict[str, Any]]) -> List[Chunk]:
        """Adaptive chunking based on record size."""
        chunks = []
        chunk_id = 0
        current_chunk: List[Any] = []
        current_size = 0

        for item in data:
            item_size = self._estimate_size(item)

            if current_size + item_size > self.config.chunk_size * 1000:
                if current_chunk:
                    chunks.append(Chunk(
                        chunk_id=chunk_id,
                        data=current_chunk,
                        metadata={"size_bytes": current_size}
                    ))
                    chunk_id += 1
                    current_chunk = []
                    current_size = 0

            current_chunk.append(item)
            current_size += item_size

        if current_chunk:
            chunks.append(Chunk(
                chunk_id=chunk_id,
                data=current_chunk,
                metadata={"size_bytes": current_size}
            ))

        return chunks

    def _estimate_size(self, item: Any) -> int:
        """Estimate size of item in bytes."""
        import sys
        return len(str(item).encode('utf-8'))


class ChunkProcessor:
    """Processes data chunks."""

    def __init__(self, chunker: DataChunker):
        self.chunker = chunker

    def process_chunks(
        self,
        data: List[Any],
        processor: Callable[[Chunk], Any]
    ) -> List[Any]:
        """Process all chunks sequentially."""
        chunks = self.chunker.chunk_list(data)
        results = []

        for chunk in chunks:
            try:
                result = processor(chunk)
                results.append(result)
            except Exception as e:
                logger.error(f"Chunk processing error: {e}")
                results.append(None)

        return results

    async def process_chunks_async(
        self,
        data: List[Any],
        processor: Callable[[Chunk], Any]
    ) -> List[Any]:
        """Process chunks asynchronously."""
        chunks = self.chunker.chunk_list(data)
        results = []

        for chunk in chunks:
            try:
                if asyncio.iscoroutinefunction(processor):
                    result = await processor(chunk)
                else:
                    result = processor(chunk)
                results.append(result)
            except Exception as e:
                logger.error(f"Chunk processing error: {e}")
                results.append(None)

        return results


class ChunkIterator:
    """Iterates over chunks."""

    def __init__(self, chunker: DataChunker, data: List[Any]):
        self.chunker = chunker
        self.data = data
        self.chunks = chunker.chunk_list(data)
        self.index = 0

    def __iter__(self):
        return self

    def __next__(self) -> Chunk:
        if self.index >= len(self.chunks):
            raise StopIteration

        chunk = self.chunks[self.index]
        self.index += 1
        return chunk

    def __len__(self) -> int:
        return len(self.chunks)


def main():
    """Demonstrate data chunking."""
    config = ChunkConfig(chunk_size=5, strategy=ChunkStrategy.FIXED_SIZE)
    chunker = DataChunker(config)

    data = list(range(23))
    chunks = chunker.chunk_list(data)

    print(f"Data size: {len(data)}")
    print(f"Number of chunks: {len(chunks)}")

    for chunk in chunks:
        print(f"Chunk {chunk.chunk_id}: {len(chunk.data)} items")


if __name__ == "__main__":
    main()
