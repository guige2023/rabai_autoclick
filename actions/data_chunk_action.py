"""
Data Chunk Action - Chunks data into smaller batches.

This module provides data chunking capabilities for
processing large datasets in batches.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class ChunkResult:
    """Result of chunking operation."""
    chunks: list[list[dict[str, Any]]]
    chunk_count: int
    record_count: int
    chunk_size: int


class DataChunker:
    """Chunks data into smaller batches."""
    
    def __init__(self) -> None:
        pass
    
    def chunk_by_size(
        self,
        data: list[dict[str, Any]],
        chunk_size: int,
    ) -> list[list[dict[str, Any]]]:
        """Chunk data by size."""
        return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    
    def chunk_by_count(
        self,
        data: list[dict[str, Any]],
        chunk_count: int,
    ) -> list[list[dict[str, Any]]]:
        """Chunk data into specified number of chunks."""
        chunk_size = len(data) // chunk_count + 1
        return self.chunk_by_size(data, chunk_size)


class DataChunkAction:
    """Data chunk action for automation workflows."""
    
    def __init__(self) -> None:
        self.chunker = DataChunker()
    
    async def chunk_by_size(
        self,
        data: list[dict[str, Any]],
        chunk_size: int,
    ) -> ChunkResult:
        """Chunk data by size."""
        chunks = self.chunker.chunk_by_size(data, chunk_size)
        return ChunkResult(
            chunks=chunks,
            chunk_count=len(chunks),
            record_count=len(data),
            chunk_size=chunk_size,
        )


__all__ = ["ChunkResult", "DataChunker", "DataChunkAction"]
