"""Data Chunking and Batching Utility.

This module provides data chunking for batch processing:
- Configurable chunk sizes
- Overlap support
- Multiple chunking strategies
- Stream processing support

Example:
    >>> from actions.data_chunking_action import DataChunker
    >>> chunker = DataChunker(chunk_size=1000, overlap=100)
    >>> chunks = chunker.chunk_list(large_list)
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable, Generator, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class DataChunker:
    """Chunks data for batch processing."""

    def __init__(
        self,
        chunk_size: int = 1000,
        overlap: int = 0,
        strategy: str = "fixed",
    ) -> None:
        """Initialize the chunker.

        Args:
            chunk_size: Target size of each chunk.
            overlap: Number of overlapping elements between chunks.
            strategy: "fixed", "dynamic", or "record_boundary".
        """
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if overlap < 0:
            raise ValueError("overlap must be non-negative")
        if overlap >= chunk_size:
            raise ValueError("overlap must be less than chunk_size")

        self._chunk_size = chunk_size
        self._overlap = overlap
        self._strategy = strategy
        self._lock = threading.Lock()

    def chunk_list(self, items: list[T]) -> list[list[T]]:
        """Split a list into chunks.

        Args:
            items: List to chunk.

        Returns:
            List of chunks.
        """
        if not items:
            return []

        with self._lock:
            if self._strategy == "fixed":
                return self._chunk_fixed(items)
            elif self._strategy == "dynamic":
                return self._chunk_dynamic(items)
            else:
                return self._chunk_fixed(items)

    def _chunk_fixed(self, items: list[T]) -> list[list[T]]:
        """Fixed-size chunking with optional overlap."""
        chunks = []
        step = self._chunk_size - self._overlap

        for i in range(0, len(items), step):
            chunk = items[i:i + self._chunk_size]
            if chunk:
                chunks.append(chunk)

        return chunks

    def _chunk_dynamic(self, items: list[T]) -> list[list[T]]:
        """Dynamic chunking based on item content."""
        chunks = []
        current_chunk = []
        current_size = 0

        for item in items:
            item_size = self._estimate_size(item)
            if current_size + item_size > self._chunk_size and current_chunk:
                chunks.append(current_chunk)
                overlap_start = max(0, len(current_chunk) - self._overlap)
                current_chunk = current_chunk[overlap_start:]
                current_size = sum(self._estimate_size(x) for x in current_chunk)

            current_chunk.append(item)
            current_size += item_size

        if current_chunk:
            chunks.append(current_chunk)

        return chunks

    def chunk_dict(
        self,
        data: dict[str, Any],
        key_groups: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """Split a dictionary into chunks.

        Args:
            data: Dict to chunk.
            key_groups: Number of chunk groups. None = use chunk_size.

        Returns:
            List of dict chunks.
        """
        keys = list(data.keys())
        if key_groups:
            chunk_size = max(1, len(keys) // key_groups)
        else:
            chunk_size = self._chunk_size

        chunks = []
        for i in range(0, len(keys), chunk_size):
            chunk_keys = keys[i:i + chunk_size]
            chunks.append({k: data[k] for k in chunk_keys})

        return chunks

    def chunk_by_keys(
        self,
        items: list[dict[str, Any]],
        key: str,
    ) -> dict[Any, list[dict[str, Any]]]:
        """Group items by a key and return as chunks.

        Args:
            items: List of dicts.
            key: Key to group by.

        Returns:
            Dict mapping key value to list of items.
        """
        groups: dict[Any, list[dict[str, Any]]] = {}
        for item in items:
            group_key = item.get(key)
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(item)

        return groups

    def stream_chunks(
        self,
        items: list[T],
    ) -> Generator[list[T], None, None]:
        """Stream chunks one at a time (memory efficient).

        Args:
            items: List to chunk.

        Yields:
            Chunks one at a time.
        """
        step = self._chunk_size - self._overlap
        for i in range(0, len(items), step):
            yield items[i:i + self._chunk_size]

    def merge_chunks(self, chunks: list[list[T]]) -> list[T]:
        """Merge chunks back into a single list.

        Args:
            chunks: List of chunks.

        Returns:
            Merged list.
        """
        result = []
        for chunk in chunks:
            result.extend(chunk)
        return result

    def _estimate_size(self, item: Any) -> int:
        """Estimate size of an item in bytes."""
        import sys
        try:
            return len(str(item))
        except Exception:
            return 1

    @property
    def chunk_size(self) -> int:
        """Get current chunk size."""
        return self._chunk_size

    @property
    def overlap(self) -> int:
        """Get current overlap."""
        return self._overlap
