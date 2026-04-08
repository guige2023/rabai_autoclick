"""
Storage Buffer Utilities

Provides utilities for buffering data storage
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable
import time


class StorageBuffer:
    """
    Buffers data before writing to storage.
    
    Aggregates multiple writes and flushes
    when buffer is full or timeout expires.
    """

    def __init__(
        self,
        max_size: int = 100,
        flush_interval_seconds: float = 60.0,
        writer: Callable[[list[Any]], None] | None = None,
    ) -> None:
        self._max_size = max_size
        self._flush_interval = flush_interval_seconds
        self._writer = writer
        self._buffer: list[Any] = []
        self._last_flush = time.time()

    def add(self, item: Any) -> None:
        """Add item to buffer."""
        self._buffer.append(item)
        if len(self._buffer) >= self._max_size:
            self.flush()

    def flush(self) -> int:
        """
        Flush buffer to storage.
        
        Returns:
            Number of items flushed.
        """
        if not self._buffer:
            return 0
        if self._writer:
            self._writer(self._buffer)
        count = len(self._buffer)
        self._buffer.clear()
        self._last_flush = time.time()
        return count

    def should_flush(self) -> bool:
        """Check if buffer should be flushed."""
        if len(self._buffer) >= self._max_size:
            return True
        elapsed = time.time() - self._last_flush
        if elapsed >= self._flush_interval and self._buffer:
            return True
        return False

    def size(self) -> int:
        """Get current buffer size."""
        return len(self._buffer)
