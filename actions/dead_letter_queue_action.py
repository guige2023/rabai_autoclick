"""Dead Letter Queue Action Module.

Handle failed messages with retry and inspection capabilities.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class DLQReason(Enum):
    """Reason for message going to DLQ."""
    MAX_RETRIES_EXCEEDED = "max_retries_exceeded"
    UNHANDLED_EXCEPTION = "unhandled_exception"
    INVALID_MESSAGE = "invalid_message"
    PROCESSING_TIMEOUT = "processing_timeout"
    SCHEMA_VALIDATION_FAILED = "schema_validation_failed"


@dataclass
class DLQEntry(Generic[T]):
    """Dead letter queue entry."""
    entry_id: str
    original_message: T
    reason: DLQReason
    error_message: str
    retry_count: int
    first_failed_at: float
    last_failed_at: float
    metadata: dict = field(default_factory=dict)
    original_topic: str | None = None
    original_partition: int | None = None


@dataclass
class DLQStats:
    """Dead letter queue statistics."""
    total_entries: int = 0
    by_reason: dict[DLQReason, int] = field(default_factory=dict)
    oldest_entry_age: float = 0.0
    newest_entry_age: float = 0.0


class DeadLetterQueue(Generic[T]):
    """Dead letter queue for failed messages."""

    def __init__(
        self,
        max_size: int = 10000,
        max_age_seconds: float | None = None
    ) -> None:
        self.max_size = max_size
        self.max_age = max_age_seconds
        self._queue: deque[DLQEntry] = deque(maxlen=max_size)
        self._lock = asyncio.Lock()
        self._handlers: list[Callable[[DLQEntry], Any]] = []

    async def put(
        self,
        message: T,
        reason: DLQReason,
        error: str,
        retry_count: int = 0,
        metadata: dict | None = None,
        topic: str | None = None,
        partition: int | None = None
    ) -> str:
        """Add entry to dead letter queue."""
        import uuid
        entry_id = str(uuid.uuid4())
        now = time.time()
        entry = DLQEntry(
            entry_id=entry_id,
            original_message=message,
            reason=reason,
            error_message=error,
            retry_count=retry_count,
            first_failed_at=now,
            last_failed_at=now,
            metadata=metadata or {},
            original_topic=topic,
            original_partition=partition
        )
        async with self._lock:
            self._queue.append(entry)
            if self.max_age:
                self._evict_old()
        for handler in self._handlers:
            result = handler(entry)
            if asyncio.iscoroutine(result):
                await result
        return entry_id

    async def get(self, count: int = 1) -> list[DLQEntry]:
        """Get entries from DLQ without removing."""
        async with self._lock:
            entries = list(self._queue)[-count:]
            return entries

    async def get_by_reason(self, reason: DLQReason, limit: int = 100) -> list[DLQEntry]:
        """Get entries by reason."""
        async with self._lock:
            return [e for e in self._queue if e.reason == reason][-limit:]

    async def requeue(
        self,
        entry_id: str,
        target_queue: Callable[[T], Any]
    ) -> bool:
        """Requeue message from DLQ for retry."""
        async with self._lock:
            for i, entry in enumerate(self._queue):
                if entry.entry_id == entry_id:
                    self._queue.remove(entry)
                    result = target_queue(entry.original_message)
                    if asyncio.iscoroutine(result):
                        await result
                    return True
            return False

    async def remove(self, entry_id: str) -> bool:
        """Remove entry from DLQ."""
        async with self._lock:
            for i, entry in enumerate(self._queue):
                if entry.entry_id == entry_id:
                    self._queue.remove(entry)
                    return True
            return False

    async def clear(self) -> int:
        """Clear all entries. Returns count cleared."""
        async with self._lock:
            count = len(self._queue)
            self._queue.clear()
            return count

    async def get_stats(self) -> DLQStats:
        """Get DLQ statistics."""
        async with self._lock:
            now = time.time()
            stats = DLQStats(total_entries=len(self._queue))
            for entry in self._queue:
                reason_count = stats.by_reason.get(entry.reason, 0)
                stats.by_reason[entry.reason] = reason_count + 1
            if self._queue:
                stats.oldest_entry_age = now - self._queue[0].last_failed_at
                stats.newest_entry_age = now - self._queue[-1].last_failed_at
            return stats

    def on_entry(self, handler: Callable[[DLQEntry], Any]) -> None:
        """Register handler for new DLQ entries."""
        self._handlers.append(handler)

    async def _evict_old(self) -> None:
        """Evict entries older than max age."""
        if not self.max_age:
            return
        now = time.time()
        while self._queue and (now - self._queue[0].last_failed_at) > self.max_age:
            self._queue.popleft()
