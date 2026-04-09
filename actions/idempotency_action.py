"""Idempotency Action Module.

Ensure operations are idempotent with deduplication and caching.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")
R = TypeVar("R")


class IdempotencyStatus(Enum):
    """Idempotency check status."""
    NEW = "new"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class IdempotencyEntry:
    """Idempotency record."""
    key: str
    status: IdempotencyStatus
    result: Any = None
    error: str | None = None
    created_at: float = 0.0
    completed_at: float | None = None
    expires_at: float | None = None


class IdempotencyStore(Generic[T, R]):
    """Store for idempotency keys with result caching."""

    def __init__(self, ttl_seconds: float = 3600, cleanup_interval: float = 300) -> None:
        self.ttl = ttl_seconds
        self._store: dict[str, IdempotencyEntry] = {}
        self._lock = asyncio.Lock()
        self._cleanup_task: asyncio.Task | None = None
        self._cleanup_interval = cleanup_interval

    async def start(self) -> None:
        """Start cleanup task."""
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def stop(self) -> None:
        """Stop cleanup task."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

    async def check(self, key: str) -> IdempotencyStatus | None:
        """Check idempotency status."""
        async with self._lock:
            entry = self._store.get(key)
            if not entry:
                return None
            if entry.expires_at and time.time() > entry.expires_at:
                del self._store[key]
                return None
            return entry.status

    async def start_operation(self, key: str) -> IdempotencyEntry:
        """Mark operation as in progress."""
        async with self._lock:
            entry = IdempotencyEntry(
                key=key,
                status=IdempotencyStatus.IN_PROGRESS,
                created_at=time.time(),
                expires_at=time.time() + self.ttl
            )
            self._store[key] = entry
            return entry

    async def complete_operation(
        self,
        key: str,
        result: R | None = None,
        error: str | None = None
    ) -> None:
        """Mark operation as completed."""
        async with self._lock:
            entry = self._store.get(key)
            if entry:
                entry.status = IdempotencyStatus.COMPLETED if error is None else IdempotencyStatus.FAILED
                entry.result = result
                entry.error = error
                entry.completed_at = time.time()

    async def execute_idempotent(
        self,
        key: str,
        operation: Callable[[], T | asyncio.coroutine],
        *args,
        **kwargs
    ) -> tuple[T | None, IdempotencyStatus, bool]:
        """Execute operation idempotently.

        Returns: (result, status, was_executed)
        """
        status = await self.check(key)
        if status == IdempotencyStatus.COMPLETED:
            entry = self._store.get(key)
            return entry.result, IdempotencyStatus.COMPLETED, False
        if status == IdempotencyStatus.IN_PROGRESS:
            entry = self._store.get(key)
            return None, IdempotencyStatus.IN_PROGRESS, False
        await self.start_operation(key)
        try:
            result = operation(*args, **kwargs)
            if asyncio.iscoroutine(result):
                result = await result
            await self.complete_operation(key, result=result)
            return result, IdempotencyStatus.COMPLETED, True
        except Exception as e:
            await self.complete_operation(key, error=str(e))
            raise

    def generate_key(self, *args, **kwargs) -> str:
        """Generate idempotency key from arguments."""
        content = str(args) + str(sorted(kwargs.items()))
        return hashlib.sha256(content.encode()).hexdigest()

    async def _cleanup_loop(self) -> None:
        """Periodically clean up expired entries."""
        while True:
            await asyncio.sleep(self._cleanup_interval)
            async with self._lock:
                now = time.time()
                expired = [
                    k for k, v in self._store.items()
                    if v.expires_at and now > v.expires_at
                ]
                for k in expired:
                    del self._store[k]
