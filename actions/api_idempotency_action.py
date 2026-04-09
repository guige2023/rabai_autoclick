"""
API Idempotency Action Module.

Provides idempotency key management for safe API retries
with automatic deduplication and key lifecycle management.
"""

import asyncio
import hashlib
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


class IdempotencyStatus(Enum):
    """Idempotency key status."""
    NEW = "new"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    EXPIRED = "expired"


@dataclass
class IdempotencyKey:
    """Idempotency key record."""
    key: str
    status: IdempotencyStatus
    request_hash: str
    response: Any = None
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    expires_at: float = field(default_factory=lambda: time.time() + 86400)
    retry_count: int = 0
    metadata: dict = field(default_factory=dict)


@dataclass
class IdempotencyConfig:
    """Idempotency configuration."""
    ttl: float = 86400.0
    max_retries: int = 3
    lock_timeout: float = 30.0
    cleanup_interval: float = 3600.0


class InMemoryIdempotencyStore:
    """In-memory idempotency store."""

    def __init__(self):
        self._keys: dict[str, IdempotencyKey] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[IdempotencyKey]:
        """Get idempotency key."""
        return self._keys.get(key)

    async def set(self, record: IdempotencyKey) -> None:
        """Set idempotency key."""
        self._keys[record.key] = record

    async def delete(self, key: str) -> None:
        """Delete idempotency key."""
        if key in self._keys:
            del self._keys[key]

    async def cleanup_expired(self) -> int:
        """Remove expired keys."""
        now = time.time()
        expired = [
            k for k, v in self._keys.items()
            if v.expires_at < now
        ]
        for k in expired:
            del self._keys[k]
        return len(expired)


class APIIdempotencyAction:
    """
    API idempotency with key management.

    Example:
        idempotency = APIIdempotencyAction(ttl=3600)

        key = await idempotency.generate_key(request_data)
        result = await idempotency.execute(key, api_call)
    """

    def __init__(
        self,
        ttl: float = 86400.0,
        max_retries: int = 3
    ):
        self.config = IdempotencyConfig(
            ttl=ttl,
            max_retries=max_retries
        )
        self._store = InMemoryIdempotencyStore()
        self._running = False
        self._cleanup_task: Optional[asyncio.Task] = None

    def generate_key(
        self,
        request_data: Any,
        key_prefix: str = ""
    ) -> str:
        """Generate idempotency key from request data."""
        if isinstance(request_data, dict):
            data_str = str(sorted(request_data.items()))
        else:
            data_str = str(request_data)

        hash_value = hashlib.sha256(data_str.encode()).hexdigest()[:16]

        if key_prefix:
            return f"{key_prefix}:{hash_value}"
        return hash_value

    async def get_status(self, key: str) -> Optional[IdempotencyStatus]:
        """Get key status."""
        record = await self._store.get(key)
        if record:
            if record.expires_at < time.time():
                return IdempotencyStatus.EXPIRED
            return record.status
        return None

    async def execute(
        self,
        idempotency_key: str,
        func: Callable[[], Any],
        request_data: Any = None,
        *args: Any,
        **kwargs: Any
    ) -> tuple[Any, bool]:
        """
        Execute with idempotency check.

        Returns:
            Tuple of (result, is_cached)
        """
        existing = await self._store.get(idempotency_key)

        if existing:
            if existing.status == IdempotencyStatus.COMPLETED:
                return existing.response, True
            elif existing.status == IdempotencyStatus.IN_PROGRESS:
                while True:
                    await asyncio.sleep(0.5)
                    record = await self._store.get(idempotency_key)
                    if record and record.status == IdempotencyStatus.COMPLETED:
                        return record.response, True
                    elif not record or record.status == IdempotencyStatus.EXPIRED:
                        break

        request_hash = ""
        if request_data:
            if isinstance(request_data, dict):
                request_hash = hashlib.sha256(
                    str(sorted(request_data.items())).encode()
                ).hexdigest()

        record = IdempotencyKey(
            key=idempotency_key,
            status=IdempotencyStatus.IN_PROGRESS,
            request_hash=request_hash,
            expires_at=time.time() + self.config.ttl
        )
        await self._store.set(record)

        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = await asyncio.to_thread(func, *args, **kwargs)

            record.response = result
            record.status = IdempotencyStatus.COMPLETED
            record.completed_at = time.time()
            await self._store.set(record)

            return result, False

        except Exception as e:
            record.retry_count += 1

            if record.retry_count >= self.config.max_retries:
                record.status = IdempotencyStatus.EXPIRED
                await self._store.set(record)

            raise

    async def clear(self, key: str) -> None:
        """Clear idempotency key."""
        await self._store.delete(key)

    async def clear_expired(self) -> int:
        """Clear expired keys."""
        return await self._store.cleanup_expired()

    async def start_cleanup(self) -> None:
        """Start automatic cleanup."""
        self._running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def stop_cleanup(self) -> None:
        """Stop automatic cleanup."""
        self._running = False
        if self._cleanup_task:
            self._cleanup_task.cancel()

    async def _cleanup_loop(self) -> None:
        """Cleanup loop."""
        while self._running:
            await asyncio.sleep(self.config.cleanup_interval)
            await self.clear_expired()
