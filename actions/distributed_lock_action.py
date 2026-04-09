"""Distributed Lock Action Module.

Distributed locking for coordination across processes/nodes.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class LockState(Enum):
    """Lock state."""
    UNLOCKED = "unlocked"
    LOCKED = "locked"
    EXPIRED = "expired"


@dataclass
class LockResult:
    """Result of lock acquisition."""
    acquired: bool
    lock_id: str | None = None
    holder_id: str | None = None
    expires_at: float | None = None
    wait_time: float = 0.0


class DistributedLock(Generic[T]):
    """Distributed lock with TTL and auto-release."""

    def __init__(
        self,
        name: str,
        ttl_seconds: float = 30.0,
        storage: Generic[T] | None = None
    ) -> None:
        self.name = name
        self.ttl = ttl_seconds
        self._storage = storage
        self._lock_id: str | None = None
        self._holder_id: str | None = None
        self._expires_at: float | None = None
        self._lock = asyncio.Lock()

    async def acquire(
        self,
        holder_id: str | None = None,
        timeout: float | None = None,
        retry_interval: float = 0.1
    ) -> LockResult:
        """Acquire the lock."""
        holder_id = holder_id or str(uuid.uuid4())
        start = time.monotonic()
        while True:
            async with self._lock:
                if self._is_available():
                    self._lock_id = str(uuid.uuid4())
                    self._holder_id = holder_id
                    self._expires_at = time.time() + self.ttl
                    if self._storage:
                        await self._storage.set(self.name, {
                            "lock_id": self._lock_id,
                            "holder_id": holder_id,
                            "expires_at": self._expires_at
                        })
                    return LockResult(
                        acquired=True,
                        lock_id=self._lock_id,
                        holder_id=holder_id,
                        expires_at=self._expires_at,
                        wait_time=time.monotonic() - start
                    )
            if timeout and (time.monotonic() - start) >= timeout:
                return LockResult(
                    acquired=False,
                    wait_time=time.monotonic() - start
                )
            await asyncio.sleep(retry_interval)

    async def release(self, lock_id: str | None = None) -> bool:
        """Release the lock."""
        async with self._lock:
            if lock_id and lock_id != self._lock_id:
                return False
            if self._lock_id is None:
                return True
            self._lock_id = None
            self._holder_id = None
            self._expires_at = None
            if self._storage:
                await self._storage.delete(self.name)
            return True

    async def extend(self, additional_seconds: float, lock_id: str | None = None) -> bool:
        """Extend lock TTL."""
        async with self._lock:
            if lock_id and lock_id != self._lock_id:
                return False
            if self._lock_id is None:
                return False
            self._expires_at = time.time() + additional_seconds
            return True

    def _is_available(self) -> bool:
        """Check if lock is available."""
        if self._lock_id is None:
            return True
        if self._expires_at and time.time() > self._expires_at:
            self._lock_id = None
            self._holder_id = None
            self._expires_at = None
            return True
        return False

    def is_locked(self) -> bool:
        """Check if lock is currently held."""
        return self._lock_id is not None and not self._is_available()


class LockManager:
    """Manager for multiple distributed locks."""

    def __init__(self) -> None:
        self._locks: dict[str, DistributedLock] = {}
        self._lock = asyncio.Lock()

    async def get_lock(
        self,
        name: str,
        ttl_seconds: float = 30.0
    ) -> DistributedLock:
        """Get or create a lock."""
        async with self._lock:
            if name not in self._locks:
                self._locks[name] = DistributedLock(name, ttl_seconds)
            return self._locks[name]

    async def acquire_multi(
        self,
        lock_names: list[str],
        timeout: float | None = None
    ) -> tuple[bool, list[str]]:
        """Acquire multiple locks atomically."""
        acquired = []
        for name in lock_names:
            lock = await self.get_lock(name)
            result = await lock.acquire(timeout=timeout)
            if result.acquired:
                acquired.append(name)
            else:
                for acquired_name in acquired:
                    lock = self._locks[acquired_name]
                    await lock.release()
                return False, []
        return True, acquired
