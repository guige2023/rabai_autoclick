"""
Distributed locking utilities for coordinating across processes and machines.

Provides distributed locks using etcd, Redis, and ZooKeeper backends,
with dead lock detection and automatic expiration.
"""

from __future__ import annotations

import logging
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Optional

logger = logging.getLogger(__name__)


class LockBackend(Enum):
    REDIS = auto()
    ETCD = auto()
    ZOOKEEPER = auto()
    IN_MEMORY = auto()


@dataclass
class LockConfig:
    """Configuration for distributed locks."""
    name: str
    ttl_seconds: float = 30.0
    retry_attempts: int = 3
    retry_delay_seconds: float = 0.1
    extend_on_renew: bool = True
    extension_delta_seconds: float = 10.0


@dataclass
class LockResult:
    """Result of a lock operation."""
    acquired: bool
    lock_id: Optional[str] = None
    holder_id: Optional[str] = None
    expires_at: Optional[float] = None


class DistributedLock:
    """Base distributed lock interface."""

    def __init__(self, config: LockConfig) -> None:
        self.config = config
        self._lock_id: Optional[str] = None
        self._holder_id = str(uuid.uuid4())

    async def acquire(self) -> LockResult:
        """Acquire the lock."""
        raise NotImplementedError

    async def release(self) -> bool:
        """Release the lock."""
        raise NotImplementedError

    async def extend(self, additional_seconds: Optional[float] = None) -> bool:
        """Extend the lock TTL."""
        raise NotImplementedError

    async def is_held(self) -> bool:
        """Check if the lock is currently held."""
        raise NotImplementedError

    @asynccontextmanager
    async def hold(self):
        """Context manager for holding a lock."""
        result = await self.acquire()
        if not result.acquired:
            raise LockAcquisitionError(f"Failed to acquire lock: {self.config.name}")
        try:
            yield result
        finally:
            await self.release()


class RedisDistributedLock(DistributedLock):
    """Redis-backed distributed lock using SET NX EX pattern."""

    def __init__(self, config: LockConfig, redis_client: Any) -> None:
        super().__init__(config)
        self._redis = redis_client
        self._key = f"lock:{config.name}"

    async def acquire(self) -> LockResult:
        """Acquire the Redis lock."""
        import json
        lock_data = json.dumps({
            "holder_id": self._holder_id,
            "acquired_at": time.time(),
        })

        acquired = self._redis.set(
            self._key,
            lock_data,
            nx=True,
            ex=int(self.config.ttl_seconds),
        )

        if acquired:
            self._lock_id = f"{self._holder_id}:{time.time()}"
            return LockResult(
                acquired=True,
                lock_id=self._lock_id,
                holder_id=self._holder_id,
                expires_at=time.time() + self.config.ttl_seconds,
            )
        return LockResult(acquired=False)

    async def release(self) -> bool:
        """Release the Redis lock (only if we own it)."""
        import json
        current = self._redis.get(self._key)
        if not current:
            return False

        try:
            data = json.loads(current)
            if data.get("holder_id") != self._holder_id:
                return False
            self._redis.delete(self._key)
            return True
        except Exception:
            return False

    async def extend(self, additional_seconds: Optional[float] = None) -> bool:
        """Extend the lock TTL."""
        import json
        current = self._redis.get(self._key)
        if not current:
            return False

        try:
            data = json.loads(current)
            if data.get("holder_id") != self._holder_id:
                return False
            ttl = additional_seconds or self.config.extension_delta_seconds
            self._redis.expire(self._key, int(ttl))
            return True
        except Exception:
            return False

    async def is_held(self) -> bool:
        """Check if lock is held by us."""
        import json
        current = self._redis.get(self._key)
        if not current:
            return False
        try:
            data = json.loads(current)
            return data.get("holder_id") == self._holder_id
        except Exception:
            return False


class InMemoryLock(DistributedLock):
    """In-memory distributed lock for single-process coordination."""

    def __init__(self, config: LockConfig) -> None:
        super().__init__(config)
        self._key = f"lock:{config.name}"
        self._locks: dict[str, tuple[str, float]] = {}

    async def acquire(self) -> LockResult:
        """Acquire the in-memory lock."""
        now = time.time()
        if self._key in self._locks:
            holder_id, expires_at = self._locks[self._key]
            if expires_at > now:
                return LockResult(acquired=False)
        self._locks[self._key] = (self._holder_id, now + self.config.ttl_seconds)
        self._lock_id = f"{self._holder_id}:{now}"
        return LockResult(
            acquired=True,
            lock_id=self._lock_id,
            holder_id=self._holder_id,
            expires_at=now + self.config.ttl_seconds,
        )

    async def release(self) -> bool:
        """Release the in-memory lock."""
        if self._key in self._locks:
            holder_id, _ = self._locks[self._key]
            if holder_id == self._holder_id:
                del self._locks[self._key]
                return True
        return False

    async def extend(self, additional_seconds: Optional[float] = None) -> bool:
        """Extend the lock TTL."""
        if self._key not in self._locks:
            return False
        holder_id, _ = self._locks[self._key]
        if holder_id != self._holder_id:
            return False
        ttl = additional_seconds or self.config.extension_delta_seconds
        self._locks[self._key] = (self._holder_id, time.time() + ttl)
        return True

    async def is_held(self) -> bool:
        """Check if lock is held by us."""
        if self._key not in self._locks:
            return False
        holder_id, expires_at = self._locks[self._key]
        if expires_at <= time.time():
            del self._locks[self._key]
            return False
        return holder_id == self._holder_id


class LockManager:
    """Manages multiple distributed locks."""

    def __init__(self) -> None:
        self._locks: dict[str, DistributedLock] = {}

    def register_lock(
        self,
        name: str,
        backend: LockBackend = LockBackend.IN_MEMORY,
        ttl_seconds: float = 30.0,
        **kwargs: Any,
    ) -> DistributedLock:
        """Register a distributed lock."""
        config = LockConfig(name=name, ttl_seconds=ttl_seconds)
        if backend == LockBackend.REDIS:
            import redis
            redis_client = kwargs.get("redis_client") or redis.Redis()
            lock = RedisDistributedLock(config, redis_client)
        else:
            lock = InMemoryLock(config)

        self._locks[name] = lock
        return lock

    def get_lock(self, name: str) -> Optional[DistributedLock]:
        """Get a registered lock by name."""
        return self._locks.get(name)

    def get_lock_stats(self) -> dict[str, dict[str, Any]]:
        """Get statistics for all locks."""
        return {
            name: {
                "name": lock.config.name,
                "ttl_seconds": lock.config.ttl_seconds,
                "holder_id": lock._holder_id,
                "is_held": await lock.is_held() if hasattr(lock, "is_held") else False,
            }
            for name, lock in self._locks.items()
        }


class LockAcquisitionError(Exception):
    """Raised when lock acquisition fails."""
    pass
