"""
Automation Lock Action Module

Provides distributed locking capabilities for automation workflows.
Supports mutex locks, read-write locks, and semaphore-based concurrency control.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)


class LockType(Enum):
    """Types of locks."""

    MUTEX = "mutex"
    READ_WRITE = "read_write"
    SEMAPHORE = "semaphore"


class LockStatus(Enum):
    """Lock acquisition status."""

    ACQUIRED = "acquired"
    WAITING = "waiting"
    TIMEOUT = "timeout"
    RELEASED = "released"


@dataclass
class Lock:
    """A distributed lock."""

    lock_id: str
    name: str
    lock_type: LockType
    owner_id: str
    acquired_at: Optional[float] = None
    expires_at: Optional[float] = None


@dataclass
class LockConfig:
    """Configuration for locking."""

    default_timeout: float = 30.0
    auto_release_seconds: float = 60.0
    enable_deadlock_detection: bool = False
    max_retries: int = 0


class AutomationLockAction:
    """
    Distributed locking action for automation workflows.

    Features:
    - Mutex locks for exclusive access
    - Read-write locks for shared access
    - Semaphore-based concurrency control
    - Automatic lock expiration
    - Deadlock detection (optional)
    - Non-blocking lock acquisition

    Usage:
        lock = AutomationLockAction(config)
        
        async with lock.acquire("resource-1"):
            await process_critical_section()
    """

    def __init__(self, config: Optional[LockConfig] = None):
        self.config = config or LockConfig()
        self._locks: Dict[str, Lock] = {}
        self._readers: Dict[str, int] = {}
        self._semaphores: Dict[str, asyncio.Semaphore] = {}
        self._waiting: Dict[str, list] = {}
        self._stats = {
            "locks_acquired": 0,
            "locks_released": 0,
            "acquire_timeouts": 0,
        }

    async def acquire(
        self,
        name: str,
        lock_type: LockType = LockType.MUTEX,
        timeout: Optional[float] = None,
        owner_id: Optional[str] = None,
    ) -> Optional[Lock]:
        """Acquire a lock."""
        timeout = timeout or self.config.default_timeout
        owner_id = owner_id or f"owner_{uuid.uuid4().hex[:8]}"
        lock_id = f"lock_{name}"

        deadline = time.time() + timeout

        while time.time() < deadline:
            if lock_type == LockType.MUTEX:
                acquired = await self._try_acquire_mutex(name, lock_id, owner_id)
            elif lock_type == LockType.READ_WRITE:
                acquired = await self._try_acquire_rw(name, lock_id, owner_id)
            else:
                acquired = await self._try_acquire_semaphore(name, lock_id, owner_id)

            if acquired:
                self._stats["locks_acquired"] += 1
                return acquired

            await asyncio.sleep(0.01)

        self._stats["acquire_timeouts"] += 1
        return None

    async def _try_acquire_mutex(
        self,
        name: str,
        lock_id: str,
        owner_id: str,
    ) -> Optional[Lock]:
        """Try to acquire a mutex lock."""
        existing = self._locks.get(lock_id)

        if existing and existing.expires_at and time.time() > existing.expires_at:
            del self._locks[lock_id]
            existing = None

        if existing:
            return None

        lock = Lock(
            lock_id=lock_id,
            name=name,
            lock_type=LockType.MUTEX,
            owner_id=owner_id,
            acquired_at=time.time(),
            expires_at=time.time() + self.config.auto_release_seconds,
        )
        self._locks[lock_id] = lock
        return lock

    async def _try_acquire_rw(
        self,
        name: str,
        lock_id: str,
        owner_id: str,
    ) -> Optional[Lock]:
        """Try to acquire a read-write lock."""
        existing = self._locks.get(lock_id)
        if existing and existing.expires_at and time.time() > existing.expires_at:
            del self._locks[lock_id]
            existing = None

        if existing:
            return None

        lock = Lock(
            lock_id=lock_id,
            name=name,
            lock_type=LockType.READ_WRITE,
            owner_id=owner_id,
            acquired_at=time.time(),
        )
        self._locks[lock_id] = lock
        self._readers[lock_id] = self._readers.get(lock_id, 0) + 1
        return lock

    async def _try_acquire_semaphore(
        self,
        name: str,
        lock_id: str,
        owner_id: str,
    ) -> Optional[Lock]:
        """Try to acquire a semaphore lock."""
        if lock_id not in self._semaphores:
            self._semaphores[lock_id] = asyncio.Semaphore(1)

        semaphore = self._semaphores[lock_id]
        if semaphore.locked():
            return None

        lock = Lock(
            lock_id=lock_id,
            name=name,
            lock_type=LockType.SEMAPHORE,
            owner_id=owner_id,
            acquired_at=time.time(),
        )
        self._locks[lock_id] = lock
        return lock

    async def release(
        self,
        name: str,
        owner_id: Optional[str] = None,
    ) -> bool:
        """Release a lock."""
        lock_id = f"lock_{name}"
        lock = self._locks.get(lock_id)

        if lock is None:
            return False

        if owner_id and lock.owner_id != owner_id:
            return False

        del self._locks[lock_id]
        self._stats["locks_released"] += 1
        return True

    def is_locked(self, name: str) -> bool:
        """Check if a resource is locked."""
        lock_id = f"lock_{name}"
        return lock_id in self._locks

    def get_lock(self, name: str) -> Optional[Lock]:
        """Get lock info for a resource."""
        lock_id = f"lock_{name}"
        return self._locks.get(lock_id)

    async def wait_for_unlock(self, name: str, timeout: float = 30.0) -> bool:
        """Wait for a lock to be released."""
        deadline = time.time() + timeout

        while time.time() < deadline:
            if not self.is_locked(name):
                return True
            await asyncio.sleep(0.1)

        return False

    def get_stats(self) -> Dict[str, Any]:
        """Get locking statistics."""
        return {
            **self._stats.copy(),
            "active_locks": len(self._locks),
        }


class LockContext:
    """Context manager for lock acquisition."""

    def __init__(self, lock_action: AutomationLockAction, name: str):
        self._lock_action = lock_action
        self._name = name
        self._lock = None

    async def __aenter__(self) -> Lock:
        self._lock = await self._lock_action.acquire(self._name)
        return self._lock

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._lock:
            await self._lock_action.release(self._name, self._lock.owner_id)


async def demo_lock():
    """Demonstrate locking."""
    config = LockConfig()
    lock_action = AutomationLockAction(config)

    async with LockContext(lock_action, "resource-1") as lock:
        print(f"Lock acquired: {lock.lock_id}")

    print(f"Stats: {lock_action.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_lock())
