"""Distributed locking for concurrent automation execution.

Provides mutual exclusion primitives for automation workflows
to prevent race conditions and ensure correct concurrent behavior.
"""

from __future__ import annotations

import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

import copy
import heapq


class LockType(Enum):
    """Type of lock."""
    EXCLUSIVE = "exclusive"
    SHARED = "shared"
    READ = "read"
    WRITE = "write"


class LockState(Enum):
    """State of a lock."""
    UNLOCKED = "unlocked"
    LOCKED = "locked"
    EXPIRED = "expired"


@dataclass
class LockToken:
    """Token representing a lock acquisition."""
    lock_id: str
    owner: str
    lock_type: LockType
    acquired_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None
    reference_count: int = 1


@dataclass(order=True)
class _WaitEntry:
    """Entry in the wait queue for a lock."""
    priority: int
    entry_id: str = field(compare=False)
    owner: str = field(compare=False)
    lock_type: LockType = field(compare=False)
    condition: threading.Condition = field(compare=False)
    acquired: bool = field(default=False, compare=False)


class LockRegistry:
    """Registry of active locks with ownership tracking."""

    def __init__(self, default_timeout: float = 30.0):
        self._locks: Dict[str, LockToken] = {}
        self._waiters: Dict[str, List[_WaitEntry]] = {}
        self._owners: Dict[str, Set[str]] = {}
        self._lock = threading.RLock()
        self._default_timeout = default_timeout

    def acquire(
        self,
        lock_id: str,
        owner: str,
        lock_type: LockType = LockType.EXCLUSIVE,
        timeout: Optional[float] = None,
        lease_duration: Optional[float] = None,
    ) -> Optional[LockToken]:
        """Attempt to acquire a lock.

        Args:
            lock_id: Unique identifier for the lock
            owner: Identifier of the lock owner
            lock_type: Type of lock to acquire
            timeout: Max time to wait for lock acquisition (0 = non-blocking)
            lease_duration: How long until lock automatically expires

        Returns:
            LockToken if acquired, None if failed (non-blocking) or timeout exceeded
        """
        timeout = timeout if timeout is not None else self._default_timeout
        deadline = time.time() + timeout if timeout > 0 else None

        with self._lock:
            existing = self._locks.get(lock_id)

            if existing and existing.owner == owner:
                existing.reference_count += 1
                return existing

            if existing:
                if existing.expires_at and existing.expires_at < time.time():
                    self._locks[lock_id] = LockToken(
                        lock_id=lock_id,
                        owner=owner,
                        lock_type=lock_type,
                        expires_at=time.time() + (lease_duration or self._default_timeout),
                    )
                    self._owners.setdefault(owner, set()).add(lock_id)
                    return self._locks[lock_id]

                if timeout == 0:
                    return None

            if lock_id not in self._waiters:
                self._waiters[lock_id] = []

            condition = threading.Condition()
            entry = _WaitEntry(
                priority=int(time.time() * 1000000),
                entry_id=str(uuid.uuid4()),
                owner=owner,
                lock_type=lock_type,
                condition=condition,
            )
            heapq.heappush(self._waiters[lock_id], entry)

        with condition:
            while True:
                with self._lock:
                    existing = self._locks.get(lock_id)

                    if not existing or (
                        existing.expires_at and existing.expires_at < time.time()
                    ):
                        token = LockToken(
                            lock_id=lock_id,
                            owner=owner,
                            lock_type=lock_type,
                            expires_at=time.time() + (lease_duration or self._default_timeout),
                        )
                        self._locks[lock_id] = token
                        self._owners.setdefault(owner, set()).add(lock_id)
                        entry.acquired = True

                        new_waiters = []
                        for w in self._waiters.get(lock_id, []):
                            if w.entry_id != entry.entry_id:
                                new_waiters.append(w)
                        self._waiters[lock_id] = new_waiters

                        return token

                    if deadline and time.time() >= deadline:
                        new_waiters = []
                        for w in self._waiters.get(lock_id, []):
                            if w.entry_id != entry.entry_id:
                                new_waiters.append(w)
                        self._waiters[lock_id] = new_waiters
                        return None

                if deadline:
                    remaining = deadline - time.time()
                    condition.wait(timeout=min(remaining, 0.1))
                else:
                    condition.wait(timeout=0.1)

    def release(self, lock_id: str, owner: str) -> bool:
        """Release a lock owned by the given owner."""
        with self._lock:
            token = self._locks.get(lock_id)
            if not token or token.owner != owner:
                return False

            token.reference_count -= 1
            if token.reference_count <= 0:
                old_expires = self._locks.pop(lock_id, None)
                if old_expires and owner in self._owners:
                    self._owners[owner].discard(lock_id)

                for waiter in self._waiters.get(lock_id, []):
                    with waiter.condition:
                        waiter.condition.notify_all()

            return True

    def extend(
        self,
        lock_id: str,
        owner: str,
        additional_time: float,
    ) -> bool:
        """Extend the lease of a lock."""
        with self._lock:
            token = self._locks.get(lock_id)
            if not token or token.owner != owner:
                return False
            if token.expires_at:
                token.expires_at += additional_time
            else:
                token.expires_at = time.time() + additional_time
            return True

    def get_lock_info(self, lock_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a lock."""
        with self._lock:
            token = self._locks.get(lock_id)
            if not token:
                return None
            return {
                "lock_id": token.lock_id,
                "owner": token.owner,
                "lock_type": token.lock_type.value,
                "acquired_at": datetime.fromtimestamp(token.acquired_at).isoformat(),
                "expires_at": (
                    datetime.fromtimestamp(token.expires_at).isoformat()
                    if token.expires_at else None
                ),
                "reference_count": token.reference_count,
                "waiter_count": len(self._waiters.get(lock_id, [])),
            }

    def get_owner_locks(self, owner: str) -> List[Dict[str, Any]]:
        """Get all locks owned by a specific owner."""
        with self._lock:
            lock_ids = self._owners.get(owner, set())
            return [
                self.get_lock_info(lid) or {}
                for lid in lock_ids
            ]

    def cleanup_expired(self) -> int:
        """Remove expired locks. Returns count of removed locks."""
        with self._lock:
            now = time.time()
            expired = [
                lid for lid, token in self._locks.items()
                if token.expires_at and token.expires_at < now
            ]
            for lid in expired:
                self._locks.pop(lid, None)
                for waiter in self._waiters.get(lid, []):
                    with waiter.condition:
                        waiter.condition.notify_all()
            return len(expired)


class AutomationLockAction:
    """Action providing locking capabilities for automation workflows."""

    def __init__(self, registry: Optional[LockRegistry] = None):
        self._registry = registry or LockRegistry()

    @contextmanager
    def lock(
        self,
        lock_id: str,
        owner: Optional[str] = None,
        lock_type: LockType = LockType.EXCLUSIVE,
        timeout: Optional[float] = None,
        lease_duration: Optional[float] = None,
    ):
        """Context manager for acquiring a lock.

        Usage:
            with action.lock("my-lock", owner="worker-1"):
                # critical section
                pass
        """
        owner = owner or str(uuid.uuid4())
        token = self._registry.acquire(
            lock_id=lock_id,
            owner=owner,
            lock_type=lock_type,
            timeout=timeout,
            lease_duration=lease_duration,
        )
        if not token:
            raise TimeoutError(f"Failed to acquire lock '{lock_id}' within timeout")
        try:
            yield token
        finally:
            self._registry.release(lock_id, owner)

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an action with lock protection.

        Required params:
            lock_id: str - Unique lock identifier
            operation: callable - Function to execute while holding lock

        Optional params:
            owner: str - Lock owner identifier
            lock_type: str - Type of lock (exclusive, shared, read, write)
            timeout: float - Acquisition timeout in seconds
            lease_duration: float - Lock lease duration in seconds
        """
        lock_id = params.get("lock_id")
        operation = params.get("operation")
        owner = params.get("owner")
        lock_type_str = params.get("lock_type", "exclusive")
        timeout = params.get("timeout")
        lease_duration = params.get("lease_duration")

        if not lock_id:
            raise ValueError("lock_id is required")
        if not callable(operation):
            raise ValueError("operation must be a callable")

        try:
            lock_type = LockType(lock_type_str.lower())
        except ValueError:
            lock_type = LockType.EXCLUSIVE

        owner = owner or f"automation-{uuid.uuid4().hex[:8]}"

        with self.lock(lock_id, owner, lock_type, timeout, lease_duration):
            result = operation(context=context, params=params.get("payload", {}))

        return {
            "lock_id": lock_id,
            "owner": owner,
            "result": result,
            "acquired": True,
        }

    def get_lock_status(self, lock_id: str) -> Dict[str, Any]:
        """Get the status of a lock."""
        info = self._registry.get_lock_info(lock_id)
        if not info:
            return {"lock_id": lock_id, "exists": False}
        return {**info, "exists": True}

    def release_lock(self, lock_id: str, owner: str) -> bool:
        """Manually release a lock."""
        return self._registry.release(lock_id, owner)

    def cleanup(self) -> int:
        """Clean up expired locks."""
        return self._registry.cleanup_expired()
