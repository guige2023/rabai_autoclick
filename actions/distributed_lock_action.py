"""Distributed Lock Action Module.

Provides distributed locking mechanism for coordinating access
to shared resources across processes or machines.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class LockStatus(Enum):
    """Lock status."""
    ACQUIRED = "acquired"
    RELEASED = "released"
    EXPIRED = "expired"
    CONFLICT = "conflict"


@dataclass
class Lock:
    """Distributed lock."""
    key: str
    owner_id: str
    acquired_at: float
    expires_at: float
    ttl_seconds: float
    metadata: Dict[str, Any] = field(default_factory=dict)


class DistributedLockStore:
    """In-memory distributed lock store."""

    def __init__(self):
        self._locks: Dict[str, Lock] = {}

    def acquire(self, key: str, owner_id: str,
               ttl_seconds: float = 60.0,
               metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Acquire lock."""
        now = time.time()

        if key in self._locks:
            existing = self._locks[key]
            if existing.expires_at > now:
                if existing.owner_id != owner_id:
                    return False

        lock = Lock(
            key=key,
            owner_id=owner_id,
            acquired_at=now,
            expires_at=now + ttl_seconds,
            ttl_seconds=ttl_seconds,
            metadata=metadata or {}
        )
        self._locks[key] = lock
        return True

    def release(self, key: str, owner_id: str) -> bool:
        """Release lock."""
        if key not in self._locks:
            return False

        lock = self._locks[key]
        if lock.owner_id != owner_id:
            return False

        del self._locks[key]
        return True

    def extend(self, key: str, owner_id: str,
              additional_seconds: float) -> bool:
        """Extend lock TTL."""
        if key not in self._locks:
            return False

        lock = self._locks[key]
        if lock.owner_id != owner_id:
            return False

        lock.expires_at += additional_seconds
        lock.ttl_seconds += additional_seconds
        return True

    def is_locked(self, key: str) -> bool:
        """Check if key is locked."""
        if key not in self._locks:
            return False

        lock = self._locks[key]
        if time.time() > lock.expires_at:
            del self._locks[key]
            return False

        return True

    def get_lock(self, key: str) -> Optional[Lock]:
        """Get lock info."""
        if key in self._locks:
            lock = self._locks[key]
            if time.time() > lock.expires_at:
                del self._locks[key]
                return None
            return lock
        return None

    def cleanup_expired(self) -> int:
        """Remove expired locks."""
        now = time.time()
        expired = [k for k, v in self._locks.items() if v.expires_at <= now]
        for k in expired:
            del self._locks[k]
        return len(expired)


_global_store = DistributedLockStore()


class DistributedLockAction:
    """Distributed lock action.

    Example:
        action = DistributedLockAction()

        if action.acquire("resource-1", ttl=30):
            try:
                process_resource()
            finally:
                action.release("resource-1")
    """

    def __init__(self, store: Optional[DistributedLockStore] = None):
        self._store = store or _global_store
        self._local_owner = uuid.uuid4().hex

    def acquire(self, key: str, owner_id: Optional[str] = None,
               ttl_seconds: float = 60.0,
               metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Acquire lock."""
        owner = owner_id or self._local_owner
        acquired = self._store.acquire(key, owner, ttl_seconds, metadata)

        if acquired:
            return {
                "success": True,
                "key": key,
                "owner_id": owner,
                "ttl_seconds": ttl_seconds,
                "message": f"Acquired lock: {key}"
            }

        lock = self._store.get_lock(key)
        return {
            "success": False,
            "key": key,
            "owner_id": lock.owner_id if lock else None,
            "message": "Lock already held"
        }

    def release(self, key: str, owner_id: Optional[str] = None) -> Dict[str, Any]:
        """Release lock."""
        owner = owner_id or self._local_owner
        released = self._store.release(key, owner)

        if released:
            return {
                "success": True,
                "key": key,
                "message": f"Released lock: {key}"
            }

        return {
            "success": False,
            "key": key,
            "message": "Not lock owner or lock not found"
        }

    def extend(self, key: str, additional_seconds: float,
              owner_id: Optional[str] = None) -> Dict[str, Any]:
        """Extend lock TTL."""
        owner = owner_id or self._local_owner
        extended = self._store.extend(key, owner, additional_seconds)

        if extended:
            return {
                "success": True,
                "key": key,
                "additional_seconds": additional_seconds,
                "message": f"Extended lock: {key}"
            }

        return {
            "success": False,
            "key": key,
            "message": "Not lock owner or lock not found"
        }

    def is_locked(self, key: str) -> Dict[str, Any]:
        """Check if key is locked."""
        locked = self._store.is_locked(key)
        lock = self._store.get_lock(key) if locked else None

        return {
            "success": True,
            "key": key,
            "locked": locked,
            "owner_id": lock.owner_id if lock else None,
            "ttl_remaining": (lock.expires_at - time.time()) if lock else 0
        }

    def get_lock_info(self, key: str) -> Dict[str, Any]:
        """Get detailed lock info."""
        lock = self._store.get_lock(key)
        if lock:
            return {
                "success": True,
                "key": key,
                "owner_id": lock.owner_id,
                "acquired_at": lock.acquired_at,
                "expires_at": lock.expires_at,
                "ttl_seconds": lock.ttl_seconds,
                "metadata": lock.metadata
            }

        return {
            "success": False,
            "key": key,
            "message": "Lock not found or expired"
        }

    def list_locks(self) -> Dict[str, Any]:
        """List all active locks."""
        locks = list(self._store._locks.values())
        return {
            "success": True,
            "locks": [
                {
                    "key": l.key,
                    "owner_id": l.owner_id,
                    "acquired_at": l.acquired_at,
                    "expires_at": l.expires_at,
                    "ttl_remaining": l.expires_at - time.time()
                }
                for l in locks
            ],
            "count": len(locks)
        }

    def cleanup(self) -> Dict[str, Any]:
        """Cleanup expired locks."""
        count = self._store.cleanup_expired()
        return {
            "success": True,
            "cleaned": count,
            "message": f"Cleaned {count} expired locks"
        }


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute distributed lock action."""
    operation = params.get("operation", "")
    action = DistributedLockAction()

    try:
        if operation == "acquire":
            key = params.get("key", "")
            if not key:
                return {"success": False, "message": "key required"}
            return action.acquire(
                key=key,
                owner_id=params.get("owner_id"),
                ttl_seconds=params.get("ttl_seconds", 60.0),
                metadata=params.get("metadata")
            )

        elif operation == "release":
            key = params.get("key", "")
            if not key:
                return {"success": False, "message": "key required"}
            return action.release(key, params.get("owner_id"))

        elif operation == "extend":
            key = params.get("key", "")
            additional_seconds = params.get("additional_seconds", 60.0)
            if not key:
                return {"success": False, "message": "key required"}
            return action.extend(key, additional_seconds, params.get("owner_id"))

        elif operation == "is_locked":
            key = params.get("key", "")
            if not key:
                return {"success": False, "message": "key required"}
            return action.is_locked(key)

        elif operation == "get_info":
            key = params.get("key", "")
            if not key:
                return {"success": False, "message": "key required"}
            return action.get_lock_info(key)

        elif operation == "list":
            return action.list_locks()

        elif operation == "cleanup":
            return action.cleanup()

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"Distributed lock error: {str(e)}"}
