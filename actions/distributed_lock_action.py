"""Distributed Lock Action Module.

Provides distributed locking mechanism for
coordinating access across processes/nodes.
"""

import time
import threading
import uuid
from typing import Any, Dict, Optional
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Lock:
    """Distributed lock."""
    lock_id: str
    name: str
    holder_id: str
    acquired_at: float
    expires_at: float
    recursive: bool = False


class DistributedLockManager:
    """Manages distributed locks."""

    def __init__(self, default_ttl: float = 30.0):
        self.default_ttl = default_ttl
        self._locks: Dict[str, Lock] = {}
        self._lock_objects: Dict[str, threading.Lock] = {}
        self._lock = threading.RLock()

    def acquire(
        self,
        name: str,
        holder_id: Optional[str] = None,
        ttl: Optional[float] = None,
        blocking: bool = True,
        timeout: float = 30.0
    ) -> tuple[bool, Optional[str]]:
        """Acquire a lock."""
        if name not in self._lock_objects:
            self._lock_objects[name] = threading.Lock()

        lock_obj = self._lock_objects[name]
        holder_id = holder_id or str(uuid.uuid4())
        ttl = ttl or self.default_ttl

        if blocking:
            start = time.time()
            while True:
                acquired, lock_id = self._try_acquire(name, holder_id, ttl)
                if acquired:
                    return True, lock_id

                if time.time() - start >= timeout:
                    return False, None

                time.sleep(0.1)
        else:
            return self._try_acquire(name, holder_id, ttl)

    def _try_acquire(
        self,
        name: str,
        holder_id: str,
        ttl: float
    ) -> tuple[bool, Optional[str]]:
        """Try to acquire lock."""
        with self._lock:
            existing = self._locks.get(name)

            if existing:
                if existing.expires_at > time.time():
                    if existing.holder_id == holder_id and existing.recursive:
                        existing.expires_at = time.time() + ttl
                        return True, existing.lock_id
                    return False, None
                else:
                    del self._locks[name]

            lock_id = str(uuid.uuid4())
            self._locks[name] = Lock(
                lock_id=lock_id,
                name=name,
                holder_id=holder_id,
                acquired_at=time.time(),
                expires_at=time.time() + ttl
            )

            return True, lock_id

    def release(self, name: str, holder_id: str) -> bool:
        """Release a lock."""
        with self._lock:
            existing = self._locks.get(name)

            if not existing:
                return True

            if existing.holder_id != holder_id:
                return False

            del self._locks[name]
            return True

    def extend(
        self,
        name: str,
        holder_id: str,
        ttl: Optional[float] = None
    ) -> bool:
        """Extend lock TTL."""
        with self._lock:
            existing = self._locks.get(name)

            if not existing or existing.holder_id != holder_id:
                return False

            ttl = ttl or self.default_ttl
            existing.expires_at = time.time() + ttl
            return True

    def is_locked(self, name: str) -> bool:
        """Check if name is locked."""
        with self._lock:
            existing = self._locks.get(name)
            if not existing:
                return False
            return existing.expires_at > time.time()

    def get_lock_info(self, name: str) -> Optional[Dict]:
        """Get lock information."""
        with self._lock:
            existing = self._locks.get(name)
            if not existing:
                return None

            return {
                "lock_id": existing.lock_id,
                "name": existing.name,
                "holder_id": existing.holder_id,
                "acquired_at": existing.acquired_at,
                "expires_at": existing.expires_at,
                "is_expired": existing.expires_at <= time.time()
            }


class DistributedLockAction(BaseAction):
    """Action for distributed lock operations."""

    def __init__(self):
        super().__init__("distributed_lock")
        self._manager = DistributedLockManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute distributed lock action."""
        try:
            operation = params.get("operation", "acquire")

            if operation == "acquire":
                return self._acquire(params)
            elif operation == "release":
                return self._release(params)
            elif operation == "extend":
                return self._extend(params)
            elif operation == "is_locked":
                return self._is_locked(params)
            elif operation == "info":
                return self._info(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _acquire(self, params: Dict) -> ActionResult:
        """Acquire lock."""
        acquired, lock_id = self._manager.acquire(
            name=params.get("name", ""),
            holder_id=params.get("holder_id"),
            ttl=params.get("ttl"),
            blocking=params.get("blocking", True),
            timeout=params.get("timeout", 30.0)
        )
        return ActionResult(success=acquired, data={
            "acquired": acquired,
            "lock_id": lock_id
        })

    def _release(self, params: Dict) -> ActionResult:
        """Release lock."""
        success = self._manager.release(
            params.get("name", ""),
            params.get("holder_id", "")
        )
        return ActionResult(success=success)

    def _extend(self, params: Dict) -> ActionResult:
        """Extend lock."""
        success = self._manager.extend(
            params.get("name", ""),
            params.get("holder_id", ""),
            params.get("ttl")
        )
        return ActionResult(success=success)

    def _is_locked(self, params: Dict) -> ActionResult:
        """Check if locked."""
        locked = self._manager.is_locked(params.get("name", ""))
        return ActionResult(success=True, data={"locked": locked})

    def _info(self, params: Dict) -> ActionResult:
        """Get lock info."""
        info = self._manager.get_lock_info(params.get("name", ""))
        if not info:
            return ActionResult(success=False, message="Lock not found")
        return ActionResult(success=True, data=info)
