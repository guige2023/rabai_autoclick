"""Distributed Lock V2 Action Module.

Provides distributed locking with
TTL and auto-renewal.
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
    resource: str
    owner: str
    acquired_at: float
    expires_at: float
    auto_renew: bool = False


class DistributedLockV2Manager:
    """Manages distributed locks."""

    def __init__(self):
        self._locks: Dict[str, Lock] = {}
        self._lock = threading.RLock()

    def acquire(
        self,
        resource: str,
        owner: Optional[str] = None,
        ttl_seconds: float = 30.0,
        auto_renew: bool = False
    ) -> Optional[str]:
        """Acquire lock."""
        with self._lock:
            now = time.time()

            if resource in self._locks:
                existing = self._locks[resource]
                if now < existing.expires_at:
                    return None

            lock_id = str(uuid.uuid4())
            owner = owner or f"owner_{int(now * 1000)}"

            self._locks[resource] = Lock(
                lock_id=lock_id,
                resource=resource,
                owner=owner,
                acquired_at=now,
                expires_at=now + ttl_seconds,
                auto_renew=auto_renew
            )

            return lock_id

    def release(self, resource: str, lock_id: str) -> bool:
        """Release lock."""
        with self._lock:
            if resource not in self._locks:
                return False

            lock = self._locks[resource]
            if lock.lock_id != lock_id:
                return False

            del self._locks[resource]
            return True

    def renew(self, resource: str, lock_id: str, ttl_seconds: float = 30.0) -> bool:
        """Renew lock TTL."""
        with self._lock:
            if resource not in self._locks:
                return False

            lock = self._locks[resource]
            if lock.lock_id != lock_id:
                return False

            now = time.time()
            lock.expires_at = now + ttl_seconds
            return True

    def get_lock_info(self, resource: str) -> Optional[Dict]:
        """Get lock info."""
        with self._lock:
            if resource not in self._locks:
                return None

            lock = self._locks[resource]
            now = time.time()

            return {
                "resource": lock.resource,
                "owner": lock.owner,
                "acquired_at": lock.acquired_at,
                "expires_at": lock.expires_at,
                "is_active": now < lock.expires_at
            }


class DistributedLockV2Action(BaseAction):
    """Action for distributed lock operations."""

    def __init__(self):
        super().__init__("distributed_lock_v2")
        self._manager = DistributedLockV2Manager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute distributed lock action."""
        try:
            operation = params.get("operation", "acquire")

            if operation == "acquire":
                return self._acquire(params)
            elif operation == "release":
                return self._release(params)
            elif operation == "renew":
                return self._renew(params)
            elif operation == "info":
                return self._info(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _acquire(self, params: Dict) -> ActionResult:
        """Acquire lock."""
        lock_id = self._manager.acquire(
            resource=params.get("resource", ""),
            owner=params.get("owner"),
            ttl_seconds=params.get("ttl_seconds", 30),
            auto_renew=params.get("auto_renew", False)
        )
        return ActionResult(success=lock_id is not None, data={"lock_id": lock_id})

    def _release(self, params: Dict) -> ActionResult:
        """Release lock."""
        success = self._manager.release(
            resource=params.get("resource", ""),
            lock_id=params.get("lock_id", "")
        )
        return ActionResult(success=success)

    def _renew(self, params: Dict) -> ActionResult:
        """Renew lock."""
        success = self._manager.renew(
            resource=params.get("resource", ""),
            lock_id=params.get("lock_id", ""),
            ttl_seconds=params.get("ttl_seconds", 30)
        )
        return ActionResult(success=success)

    def _info(self, params: Dict) -> ActionResult:
        """Get lock info."""
        info = self._manager.get_lock_info(params.get("resource", ""))
        if not info:
            return ActionResult(success=False, message="Lock not found")
        return ActionResult(success=True, data=info)
