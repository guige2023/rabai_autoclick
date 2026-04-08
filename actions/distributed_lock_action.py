"""Distributed lock action module for RabAI AutoClick.

Provides distributed locking operations:
- LockAcquireAction: Acquire a lock
- LockReleaseAction: Release a lock
- LockStatsAction: Get lock statistics
- LockContextAction: Lock context manager
"""

import threading
import time
import uuid
from typing import Any, Callable, Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class Lock:
    """Represents a lock."""
    name: str
    lock_id: str
    holder_id: str
    acquired_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    recursive: bool = False
    hold_count: int = 1


class DistributedLockManager:
    """Manages distributed locks (in-memory implementation)."""
    def __init__(self):
        self._locks: Dict[str, Lock] = {}
        self._lock = threading.RLock()
        self._holders: Dict[str, str] = {}

    def acquire(
        self,
        name: str,
        holder_id: str,
        timeout: Optional[float] = None,
        ttl_seconds: Optional[float] = None,
        recursive: bool = False
    ) -> Optional[str]:
        lock_id = str(uuid.uuid4())
        start = time.time()

        while True:
            with self._lock:
                if name not in self._locks:
                    expires = None
                    if ttl_seconds:
                        expires = datetime.utcnow() + timedelta(seconds=ttl_seconds)
                    self._locks[name] = Lock(
                        name=name,
                        lock_id=lock_id,
                        holder_id=holder_id,
                        expires_at=expires,
                        recursive=recursive
                    )
                    self._holders[holder_id] = name
                    return lock_id

                existing_lock = self._locks[name]
                if existing_lock.expires_at and datetime.utcnow() > existing_lock.expires_at:
                    del self._locks[name]
                    if existing_lock.holder_id in self._holders:
                        del self._holders[existing_lock.holder_id]
                    continue

                if existing_lock.holder_id == holder_id and recursive:
                    existing_lock.hold_count += 1
                    return lock_id

            if timeout and (time.time() - start) >= timeout:
                return None
            time.sleep(0.01)

    def release(self, name: str, holder_id: str) -> bool:
        with self._lock:
            if name not in self._locks:
                return True
            lock = self._locks[name]
            if lock.holder_id != holder_id:
                return False
            lock.hold_count -= 1
            if lock.hold_count <= 0:
                del self._locks[name]
                if holder_id in self._holders:
                    del self._holders[holder_id]
            return True

    def is_locked(self, name: str) -> bool:
        with self._lock:
            if name not in self._locks:
                return False
            lock = self._locks[name]
            if lock.expires_at and datetime.utcnow() > lock.expires_at:
                del self._locks[name]
                if lock.holder_id in self._holders:
                    del self._holders[lock.holder_id]
                return False
            return True

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            now = datetime.utcnow()
            active = []
            for lock in self._locks.values():
                if not lock.expires_at or lock.expires_at > now:
                    active.append({
                        "name": lock.name,
                        "holder_id": lock.holder_id,
                        "acquired_at": lock.acquired_at.isoformat(),
                        "recursive": lock.recursive,
                        "hold_count": lock.hold_count
                    })
            return {
                "total_locks": len(active),
                "locks": active
            }


_lock_manager = DistributedLockManager()


class LockAcquireAction(BaseAction):
    """Acquire a lock."""
    action_type = "lock_acquire"
    display_name = "获取锁"
    description = "获取分布式锁"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            holder_id = params.get("holder_id", str(uuid.uuid4()))
            timeout = params.get("timeout", 10.0)
            ttl = params.get("ttl_seconds", None)
            recursive = params.get("recursive", False)

            if not name:
                return ActionResult(success=False, message="name is required")

            lock_id = _lock_manager.acquire(
                name=name,
                holder_id=holder_id,
                timeout=timeout,
                ttl_seconds=ttl,
                recursive=recursive
            )

            if lock_id:
                return ActionResult(
                    success=True,
                    message=f"Lock '{name}' acquired",
                    data={"lock_id": lock_id, "name": name, "holder_id": holder_id}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"Failed to acquire lock '{name}' (timeout)",
                    data={"name": name, "timeout": timeout}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Lock acquire failed: {str(e)}")


class LockReleaseAction(BaseAction):
    """Release a lock."""
    action_type = "lock_release"
    display_name = "释放锁"
    description = "释放分布式锁"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            holder_id = params.get("holder_id", "")

            if not name:
                return ActionResult(success=False, message="name is required")
            if not holder_id:
                return ActionResult(success=False, message="holder_id is required")

            released = _lock_manager.release(name, holder_id)

            return ActionResult(
                success=released,
                message=f"Lock '{name}' released: {released}",
                data={"name": name, "holder_id": holder_id, "released": released}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Lock release failed: {str(e)}")


class LockStatsAction(BaseAction):
    """Get lock statistics."""
    action_type = "lock_stats"
    display_name = "锁统计"
    description = "获取锁统计"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stats = _lock_manager.get_stats()
            return ActionResult(
                success=True,
                message=f"{stats['total_locks']} active locks",
                data=stats
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Lock stats failed: {str(e)}")


class LockContextAction(BaseAction):
    """Lock context manager."""
    action_type = "lock_context"
    display_name = "锁上下文"
    description = "锁上下文管理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            operation = params.get("operation", "check")

            if not name:
                return ActionResult(success=False, message="name is required")

            if operation == "check":
                is_locked = _lock_manager.is_locked(name)
                return ActionResult(
                    success=True,
                    message=f"Lock '{name}' is {'locked' if is_locked else 'available'}",
                    data={"name": name, "is_locked": is_locked}
                )
            elif operation == "wait":
                holder_id = params.get("holder_id", str(uuid.uuid4()))
                timeout = params.get("timeout", 10.0)
                ttl = params.get("ttl_seconds", 60.0)

                lock_id = _lock_manager.acquire(name, holder_id, timeout=timeout, ttl_seconds=ttl)
                if lock_id:
                    return ActionResult(
                        success=True,
                        message=f"Lock '{name}' acquired after wait",
                        data={"lock_id": lock_id, "name": name, "holder_id": holder_id}
                    )
                else:
                    return ActionResult(success=False, message=f"Timeout waiting for lock '{name}'")
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Lock context failed: {str(e)}")
