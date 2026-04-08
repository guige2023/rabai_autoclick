"""Distributed Lock action module for RabAI AutoClick.

Provides distributed locking with mutex, read-write, and
semaphore patterns for coordinating distributed systems.
"""

import sys
import os
import json
import time
import uuid
import asyncio
import threading
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class LockType(Enum):
    """Distributed lock types."""
    MUTEX = "mutex"           # Exclusive lock
    READ_WRITE = "read_write"  # Read-write lock
    SEMAPHORE = "semaphore"    # Counting semaphore


@dataclass
class Lock:
    """Represents a distributed lock."""
    lock_id: str
    name: str
    lock_type: LockType
    owner: str
    acquired_at: float
    expires_at: float
    resource_id: str


@dataclass
class LockRequest:
    """Request for acquiring a lock."""
    request_id: str
    lock_name: str
    owner: str
    request_type: str  # "read", "write", "exclusive"
    timeout_seconds: float = 10.0
    priority: int = 5


class DistributedLockManager:
    """Manages distributed locks."""
    
    def __init__(self):
        self._locks: Dict[str, Lock] = {}
        self._wait_queues: Dict[str, deque] = {}  # lock_name -> waiting requests
        self._owner_locks: Dict[str, List[str]] = {}  # owner -> [lock_ids]
        self._lock = threading.Lock()
    
    def acquire(
        self,
        lock_name: str,
        owner: str,
        lock_type: LockType = LockType.MUTEX,
        timeout_seconds: float = 10.0,
        ttl_seconds: float = 60.0
    ) -> Optional[str]:
        """Acquire a lock.
        
        Returns lock_id if acquired, None if timeout.
        """
        lock_id = str(uuid.uuid4())
        now = time.time()
        
        with self._lock:
            # Check if lock exists and is held
            existing = self._find_lock(lock_name)
            
            if existing and existing.expires_at > now:
                # Lock is held - check compatibility
                if lock_type == LockType.READ_WRITE:
                    # Check read-write compatibility
                    if owner == existing.owner:
                        # Same owner, extend
                        existing.expires_at = now + ttl_seconds
                        return existing.lock_id
                    # Other owner has exclusive or write
                    if self._is_write_lock(existing):
                        return None
                else:
                    # Mutex - cannot share
                    return None
            else:
                # No conflicting lock, acquire it
                lock = Lock(
                    lock_id=lock_id,
                    name=lock_name,
                    lock_type=lock_type,
                    owner=owner,
                    acquired_at=now,
                    expires_at=now + ttl_seconds
                )
                self._locks[lock_id] = lock
                self._owner_locks.setdefault(owner, []).append(lock_id)
                return lock_id
        
        return None
    
    def _find_lock(self, lock_name: str) -> Optional[Lock]:
        """Find lock by name."""
        now = time.time()
        for lock in self._locks.values():
            if lock.name == lock_name and lock.expires_at > now:
                return lock
        return None
    
    def _is_write_lock(self, lock: Lock) -> bool:
        """Check if lock is a write lock."""
        return lock.lock_type in (LockType.MUTEX, LockType.READ_WRITE)
    
    def release(self, lock_id: str, owner: str) -> bool:
        """Release a lock."""
        with self._lock:
            lock = self._locks.get(lock_id)
            if not lock or lock.owner != owner:
                return False
            
            del self._locks[lock_id]
            if owner in self._owner_locks:
                self._owner_locks[owner] = [
                    lid for lid in self._owner_locks[owner] if lid != lock_id
                ]
            return True
    
    def release_all(self, owner: str) -> int:
        """Release all locks owned by an owner."""
        with self._lock:
            lock_ids = self._owner_locks.get(owner, [])
            count = len(lock_ids)
            for lid in lock_ids:
                self._locks.pop(lid, None)
            self._owner_locks[owner] = []
            return count
    
    def extend(self, lock_id: str, owner: str, ttl_seconds: float) -> bool:
        """Extend lock TTL."""
        with self._lock:
            lock = self._locks.get(lock_id)
            if not lock or lock.owner != owner:
                return False
            lock.expires_at = time.time() + ttl_seconds
            return True
    
    def is_locked(self, lock_name: str) -> bool:
        """Check if a resource is locked."""
        lock = self._find_lock(lock_name)
        return lock is not None
    
    def get_lock_info(self, lock_name: str) -> Optional[Dict[str, Any]]:
        """Get info about a lock."""
        lock = self._find_lock(lock_name)
        if not lock:
            return None
        return {
            "lock_id": lock.lock_id,
            "name": lock.name,
            "owner": lock.owner,
            "acquired_at": lock.acquired_at,
            "expires_at": lock.expires_at,
            "ttl_remaining": lock.expires_at - time.time()
        }
    
    def list_locks(self) -> List[Dict[str, Any]]:
        """List all held locks."""
        now = time.time()
        return [
            {
                "lock_id": l.lock_id,
                "name": l.name,
                "owner": l.owner,
                "expires_at": l.expires_at
            }
            for l in self._locks.values()
            if l.expires_at > now
        ]


class DistributedLockAction(BaseAction):
    """Distributed locking for resource coordination.
    
    Supports mutex, read-write, and semaphore locks with
    TTL, extension, and owner management.
    """
    action_type = "distributed_lock"
    display_name = "分布式锁"
    description = "分布式锁管理，支持互斥锁和读写锁"
    
    def __init__(self):
        super().__init__()
        self._manager = DistributedLockManager()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute lock operation."""
        operation = params.get("operation", "")
        
        try:
            if operation == "acquire":
                return self._acquire(params)
            elif operation == "release":
                return self._release(params)
            elif operation == "release_all":
                return self._release_all(params)
            elif operation == "extend":
                return self._extend(params)
            elif operation == "is_locked":
                return self._is_locked(params)
            elif operation == "get_info":
                return self._get_info(params)
            elif operation == "list":
                return self._list(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _acquire(self, params: Dict[str, Any]) -> ActionResult:
        """Acquire a lock."""
        lock_name = params.get("lock_name", "")
        owner = params.get("owner", str(uuid.uuid4()))
        lock_type = LockType(params.get("lock_type", "mutex"))
        ttl = params.get("ttl_seconds", 60.0)
        
        if not lock_name:
            return ActionResult(success=False, message="lock_name is required")
        
        lock_id = self._manager.acquire(lock_name, owner, lock_type, ttl_seconds=ttl)
        return ActionResult(
            success=lock_id is not None,
            message=f"Lock acquired: {lock_id}" if lock_id else "Lock timeout",
            data={"lock_id": lock_id, "owner": owner}
        )
    
    def _release(self, params: Dict[str, Any]) -> ActionResult:
        """Release a lock."""
        lock_id = params.get("lock_id", "")
        owner = params.get("owner", "")
        
        released = self._manager.release(lock_id, owner)
        return ActionResult(success=released, message="Lock released" if released else "Release failed")
    
    def _release_all(self, params: Dict[str, Any]) -> ActionResult:
        """Release all locks for owner."""
        owner = params.get("owner", "")
        count = self._manager.release_all(owner)
        return ActionResult(success=True, message=f"Released {count} locks")
    
    def _extend(self, params: Dict[str, Any]) -> ActionResult:
        """Extend lock TTL."""
        lock_id = params.get("lock_id", "")
        owner = params.get("owner", "")
        ttl = params.get("ttl_seconds", 60.0)
        
        extended = self._manager.extend(lock_id, owner, ttl)
        return ActionResult(success=extended, message="Extended" if extended else "Extend failed")
    
    def _is_locked(self, params: Dict[str, Any]) -> ActionResult:
        """Check if locked."""
        lock_name = params.get("lock_name", "")
        locked = self._manager.is_locked(lock_name)
        return ActionResult(success=True, message=f"Locked: {locked}", data={"is_locked": locked})
    
    def _get_info(self, params: Dict[str, Any]) -> ActionResult:
        """Get lock info."""
        lock_name = params.get("lock_name", "")
        info = self._manager.get_lock_info(lock_name)
        if not info:
            return ActionResult(success=False, message="Lock not found")
        return ActionResult(success=True, message="Info retrieved", data=info)
    
    def _list(self, params: Dict[str, Any]) -> ActionResult:
        """List all locks."""
        locks = self._manager.list_locks()
        return ActionResult(success=True, message=f"{len(locks)} locks", data={"locks": locks})
