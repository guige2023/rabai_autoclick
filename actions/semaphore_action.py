"""Semaphore action module for RabAI AutoClick.

Provides concurrency control actions using semaphores,
locks, read-write locks, and condition variables.
"""

import sys
import os
import threading
import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass
from enum import Enum
from contextlib import contextmanager

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class LockType(Enum):
    """Lock type enumeration."""
    SEMAPHORE = "semaphore"
    RLOCK = "rlock"
    LOCK = "lock"
    READ_WRITE = "read_write"
    CONDITION = "condition"


class ResourceLock:
    """Thread-safe resource lock manager."""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._locks = {}
                    cls._instance._stats = {}
                    cls._instance._rw_locks = {}
        return cls._instance
    
    def create_semaphore(self, name: str, value: int = 1) -> bool:
        """Create a counting semaphore.
        
        Args:
            name: Lock name.
            value: Initial count.
        
        Returns:
            True if created.
        """
        if name in self._locks:
            return False
        self._locks[name] = threading.Semaphore(value)
        self._stats[name] = {"acquired": 0, "released": 0, "contended": 0}
        return True
    
    def create_rlock(self, name: str) -> bool:
        """Create a reentrant lock."""
        if name in self._locks:
            return False
        self._locks[name] = threading.RLock()
        self._stats[name] = {"acquired": 0, "released": 0, "contended": 0}
        return True
    
    def create_lock(self, name: str) -> bool:
        """Create a simple lock."""
        if name in self._locks:
            return False
        self._locks[name] = threading.Lock()
        self._stats[name] = {"acquired": 0, "released": 0, "contended": 0}
        return True
    
    def create_read_write_lock(self, name: str) -> bool:
        """Create a read-write lock."""
        if name in self._locks or name in self._rw_locks:
            return False
        self._rw_locks[name] = ReadWriteLock()
        self._stats[name] = {"read_acquired": 0, "read_released": 0,
                             "write_acquired": 0, "write_released": 0}
        return True
    
    def acquire(self, name: str, timeout: float = -1.0, blocking: bool = True) -> bool:
        """Acquire a lock.
        
        Args:
            name: Lock name.
            timeout: Wait timeout in seconds (-1 = infinite).
            blocking: Whether to block.
        
        Returns:
            True if acquired.
        """
        if name not in self._locks:
            return False
        
        lock = self._locks[name]
        
        if timeout > 0:
            start = time.time()
            while True:
                if blocking:
                    acquired = lock.acquire(timeout=timeout)
                else:
                    acquired = lock.acquire(blocking=False)
                
                if acquired:
                    self._stats[name]["acquired"] += 1
                    return True
                
                elapsed = time.time() - start
                remaining = timeout - elapsed
                if remaining <= 0:
                    self._stats[name]["contended"] += 1
                    return False
                timeout = remaining
        else:
            if blocking:
                acquired = lock.acquire()
            else:
                acquired = lock.acquire(blocking=False)
            
            if acquired:
                self._stats[name]["acquired"] += 1
            else:
                self._stats[name]["contended"] += 1
            return acquired
    
    def release(self, name: str) -> bool:
        """Release a lock."""
        if name not in self._locks:
            return False
        
        try:
            self._locks[name].release()
            self._stats[name]["released"] += 1
            return True
        except RuntimeError:
            return False
    
    def acquire_read(self, name: str, timeout: float = -1.0) -> bool:
        """Acquire read lock."""
        if name not in self._rw_locks:
            return False
        acquired = self._rw_locks[name].acquire_read(timeout=timeout)
        if acquired:
            self._stats[name]["read_acquired"] += 1
        return acquired
    
    def acquire_write(self, name: str, timeout: float = -1.0) -> bool:
        """Acquire write lock."""
        if name not in self._rw_locks:
            return False
        acquired = self._rw_locks[name].acquire_write(timeout=timeout)
        if acquired:
            self._stats[name]["write_acquired"] += 1
        return acquired
    
    def release_read(self, name: str) -> bool:
        """Release read lock."""
        if name not in self._rw_locks:
            return False
        self._rw_locks[name].release_read()
        self._stats[name]["read_released"] += 1
        return True
    
    def release_write(self, name: str) -> bool:
        """Release write lock."""
        if name not in self._rw_locks:
            return False
        self._rw_locks[name].release_write()
        self._stats[name]["write_released"] += 1
        return True
    
    def get_stats(self, name: str) -> Dict[str, Any]:
        """Get lock statistics."""
        return self._stats.get(name, {}).copy()
    
    def list_locks(self) -> List[str]:
        """List all lock names."""
        return list(set(list(self._locks.keys()) + list(self._rw_locks.keys())))
    
    def delete_lock(self, name: str) -> bool:
        """Delete a lock."""
        if name in self._locks:
            del self._locks[name]
            del self._stats[name]
            return True
        elif name in self._rw_locks:
            del self._rw_locks[name]
            del self._stats[name]
            return True
        return False


class ReadWriteLock:
    """Read-write lock implementation.
    
    Allows multiple readers OR a single writer.
    """
    
    def __init__(self):
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0
        self._writers_waiting = 0
        self._writer_active = False
    
    def acquire_read(self, timeout: float = -1.0) -> bool:
        """Acquire read lock."""
        with self._read_ready:
            if self._writer_active:
                if timeout < 0:
                    while self._writer_active:
                        self._read_ready.wait()
                else:
                    end_time = time.time() + timeout
                    while self._writer_active:
                        remaining = end_time - time.time()
                        if remaining <= 0:
                            return False
                        self._read_ready.wait(timeout=remaining)
            
            self._readers += 1
            return True
    
    def release_read(self):
        """Release read lock."""
        with self._read_ready:
            self._readers -= 1
            if self._readers == 0 and self._writers_waiting > 0:
                self._read_ready.notify_all()
    
    def acquire_write(self, timeout: float = -1.0) -> bool:
        """Acquire write lock."""
        with self._read_ready:
            self._writers_waiting += 1
            try:
                if self._readers > 0 or self._writer_active:
                    if timeout < 0:
                        while self._readers > 0 or self._writer_active:
                            self._read_ready.wait()
                    else:
                        end_time = time.time() + timeout
                        while self._readers > 0 or self._writer_active:
                            remaining = end_time - time.time()
                            if remaining <= 0:
                                return False
                            self._read_ready.wait(timeout=remaining)
                
                self._writer_active = True
                return True
            finally:
                self._writers_waiting -= 1
    
    def release_write(self):
        """Release write lock."""
        with self._read_ready:
            self._writer_active = False
            self._read_ready.notify_all()


@contextmanager
def lock_context(manager: ResourceLock, name: str, timeout: float = -1.0,
                  lock_type: str = "semaphore"):
    """Context manager for lock acquisition."""
    acquired = False
    
    if lock_type in ("read", "write"):
        if lock_type == "read":
            acquired = manager.acquire_read(name, timeout)
        else:
            acquired = manager.acquire_write(name, timeout)
    else:
        acquired = manager.acquire(name, timeout=timeout)
    
    if not acquired:
        raise TimeoutError(f"Failed to acquire lock '{name}' within {timeout}s")
    
    try:
        yield acquired
    finally:
        if lock_type == "read":
            manager.release_read(name)
        elif lock_type == "write":
            manager.release_write(name)
        else:
            manager.release(name)


class CreateLockAction(BaseAction):
    """Create a new lock/semaphore."""
    action_type = "create_lock"
    display_name = "创建锁"
    description = "创建并发控制锁"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Create a lock.
        
        Args:
            context: Execution context.
            params: Dict with keys: name, lock_type, initial_value.
        
        Returns:
            ActionResult with creation status.
        """
        name = params.get('name', 'default')
        lock_type_str = params.get('lock_type', 'semaphore')
        
        try:
            lock_type = LockType(lock_type_str)
        except ValueError:
            lock_type = LockType.SEMAPHORE
        
        manager = ResourceLock()
        
        if name in manager.list_locks():
            return ActionResult(
                success=False,
                message=f"Lock '{name}' already exists"
            )
        
        if lock_type == LockType.SEMAPHORE:
            value = params.get('initial_value', 1)
            success = manager.create_semaphore(name, value)
        elif lock_type == LockType.RLOCK:
            success = manager.create_rlock(name)
        elif lock_type == LockType.LOCK:
            success = manager.create_lock(name)
        elif lock_type == LockType.READ_WRITE:
            success = manager.create_read_write_lock(name)
        else:
            success = False
        
        if success:
            return ActionResult(
                success=True,
                message=f"Created {lock_type.value} '{name}'",
                data={"name": name, "type": lock_type.value}
            )
        else:
            return ActionResult(
                success=False,
                message=f"Failed to create lock '{name}'"
            )


class AcquireLockAction(BaseAction):
    """Acquire a lock/semaphore."""
    action_type = "acquire_lock"
    display_name = "获取锁"
    description = "获取并发控制锁"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Acquire a lock.
        
        Args:
            context: Execution context.
            params: Dict with keys: name, timeout, blocking, lock_mode.
        
        Returns:
            ActionResult with acquisition status.
        """
        name = params.get('name', 'default')
        timeout = params.get('timeout', -1.0)
        blocking = params.get('blocking', True)
        lock_mode = params.get('lock_mode', 'default')
        
        manager = ResourceLock()
        
        if name not in manager.list_locks():
            return ActionResult(
                success=False,
                message=f"Lock '{name}' does not exist"
            )
        
        if lock_mode in ("read", "write"):
            if lock_mode == "read":
                acquired = manager.acquire_read(name, timeout if timeout > 0 else -1.0)
            else:
                acquired = manager.acquire_write(name, timeout if timeout > 0 else -1.0)
        else:
            acquired = manager.acquire(name, timeout if timeout > 0 else -1.0, blocking)
        
        if acquired:
            stats = manager.get_stats(name)
            return ActionResult(
                success=True,
                message=f"Acquired lock '{name}'",
                data={"name": name, "stats": stats}
            )
        else:
            return ActionResult(
                success=False,
                message=f"Failed to acquire lock '{name}' within {timeout}s"
            )


class ReleaseLockAction(BaseAction):
    """Release a lock/semaphore."""
    action_type = "release_lock"
    display_name = "释放锁"
    description = "释放并发控制锁"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Release a lock.
        
        Args:
            context: Execution context.
            params: Dict with keys: name, lock_mode.
        
        Returns:
            ActionResult with release status.
        """
        name = params.get('name', 'default')
        lock_mode = params.get('lock_mode', 'default')
        
        manager = ResourceLock()
        
        if name not in manager.list_locks():
            return ActionResult(
                success=False,
                message=f"Lock '{name}' does not exist"
            )
        
        if lock_mode in ("read", "write"):
            if lock_mode == "read":
                success = manager.release_read(name)
            else:
                success = manager.release_write(name)
        else:
            success = manager.release(name)
        
        if success:
            stats = manager.get_stats(name)
            return ActionResult(
                success=True,
                message=f"Released lock '{name}'",
                data={"name": name, "stats": stats}
            )
        else:
            return ActionResult(
                success=False,
                message=f"Failed to release lock '{name}'"
            )


class LockStatsAction(BaseAction):
    """Get lock statistics."""
    action_type = "lock_stats"
    display_name = "锁统计"
    description = "获取锁统计信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Get lock statistics.
        
        Args:
            context: Execution context.
            params: Dict with keys: name (optional).
        
        Returns:
            ActionResult with statistics.
        """
        name = params.get('name')
        manager = ResourceLock()
        
        if name:
            stats = manager.get_stats(name)
            return ActionResult(
                success=True,
                message=f"Stats for '{name}'",
                data={"name": name, "stats": stats}
            )
        else:
            all_stats = {n: manager.get_stats(n) for n in manager.list_locks()}
            return ActionResult(
                success=True,
                message="All lock statistics",
                data=all_stats
            )
