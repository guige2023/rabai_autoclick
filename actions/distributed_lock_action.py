"""Distributed lock action module for RabAI AutoClick.

Provides distributed locking mechanisms for coordinating
across threads and processes including mutex, read-write locks,
and distributed semaphores.
"""

import time
import threading
import sys
import os
import fcntl
import hashlib
from typing import Any, Dict, Optional
from dataclasses import dataclass
from contextlib import contextmanager

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class LockInfo:
    """Information about a lock.
    
    Attributes:
        name: Lock name.
        locked: Whether the lock is currently held.
        holder: Identifier of the current holder.
        acquired_at: When the lock was acquired.
        timeout: Lock timeout in seconds.
    """
    name: str
    locked: bool
    holder: Optional[str] = None
    acquired_at: Optional[float] = None
    timeout: int = 0


class Mutex:
    """Thread-safe mutex (mutual exclusion) lock.
    
    Supports non-blocking and blocking acquire with timeout.
    """
    
    def __init__(self, name: str, timeout: int = 30):
        """Initialize mutex.
        
        Args:
            name: Lock name.
            timeout: Default timeout in seconds.
        """
        self.name = name
        self.timeout = timeout
        self._locked = False
        self._holder: Optional[str] = None
        self._acquired_at: Optional[float] = None
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
    
    def acquire(self, holder: str, blocking: bool = True, timeout: float = None) -> bool:
        """Acquire the mutex.
        
        Args:
            holder: Identifier for the lock holder.
            blocking: Whether to wait for the lock.
            timeout: Max seconds to wait (uses default if None).
        
        Returns:
            True if acquired, False otherwise.
        """
        if timeout is None:
            timeout = self.timeout
        
        start = time.time()
        
        while True:
            with self._lock:
                if not self._locked:
                    self._locked = True
                    self._holder = holder
                    self._acquired_at = time.time()
                    return True
                
                if not blocking:
                    return False
                
                if timeout > 0 and (time.time() - start) >= timeout:
                    return False
                
                self._condition.wait(timeout=min(0.1, timeout))
    
    def release(self, holder: str) -> bool:
        """Release the mutex.
        
        Args:
            holder: Identifier of the lock holder.
        
        Returns:
            True if released, False if not the holder.
        """
        with self._lock:
            if not self._locked or self._holder != holder:
                return False
            
            self._locked = False
            self._holder = None
            self._acquired_at = None
            self._condition.notify_all()
            return True
    
    def is_locked(self) -> bool:
        """Check if the lock is currently held."""
        with self._lock:
            return self._locked
    
    def get_info(self) -> LockInfo:
        """Get lock information."""
        with self._lock:
            return LockInfo(
                name=self.name,
                locked=self._locked,
                holder=self._holder,
                acquired_at=self._acquired_at,
                timeout=self.timeout
            )
    
    @contextmanager
    def hold(self, holder: str, timeout: float = None):
        """Context manager for acquiring and releasing the lock.
        
        Args:
            holder: Lock holder identifier.
            timeout: Acquisition timeout.
        
        Yields:
            True if acquired successfully.
        """
        acquired = self.acquire(holder, timeout=timeout)
        try:
            yield acquired
        finally:
            if acquired:
                self.release(holder)


class ReadWriteLock:
    """Reader-writer lock allowing multiple readers or one writer.
    
    Writers have priority to prevent starvation.
    """
    
    def __init__(self, name: str):
        """Initialize read-write lock.
        
        Args:
            name: Lock name.
        """
        self.name = name
        self._readers = 0
        self._writers_waiting = 0
        self._writer_active = False
        self._lock = threading.RLock()
        self._readers_condition = threading.Condition(self._lock)
        self._writers_condition = threading.Condition(self._lock)
    
    def acquire_read(self, holder: str, blocking: bool = True, timeout: float = None) -> bool:
        """Acquire a read lock.
        
        Args:
            holder: Lock holder identifier.
            blocking: Whether to wait.
            timeout: Max wait time.
        
        Returns:
            True if acquired.
        """
        start = time.time()
        
        while True:
            with self._lock:
                if not self._writer_active and self._writers_waiting == 0:
                    self._readers += 1
                    return True
                
                if not blocking:
                    return False
                
                remaining = float('inf') if timeout is None else timeout - (time.time() - start)
                if remaining <= 0:
                    return False
                
                self._readers_condition.wait(timeout=min(0.1, remaining))
        
        return False
    
    def release_read(self, holder: str) -> bool:
        """Release a read lock."""
        with self._lock:
            if self._readers > 0:
                self._readers -= 1
                if self._readers == 0:
                    self._writers_condition.notify_all()
                return True
            return False
    
    def acquire_write(self, holder: str, blocking: bool = True, timeout: float = None) -> bool:
        """Acquire a write lock.
        
        Args:
            holder: Lock holder identifier.
            blocking: Whether to wait.
            timeout: Max wait time.
        
        Returns:
            True if acquired.
        """
        start = time.time()
        
        with self._lock:
            self._writers_waiting += 1
        
        try:
            while True:
                with self._lock:
                    if self._readers == 0 and not self._writer_active:
                        self._writers_waiting -= 1
                        self._writer_active = True
                        return True
                    
                    if not blocking:
                        self._writers_waiting -= 1
                        return False
                    
                    remaining = float('inf') if timeout is None else timeout - (time.time() - start)
                    if remaining <= 0:
                        self._writers_waiting -= 1
                        return False
                    
                    self._writers_condition.wait(timeout=min(0.1, remaining))
        finally:
            with self._lock:
                self._writers_waiting -= 1
    
    def release_write(self, holder: str) -> bool:
        """Release a write lock."""
        with self._lock:
            if self._writer_active:
                self._writer_active = False
                self._readers_condition.notify_all()
                self._writers_condition.notify_all()
                return True
            return False


class Semaphore:
    """Counting semaphore for limiting concurrent access."""
    
    def __init__(self, name: str, permits: int = 1):
        """Initialize semaphore.
        
        Args:
            name: Semaphore name.
            permits: Number of concurrent permits.
        """
        self.name = name
        self.permits = permits
        self._available = permits
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._holders: Dict[str, int] = {}
    
    def acquire(self, holder: str, permits: int = 1, blocking: bool = True, timeout: float = None) -> bool:
        """Acquire permits.
        
        Args:
            holder: Identifier of the acquirer.
            permits: Number of permits to acquire.
            blocking: Whether to wait.
            timeout: Max wait time.
        
        Returns:
            True if acquired.
        """
        start = time.time()
        
        while True:
            with self._lock:
                if self._available >= permits:
                    self._available -= permits
                    self._holders[holder] = self._holders.get(holder, 0) + permits
                    return True
                
                if not blocking:
                    return False
                
                remaining = float('inf') if timeout is None else timeout - (time.time() - start)
                if remaining <= 0:
                    return False
                
                self._condition.wait(timeout=min(0.1, remaining))
        
        return False
    
    def release(self, holder: str, permits: int = 1) -> bool:
        """Release permits.
        
        Args:
            holder: Identifier of the releaser.
            permits: Number of permits to release.
        
        Returns:
            True if released.
        """
        with self._lock:
            current = self._holders.get(holder, 0)
            release_amount = min(current, permits)
            
            if release_amount > 0:
                self._holders[holder] = current - release_amount
                self._available += release_amount
                self._condition.notify_all()
                return True
            
            return False
    
    def available(self) -> int:
        """Get number of available permits."""
        with self._lock:
            return self._available


# Global lock storage
_locks: Dict[str, Mutex] = {}
_rw_locks: Dict[str, ReadWriteLock] = {}
_semaphores: Dict[str, Semaphore] = {}
_lock_storage_lock = threading.Lock()


class LockAcquireAction(BaseAction):
    """Acquire a mutex lock."""
    action_type = "lock_acquire"
    display_name = "获取互斥锁"
    description = "获取分布式互斥锁"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Acquire a lock.
        
        Args:
            context: Execution context.
            params: Dict with keys: lock_name, holder, blocking, timeout.
        
        Returns:
            ActionResult with acquisition status.
        """
        lock_name = params.get('lock_name', 'default')
        holder = params.get('holder', f"process_{os.getpid()}")
        blocking = params.get('blocking', True)
        timeout = params.get('timeout', 30.0)
        
        with _lock_storage_lock:
            if lock_name not in _locks:
                _locks[lock_name] = Mutex(lock_name)
            lock = _locks[lock_name]
        
        acquired = lock.acquire(holder, blocking=blocking, timeout=timeout)
        
        if acquired:
            return ActionResult(success=True, message=f"Lock {lock_name} acquired", data={"lock_name": lock_name, "holder": holder})
        else:
            return ActionResult(success=False, message=f"Failed to acquire lock {lock_name} (timeout)")


class LockReleaseAction(BaseAction):
    """Release a mutex lock."""
    action_type = "lock_release"
    display_name = "释放互斥锁"
    description = "释放分布式互斥锁"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Release a lock.
        
        Args:
            context: Execution context.
            params: Dict with keys: lock_name, holder.
        
        Returns:
            ActionResult with release status.
        """
        lock_name = params.get('lock_name', 'default')
        holder = params.get('holder', f"process_{os.getpid()}")
        
        with _lock_storage_lock:
            if lock_name not in _locks:
                return ActionResult(success=False, message=f"Lock {lock_name} not found")
            lock = _locks[lock_name]
        
        released = lock.release(holder)
        
        if released:
            return ActionResult(success=True, message=f"Lock {lock_name} released", data={"lock_name": lock_name})
        else:
            return ActionResult(success=False, message=f"Not the holder of lock {lock_name}")


class LockStatusAction(BaseAction):
    """Get the status of a lock."""
    action_type = "lock_status"
    display_name = "锁状态"
    description = "查看锁状态"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get lock status.
        
        Args:
            context: Execution context.
            params: Dict with keys: lock_name.
        
        Returns:
            ActionResult with lock information.
        """
        lock_name = params.get('lock_name', 'default')
        
        with _lock_storage_lock:
            if lock_name not in _locks:
                return ActionResult(success=True, message=f"Lock {lock_name} not found", data={"exists": False})
            lock = _locks[lock_name]
        
        info = lock.get_info()
        
        return ActionResult(
            success=True,
            message=f"Lock {lock_name} status",
            data={
                "name": info.name,
                "locked": info.locked,
                "holder": info.holder,
                "acquired_at": info.acquired_at,
                "timeout": info.timeout
            }
        )


class RWLockReadAction(BaseAction):
    """Acquire a read lock."""
    action_type = "rwlock_read_acquire"
    display_name = "获取读锁"
    description = "获取读写锁的读锁"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Acquire a read lock.
        
        Args:
            context: Execution context.
            params: Dict with keys: lock_name, holder, blocking, timeout.
        
        Returns:
            ActionResult with acquisition status.
        """
        lock_name = params.get('lock_name', 'default')
        holder = params.get('holder', f"reader_{os.getpid()}")
        blocking = params.get('blocking', True)
        timeout = params.get('timeout', 30.0)
        
        with _lock_storage_lock:
            if lock_name not in _rw_locks:
                _rw_locks[lock_name] = ReadWriteLock(lock_name)
            lock = _rw_locks[lock_name]
        
        acquired = lock.acquire_read(holder, blocking=blocking, timeout=timeout)
        
        if acquired:
            return ActionResult(success=True, message=f"Read lock {lock_name} acquired", data={"lock_name": lock_name})
        else:
            return ActionResult(success=False, message=f"Failed to acquire read lock {lock_name}")


class RWLockWriteAction(BaseAction):
    """Acquire a write lock."""
    action_type = "rwlock_write_acquire"
    display_name = "获取写锁"
    description = "获取读写锁的写锁"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Acquire a write lock.
        
        Args:
            context: Execution context.
            params: Dict with keys: lock_name, holder, blocking, timeout.
        
        Returns:
            ActionResult with acquisition status.
        """
        lock_name = params.get('lock_name', 'default')
        holder = params.get('holder', f"writer_{os.getpid()}")
        blocking = params.get('blocking', True)
        timeout = params.get('timeout', 30.0)
        
        with _lock_storage_lock:
            if lock_name not in _rw_locks:
                _rw_locks[lock_name] = ReadWriteLock(lock_name)
            lock = _rw_locks[lock_name]
        
        acquired = lock.acquire_write(holder, blocking=blocking, timeout=timeout)
        
        if acquired:
            return ActionResult(success=True, message=f"Write lock {lock_name} acquired", data={"lock_name": lock_name})
        else:
            return ActionResult(success=False, message=f"Failed to acquire write lock {lock_name}")


class SemaphoreAcquireAction(BaseAction):
    """Acquire permits from a semaphore."""
    action_type = "semaphore_acquire"
    display_name = "信号量获取"
    description = "获取信号量许可"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Acquire semaphore permits.
        
        Args:
            context: Execution context.
            params: Dict with keys: semaphore_name, holder, permits,
                   blocking, timeout.
        
        Returns:
            ActionResult with acquisition status.
        """
        semaphore_name = params.get('semaphore_name', 'default')
        holder = params.get('holder', f"process_{os.getpid()}")
        permits = params.get('permits', 1)
        blocking = params.get('blocking', True)
        timeout = params.get('timeout', 30.0)
        
        with _lock_storage_lock:
            if semaphore_name not in _semaphores:
                _semaphores[semaphore_name] = Semaphore(semaphore_name, permits=permits)
            semaphore = _semaphores[semaphore_name]
        
        acquired = semaphore.acquire(holder, permits=permits, blocking=blocking, timeout=timeout)
        
        if acquired:
            return ActionResult(success=True, message=f"Acquired {permits} permits from {semaphore_name}", data={"semaphore_name": semaphore_name, "permits": permits, "available": semaphore.available()})
        else:
            return ActionResult(success=False, message=f"Failed to acquire permits from {semaphore_name}")


class SemaphoreReleaseAction(BaseAction):
    """Release permits to a semaphore."""
    action_type = "semaphore_release"
    display_name = "信号量释放"
    description = "释放信号量许可"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Release semaphore permits.
        
        Args:
            context: Execution context.
            params: Dict with keys: semaphore_name, holder, permits.
        
        Returns:
            ActionResult with release status.
        """
        semaphore_name = params.get('semaphore_name', 'default')
        holder = params.get('holder', f"process_{os.getpid()}")
        permits = params.get('permits', 1)
        
        with _lock_storage_lock:
            if semaphore_name not in _semaphores:
                return ActionResult(success=False, message=f"Semaphore {semaphore_name} not found")
            semaphore = _semaphores[semaphore_name]
        
        released = semaphore.release(holder, permits=permits)
        
        if released:
            return ActionResult(success=True, message=f"Released {permits} permits to {semaphore_name}", data={"available": semaphore.available()})
        else:
            return ActionResult(success=False, message=f"Not a permit holder of {semaphore_name}")
