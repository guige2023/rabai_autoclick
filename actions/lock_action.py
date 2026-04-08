"""Lock action module for RabAI AutoClick.

Provides locking utilities:
- ReadWriteLock: Read/write lock
- SemaphoreLock: Semaphore-based lock
- TicketLock: Ticket lock for fairness
"""

from typing import Any, Callable, Dict, List, Optional
import threading
import time
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ReadWriteLock:
    """Read-write lock implementation."""

    def __init__(self):
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0
        self._writers_waiting = 0
        self._writer_active = False

    def acquire_read(self) -> None:
        """Acquire read lock."""
        with self._read_ready:
            while self._writer_active or self._writers_waiting > 0:
                self._read_ready.wait()
            self._readers += 1

    def release_read(self) -> None:
        """Release read lock."""
        with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notify_all()

    def acquire_write(self) -> None:
        """Acquire write lock."""
        with self._read_ready:
            self._writers_waiting += 1
            while self._readers > 0 or self._writer_active:
                self._read_ready.wait()
            self._writers_waiting -= 1
            self._writer_active = True

    def release_write(self) -> None:
        """Release write lock."""
        with self._read_ready:
            self._writer_active = False
            self._read_ready.notify_all()


class SemaphoreLock:
    """Semaphore-based lock."""

    def __init__(self, permits: int = 1):
        self._semaphore = threading.Semaphore(permits)
        self._permits = permits
        self._holder: Optional[str] = None
        self._holder_lock = threading.Lock()

    def acquire(self, timeout: Optional[float] = None) -> bool:
        """Acquire lock."""
        acquired = self._semaphore.acquire(timeout=timeout if timeout else -1)
        if acquired:
            with self._holder_lock:
                self._holder = str(uuid.uuid4())
        return acquired

    def release(self) -> None:
        """Release lock."""
        with self._holder_lock:
            self._holder = None
        self._semaphore.release()

    def is_locked(self) -> bool:
        """Check if locked."""
        return self._semaphore._value < 1


class TicketLock:
    """Ticket lock for fair ordering."""

    def __init__(self):
        self._ticket = threading.Lock()
        self._turn = 0
        self._now_serving = 0

    def acquire(self) -> None:
        """Acquire lock (get ticket)."""
        with self._ticket:
            my_ticket = self._turn
            self._turn += 1

        while True:
            with self._ticket:
                if self._now_serving == my_ticket:
                    return
            time.sleep(0.001)

    def release(self) -> None:
        """Release lock (serve next)."""
        with self._ticket:
            self._now_serving += 1


class ReentrantLock:
    """Reentrant lock (same thread can acquire multiple times)."""

    def __init__(self):
        self._lock = threading.Lock()
        self._owner: Optional[int] = None
        self._count = 0

    def acquire(self) -> None:
        """Acquire lock."""
        me = threading.current_thread().ident
        with self._lock:
            if self._owner == me:
                self._count += 1
            else:
                self._lock.release()
                self._lock.acquire()
                self._owner = me
                self._count = 1

    def release(self) -> None:
        """Release lock."""
        me = threading.current_thread().ident
        with self._lock:
            if self._owner != me:
                raise RuntimeError("Lock not held")
            self._count -= 1
            if self._count == 0:
                self._owner = None


class LockAction(BaseAction):
    """Lock management action."""
    action_type = "lock"
    display_name = "锁管理"
    description = "多线程锁"

    def __init__(self):
        super().__init__()
        self._locks: Dict[str, Any] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")

            if operation == "create_rw":
                return self._create_rw_lock(params)
            elif operation == "create_semaphore":
                return self._create_semaphore(params)
            elif operation == "create_ticket":
                return self._create_ticket(params)
            elif operation == "create_reentrant":
                return self._create_reentrant(params)
            elif operation == "acquire":
                return self._acquire(params)
            elif operation == "release":
                return self._release(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Lock error: {str(e)}")

    def _create_rw_lock(self, params: Dict[str, Any]) -> ActionResult:
        """Create read-write lock."""
        name = params.get("name", str(uuid.uuid4()))

        lock = ReadWriteLock()
        self._locks[name] = lock

        return ActionResult(success=True, message=f"RW lock created: {name}", data={"name": name})

    def _create_semaphore(self, params: Dict[str, Any]) -> ActionResult:
        """Create semaphore lock."""
        name = params.get("name", str(uuid.uuid4()))
        permits = params.get("permits", 1)

        lock = SemaphoreLock(permits)
        self._locks[name] = lock

        return ActionResult(success=True, message=f"Semaphore created: {name}", data={"name": name})

    def _create_ticket(self, params: Dict[str, Any]) -> ActionResult:
        """Create ticket lock."""
        name = params.get("name", str(uuid.uuid4()))

        lock = TicketLock()
        self._locks[name] = lock

        return ActionResult(success=True, message=f"Ticket lock created: {name}", data={"name": name})

    def _create_reentrant(self, params: Dict[str, Any]) -> ActionResult:
        """Create reentrant lock."""
        name = params.get("name", str(uuid.uuid4()))

        lock = ReentrantLock()
        self._locks[name] = lock

        return ActionResult(success=True, message=f"Reentrant lock created: {name}", data={"name": name})

    def _acquire(self, params: Dict[str, Any]) -> ActionResult:
        """Acquire a lock."""
        name = params.get("name")
        lock_type = params.get("type", "write")
        timeout = params.get("timeout")

        if not name:
            return ActionResult(success=False, message="name is required")

        lock = self._locks.get(name)
        if not lock:
            return ActionResult(success=False, message=f"Lock not found: {name}")

        if isinstance(lock, ReadWriteLock):
            if lock_type == "read":
                lock.acquire_read()
            else:
                lock.acquire_write()
        elif isinstance(lock, SemaphoreLock):
            success = lock.acquire(timeout=timeout)
            if not success:
                return ActionResult(success=False, message="Acquire timeout")
        elif isinstance(lock, (TicketLock, ReentrantLock)):
            lock.acquire()

        return ActionResult(success=True, message=f"Lock acquired: {name}")

    def _release(self, params: Dict[str, Any]) -> ActionResult:
        """Release a lock."""
        name = params.get("name")
        lock_type = params.get("type", "write")

        if not name:
            return ActionResult(success=False, message="name is required")

        lock = self._locks.get(name)
        if not lock:
            return ActionResult(success=False, message=f"Lock not found: {name}")

        if isinstance(lock, ReadWriteLock):
            if lock_type == "read":
                lock.release_read()
            else:
                lock.release_write()
        elif isinstance(lock, SemaphoreLock):
            lock.release()
        elif isinstance(lock, (TicketLock, ReentrantLock)):
            lock.release()

        return ActionResult(success=True, message=f"Lock released: {name}")
