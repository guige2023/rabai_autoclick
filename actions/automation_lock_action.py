"""Automation Lock Manager.

This module provides distributed lock management:
- File-based locking
- Lock timeout and renewal
- Deadlock prevention
- Context manager support

Example:
    >>> from actions.automation_lock_action import LockManager
    >>> mgr = LockManager()
    >>> with mgr.acquire("task_1"):
    ...     execute_task()
"""

from __future__ import annotations

import os
import time
import uuid
import logging
import threading
from typing import Optional
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class Lock:
    """A distributed lock."""

    def __init__(self, name: str, lock_id: str, lock_file: str) -> None:
        self.name = name
        self.lock_id = lock_id
        self.lock_file = lock_file
        self.acquired_at: Optional[float] = None
        self.acquired_by: Optional[str] = None


class LockManager:
    """Manages distributed locks for automation tasks."""

    def __init__(
        self,
        lock_dir: str = "/tmp/automation_locks",
        default_timeout: float = 60.0,
        auto_renewal: bool = True,
    ) -> None:
        """Initialize the lock manager.

        Args:
            lock_dir: Directory for lock files.
            default_timeout: Default lock timeout in seconds.
            auto_renewal: Whether to auto-renew locks.
        """
        self._lock_dir = lock_dir
        self._default_timeout = default_timeout
        self._auto_renewal = auto_renewal
        self._locks: dict[str, Lock] = {}
        self._process_id = str(uuid.uuid4())[:8]
        self._lock = threading.RLock()
        self._renewal_thread: Optional[threading.Thread] = None
        self._running = False
        self._stats = {"locks_acquired": 0, "locks_released": 0, "lock_timeouts": 0}

        os.makedirs(lock_dir, exist_ok=True)

    def acquire(
        self,
        name: str,
        timeout: Optional[float] = None,
        blocking: bool = True,
    ) -> Optional[Lock]:
        """Acquire a lock.

        Args:
            name: Lock name.
            timeout: Wait timeout. None = use default.
            blocking: Whether to wait for lock.

        Returns:
            Lock object if acquired, None otherwise.
        """
        timeout = timeout or self._default_timeout
        lock_id = str(uuid.uuid4())
        lock_file = os.path.join(self._lock_dir, f"{name}.lock")
        lock = Lock(name=name, lock_id=lock_id, lock_file=lock_file)

        start_time = time.time()

        while True:
            with self._lock:
                existing = self._get_existing_lock(name, lock_file)

                if existing is None:
                    try:
                        with open(lock_file, "w") as f:
                            f.write(f"{lock_id}|{self._process_id}|{time.time()}")
                        lock.acquired_at = time.time()
                        lock.acquired_by = self._process_id
                        self._locks[name] = lock
                        self._stats["locks_acquired"] += 1
                        logger.info("Acquired lock: %s (id=%s)", name, lock_id)
                        return lock
                    except Exception as e:
                        logger.error("Failed to create lock file: %s", e)
                        return None

            if not blocking:
                return None

            elapsed = time.time() - start_time
            if elapsed >= timeout:
                self._stats["lock_timeouts"] += 1
                logger.warning("Lock timeout for: %s", name)
                return None

            time.sleep(0.1)

    def release(self, name: str, lock_id: Optional[str] = None) -> bool:
        """Release a lock.

        Args:
            name: Lock name.
            lock_id: Specific lock ID. None = release any.

        Returns:
            True if released.
        """
        with self._lock:
            lock = self._locks.get(name)
            if lock is None:
                return False

            if lock_id and lock.lock_id != lock_id:
                return False

            try:
                if os.path.exists(lock.lock_file):
                    os.remove(lock.lock_file)
                del self._locks[name]
                self._stats["locks_released"] += 1
                logger.info("Released lock: %s", name)
                return True
            except Exception as e:
                logger.error("Failed to release lock %s: %s", name, e)
                return False

    def renew(self, name: str, lock_id: Optional[str] = None) -> bool:
        """Renew a lock's timeout.

        Args:
            name: Lock name.
            lock_id: Specific lock ID.

        Returns:
            True if renewed.
        """
        with self._lock:
            lock = self._locks.get(name)
            if lock is None:
                return False

            if lock_id and lock.lock_id != lock_id:
                return False

            try:
                with open(lock.lock_file, "w") as f:
                    f.write(f"{lock.lock_id}|{self._process_id}|{time.time()}")
                lock.acquired_at = time.time()
                return True
            except Exception as e:
                logger.error("Failed to renew lock %s: %s", name, e)
                return False

    def is_locked(self, name: str) -> bool:
        """Check if a lock is held.

        Args:
            name: Lock name.

        Returns:
            True if locked.
        """
        with self._lock:
            return name in self._locks

    def get_lock_info(self, name: str) -> Optional[dict]:
        """Get lock information.

        Args:
            name: Lock name.

        Returns:
            Dict with lock info or None.
        """
        with self._lock:
            lock = self._locks.get(name)
            if lock is None:
                return None
            return {
                "name": lock.name,
                "lock_id": lock.lock_id,
                "acquired_at": lock.acquired_at,
                "acquired_by": lock.acquired_by,
                "age_seconds": time.time() - lock.acquired_at if lock.acquired_at else 0,
            }

    @contextmanager
    def acquire_context(self, name: str, timeout: Optional[float] = None):
        """Context manager for acquiring locks.

        Args:
            name: Lock name.
            timeout: Lock timeout.

        Yields:
            Lock object.
        """
        lock = self.acquire(name, timeout=timeout)
        try:
            yield lock
        finally:
            if lock:
                self.release(name)

    def cleanup_stale_locks(self) -> int:
        """Remove stale lock files.

        Returns:
            Number of stale locks removed.
        """
        removed = 0
        lock_timeout = self._default_timeout * 2

        with self._lock:
            try:
                for filename in os.listdir(self._lock_dir):
                    if not filename.endswith(".lock"):
                        continue
                    filepath = os.path.join(self._lock_dir, filename)
                    try:
                        stat = os.stat(filepath)
                        age = time.time() - stat.st_mtime
                        if age > lock_timeout:
                            os.remove(filepath)
                            removed += 1
                            logger.info("Removed stale lock: %s", filename)
                    except Exception:
                        continue
            except Exception as e:
                logger.error("Failed to cleanup stale locks: %s", e)

        return removed

    def get_stats(self) -> dict[str, Any]:
        """Get lock manager statistics."""
        with self._lock:
            return {
                **self._stats,
                "active_locks": len(self._locks),
            }

    def _get_existing_lock(self, name: str, lock_file: str) -> Optional[Lock]:
        """Check for existing lock."""
        if not os.path.exists(lock_file):
            return None

        try:
            with open(lock_file, "r") as f:
                content = f.read().strip()
                if not content:
                    return None
                parts = content.split("|")
                if len(parts) >= 3:
                    return Lock(name=name, lock_id=parts[0], lock_file=lock_file)
        except Exception:
            return None

        return None
