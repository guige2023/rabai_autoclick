"""
Data Locker Action Module.

Provides distributed locking primitives for data processing workflows,
supporting mutex locks, read-write locks, and semaphores.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum, auto
import threading
import time
import uuid
import logging

logger = logging.getLogger(__name__)


class LockType(Enum):
    """Types of locks."""
    MUTEX = auto()
    READ_WRITE = auto()
    SEMAPHORE = auto()
    REENTRANT = auto()


@dataclass
class LockToken:
    """Token representing a lock holder."""
    token_id: str
    holder_id: str
    acquired_at: datetime
    expires_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_expired(self) -> bool:
        """Check if token has expired."""
        if self.expires_at is None:
            return False
        return datetime.now(timezone.utc) > self.expires_at


@dataclass
class LockResult:
    """Result of a lock operation."""
    success: bool
    token: Optional[LockToken] = None
    error: Optional[str] = None
    wait_time_ms: float = 0.0


class ReadWriteLock:
    """Read-Write lock implementation."""

    def __init__(self):
        """Initialize the read-write lock."""
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0
        self._writer_active = False

    def acquire_read(self, timeout: Optional[float] = None) -> bool:
        """
        Acquire read lock.

        Args:
            timeout: Optional timeout in seconds.

        Returns:
            True if acquired.
        """
        deadline = time.time() + timeout if timeout else None

        with self._read_ready:
            while self._writer_active:
                if deadline:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        return False
                    if not self._read_ready.wait(timeout=remaining):
                        return False
                else:
                    self._read_ready.wait()

            self._readers += 1
            return True

    def release_read(self) -> None:
        """Release read lock."""
        with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notify_all()

    def acquire_write(self, timeout: Optional[float] = None) -> bool:
        """
        Acquire write lock.

        Args:
            timeout: Optional timeout in seconds.

        Returns:
            True if acquired.
        """
        deadline = time.time() + timeout if timeout else None

        with self._read_ready:
            while self._writer_active or self._readers > 0:
                if deadline:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        return False
                    if not self._read_ready.wait(timeout=remaining):
                        return False
                else:
                    self._read_ready.wait()

            self._writer_active = True
            return True

    def release_write(self) -> None:
        """Release write lock."""
        with self._read_ready:
            self._writer_active = False
            self._read_ready.notify_all()


class DataLockerAction:
    """
    Provides distributed locking for data processing.

    This action implements various locking primitives including mutex locks,
    read-write locks, and semaphores for coordinating access to shared
    resources in data processing workflows.

    Example:
        >>> locker = DataLockerAction()
        >>> result = await locker.acquire("resource:123")
        >>> if result.success:
        ...     try:
        ...         await process_resource()
        ...     finally:
        ...         await locker.release("resource:123")
    """

    def __init__(
        self,
        default_timeout: float = 30.0,
        default_ttl: float = 60.0,
        auto_extend: bool = True,
    ):
        """
        Initialize the Data Locker.

        Args:
            default_timeout: Default wait timeout in seconds.
            default_ttl: Default lock TTL in seconds.
            auto_extend: Whether to auto-extend locks near expiration.
        """
        self.default_timeout = default_timeout
        self.default_ttl = default_ttl
        self.auto_extend = auto_extend

        self._locks: Dict[str, LockToken] = {}
        self._rw_locks: Dict[str, ReadWriteLock] = {}
        self._semaphores: Dict[str, threading.Semaphore] = {}
        self._holders: Dict[str, Set[str]] = {}
        self._holder_id = str(uuid.uuid4())
        self._lock = threading.RLock()
        self._extend_threads: Dict[str, threading.Thread] = {}

    async def acquire(
        self,
        resource: str,
        lock_type: LockType = LockType.MUTEX,
        timeout: Optional[float] = None,
        ttl: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> LockResult:
        """
        Acquire a lock on a resource.

        Args:
            resource: Resource identifier.
            lock_type: Type of lock to acquire.
            timeout: Wait timeout in seconds.
            ttl: Lock TTL in seconds.
            metadata: Optional metadata.

        Returns:
            LockResult with lock token if successful.
        """
        timeout = timeout or self.default_timeout
        ttl = ttl or self.default_ttl
        start_time = time.time()

        if lock_type == LockType.READ_WRITE:
            return await self._acquire_rw_lock(resource, timeout, ttl, metadata)

        elif lock_type == LockType.SEMAPHORE:
            return await self._acquire_semaphore(resource, timeout, ttl, metadata)

        return await self._acquire_mutex(resource, timeout, ttl, metadata, start_time)

    async def _acquire_mutex(
        self,
        resource: str,
        timeout: float,
        ttl: float,
        metadata: Optional[Dict[str, Any]],
        start_time: float,
    ) -> LockResult:
        """Acquire a mutex lock."""
        deadline = time.time() + timeout

        while time.time() < deadline:
            with self._lock:
                existing = self._locks.get(resource)

                if existing and not existing.is_expired():
                    wait_time = (time.time() - start_time) * 1000
                    return LockResult(
                        success=False,
                        error="Lock held by another process",
                        wait_time_ms=wait_time,
                    )

                token = LockToken(
                    token_id=str(uuid.uuid4()),
                    holder_id=self._holder_id,
                    acquired_at=datetime.now(timezone.utc),
                    expires_at=datetime.now(timezone.utc) + timedelta(seconds=ttl),
                    metadata=metadata or {},
                )

                self._locks[resource] = token
                if self._holder_id not in self._holders:
                    self._holders[self._holder_id] = set()
                self._holders[self._holder_id].add(resource)

                if self.auto_extend:
                    self._start_extend_thread(resource, ttl)

                wait_time = (time.time() - start_time) * 1000
                return LockResult(success=True, token=token, wait_time_ms=wait_time)

            await self._sleep(0.01)

        wait_time = (time.time() - start_time) * 1000
        return LockResult(success=False, error="Timeout", wait_time_ms=wait_time)

    async def _acquire_rw_lock(
        self,
        resource: str,
        timeout: float,
        ttl: float,
        metadata: Optional[Dict[str, Any]],
    ) -> LockResult:
        """Acquire a read-write lock."""
        if resource not in self._rw_locks:
            with self._lock:
                if resource not in self._rw_locks:
                    self._rw_locks[resource] = ReadWriteLock()

        rw_lock = self._rw_locks[resource]
        acquired = rw_lock.acquire_write(timeout=timeout)

        if acquired:
            token = LockToken(
                token_id=str(uuid.uuid4()),
                holder_id=self._holder_id,
                acquired_at=datetime.now(timezone.utc),
                expires_at=datetime.now(timezone.utc) + timedelta(seconds=ttl),
                metadata=metadata or {},
            )
            self._locks[resource] = token
            return LockResult(success=True, token=token)

        return LockResult(success=False, error="Timeout acquiring write lock")

    async def _acquire_semaphore(
        self,
        resource: str,
        timeout: float,
        ttl: float,
        metadata: Optional[Dict[str, Any]],
    ) -> LockResult:
        """Acquire a semaphore."""
        if resource not in self._semaphores:
            with self._lock:
                if resource not in self._semaphores:
                    max_count = metadata.get("max_count", 1) if metadata else 1
                    self._semaphores[resource] = threading.Semaphore(max_count)

        semaphore = self._semaphores[resource]
        acquired = semaphore.acquire(timeout=timeout)

        if acquired:
            token = LockToken(
                token_id=str(uuid.uuid4()),
                holder_id=self._holder_id,
                acquired_at=datetime.now(timezone.utc),
                expires_at=datetime.now(timezone.utc) + timedelta(seconds=ttl),
                metadata=metadata or {},
            )
            self._locks[resource] = token
            return LockResult(success=True, token=token)

        return LockResult(success=False, error="Timeout acquiring semaphore")

    async def release(
        self,
        resource: str,
        token: Optional[LockToken] = None,
    ) -> bool:
        """
        Release a lock on a resource.

        Args:
            resource: Resource identifier.
            token: Optional token for verification.

        Returns:
            True if released successfully.
        """
        with self._lock:
            existing = self._locks.get(resource)

            if not existing:
                return False

            if token and existing.token_id != token.token_id:
                return False

            del self._locks[resource]
            self._holders.get(self._holder_id, set()).discard(resource)

            if resource in self._rw_locks:
                self._rw_locks[resource].release_write()

            if resource in self._semaphores:
                self._semaphores[resource].release()

            if resource in self._extend_threads:
                self._extend_threads[resource].cancel()
                del self._extend_threads[resource]

            return True

    def extend(
        self,
        resource: str,
        additional_seconds: float,
        token: Optional[LockToken] = None,
    ) -> bool:
        """
        Extend a lock's TTL.

        Args:
            resource: Resource identifier.
            additional_seconds: Seconds to add.
            token: Optional token for verification.

        Returns:
            True if extended successfully.
        """
        with self._lock:
            existing = self._locks.get(resource)

            if not existing:
                return False

            if token and existing.token_id != token.token_id:
                return False

            if existing.expires_at:
                existing.expires_at += timedelta(seconds=additional_seconds)
            else:
                existing.expires_at = (
                    datetime.now(timezone.utc) + timedelta(seconds=additional_seconds)
                )

            return True

    def is_locked(self, resource: str) -> bool:
        """Check if a resource is locked."""
        with self._lock:
            existing = self._locks.get(resource)
            if existing is None:
                return False
            if existing.is_expired():
                del self._locks[resource]
                return False
            return True

    def get_lock_info(self, resource: str) -> Optional[Dict[str, Any]]:
        """Get information about a lock."""
        with self._lock:
            existing = self._locks.get(resource)
            if not existing or existing.is_expired():
                return None

            return {
                "resource": resource,
                "holder_id": existing.holder_id,
                "acquired_at": existing.acquired_at.isoformat(),
                "expires_at": existing.expires_at.isoformat() if existing.expires_at else None,
                "metadata": existing.metadata,
            }

    def get_holder_locks(self) -> List[str]:
        """Get all locks held by this instance."""
        with self._lock:
            return list(self._holders.get(self._holder_id, set()))

    def force_release(self, resource: str) -> bool:
        """Force release a lock (admin operation)."""
        with self._lock:
            if resource in self._locks:
                del self._locks[resource]
                return True
            return False

    def _start_extend_thread(self, resource: str, ttl: float) -> None:
        """Start auto-extend thread for a lock."""
        def extend_loop():
            while True:
                time.sleep(ttl * 0.8)
                with self._lock:
                    if resource not in self._locks:
                        break
                    self.extend(resource, ttl * 0.5)

        thread = threading.Thread(target=extend_loop, daemon=True)
        thread.start()
        self._extend_threads[resource] = thread

    @staticmethod
    async def _sleep(seconds: float) -> None:
        """Async sleep helper."""
        import asyncio
        await asyncio.sleep(seconds)

    def cleanup_expired(self) -> int:
        """Remove all expired locks."""
        with self._lock:
            expired = [
                resource
                for resource, token in self._locks.items()
                if token.is_expired()
            ]
            for resource in expired:
                del self._locks[resource]
                if resource in self._rw_locks:
                    del self._rw_locks[resource]
            return len(expired)


def create_locker_action(**kwargs) -> DataLockerAction:
    """Factory function to create a DataLockerAction."""
    return DataLockerAction(**kwargs)
