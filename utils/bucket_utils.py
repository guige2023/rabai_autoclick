"""
Distributed Lock Utilities

Provides distributed locking for coordinating access
to shared resources across processes or machines.
"""

from __future__ import annotations

import copy
import threading
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar

T = TypeVar("T")


class LockAcquisitionError(Exception):
    """Raised when a lock cannot be acquired."""
    pass


class LockTimeoutError(LockAcquisitionError):
    """Raised when lock acquisition times out."""
    pass


@dataclass
class Lock:
    """A distributed lock."""
    name: str
    token: str = field(default_factory=lambda: uuid.uuid4().hex)
    acquired_at: float = field(default_factory=time.time)
    expires_at: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class LockBackend(ABC):
    """Abstract backend for distributed locks."""

    @abstractmethod
    def acquire(
        self,
        name: str,
        token: str,
        timeout: float,
        lease_time: float,
    ) -> bool:
        """
        Attempt to acquire a lock.

        Returns:
            True if lock was acquired.
        """
        pass

    @abstractmethod
    def release(self, name: str, token: str) -> bool:
        """
        Release a lock.

        Returns:
            True if lock was released.
        """
        pass

    @abstractmethod
    def extend(self, name: str, token: str, additional_time: float) -> bool:
        """
        Extend the lease time of a lock.

        Returns:
            True if lock was extended.
        """
        pass

    @abstractmethod
    def is_locked(self, name: str) -> bool:
        """Check if a lock is currently held."""
        pass


class InMemoryLockBackend(LockBackend):
    """In-memory lock backend (single process only)."""

    def __init__(self):
        self._locks: dict[str, Lock] = {}
        self._lock = threading.RLock()

    def acquire(
        self,
        name: str,
        token: str,
        timeout: float,
        lease_time: float,
    ) -> bool:
        """Acquire a lock."""
        with self._lock:
            if name in self._locks:
                existing = self._locks[name]

                # Check if expired
                if existing.expires_at and time.time() > existing.expires_at:
                    del self._locks[name]
                else:
                    return False

            expires_at = time.time() + lease_time if lease_time > 0 else None

            self._locks[name] = Lock(
                name=name,
                token=token,
                acquired_at=time.time(),
                expires_at=expires_at,
            )
            return True

    def release(self, name: str, token: str) -> bool:
        """Release a lock."""
        with self._lock:
            if name in self._locks:
                if self._locks[name].token == token:
                    del self._locks[name]
                    return True
            return False

    def extend(self, name: str, token: str, additional_time: float) -> bool:
        """Extend a lock's lease."""
        with self._lock:
            if name in self._locks:
                lock = self._locks[name]
                if lock.token == token and lock.expires_at:
                    lock.expires_at += additional_time
                    return True
            return False

    def is_locked(self, name: str) -> bool:
        """Check if locked."""
        with self._lock:
            if name not in self._locks:
                return False

            lock = self._locks[name]
            if lock.expires_at and time.time() > lock.expires_at:
                del self._locks[name]
                return False

            return True


class DistributedLock(Generic[T]):
    """
    Distributed lock with automatic cleanup and extension.
    """

    def __init__(
        self,
        backend: LockBackend | None = None,
        name: str = "",
        lease_time: float = 30.0,
        auto_extend: bool = True,
    ):
        self._backend = backend or InMemoryLockBackend()
        self._name = name
        self._lease_time = lease_time
        self._auto_extend = auto_extend
        self._token = uuid.uuid4().hex
        self._held = False
        self._extend_thread: threading.Thread | None = None
        self._stop_extend = threading.Event()

    @property
    def name(self) -> str:
        """Get lock name."""
        return self._name

    @property
    def is_held(self) -> bool:
        """Check if lock is currently held."""
        return self._held and self._backend.is_locked(self._name)

    def acquire(self, timeout: float = 10.0) -> bool:
        """
        Acquire the lock.

        Args:
            timeout: Maximum time to wait for lock acquisition.

        Returns:
            True if lock was acquired.
        """
        if self._held:
            return True

        start = time.time()

        while time.time() - start < timeout:
            if self._backend.acquire(
                self._name,
                self._token,
                timeout=timeout,
                lease_time=self._lease_time,
            ):
                self._held = True

                if self._auto_extend:
                    self._start_extend_thread()

                return True

            time.sleep(0.1)

        return False

    def release(self) -> None:
        """Release the lock."""
        if not self._held:
            return

        self._stop_extend.set()

        if self._extend_thread:
            self._extend_thread.join(timeout=1.0)
            self._extend_thread = None

        self._backend.release(self._name, self._token)
        self._held = False

    def extend(self, additional_time: float | None = None) -> bool:
        """Extend the lock's lease time."""
        if not self._held:
            return False

        additional = additional_time or self._lease_time
        return self._backend.extend(self._name, self._token, additional)

    def _start_extend_thread(self) -> None:
        """Start background thread to auto-extend lock."""
        self._stop_extend.clear()
        self._extend_thread = threading.Thread(target=self._extend_loop, daemon=True)
        self._extend_thread.start()

    def _extend_loop(self) -> None:
        """Background loop to extend lock."""
        extend_interval = self._lease_time * 0.5  # Extend at 50% of lease time

        while not self._stop_extend.wait(extend_interval):
            if self._held:
                self.extend()

    def __enter__(self) -> DistributedLock:
        """Context manager entry."""
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.release()

    def __del__(self) -> None:
        """Cleanup on deletion."""
        if self._held:
            self.release()


class LockManager:
    """
    Manager for multiple distributed locks.
    """

    def __init__(self, backend: LockBackend | None = None):
        self._backend = backend or InMemoryLockBackend()
        self._locks: dict[str, DistributedLock] = {}

    def get_lock(
        self,
        name: str,
        lease_time: float = 30.0,
        auto_extend: bool = True,
    ) -> DistributedLock:
        """Get or create a lock."""
        if name not in self._locks:
            self._locks[name] = DistributedLock(
                backend=self._backend,
                name=name,
                lease_time=lease_time,
                auto_extend=auto_extend,
            )
        return self._locks[name]

    def acquire(
        self,
        name: str,
        timeout: float = 10.0,
        lease_time: float = 30.0,
    ) -> DistributedLock:
        """Acquire a named lock."""
        lock = self.get_lock(name, lease_time)
        lock.acquire(timeout)
        return lock

    def release_all(self) -> None:
        """Release all locks."""
        for lock in self._locks.values():
            if lock.is_held:
                lock.release()


@dataclass
class LockMetrics:
    """Metrics for lock operations."""
    total_acquires: int = 0
    successful_acquires: int = 0
    failed_acquires: int = 0
    total_releases: int = 0
    current_held: int = 0


class MeasuredLock(DistributedLock):
    """Lock wrapper that collects metrics."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._metrics = LockMetrics()

    def acquire(self, timeout: float = 10.0) -> bool:
        """Acquire with metrics."""
        self._metrics.total_acquires += 1
        result = super().acquire(timeout)

        if result:
            self._metrics.successful_acquires += 1
            self._metrics.current_held += 1
        else:
            self._metrics.failed_acquires += 1

        return result

    def release(self) -> None:
        """Release with metrics."""
        super().release()
        self._metrics.total_releases += 1
        self._metrics.current_held = max(0, self._metrics.current_held - 1)

    @property
    def metrics(self) -> LockMetrics:
        """Get lock metrics."""
        return copy.copy(self._metrics)
