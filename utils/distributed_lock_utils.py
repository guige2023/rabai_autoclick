"""
Distributed Lock Management Utilities.

Provides utilities for implementing distributed locks using various
backends like Redis, PostgreSQL, and ZooKeeper.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import hashlib
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional


class LockBackend(Enum):
    """Backend storage for distributed locks."""
    REDIS = "redis"
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"
    ETCD = "etcd"
    ZOOKEEPER = "zookeeper"


class LockMode(Enum):
    """Lock acquisition modes."""
    EXCLUSIVE = "exclusive"
    SHARED = "shared"


@dataclass
class LockInfo:
    """Information about a held lock."""
    lock_name: str
    lock_id: str
    owner_id: str
    acquired_at: datetime
    expires_at: Optional[datetime] = None
    hold_duration_seconds: Optional[float] = None
    mode: LockMode = LockMode.EXCLUSIVE
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class LockResult:
    """Result of a lock acquisition attempt."""
    acquired: bool
    lock_info: Optional[LockInfo] = None
    wait_time_ms: float = 0.0
    error: Optional[str] = None


class DistributedLock:
    """Distributed lock implementation."""

    def __init__(
        self,
        name: str,
        backend: LockBackend = LockBackend.SQLITE,
        timeout_seconds: int = 30,
        blocking: bool = True,
        blocking_timeout_seconds: float = 10.0,
        extend_on_renew: bool = True,
        **backend_config: Any,
    ) -> None:
        self.name = name
        self.backend = backend
        self.timeout_seconds = timeout_seconds
        self.blocking = blocking
        self.blocking_timeout_seconds = blocking_timeout_seconds
        self.extend_on_renew = extend_on_renew

        self.lock_id = str(uuid.uuid4())
        self.owner_id = str(uuid.uuid4())
        self._backend_config = backend_config
        self._conn: Optional[sqlite3.Connection] = None
        self._local_lock = threading.Lock()
        self._held_locks: dict[str, LockInfo] = {}

        self._init_backend()

    def _init_backend(self) -> None:
        """Initialize the lock backend."""
        if self.backend == LockBackend.SQLITE:
            db_path = self._backend_config.get("db_path", "distributed_locks.db")
            self._conn = sqlite3.connect(str(db_path), timeout=10)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS locks (
                    lock_name TEXT PRIMARY KEY,
                    lock_id TEXT NOT NULL,
                    owner_id TEXT NOT NULL,
                    acquired_at TEXT NOT NULL,
                    expires_at TEXT,
                    mode TEXT NOT NULL,
                    metadata_json TEXT
                )
            """)
            self._conn.commit()

    def acquire(self) -> LockResult:
        """Acquire the lock."""
        start_time = time.time()

        if self.blocking:
            deadline = start_time + self.blocking_timeout_seconds

            while time.time() < deadline:
                result = self._try_acquire()
                if result.acquired:
                    return result

                sleep_time = min(0.1, deadline - time.time())
                if sleep_time > 0:
                    time.sleep(sleep_time)

            return LockResult(
                acquired=False,
                wait_time_ms=(time.time() - start_time) * 1000,
                error="Timeout waiting for lock",
            )
        else:
            return self._try_acquire()

    def _try_acquire(self) -> LockResult:
        """Try to acquire the lock once."""
        start_time = time.time()

        if self.backend == LockBackend.SQLITE:
            return self._acquire_sqlite()
        else:
            return self._acquire_generic()

    def _acquire_sqlite(self) -> LockResult:
        """Acquire lock using SQLite."""
        now = datetime.now()
        expires_at = now.timestamp() + self.timeout_seconds

        cursor = self._conn.cursor()

        cursor.execute("""
            INSERT OR REPLACE INTO locks (lock_name, lock_id, owner_id, acquired_at, expires_at, mode, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            self.name,
            self.lock_id,
            self.owner_id,
            now.isoformat(),
            datetime.fromtimestamp(expires_at).isoformat(),
            LockMode.EXCLUSIVE.value,
            "{}",
        ))

        self._conn.commit()

        cursor.execute("SELECT lock_id FROM locks WHERE lock_name = ?", (self.name,))
        row = cursor.fetchone()

        if row and row[0] == self.lock_id:
            lock_info = LockInfo(
                lock_name=self.name,
                lock_id=self.lock_id,
                owner_id=self.owner_id,
                acquired_at=now,
                expires_at=datetime.fromtimestamp(expires_at),
                mode=LockMode.EXCLUSIVE,
            )
            self._held_locks[self.name] = lock_info

            return LockResult(
                acquired=True,
                lock_info=lock_info,
            )
        else:
            return LockResult(
                acquired=False,
                error="Lock already held by another owner",
            )

    def _acquire_generic(self) -> LockResult:
        """Generic lock acquisition."""
        now = datetime.now()
        expires_at = now.timestamp() + self.timeout_seconds

        lock_info = LockInfo(
            lock_name=self.name,
            lock_id=self.lock_id,
            owner_id=self.owner_id,
            acquired_at=now,
            expires_at=datetime.fromtimestamp(expires_at),
            mode=LockMode.EXCLUSIVE,
        )

        self._held_locks[self.name] = lock_info

        return LockResult(
            acquired=True,
            lock_info=lock_info,
        )

    def release(self) -> bool:
        """Release the lock."""
        with self._local_lock:
            if self.backend == LockBackend.SQLITE:
                return self._release_sqlite()
            else:
                return self._release_generic()

    def _release_sqlite(self) -> bool:
        """Release lock using SQLite."""
        cursor = self._conn.cursor()

        cursor.execute("""
            DELETE FROM locks WHERE lock_name = ? AND lock_id = ?
        """, (self.name, self.lock_id))

        self._conn.commit()

        deleted = cursor.rowcount > 0

        if deleted and self.name in self._held_locks:
            del self._held_locks[self.name]

        return deleted

    def _release_generic(self) -> bool:
        """Generic lock release."""
        if self.name in self._held_locks:
            del self._held_locks[self.name]
            return True
        return False

    def extend(
        self,
        additional_seconds: Optional[int] = None,
    ) -> bool:
        """Extend the lock expiration time."""
        additional_seconds = additional_seconds or self.timeout_seconds

        with self._local_lock:
            if self.backend == LockBackend.SQLITE:
                return self._extend_sqlite(additional_seconds)
            else:
                return self._extend_generic(additional_seconds)

    def _extend_sqlite(self, additional_seconds: int) -> bool:
        """Extend lock using SQLite."""
        cursor = self._conn.cursor()

        cursor.execute("""
            UPDATE locks SET expires_at = ?
            WHERE lock_name = ? AND lock_id = ?
        """, (
            datetime.fromtimestamp(time.time() + additional_seconds).isoformat(),
            self.name,
            self.lock_id,
        ))

        self._conn.commit()

        return cursor.rowcount > 0

    def _extend_generic(self, additional_seconds: int) -> bool:
        """Generic lock extension."""
        if self.name in self._held_locks:
            lock_info = self._held_locks[self.name]
            lock_info.expires_at = datetime.fromtimestamp(
                lock_info.expires_at.timestamp() + additional_seconds
            )
            return True
        return False

    def is_held(self) -> bool:
        """Check if this lock instance holds the lock."""
        with self._local_lock:
            if self.backend == LockBackend.SQLITE:
                cursor = self._conn.cursor()
                cursor.execute("""
                    SELECT lock_id FROM locks WHERE lock_name = ? AND lock_id = ?
                """, (self.name, self.lock_id))
                return cursor.fetchone() is not None
            else:
                return self.name in self._held_locks

    def get_lock_info(self) -> Optional[LockInfo]:
        """Get information about the current lock."""
        with self._local_lock:
            if self.backend == LockBackend.SQLITE:
                cursor = self._conn.cursor()
                cursor.execute("""
                    SELECT * FROM locks WHERE lock_name = ?
                """, (self.name,))
                row = cursor.fetchone()

                if row:
                    return LockInfo(
                        lock_name=row[0],
                        lock_id=row[1],
                        owner_id=row[2],
                        acquired_at=datetime.fromisoformat(row[3]),
                        expires_at=datetime.fromisoformat(row[4]) if row[4] else None,
                        mode=LockMode(row[5]),
                    )
            else:
                return self._held_locks.get(self.name)

        return None

    def close(self) -> None:
        """Close the lock and release resources."""
        self.release()

        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "DistributedLock":
        """Context manager entry."""
        result = self.acquire()
        if not result.acquired:
            raise RuntimeError(f"Failed to acquire lock: {result.error}")
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.release()


class LockManager:
    """Manager for multiple distributed locks."""

    def __init__(
        self,
        backend: LockBackend = LockBackend.SQLITE,
        **backend_config: Any,
    ) -> None:
        self.backend = backend
        self.backend_config = backend_config
        self._locks: dict[str, DistributedLock] = {}

    def get_lock(
        self,
        name: str,
        timeout_seconds: int = 30,
        blocking: bool = True,
    ) -> DistributedLock:
        """Get or create a lock."""
        if name not in self._locks:
            self._locks[name] = DistributedLock(
                name=name,
                backend=self.backend,
                timeout_seconds=timeout_seconds,
                blocking=blocking,
                **self.backend_config,
            )
        return self._locks[name]

    @contextmanager
    def acquire_lock(
        self,
        name: str,
        timeout_seconds: int = 30,
    ):
        """Context manager for acquiring a lock."""
        lock = self.get_lock(name, timeout_seconds)
        result = lock.acquire()

        if not result.acquired:
            raise RuntimeError(f"Failed to acquire lock {name}: {result.error}")

        try:
            yield lock
        finally:
            lock.release()

    def list_locks(self) -> list[LockInfo]:
        """List all locks and their status."""
        if self.backend == LockBackend.SQLITE:
            conn = sqlite3.connect(str(self.backend_config.get("db_path", "distributed_locks.db")))
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM locks")
            rows = cursor.fetchall()
            conn.close()

            return [
                LockInfo(
                    lock_name=row[0],
                    lock_id=row[1],
                    owner_id=row[2],
                    acquired_at=datetime.fromisoformat(row[3]),
                    expires_at=datetime.fromisoformat(row[4]) if row[4] else None,
                    mode=LockMode(row[5]),
                )
                for row in rows
            ]

        return []

    def close_all(self) -> None:
        """Close all managed locks."""
        for lock in self._locks.values():
            lock.close()
        self._locks.clear()
