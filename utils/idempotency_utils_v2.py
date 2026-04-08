"""
Idempotency key utilities for deduplication and safe retries.

Provides idempotency key generation, validation, storage,
and lookup utilities for building retry-safe APIs and workflows.

Example:
    >>> from utils.idempotency_utils_v2 import IdempotencyKey, generate_key
    >>> key = IdempotencyKey(store=RedisStore())
    >>> key.check("unique-request-id")
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union


@dataclass
class IdempotencyRecord:
    """Record of an idempotent operation."""
    key: str
    status: str
    result: Optional[Any] = None
    created_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class IdempotencyStore(ABC):
    """Abstract base class for idempotency key storage."""

    @abstractmethod
    def get(self, key: str) -> Optional[IdempotencyRecord]: ...

    @abstractmethod
    def set(self, record: IdempotencyRecord) -> None: ...

    @abstractmethod
    def delete(self, key: str) -> None: ...

    @abstractmethod
    def cleanup(self, max_age: float) -> int: ...


class InMemoryIdempotencyStore(IdempotencyStore):
    """
    In-memory idempotency store.

    Suitable for single-instance applications or testing.
    """

    def __init__(self) -> None:
        """Initialize the in-memory store."""
        self._store: Dict[str, IdempotencyRecord] = {}

    def get(self, key: str) -> Optional[IdempotencyRecord]:
        """Get a record by key."""
        return self._store.get(key)

    def set(self, record: IdempotencyRecord) -> None:
        """Store a record."""
        self._store[record.key] = record

    def delete(self, key: str) -> None:
        """Delete a record."""
        self._store.pop(key, None)

    def cleanup(self, max_age: float) -> int:
        """Remove expired records."""
        now = time.time()
        expired = [
            k for k, v in self._store.items()
            if v.expires_at and v.expires_at < now
        ]
        for k in expired:
            del self._store[k]
        return len(expired)

    def clear(self) -> None:
        """Clear all records."""
        self._store.clear()


class FileIdempotencyStore(IdempotencyStore):
    """
    File-based idempotency store.

    Persists records to JSON files for durability.
    """

    def __init__(self, storage_dir: Union[str, Path] = "/tmp/idempotency") -> None:
        """
        Initialize the file store.

        Args:
            storage_dir: Directory for storage files.
        """
        self._storage_dir = Path(storage_dir)
        self._storage_dir.mkdir(parents=True, exist_ok=True)

    def _get_path(self, key: str) -> Path:
        """Get the file path for a key."""
        safe_key = hashlib.sha256(key.encode()).hexdigest()[:16]
        return self._storage_dir / f"{safe_key}.json"

    def get(self, key: str) -> Optional[IdempotencyRecord]:
        """Get a record by key."""
        path = self._get_path(key)
        if not path.exists():
            return None

        try:
            with open(path) as f:
                data = json.load(f)
            return IdempotencyRecord(
                key=data["key"],
                status=data["status"],
                result=data.get("result"),
                created_at=data["created_at"],
                expires_at=data.get("expires_at"),
                metadata=data.get("metadata", {}),
            )
        except (json.JSONDecodeError, KeyError):
            return None

    def set(self, record: IdempotencyRecord) -> None:
        """Store a record."""
        path = self._get_path(record.key)
        data = {
            "key": record.key,
            "status": record.status,
            "result": record.result,
            "created_at": record.created_at,
            "expires_at": record.expires_at,
            "metadata": record.metadata,
        }
        with open(path, "w") as f:
            json.dump(data, f)

    def delete(self, key: str) -> None:
        """Delete a record."""
        path = self._get_path(key)
        path.unlink(missing_ok=True)

    def cleanup(self, max_age: float) -> int:
        """Remove expired records."""
        now = time.time()
        removed = 0
        for path in self._storage_dir.glob("*.json"):
            try:
                with open(path) as f:
                    data = json.load(f)
                if data.get("expires_at") and data["expires_at"] < now:
                    path.unlink()
                    removed += 1
            except (json.JSONDecodeError, KeyError):
                pass
        return removed


class IdempotencyKey:
    """
    Idempotency key manager with store backend.

    Provides check-and-set semantics for deduplicating
    operations across retries.

    Attributes:
        store: Storage backend for idempotency records.
        default_ttl: Default time-to-live in seconds.
    """

    def __init__(
        self,
        store: Optional[IdempotencyStore] = None,
        default_ttl: float = 86400.0,
    ) -> None:
        """
        Initialize the idempotency key manager.

        Args:
            store: Storage backend (uses in-memory if None).
            default_ttl: Default TTL in seconds.
        """
        self.store = store or InMemoryIdempotencyStore()
        self.default_ttl = default_ttl

    def check(
        self,
        key: str,
        ttl: Optional[float] = None,
    ) -> Optional[IdempotencyRecord]:
        """
        Check if an idempotency key exists and is valid.

        Args:
            key: Idempotency key.
            ttl: Time-to-live in seconds.

        Returns:
            Existing record if found and not expired, None otherwise.
        """
        record = self.store.get(key)
        if record is None:
            return None

        now = time.time()
        if record.expires_at and record.expires_at < now:
            self.store.delete(key)
            return None

        return record

    def begin(
        self,
        key: str,
        ttl: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> IdempotencyRecord:
        """
        Begin a new idempotent operation.

        Args:
            key: Idempotency key.
            ttl: Time-to-live in seconds.
            metadata: Additional metadata.

        Returns:
            Created record.
        """
        ttl = ttl or self.default_ttl
        record = IdempotencyRecord(
            key=key,
            status="in_progress",
            created_at=time.time(),
            expires_at=time.time() + ttl,
            metadata=metadata or {},
        )
        self.store.set(record)
        return record

    def complete(
        self,
        key: str,
        result: Any,
        status: str = "completed",
    ) -> IdempotencyRecord:
        """
        Mark an idempotent operation as complete.

        Args:
            key: Idempotency key.
            result: Operation result to store.
            status: Final status.

        Returns:
            Updated record.
        """
        record = self.store.get(key)
        if record is None:
            record = IdempotencyRecord(key=key, status=status)
        record.status = status
        record.result = result
        self.store.set(record)
        return record

    def fail(
        self,
        key: str,
        error: Any,
    ) -> IdempotencyRecord:
        """
        Mark an idempotent operation as failed.

        Args:
            key: Idempotency key.
            error: Error information to store.

        Returns:
            Updated record.
        """
        return self.complete(key, result=error, status="failed")

    def delete(self, key: str) -> None:
        """Delete an idempotency key."""
        self.store.delete(key)

    def cleanup(self, max_age: Optional[float] = None) -> int:
        """
        Clean up expired records.

        Args:
            max_age: Maximum age in seconds (uses default TTL if None).

        Returns:
            Number of records removed.
        """
        max_age = max_age or self.default_ttl
        return self.store.cleanup(max_age)

    def execute(
        self,
        key: str,
        func: Callable[[], Any],
        ttl: Optional[float] = None,
    ) -> tuple[Any, bool]:
        """
        Execute a function with idempotency guarantee.

        Args:
            key: Idempotency key.
            func: Function to execute.
            ttl: Time-to-live in seconds.

        Returns:
            Tuple of (result, was_cached).
        """
        existing = self.check(key)
        if existing:
            return existing.result, True

        record = self.begin(key, ttl)
        try:
            result = func()
            self.complete(key, result)
            return result, False
        except Exception as e:
            self.fail(key, str(e))
            raise


def generate_key(
    *parts: Any,
    algorithm: str = "sha256",
) -> str:
    """
    Generate a deterministic idempotency key from parts.

    Args:
        *parts: Values to combine into the key.
        algorithm: Hash algorithm to use.

    Returns:
        Hex digest string of the combined parts.
    """
    hasher = hashlib.new(algorithm)
    for part in parts:
        hasher.update(str(part).encode("utf-8"))
    return hasher.hexdigest()


def generate_uuid_key() -> str:
    """
    Generate a random idempotency key.

    Returns:
        UUID string.
    """
    return str(uuid.uuid4())
