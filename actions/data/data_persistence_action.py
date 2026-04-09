"""Data persistence layer for automation workflows.

Provides structured data storage, retrieval, and lifecycle management
with support for multiple storage backends.
"""

from __future__ import annotations

import json
import os
import pickle
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

import shutil


class StorageBackend(Enum):
    """Available storage backends."""
    MEMORY = "memory"
    DISK = "disk"
    FILE = "file"


class PersistenceStrategy(Enum):
    """When to persist data."""
    IMMEDIATE = "immediate"
    LAZY = "lazy"
    ON_COMMIT = "on_commit"


@dataclass
class PersistedRecord:
    """A persisted data record."""
    record_id: str
    key: str
    value: Any
    backend: StorageBackend
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None
    version: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)
    checksum: Optional[str] = None
    size_bytes: Optional[int] = None


class MemoryPersistence:
    """In-memory persistence layer."""

    def __init__(self):
        self._store: Dict[str, PersistedRecord] = {}
        self._lock = threading.RLock()
        self._index: Dict[str, List[str]] = {}

    def set(
        self,
        key: str,
        value: Any,
        record_id: Optional[str] = None,
        expires_at: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> PersistedRecord:
        """Store a value in memory."""
        import hashlib

        record_id = record_id or str(uuid.uuid4())[:16]

        try:
            value_str = json.dumps(value, default=str)
            size = len(value_str.encode())
            checksum = hashlib.md5(value_str.encode()).hexdigest()
        except Exception:
            size = None
            checksum = None

        with self._lock:
            now = time.time()
            existing = self._store.get(key)

            if existing:
                record = PersistedRecord(
                    record_id=existing.record_id,
                    key=key,
                    value=value,
                    backend=StorageBackend.MEMORY,
                    created_at=existing.created_at,
                    updated_at=now,
                    expires_at=expires_at or existing.expires_at,
                    version=existing.version + 1,
                    metadata=metadata or existing.metadata,
                    checksum=checksum,
                    size_bytes=size,
                )
            else:
                record = PersistedRecord(
                    record_id=record_id,
                    key=key,
                    value=value,
                    backend=StorageBackend.MEMORY,
                    created_at=now,
                    updated_at=now,
                    expires_at=expires_at,
                    version=1,
                    metadata=metadata or {},
                    checksum=checksum,
                    size_bytes=size,
                )

            self._store[key] = record
            return record

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a value from memory."""
        with self._lock:
            record = self._store.get(key)
            if not record:
                return default
            if record.expires_at and record.expires_at < time.time():
                del self._store[key]
                return default
            return record.value

    def delete(self, key: str) -> bool:
        """Delete a value from memory."""
        with self._lock:
            return self._store.pop(key, None) is not None

    def exists(self, key: str) -> bool:
        """Check if a key exists and is not expired."""
        with self._lock:
            record = self._store.get(key)
            if not record:
                return False
            if record.expires_at and record.expires_at < time.time():
                del self._store[key]
                return False
            return True

    def list_keys(self, prefix: Optional[str] = None) -> List[str]:
        """List all keys, optionally filtered by prefix."""
        with self._lock:
            keys = list(self._store.keys())
            if prefix:
                keys = [k for k in keys if k.startswith(prefix)]
            return keys

    def get_record(self, key: str) -> Optional[PersistedRecord]:
        """Get the full record."""
        with self._lock:
            return self._store.get(key)

    def clear(self) -> int:
        """Clear all records. Returns count cleared."""
        count = len(self._store)
        self._store.clear()
        return count


class DiskPersistence:
    """Disk-based persistence layer using file system."""

    def __init__(self, base_path: str):
        self._base_path = Path(base_path)
        self._base_path.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._index_path = self._base_path / ".index.json"
        self._load_index()

    def _load_index(self) -> None:
        """Load the index from disk."""
        if self._index_path.exists():
            try:
                with open(self._index_path, "r") as f:
                    self._index = json.load(f)
            except Exception:
                self._index = {}
        else:
            self._index = {}

    def _save_index(self) -> None:
        """Save the index to disk."""
        with open(self._index_path, "w") as f:
            json.dump(self._index, f)

    def _get_file_path(self, key: str) -> Path:
        """Get the file path for a key."""
        safe_key = key.replace("/", "_").replace("\\", "_")
        return self._base_path / f"{safe_key}.data"

    def set(
        self,
        key: str,
        value: Any,
        record_id: Optional[str] = None,
        expires_at: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> PersistedRecord:
        """Store a value on disk."""
        import hashlib

        record_id = record_id or str(uuid.uuid4())[:16]
        file_path = self._get_file_path(key)

        try:
            with open(file_path, "w") as f:
                json.dump(value, f, default=str)
            size = file_path.stat().st_size

            with open(file_path, "rb") as f:
                checksum = hashlib.md5(f.read()).hexdigest()
        except Exception:
            size = None
            checksum = None

        now = time.time()
        existing = self._index.get(key)
        version = (existing.get("version", 0) + 1) if existing else 1

        record = PersistedRecord(
            record_id=record_id,
            key=key,
            value=value,
            backend=StorageBackend.DISK,
            created_at=existing.get("created_at", now) if existing else now,
            updated_at=now,
            expires_at=expires_at,
            version=version,
            metadata=metadata or {},
            checksum=checksum,
            size_bytes=size,
        )

        with self._lock:
            self._index[key] = {
                "record_id": record.record_id,
                "created_at": record.created_at,
                "updated_at": record.updated_at,
                "expires_at": record.expires_at,
                "version": record.version,
                "metadata": record.metadata,
                "checksum": record.checksum,
                "size_bytes": record.size_bytes,
            }
            self._save_index()

        return record

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a value from disk."""
        with self._lock:
            if key not in self._index:
                return default

            info = self._index[key]
            if info.get("expires_at") and info["expires_at"] < time.time():
                return default

        file_path = self._get_file_path(key)
        if not file_path.exists():
            return default

        try:
            with open(file_path, "r") as f:
                return json.load(f)
        except Exception:
            return default

    def delete(self, key: str) -> bool:
        """Delete a value from disk."""
        with self._lock:
            file_path = self._get_file_path(key)
            if file_path.exists():
                file_path.unlink()
            if key in self._index:
                del self._index[key]
                self._save_index()
                return True
            return False

    def exists(self, key: str) -> bool:
        """Check if a key exists and is not expired."""
        with self._lock:
            if key not in self._index:
                return False
            info = self._index[key]
            if info.get("expires_at") and info["expires_at"] < time.time():
                return False
            return True

    def list_keys(self, prefix: Optional[str] = None) -> List[str]:
        """List all keys, optionally filtered by prefix."""
        with self._lock:
            keys = list(self._index.keys())
            if prefix:
                keys = [k for k in keys if k.startswith(prefix)]
            return keys

    def get_record(self, key: str) -> Optional[PersistedRecord]:
        """Get the full record."""
        with self._lock:
            info = self._index.get(key)
            if not info:
                return None
            file_path = self._get_file_path(key)
            value = None
            if file_path.exists():
                try:
                    with open(file_path, "r") as f:
                        value = json.load(f)
                except Exception:
                    pass
            return PersistedRecord(
                record_id=info["record_id"],
                key=key,
                value=value,
                backend=StorageBackend.DISK,
                created_at=info["created_at"],
                updated_at=info["updated_at"],
                expires_at=info.get("expires_at"),
                version=info.get("version", 1),
                metadata=info.get("metadata", {}),
                checksum=info.get("checksum"),
                size_bytes=info.get("size_bytes"),
            )

    def clear(self) -> int:
        """Clear all records. Returns count cleared."""
        with self._lock:
            count = len(self._index)
            for key in list(self._index.keys()):
                file_path = self._get_file_path(key)
                if file_path.exists():
                    file_path.unlink()
            self._index.clear()
            self._save_index()
            return count


class AutomationPersistenceAction:
    """Action providing data persistence for automation workflows."""

    def __init__(
        self,
        backend: str = "memory",
        base_path: str = "/tmp/automation_persistence",
    ):
        if backend == "disk" or backend == "file":
            self._store = DiskPersistence(base_path)
        else:
            self._store = MemoryPersistence()
        self._backend = StorageBackend.DISK if backend != "memory" else StorageBackend.MEMORY

    def save(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Save a value to persistence."""
        expires_at = time.time() + ttl_seconds if ttl_seconds else None
        record = self._store.set(key, value, expires_at=expires_at, metadata=metadata)
        return {
            "key": record.key,
            "record_id": record.record_id,
            "version": record.version,
            "backend": record.backend.value,
            "created_at": datetime.fromtimestamp(record.created_at).isoformat(),
            "updated_at": datetime.fromtimestamp(record.updated_at).isoformat(),
        }

    def load(self, key: str, default: Any = None) -> Any:
        """Load a value from persistence."""
        return self._store.get(key, default)

    def delete(self, key: str) -> bool:
        """Delete a value from persistence."""
        return self._store.delete(key)

    def exists(self, key: str) -> bool:
        """Check if a key exists."""
        return self._store.exists(key)

    def list_keys(self, prefix: Optional[str] = None) -> List[str]:
        """List all keys."""
        return self._store.list_keys(prefix)

    def execute(
        self,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute an action with persistence.

        Required params:
            operation: callable - The operation to execute
            mode: str - 'load', 'save', or 'execute'

        For mode='save':
            key: str - Storage key
            value: Any - Value to save

        For mode='load':
            key: str - Storage key
            default: Any - Default if not found

        For mode='execute':
            key: str - Storage key (result saved here after operation)
            operation: callable - Operation to execute
        """
        mode = params.get("mode", "execute")
        operation = params.get("operation")

        if mode == "save":
            key = params.get("key")
            value = params.get("value")
            ttl = params.get("ttl_seconds")
            metadata = params.get("metadata")
            if not key:
                raise ValueError("key is required for save mode")
            return self.save(key, value, ttl, metadata)

        elif mode == "load":
            key = params.get("key")
            default = params.get("default")
            if not key:
                raise ValueError("key is required for load mode")
            value = self.load(key, default)
            return {"key": key, "value": value, "found": self.exists(key)}

        elif mode == "execute":
            key = params.get("key")
            ttl = params.get("ttl_seconds")

            if not callable(operation):
                raise ValueError("operation must be a callable")

            result = operation(context=context, params=params)

            if key:
                self.save(key, result, ttl)

            return {"result": result, "key": key}

        else:
            raise ValueError(f"Unknown mode: {mode}")

    def get_info(self, key: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a stored value."""
        record = self._store.get_record(key)
        if not record:
            return None
        return {
            "key": record.key,
            "record_id": record.record_id,
            "backend": record.backend.value,
            "created_at": datetime.fromtimestamp(record.created_at).isoformat(),
            "updated_at": datetime.fromtimestamp(record.updated_at).isoformat(),
            "expires_at": (
                datetime.fromtimestamp(record.expires_at).isoformat()
                if record.expires_at else None
            ),
            "version": record.version,
            "metadata": record.metadata,
            "checksum": record.checksum,
            "size_bytes": record.size_bytes,
        }

    def clear_all(self) -> int:
        """Clear all persisted data."""
        return self._store.clear()
