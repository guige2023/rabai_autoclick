"""State persistence utilities.

Persist and restore application state across restarts.
Supports JSON, pickle, and custom backends.

Example:
    store = StateStore(path="./state.json")
    store.save({"counter": 42, "items": [1, 2, 3]})
    state = store.load()
"""

from __future__ import annotations

import json
import logging
import os
import pickle
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class StorageBackend(ABC):
    """Abstract backend for state storage."""

    @abstractmethod
    def read(self) -> bytes | None:
        """Read raw state bytes."""
        pass

    @abstractmethod
    def write(self, data: bytes) -> None:
        """Write raw state bytes."""
        pass

    @abstractmethod
    def exists(self) -> bool:
        """Check if storage exists."""
        pass

    @abstractmethod
    def delete(self) -> None:
        """Delete stored state."""
        pass


class FileBackend(StorageBackend):
    """File-based storage backend."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def read(self) -> bytes | None:
        if not self.path.exists():
            return None
        return self.path.read_bytes()

    def write(self, data: bytes) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_bytes(data)

    def exists(self) -> bool:
        return self.path.exists()

    def delete(self) -> None:
        if self.path.exists():
            self.path.unlink()


class JSONStateStore:
    """JSON-based state store with versioning and atomic writes."""

    VERSION_KEY = "__version__"
    TIMESTAMP_KEY = "__saved_at__"

    def __init__(
        self,
        path: str | Path | None = None,
        backend: StorageBackend | None = None,
        indent: int = 2,
        version: int = 1,
    ) -> None:
        """Initialize JSON state store.

        Args:
            path: File path for JSON storage.
            backend: Custom storage backend.
            indent: JSON indentation level.
            version: State version for migrations.
        """
        self.path = Path(path) if path else None
        self.backend = backend or FileBackend(path or "./state.json")
        self.indent = indent
        self.version = version
        self._lock = threading.RLock()

    def save(self, state: dict[str, Any], atomic: bool = True) -> None:
        """Save state to storage.

        Args:
            state: State dictionary to save.
            atomic: If True, write to temp file then rename.
        """
        data = {
            **state,
            self.VERSION_KEY: self.version,
            self.TIMESTAMP_KEY: datetime.utcnow().isoformat(),
        }

        serialized = json.dumps(data, indent=self.indent, ensure_ascii=False).encode()

        if atomic and self.path:
            tmp_path = self.path.with_suffix(".tmp")
            try:
                tmp_path.write_bytes(serialized)
                os.replace(tmp_path, self.path)
            finally:
                if tmp_path.exists():
                    tmp_path.unlink()
        else:
            self.backend.write(serialized)

        logger.debug("State saved (%d bytes)", len(serialized))

    def load(self, default: dict[str, Any] | None = None) -> dict[str, Any]:
        """Load state from storage.

        Args:
            default: Default state if no stored state exists.

        Returns:
            Stored state dictionary.
        """
        with self._lock:
            raw = self.backend.read()
            if raw is None:
                return default or {}

            try:
                data = json.loads(raw.decode())
                data.pop(self.VERSION_KEY, None)
                data.pop(self.TIMESTAMP_KEY, None)
                return data
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error("Failed to load state: %s", e)
                return default or {}

    def exists(self) -> bool:
        """Check if stored state exists."""
        return self.backend.exists()

    def delete(self) -> None:
        """Delete stored state."""
        self.backend.delete()

    def get_mtime(self) -> datetime | None:
        """Get modification time of stored state."""
        if self.path and self.path.exists():
            return datetime.fromtimestamp(self.path.stat().st_mtime)
        return None


class PickleStateStore:
    """Pickle-based state store for arbitrary Python objects."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()

    def save(self, state: Any) -> None:
        """Save state using pickle."""
        with self._lock:
            with open(self.path, "wb") as f:
                pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)

    def load(self, default: Any = None) -> Any:
        """Load state using pickle."""
        with self._lock:
            if not self.path.exists():
                return default
            try:
                with open(self.path, "rb") as f:
                    return pickle.load(f)
            except Exception as e:
                logger.error("Failed to load pickle state: %s", e)
                return default

    def exists(self) -> bool:
        return self.path.exists()

    def delete(self) -> None:
        if self.path.exists():
            self.path.unlink()


@dataclass
class Snapshot:
    """A point-in-time snapshot of state."""
    id: str
    state: dict[str, Any]
    created_at: datetime = field(default_factory=datetime.utcnow)
    checksum: str | None = None


class SnapshotStore:
    """Manages multiple named snapshots for versioning and rollback."""

    def __init__(
        self,
        store: JSONStateStore,
        max_snapshots: int = 10,
    ) -> None:
        self.store = store
        self.max_snapshots = max_snapshots
        self._snapshot_prefix = "__snapshot_"

    def save_snapshot(self, name: str, state: dict[str, Any]) -> Snapshot:
        """Save a named snapshot of the current state.

        Args:
            name: Snapshot name.
            state: State to snapshot.

        Returns:
            Created Snapshot object.
        """
        import hashlib

        checksum = hashlib.md5(json.dumps(state, sort_keys=True).encode()).hexdigest()

        snapshot = Snapshot(
            id=name,
            state=dict(state),
            checksum=checksum,
        )

        all_snapshots = self._get_all_snapshots()
        all_snapshots[name] = {
            "state": snapshot.state,
            "created_at": snapshot.created_at.isoformat(),
            "checksum": snapshot.checksum,
        }

        trimmed = self._trim_snapshots(all_snapshots)
        self.store.save(trimmed)

        logger.info("Snapshot '%s' saved", name)
        return snapshot

    def load_snapshot(self, name: str) -> dict[str, Any] | None:
        """Load a named snapshot.

        Args:
            name: Snapshot name.

        Returns:
            Snapshot state or None if not found.
        """
        snapshots = self._get_all_snapshots()
        snap_data = snapshots.get(name)
        if snap_data:
            return snap_data.get("state")
        return None

    def list_snapshots(self) -> list[str]:
        """List all snapshot names."""
        return list(self._get_all_snapshots().keys())

    def delete_snapshot(self, name: str) -> bool:
        """Delete a named snapshot.

        Returns:
            True if snapshot was deleted.
        """
        all_snapshots = self._get_all_snapshots()
        if name in all_snapshots:
            del all_snapshots[name]
            self.store.save(all_snapshots)
            return True
        return False

    def _get_all_snapshots(self) -> dict[str, dict[str, Any]]:
        """Load all snapshots from store."""
        raw = self.store.backend.read()
        if raw is None:
            return {}
        try:
            return json.loads(raw.decode())
        except Exception:
            return {}

    def _trim_snapshots(
        self,
        snapshots: dict[str, dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        """Trim snapshots to max_snapshots, keeping most recent."""
        if len(snapshots) <= self.max_snapshots:
            return snapshots

        sorted_snaps = sorted(
            snapshots.items(),
            key=lambda x: x[1].get("created_at", ""),
            reverse=True,
        )

        return dict(sorted_snaps[: self.max_snapshots])


class StateMigrator:
    """Migration system for upgrading state schemas."""

    def __init__(self, store: JSONStateStore) -> None:
        self.store = store
        self._migrations: dict[int, Callable[[dict], dict]] = {}

    def register(self, from_version: int, migration: Callable[[dict], dict]) -> None:
        """Register a migration function.

        Args:
            from_version: Version to migrate from.
            migration: Function that transforms state.
        """
        self._migrations[from_version] = migration

    def migrate(self, target_version: int) -> None:
        """Run all needed migrations to reach target version.

        Args:
            target_version: Target state version.
        """
        current = self.store.load()
        current_version = current.get(JSONStateStore.VERSION_KEY, 1)

        if current_version >= target_version:
            logger.info("State already at version %d", current_version)
            return

        state = dict(current)
        for version in range(current_version, target_version):
            if version in self._migrations:
                logger.info("Migrating state from v%d to v%d", version, version + 1)
                state = self._migrations[version](state)
                state[JSONStateStore.VERSION_KEY] = version + 1

        self.store.save(state)
        logger.info("State migration complete to v%d", target_version)
