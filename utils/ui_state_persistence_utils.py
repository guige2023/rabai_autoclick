"""
UI state persistence utilities for saving and restoring automation state.

This module provides utilities for persisting and restoring UI state,
including window positions, element states, and automation context.
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum, auto
from pathlib import Path
from contextlib import contextmanager


class SerializationFormat(Enum):
    """Supported serialization formats."""
    JSON = auto()
    PICKLE = auto()
    YAML = auto()


@dataclass
class WindowState:
    """
    Represents a window's persisted state.

    Attributes:
        window_id: Unique window identifier.
        app_bundle_id: Application bundle identifier.
        title: Window title.
        x: X position.
        y: Y position.
        width: Window width.
        height: Window height.
        is_maximized: Whether window is maximized.
        is_minimized: Whether window is minimized.
        z_order: Z-order position.
    """
    window_id: Optional[str] = None
    app_bundle_id: Optional[str] = None
    title: Optional[str] = None
    x: int = 0
    y: int = 0
    width: int = 800
    height: int = 600
    is_maximized: bool = False
    is_minimized: bool = False
    z_order: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> WindowState:
        """Deserialize from dictionary."""
        return cls(**data)


@dataclass
class ElementState:
    """
    Represents a UI element's persisted state.

    Attributes:
        element_id: Unique element identifier.
        accessibility_id: AX identifier.
        x: X position.
        y: Y position.
        width: Element width.
        height: Element height.
        is_visible: Whether element is visible.
        is_enabled: Whether element is enabled.
        value: Current value (for inputs).
        label: Element label.
    """
    element_id: Optional[str] = None
    accessibility_id: Optional[str] = None
    x: int = 0
    y: int = 0
    width: int = 0
    height: int = 0
    is_visible: bool = True
    is_enabled: bool = True
    value: Optional[str] = None
    label: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ElementState:
        """Deserialize from dictionary."""
        return cls(**data)


@dataclass
class AutomationSnapshot:
    """
    A complete snapshot of automation state.

    Attributes:
        timestamp: Snapshot creation timestamp.
        windows: List of window states.
        elements: List of element states.
        metadata: Additional metadata.
        version: Snapshot format version.
    """
    timestamp: float = field(default_factory=time.time)
    windows: List[WindowState] = field(default_factory=list)
    elements: List[ElementState] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    version: str = "1.0"

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            'timestamp': self.timestamp,
            'windows': [w.to_dict() for w in self.windows],
            'elements': [e.to_dict() for e in self.elements],
            'metadata': self.metadata,
            'version': self.version,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> AutomationSnapshot:
        """Deserialize from dictionary."""
        return cls(
            timestamp=data.get('timestamp', time.time()),
            windows=[WindowState.from_dict(w) for w in data.get('windows', [])],
            elements=[ElementState.from_dict(e) for e in data.get('elements', [])],
            metadata=data.get('metadata', {}),
            version=data.get('version', '1.0'),
        )


class StatePersistenceManager:
    """
    Manages persistence of automation state to disk.

    Supports JSON and pickle serialization with versioning.
    """

    def __init__(
        self,
        storage_dir: Optional[str] = None,
        format: SerializationFormat = SerializationFormat.JSON
    ):
        """
        Initialize the persistence manager.

        Args:
            storage_dir: Directory to store state files.
            format: Serialization format to use.
        """
        if storage_dir:
            self._storage_dir = Path(storage_dir)
        else:
            self._storage_dir = Path.home() / '.cache' / 'ui_automation_state'
        self._storage_dir.mkdir(parents=True, exist_ok=True)
        self._format = format

    def save_snapshot(
        self, snapshot: AutomationSnapshot, name: str
    ) -> str:
        """
        Save a snapshot to disk.

        Args:
            snapshot: The snapshot to save.
            name: Name/key for the snapshot.

        Returns:
            Path to the saved file.
        """
        path = self._get_path(name)
        if self._format == SerializationFormat.JSON:
            with open(path, 'w') as f:
                json.dump(snapshot.to_dict(), f, indent=2)
        elif self._format == SerializationFormat.PICKLE:
            import pickle
            with open(path, 'wb') as f:
                pickle.dump(snapshot, f)
        return str(path)

    def load_snapshot(self, name: str) -> Optional[AutomationSnapshot]:
        """
        Load a snapshot from disk.

        Args:
            name: Name/key of the snapshot.

        Returns:
            The loaded snapshot, or None if not found.
        """
        path = self._get_path(name)
        if not path.exists():
            return None

        try:
            if self._format == SerializationFormat.JSON:
                with open(path, 'r') as f:
                    data = json.load(f)
                return AutomationSnapshot.from_dict(data)
            elif self._format == SerializationFormat.PICKLE:
                import pickle
                with open(path, 'rb') as f:
                    return pickle.load(f)
        except Exception:
            return None
        return None

    def delete_snapshot(self, name: str) -> bool:
        """
        Delete a snapshot from disk.

        Args:
            name: Name/key of the snapshot.

        Returns:
            True if deleted, False if not found.
        """
        path = self._get_path(name)
        if path.exists():
            path.unlink()
            return True
        return False

    def list_snapshots(self) -> List[str]:
        """
        List all saved snapshot names.

        Returns:
            List of snapshot names.
        """
        ext = self._get_extension()
        return [p.stem for p in self._storage_dir.glob(f'*{ext}')]

    def _get_path(self, name: str) -> Path:
        """Get the file path for a snapshot name."""
        return self._storage_dir / f'{name}{self._get_extension()}'

    def _get_extension(self) -> str:
        """Get file extension for current format."""
        if self._format == SerializationFormat.JSON:
            return '.json'
        elif self._format == SerializationFormat.PICKLE:
            return '.pkl'
        return '.bin'

    def snapshot_age(self, name: str) -> Optional[float]:
        """
        Get age of a snapshot in seconds.

        Args:
            name: Snapshot name.

        Returns:
            Age in seconds, or None if not found.
        """
        path = self._get_path(name)
        if not path.exists():
            return None
        mtime = path.stat().st_mtime
        return time.time() - mtime

    def prune_old_snapshots(
        self, max_age_seconds: float, filter_fn: Optional[Callable[[str], bool]] = None
    ) -> int:
        """
        Delete snapshots older than max_age_seconds.

        Args:
            max_age_seconds: Maximum age in seconds.
            filter_fn: Optional filter function (name -> bool).

        Returns:
            Number of snapshots deleted.
        """
        deleted = 0
        for name in self.list_snapshots():
            if filter_fn and not filter_fn(name):
                continue
            age = self.snapshot_age(name)
            if age is not None and age > max_age_seconds:
                if self.delete_snapshot(name):
                    deleted += 1
        return deleted


@contextmanager
def persisted_state(
    name: str,
    storage_dir: Optional[str] = None,
    format: SerializationFormat = SerializationFormat.JSON
):
    """
    Context manager for loading, using, and saving state.

    Args:
        name: Snapshot name.
        storage_dir: Storage directory.
        format: Serialization format.

    Yields:
        The loaded snapshot (or new empty one), then saves on exit.
    """
    manager = StatePersistenceManager(storage_dir, format)
    snapshot = manager.load_snapshot(name) or AutomationSnapshot()
    try:
        yield snapshot
    finally:
        manager.save_snapshot(snapshot, name)


def create_snapshot_from_windows(
    windows: List[WindowState],
    metadata: Optional[Dict[str, Any]] = None
) -> AutomationSnapshot:
    """
    Create a snapshot from current window states.

    Args:
        windows: List of window states.
        metadata: Optional metadata.

    Returns:
        AutomationSnapshot with the windows.
    """
    return AutomationSnapshot(
        timestamp=time.time(),
        windows=windows,
        metadata=metadata or {},
    )


def restore_window_states(
    snapshot: AutomationSnapshot
) -> List[WindowState]:
    """
    Get window states from a snapshot for restoration.

    Args:
        snapshot: The snapshot to extract from.

    Returns:
        List of window states.
    """
    return snapshot.windows
