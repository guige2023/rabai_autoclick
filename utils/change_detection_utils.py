"""
Change Detection Utilities

Provides utilities for detecting UI changes
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable
from enum import Enum, auto
import hashlib
import time


class ChangeType(Enum):
    """Types of detected changes."""
    ADDED = auto()
    REMOVED = auto()
    MODIFIED = auto()
    REORDERED = auto()


@dataclass
class Change:
    """Represents a detected change."""
    change_type: ChangeType
    path: str
    old_value: Any = None
    new_value: Any = None
    timestamp: float = 0.0


class ChangeDetector:
    """
    Detects changes in UI state or data structures.
    
    Uses content hashing and diff algorithms to
    identify added, removed, or modified elements.
    """

    def __init__(self) -> None:
        self._snapshots: dict[str, Any] = {}
        self._hashes: dict[str, str] = {}
        self._listeners: list[Callable[[list[Change]], None]] = []

    def capture(
        self,
        key: str,
        data: Any,
    ) -> list[Change]:
        """
        Capture a snapshot and detect changes.
        
        Args:
            key: Identifier for the snapshot.
            data: Data to capture.
            
        Returns:
            List of detected changes.
        """
        changes = []
        new_hash = self._compute_hash(data)

        if key in self._hashes:
            old_hash = self._hashes[key]
            if old_hash != new_hash:
                changes.append(Change(
                    change_type=ChangeType.MODIFIED,
                    path=key,
                    old_value=self._snapshots.get(key),
                    new_value=data,
                    timestamp=time.time(),
                ))

        self._snapshots[key] = data
        self._hashes[key] = new_hash
        return changes

    def detect_additions(
        self,
        key: str,
        new_items: list[Any],
    ) -> list[Change]:
        """Detect newly added items."""
        changes = []
        old_items = self._snapshots.get(key, [])
        old_set = set(str(i) for i in old_items)
        for item in new_items:
            if str(item) not in old_set:
                changes.append(Change(
                    change_type=ChangeType.ADDED,
                    path=key,
                    new_value=item,
                    timestamp=time.time(),
                ))
        return changes

    def detect_removals(
        self,
        key: str,
        new_items: list[Any],
    ) -> list[Change]:
        """Detect removed items."""
        changes = []
        old_items = self._snapshots.get(key, [])
        new_set = set(str(i) for i in new_items)
        for item in old_items:
            if str(item) not in new_set:
                changes.append(Change(
                    change_type=ChangeType.REMOVED,
                    path=key,
                    old_value=item,
                    timestamp=time.time(),
                ))
        return changes

    def get_snapshot(self, key: str) -> Any:
        """Get a previously captured snapshot."""
        return self._snapshots.get(key)

    def _compute_hash(self, data: Any) -> str:
        """Compute hash of data."""
        content = str(data).encode("utf-8")
        return hashlib.sha256(content).hexdigest()[:16]

    def add_listener(
        self,
        listener: Callable[[list[Change]], None]
    ) -> None:
        """Add a change listener."""
        self._listeners.append(listener)

    def notify_listeners(self, changes: list[Change]) -> None:
        """Notify all listeners of changes."""
        for listener in self._listeners:
            listener(changes)
