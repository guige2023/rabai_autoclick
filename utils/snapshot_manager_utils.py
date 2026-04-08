"""
Snapshot Manager Utilities

Provides utilities for managing UI state snapshots
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from datetime import datetime


@dataclass
class Snapshot:
    """A snapshot of UI state."""
    id: str
    timestamp: datetime
    elements: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


class SnapshotManager:
    """
    Manages UI state snapshots.
    
    Stores, retrieves, and compares
    state snapshots.
    """

    def __init__(self, max_snapshots: int = 100) -> None:
        self._snapshots: list[Snapshot] = []
        self._max_snapshots = max_snapshots
        self._snapshot_id = 0

    def take_snapshot(
        self,
        elements: list[dict[str, Any]],
        metadata: dict[str, Any] | None = None,
    ) -> Snapshot:
        """
        Take a snapshot of current state.
        
        Args:
            elements: Current UI elements.
            metadata: Optional metadata.
            
        Returns:
            Created Snapshot.
        """
        self._snapshot_id += 1
        snapshot = Snapshot(
            id=f"snap_{self._snapshot_id}",
            timestamp=datetime.now(),
            elements=list(elements),
            metadata=metadata or {},
        )
        self._snapshots.append(snapshot)
        if len(self._snapshots) > self._max_snapshots:
            self._snapshots.pop(0)
        return snapshot

    def get_snapshot(self, snapshot_id: str) -> Snapshot | None:
        """Get snapshot by ID."""
        for snap in self._snapshots:
            if snap.id == snapshot_id:
                return snap
        return None

    def get_latest(self) -> Snapshot | None:
        """Get most recent snapshot."""
        return self._snapshots[-1] if self._snapshots else None

    def compare(
        self,
        snapshot_id1: str,
        snapshot_id2: str,
    ) -> dict[str, Any]:
        """Compare two snapshots."""
        snap1 = self.get_snapshot(snapshot_id1)
        snap2 = self.get_snapshot(snapshot_id2)
        if not snap1 or not snap2:
            return {}
        return {
            "added": len(snap2.elements) - len(snap1.elements),
            "snapshot1_size": len(snap1.elements),
            "snapshot2_size": len(snap2.elements),
        }
