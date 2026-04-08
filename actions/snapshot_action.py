"""Snapshot Pattern Action Module.

Provides snapshot pattern for state
capture and restoration.
"""

import time
import json
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SnapshotType(Enum):
    """Snapshot type."""
    FULL = "full"
    INCREMENTAL = "incremental"


@dataclass
class Snapshot:
    """State snapshot."""
    snapshot_id: str
    entity_id: str
    snapshot_type: SnapshotType
    state: Dict
    timestamp: float = field(default_factory=time.time)
    metadata: Dict = field(default_factory=dict)


class SnapshotManager:
    """Manages state snapshots."""

    def __init__(self):
        self._snapshots: Dict[str, List[Snapshot]] = {}

    def take_snapshot(
        self,
        entity_id: str,
        state: Dict,
        snapshot_type: SnapshotType = SnapshotType.FULL,
        metadata: Optional[Dict] = None
    ) -> str:
        """Take a snapshot."""
        snapshot_id = f"snap_{int(time.time() * 1000)}"

        snapshot = Snapshot(
            snapshot_id=snapshot_id,
            entity_id=entity_id,
            snapshot_type=snapshot_type,
            state=state.copy(),
            metadata=metadata or {}
        )

        if entity_id not in self._snapshots:
            self._snapshots[entity_id] = []

        self._snapshots[entity_id].append(snapshot)
        return snapshot_id

    def restore(
        self,
        entity_id: str,
        snapshot_id: str
    ) -> Optional[Dict]:
        """Restore from snapshot."""
        snapshots = self._snapshots.get(entity_id, [])
        for snap in snapshots:
            if snap.snapshot_id == snapshot_id:
                return snap.state.copy()
        return None

    def get_latest(self, entity_id: str) -> Optional[Dict]:
        """Get latest snapshot."""
        snapshots = self._snapshots.get(entity_id, [])
        if not snapshots:
            return None
        return snapshots[-1].state.copy()

    def get_history(
        self,
        entity_id: str,
        limit: int = 100
    ) -> List[Dict]:
        """Get snapshot history."""
        snapshots = self._snapshots.get(entity_id, [])[-limit:]
        return [
            {
                "snapshot_id": s.snapshot_id,
                "type": s.snapshot_type.value,
                "timestamp": s.timestamp
            }
            for s in snapshots
        ]


class SnapshotPatternAction(BaseAction):
    """Action for snapshot pattern operations."""

    def __init__(self):
        super().__init__("snapshot")
        self._manager = SnapshotManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute snapshot action."""
        try:
            operation = params.get("operation", "take")

            if operation == "take":
                return self._take(params)
            elif operation == "restore":
                return self._restore(params)
            elif operation == "latest":
                return self._latest(params)
            elif operation == "history":
                return self._history(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _take(self, params: Dict) -> ActionResult:
        """Take snapshot."""
        snapshot_id = self._manager.take_snapshot(
            entity_id=params.get("entity_id", ""),
            state=params.get("state", {}),
            snapshot_type=SnapshotType(params.get("type", "full")),
            metadata=params.get("metadata")
        )
        return ActionResult(success=True, data={"snapshot_id": snapshot_id})

    def _restore(self, params: Dict) -> ActionResult:
        """Restore snapshot."""
        state = self._manager.restore(
            entity_id=params.get("entity_id", ""),
            snapshot_id=params.get("snapshot_id", "")
        )
        if state is None:
            return ActionResult(success=False, message="Snapshot not found")
        return ActionResult(success=True, data={"state": state})

    def _latest(self, params: Dict) -> ActionResult:
        """Get latest snapshot."""
        state = self._manager.get_latest(params.get("entity_id", ""))
        if state is None:
            return ActionResult(success=False, message="No snapshots found")
        return ActionResult(success=True, data={"state": state})

    def _history(self, params: Dict) -> ActionResult:
        """Get history."""
        history = self._manager.get_history(
            params.get("entity_id", ""),
            params.get("limit", 100)
        )
        return ActionResult(success=True, data={"history": history})
