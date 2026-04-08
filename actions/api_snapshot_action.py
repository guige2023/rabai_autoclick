"""
API Snapshot Action - Snapshots API responses for testing.

This module provides snapshot testing capabilities for
capturing and comparing API responses.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class SnapshotConfig:
    """Configuration for snapshots."""
    directory: str = "./snapshots"
    update_snapshots: bool = False


@dataclass
class SnapshotResult:
    """Result of snapshot comparison."""
    matched: bool
    snapshot_name: str
    actual_data: Any | None = None
    expected_data: Any | None = None
    diff: str | None = None


class SnapshotStore:
    """Stores and retrieves snapshots."""
    
    def __init__(self, directory: str = "./snapshots") -> None:
        self.directory = directory
        os.makedirs(directory, exist_ok=True)
    
    def _get_path(self, name: str) -> str:
        """Get file path for snapshot."""
        return os.path.join(self.directory, f"{name}.json")
    
    def save(self, name: str, data: Any) -> None:
        """Save snapshot."""
        path = self._get_path(name)
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)
    
    def load(self, name: str) -> Any | None:
        """Load snapshot."""
        path = self._get_path(name)
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
        return None
    
    def delete(self, name: str) -> bool:
        """Delete snapshot."""
        path = self._get_path(name)
        if os.path.exists(path):
            os.remove(path)
            return True
        return False


class APISnapshotAction:
    """API snapshot action for automation workflows."""
    
    def __init__(self, directory: str = "./snapshots", update: bool = False) -> None:
        self.config = SnapshotConfig(directory=directory, update_snapshots=update)
        self.store = SnapshotStore(directory)
    
    async def compare(self, name: str, data: Any) -> SnapshotResult:
        """Compare data against snapshot."""
        expected = self.store.load(name)
        
        if expected is None or self.config.update_snapshots:
            self.store.save(name, data)
            return SnapshotResult(matched=True, snapshot_name=name, actual_data=data, expected_data=data)
        
        matched = data == expected
        diff = None if matched else f"Expected {expected}, got {data}"
        
        return SnapshotResult(
            matched=matched,
            snapshot_name=name,
            actual_data=data,
            expected_data=expected,
            diff=diff,
        )
    
    def update_snapshot(self, name: str, data: Any) -> None:
        """Update a snapshot."""
        self.store.save(name, data)


__all__ = ["SnapshotConfig", "SnapshotResult", "SnapshotStore", "APISnapshotAction"]
