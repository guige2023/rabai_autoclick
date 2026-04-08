"""Data snapshot action module for RabAI AutoClick.

Provides data snapshot capabilities:
- DataSnapshot: Create and manage data snapshots
- SnapshotManager: Manage snapshots with versioning
- IncrementalSnapshot: Efficient incremental snapshots
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import time
import threading
import logging
import hashlib
import json
import os
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SnapshotType(Enum):
    """Snapshot types."""
    FULL = "full"
    INCREMENTAL = "incremental"
    DELTA = "delta"


@dataclass
class SnapshotMetadata:
    """Snapshot metadata."""
    snapshot_id: str
    name: str
    snapshot_type: SnapshotType
    created_at: float = field(default_factory=time.time)
    size_bytes: int = 0
    checksum: Optional[str] = None
    parent_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SnapshotConfig:
    """Configuration for snapshots."""
    snapshot_type: SnapshotType = SnapshotType.FULL
    storage_path: Optional[str] = None
    max_snapshots: int = 50
    compression: bool = False
    checksum_enabled: bool = True
    incremental_window: int = 10


class DataSnapshot:
    """Data snapshot container."""
    
    def __init__(self, snapshot_id: str, name: str, data: Any, metadata: SnapshotMetadata):
        self.snapshot_id = snapshot_id
        self.name = name
        self.data = data
        self.metadata = metadata


class SnapshotManager:
    """Manage data snapshots with versioning."""
    
    def __init__(self, name: str, config: Optional[SnapshotConfig] = None):
        self.name = name
        self.config = config or SnapshotConfig()
        self._snapshots: Dict[str, DataSnapshot] = {}
        self._snapshots_by_name: Dict[str, List[str]] = defaultdict(list)
        self._latest_by_name: Dict[str, str] = {}
        self._lock = threading.RLock()
        self._stats = {"total_snapshots": 0, "snapshots_created": 0, "snapshots_restored": 0}
    
    def _generate_id(self, name: str) -> str:
        """Generate unique snapshot ID."""
        timestamp = int(time.time() * 1000)
        hash_input = f"{name}:{timestamp}:{len(self._snapshots)}"
        hash_val = hashlib.md5(hash_input.encode()).hexdigest()[:12]
        return f"snap_{name}_{timestamp}_{hash_val}"
    
    def _compute_checksum(self, data: Any) -> Optional[str]:
        """Compute checksum of data."""
        if not self.config.checksum_enabled:
            return None
        try:
            serialized = json.dumps(data, sort_keys=True, default=str)
            return hashlib.sha256(serialized.encode()).hexdigest()
        except Exception:
            return None
    
    def create(self, name: str, data: Any, snapshot_type: Optional[SnapshotType] = None, parent_id: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> DataSnapshot:
        """Create a new snapshot."""
        snapshot_id = self._generate_id(name)
        snap_type = snapshot_type or self.config.snapshot_type
        
        if self.config.storage_path:
            self._save_to_disk(snapshot_id, data)
        
        checksum = self._compute_checksum(data)
        
        meta = SnapshotMetadata(
            snapshot_id=snapshot_id,
            name=name,
            snapshot_type=snap_type,
            size_bytes=self._estimate_size(data),
            checksum=checksum,
            parent_id=parent_id,
            metadata=metadata or {}
        )
        
        snapshot = DataSnapshot(snapshot_id=snapshot_id, name=name, data=data, metadata=meta)
        
        with self._lock:
            self._snapshots[snapshot_id] = snapshot
            self._snapshots_by_name[name].append(snapshot_id)
            self._latest_by_name[name] = snapshot_id
            self._stats["snapshots_created"] += 1
            self._stats["total_snapshots"] += 1
            
            self._enforce_max_snapshots(name)
        
        return snapshot
    
    def _estimate_size(self, data: Any) -> int:
        """Estimate size of data in bytes."""
        try:
            return len(json.dumps(data, default=str).encode())
        except Exception:
            return 0
    
    def _save_to_disk(self, snapshot_id: str, data: Any):
        """Save snapshot data to disk."""
        if not self.config.storage_path:
            return
        try:
            os.makedirs(self.config.storage_path, exist_ok=True)
            path = os.path.join(self.config.storage_path, f"{snapshot_id}.json")
            with open(path, 'w') as f:
                json.dump(data, f, default=str)
        except Exception as e:
            logging.error(f"Failed to save snapshot to disk: {e}")
    
    def get(self, snapshot_id: str) -> Optional[DataSnapshot]:
        """Get snapshot by ID."""
        with self._lock:
            return self._snapshots.get(snapshot_id)
    
    def get_latest(self, name: str) -> Optional[DataSnapshot]:
        """Get latest snapshot for name."""
        with self._lock:
            latest_id = self._latest_by_name.get(name)
            if latest_id:
                return self._snapshots.get(latest_id)
        return None
    
    def get_by_version(self, name: str, version: int) -> Optional[DataSnapshot]:
        """Get snapshot by version number."""
        with self._lock:
            ids = self._snapshots_by_name.get(name, [])
            if 0 <= version < len(ids):
                return self._snapshots.get(ids[version])
        return None
    
    def list_versions(self, name: str) -> List[SnapshotMetadata]:
        """List all versions of a snapshot."""
        with self._lock:
            ids = self._snapshots_by_name.get(name, [])
            return [self._snapshots[sid].metadata for sid in ids if sid in self._snapshots]
    
    def delete(self, snapshot_id: str) -> bool:
        """Delete a snapshot."""
        with self._lock:
            if snapshot_id not in self._snapshots:
                return False
            
            snapshot = self._snapshots[snapshot_id]
            name = snapshot.name
            
            del self._snapshots[snapshot_id]
            
            ids = self._snapshots_by_name[name]
            if snapshot_id in ids:
                ids.remove(snapshot_id)
            
            if self._latest_by_name.get(name) == snapshot_id:
                self._latest_by_name[name] = ids[-1] if ids else None
            
            return True
    
    def _enforce_max_snapshots(self, name: str):
        """Enforce maximum snapshot count per name."""
        ids = self._snapshots_by_name[name]
        while len(ids) > self.config.max_snapshots:
            oldest_id = ids.pop(0)
            if oldest_id in self._snapshots:
                del self._snapshots[oldest_id]
    
    def restore(self, snapshot_id: str) -> Tuple[bool, Any]:
        """Restore data from snapshot."""
        snapshot = self.get(snapshot_id)
        if not snapshot:
            return False, None
        
        with self._lock:
            self._stats["snapshots_restored"] += 1
        
        if self.config.storage_path and snapshot.data is None:
            path = os.path.join(self.config.storage_path, f"{snapshot_id}.json")
            if os.path.exists(path):
                try:
                    with open(path, 'r') as f:
                        data = json.load(f)
                    return True, data
                except Exception as e:
                    return False, str(e)
        
        return True, snapshot.data
    
    def get_stats(self) -> Dict[str, Any]:
        """Get snapshot statistics."""
        with self._lock:
            return {
                "name": self.name,
                "total_snapshots": len(self._snapshots),
                "tracked_names": len(self._snapshots_by_name),
                **{k: v for k, v in self._stats.items()},
            }


class DataSnapshotAction(BaseAction):
    """Data snapshot action."""
    action_type = "data_snapshot"
    display_name = "数据快照"
    description = "数据版本快照管理"
    
    def __init__(self):
        super().__init__()
        self._managers: Dict[str, SnapshotManager] = {}
        self._lock = threading.Lock()
    
    def _get_manager(self, name: str, config: Optional[SnapshotConfig] = None) -> SnapshotManager:
        """Get or create snapshot manager."""
        with self._lock:
            if name not in self._managers:
                self._managers[name] = SnapshotManager(name, config)
            return self._managers[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute snapshot operation."""
        try:
            manager_name = params.get("manager", "default")
            command = params.get("command", "create")
            
            config = SnapshotConfig(
                snapshot_type=SnapshotType[params.get("snapshot_type", "full").upper()],
                storage_path=params.get("storage_path"),
                max_snapshots=params.get("max_snapshots", 50),
                checksum_enabled=params.get("checksum_enabled", True),
            )
            
            manager = self._get_manager(manager_name, config)
            
            if command == "create":
                name = params.get("name")
                data = params.get("data")
                if not name or data is None:
                    return ActionResult(success=False, message="name and data required")
                snapshot = manager.create(name, data)
                return ActionResult(success=True, data={"snapshot_id": snapshot.snapshot_id})
            
            elif command == "get":
                snapshot_id = params.get("snapshot_id")
                snapshot = manager.get(snapshot_id)
                if snapshot:
                    return ActionResult(success=True, data={"data": snapshot.data})
                return ActionResult(success=False, message="Snapshot not found")
            
            elif command == "restore":
                snapshot_id = params.get("snapshot_id")
                success, data = manager.restore(snapshot_id)
                return ActionResult(success=success, data={"data": data})
            
            elif command == "latest":
                name = params.get("name")
                snapshot = manager.get_latest(name)
                if snapshot:
                    return ActionResult(success=True, data={"snapshot_id": snapshot.snapshot_id, "data": snapshot.data})
                return ActionResult(success=False, message="No snapshot found")
            
            elif command == "versions":
                name = params.get("name")
                versions = manager.list_versions(name)
                return ActionResult(success=True, data={"versions": [{"id": v.snapshot_id, "created": v.created_at} for v in versions]})
            
            elif command == "stats":
                stats = manager.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"DataSnapshotAction error: {str(e)}")
