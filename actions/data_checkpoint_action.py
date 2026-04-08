"""Data checkpoint action module for RabAI AutoClick.

Provides checkpoint capabilities for data processing:
- DataCheckpoint: Create checkpoints during data processing
- CheckpointStore: Persistent checkpoint storage
- IncrementalCheckpoint: Incremental checkpoint tracking
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import time
import threading
import logging
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


class CheckpointType(Enum):
    """Checkpoint types."""
    SNAPSHOT = "snapshot"
    INCREMENTAL = "incremental"
    MARKER = "marker"


@dataclass
class DataCheckpoint:
    """Data checkpoint definition."""
    checkpoint_id: str
    name: str
    checkpoint_type: CheckpointType
    position: int
    data: Any
    checksum: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CheckpointConfig:
    """Configuration for checkpointing."""
    storage_path: Optional[str] = None
    max_checkpoints: int = 100
    compression: bool = False
    auto_checkpoint_interval: int = 100
    persist_on_create: bool = True


class CheckpointStore:
    """Storage for checkpoints."""
    
    def __init__(self, config: CheckpointConfig):
        self.config = config
        self._checkpoints: Dict[str, DataCheckpoint] = {}
        self._checkpoints_by_name: Dict[str, List[str]] = defaultdict(list)
        self._lock = threading.RLock()
    
    def save(self, checkpoint: DataCheckpoint):
        """Save checkpoint to store."""
        with self._lock:
            self._checkpoints[checkpoint.checkpoint_id] = checkpoint
            self._checkpoints_by_name[checkpoint.name].append(checkpoint.checkpoint_id)
            
            self._enforce_max(checkpoint.name)
            self._persist(checkpoint)
    
    def _enforce_max(self, name: str):
        """Enforce maximum checkpoints per name."""
        ids = self._checkpoints_by_name[name]
        while len(ids) > self.config.max_checkpoints:
            oldest_id = ids.pop(0)
            self._checkpoints.pop(oldest_id, None)
    
    def _persist(self, checkpoint: DataCheckpoint):
        """Persist checkpoint to disk."""
        if not self.config.storage_path:
            return
        try:
            os.makedirs(self.config.storage_path, exist_ok=True)
            path = os.path.join(self.config.storage_path, f"{checkpoint.checkpoint_id}.json")
            data = {
                "checkpoint_id": checkpoint.checkpoint_id,
                "name": checkpoint.name,
                "type": checkpoint.checkpoint_type.value,
                "position": checkpoint.position,
                "data": checkpoint.data,
                "checksum": checkpoint.checksum,
                "created_at": checkpoint.created_at,
            }
            with open(path, 'w') as f:
                json.dump(data, f, default=str)
        except Exception as e:
            logging.error(f"Failed to persist checkpoint: {e}")
    
    def get(self, checkpoint_id: str) -> Optional[DataCheckpoint]:
        """Get checkpoint by ID."""
        with self._lock:
            return self._checkpoints.get(checkpoint_id)
    
    def get_latest(self, name: str) -> Optional[DataCheckpoint]:
        """Get latest checkpoint for name."""
        with self._lock:
            ids = self._checkpoints_by_name.get(name, [])
            if ids:
                return self._checkpoints.get(ids[-1])
        return None
    
    def list_all(self, name: Optional[str] = None) -> List[DataCheckpoint]:
        """List all checkpoints."""
        with self._lock:
            if name:
                ids = self._checkpoints_by_name.get(name, [])
                return [self._checkpoints[i] for i in ids if i in self._checkpoints]
            return list(self._checkpoints.values())


class DataCheckpointAction(BaseAction):
    """Data checkpoint action."""
    action_type = "data_checkpoint"
    display_name = "数据检查点"
    description = "数据处理检查点管理"
    
    def __init__(self):
        super().__init__()
        self._store: Optional[CheckpointStore] = None
        self._lock = threading.Lock()
    
    def _get_store(self, params: Dict[str, Any]) -> CheckpointStore:
        """Get or create checkpoint store."""
        with self._lock:
            if self._store is None:
                config = CheckpointConfig(
                    storage_path=params.get("storage_path"),
                    max_checkpoints=params.get("max_checkpoints", 100),
                    auto_checkpoint_interval=params.get("auto_checkpoint_interval", 100),
                )
                self._store = CheckpointStore(config)
            return self._store
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute checkpoint operation."""
        try:
            store = self._get_store(params)
            command = params.get("command", "create")
            
            if command == "create":
                checkpoint_id = f"cp_{params.get('name', 'default')}_{int(time.time() * 1000)}"
                cp = DataCheckpoint(
                    checkpoint_id=checkpoint_id,
                    name=params.get("name", "default"),
                    checkpoint_type=CheckpointType[params.get("checkpoint_type", "snapshot").upper()],
                    position=params.get("position", 0),
                    data=params.get("data"),
                )
                store.save(cp)
                return ActionResult(success=True, data={"checkpoint_id": checkpoint_id, "position": cp.position})
            
            elif command == "get":
                checkpoint_id = params.get("checkpoint_id")
                cp = store.get(checkpoint_id)
                if cp:
                    return ActionResult(success=True, data={"data": cp.data, "position": cp.position})
                return ActionResult(success=False, message="Checkpoint not found")
            
            elif command == "latest":
                name = params.get("name", "default")
                cp = store.get_latest(name)
                if cp:
                    return ActionResult(success=True, data={"checkpoint_id": cp.checkpoint_id, "data": cp.data, "position": cp.position})
                return ActionResult(success=False, message="No checkpoint found")
            
            elif command == "list":
                name = params.get("name")
                checkpoints = store.list_all(name)
                return ActionResult(success=True, data={"checkpoints": [{"id": c.checkpoint_id, "position": c.position} for c in checkpoints]})
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"DataCheckpointAction error: {str(e)}")
