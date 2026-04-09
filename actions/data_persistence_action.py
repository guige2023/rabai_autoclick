"""Data persistence action module for RabAI AutoClick.

Provides data persistence operations:
- DataPersistenceAction: Save and load data
- DataPersistenceJSONAction: JSON file persistence
- DataPersistenceCacheAction: Cache-based persistence
- DataPersistenceCheckpointAction: Checkpoint-based persistence
"""

import os
import json
import pickle
import time
from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os as os_module

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataPersistenceAction(BaseAction):
    """Save and load data with various backends."""
    action_type = "data_persistence"
    display_name = "数据持久化"
    description = "多种后端的数据持久化"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "save")
            data = params.get("data")
            key = params.get("key")
            backend = params.get("backend", "memory")
            path = params.get("path")
            ttl = params.get("ttl", 3600)

            if operation == "save":
                if data is None:
                    return ActionResult(success=False, message="data is required")
                return self._save_data(data, key, backend, path, ttl)

            elif operation == "load":
                if not key:
                    return ActionResult(success=False, message="key is required")
                return self._load_data(key, backend, path)

            elif operation == "delete":
                if not key:
                    return ActionResult(success=False, message="key is required")
                return self._delete_data(key, backend, path)

            elif operation == "exists":
                if not key:
                    return ActionResult(success=False, message="key is required")
                return self._exists_data(key, backend, path)

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Persistence error: {e}")

    def _save_data(self, data: Any, key: str, backend: str, path: Optional[str], ttl: int) -> ActionResult:
        """Save data to backend."""
        if backend == "memory":
            self._memory_store = getattr(self, "_memory_store", {})
            self._memory_store[key] = {"data": data, "timestamp": time.time(), "ttl": ttl}
            return ActionResult(success=True, message=f"Saved to memory: {key}")
        elif backend == "file" and path:
            try:
                with open(path, "w") as f:
                    json.dump({"key": key, "data": data, "timestamp": time.time()}, f)
                return ActionResult(success=True, message=f"Saved to file: {path}")
            except Exception as e:
                return ActionResult(success=False, message=f"File save error: {e}")
        elif backend == "pickle" and path:
            try:
                with open(path, "wb") as f:
                    pickle.dump({"key": key, "data": data, "timestamp": time.time()}, f)
                return ActionResult(success=True, message=f"Saved to pickle: {path}")
            except Exception as e:
                return ActionResult(success=False, message=f"Pickle save error: {e}")
        return ActionResult(success=False, message=f"Unknown backend: {backend}")

    def _load_data(self, key: str, backend: str, path: Optional[str]) -> ActionResult:
        """Load data from backend."""
        if backend == "memory":
            store = getattr(self, "_memory_store", {})
            if key in store:
                entry = store[key]
                age = time.time() - entry["timestamp"]
                if age < entry["ttl"]:
                    return ActionResult(success=True, message=f"Loaded from memory: {key}", data={"data": entry["data"], "age": age})
                return ActionResult(success=False, message=f"Entry expired: {key}")
            return ActionResult(success=False, message=f"Key not found: {key}")
        elif backend == "file" and path:
            try:
                with open(path, "r") as f:
                    entry = json.load(f)
                return ActionResult(success=True, message=f"Loaded from file: {path}", data={"data": entry.get("data")})
            except Exception as e:
                return ActionResult(success=False, message=f"File load error: {e}")
        return ActionResult(success=False, message=f"Unknown backend: {backend}")

    def _delete_data(self, key: str, backend: str, path: Optional[str]) -> ActionResult:
        """Delete data from backend."""
        if backend == "memory":
            store = getattr(self, "_memory_store", {})
            if key in store:
                del store[key]
                return ActionResult(success=True, message=f"Deleted: {key}")
            return ActionResult(success=False, message=f"Key not found: {key}")
        elif backend == "file" and path and os.path.exists(path):
            os.remove(path)
            return ActionResult(success=True, message=f"Deleted file: {path}")
        return ActionResult(success=False, message=f"Cannot delete: {backend}")

    def _exists_data(self, key: str, backend: str, path: Optional[str]) -> ActionResult:
        """Check if data exists."""
        if backend == "memory":
            store = getattr(self, "_memory_store", {})
            exists = key in store
            if exists:
                entry = store[key]
                age = time.time() - entry["timestamp"]
                expired = age >= entry["ttl"]
                return ActionResult(success=True, message=f"Exists: {key} (expired={expired})", data={"exists": not expired})
            return ActionResult(success=True, message=f"Not found: {key}", data={"exists": False})
        return ActionResult(success=True, message=f"Cannot check: {backend}", data={"exists": None})


class DataPersistenceJSONAction(BaseAction):
    """JSON file persistence."""
    action_type = "data_persistence_json"
    display_name = "JSON文件持久化"
    description = "JSON格式文件持久化"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "save")
            data = params.get("data")
            path = params.get("path", "/tmp/data_persistence.json")
            indent = params.get("indent", 2)
            append = params.get("append", False)

            if operation == "save":
                if data is None:
                    return ActionResult(success=False, message="data is required")
                mode = "a" if append else "w"
                with open(path, mode) as f:
                    json.dump(data, f, indent=indent, default=str)
                return ActionResult(success=True, message=f"Saved to {path}")

            elif operation == "load":
                if not os.path.exists(path):
                    return ActionResult(success=False, message=f"File not found: {path}")
                with open(path, "r") as f:
                    loaded = json.load(f)
                return ActionResult(success=True, message=f"Loaded from {path}", data={"data": loaded})

            elif operation == "append":
                if data is None:
                    return ActionResult(success=False, message="data is required")
                if os.path.exists(path):
                    with open(path, "r") as f:
                        existing = json.load(f)
                    if isinstance(existing, list):
                        existing.append(data)
                    elif isinstance(existing, dict):
                        existing.update(data)
                    data_to_write = existing
                else:
                    data_to_write = [data] if not isinstance(data, list) else data
                with open(path, "w") as f:
                    json.dump(data_to_write, f, indent=indent, default=str)
                return ActionResult(success=True, message=f"Appended to {path}")

            elif operation == "delete":
                if os.path.exists(path):
                    os.remove(path)
                    return ActionResult(success=True, message=f"Deleted: {path}")
                return ActionResult(success=False, message=f"File not found: {path}")

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"JSON persistence error: {e}")


class DataPersistenceCacheAction(BaseAction):
    """Cache-based persistence with TTL."""
    action_type = "data_persistence_cache"
    display_name = "缓存持久化"
    description = "带TTL的缓存持久化"

    def __init__(self):
        super().__init__()
        self._cache: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "set")
            key = params.get("key")
            data = params.get("data")
            ttl = params.get("ttl", 300)

            if operation == "set":
                if not key or data is None:
                    return ActionResult(success=False, message="key and data required")
                self._cache[key] = {"data": data, "timestamp": time.time(), "ttl": ttl}
                return ActionResult(success=True, message=f"Cached: {key}")

            elif operation == "get":
                if not key:
                    return ActionResult(success=False, message="key required")
                if key not in self._cache:
                    return ActionResult(success=False, message=f"Not cached: {key}")
                entry = self._cache[key]
                age = time.time() - entry["timestamp"]
                if age > entry["ttl"]:
                    del self._cache[key]
                    return ActionResult(success=False, message=f"Expired: {key}")
                return ActionResult(success=True, message=f"Cache hit: {key}", data={"data": entry["data"], "age": age})

            elif operation == "delete":
                if key and key in self._cache:
                    del self._cache[key]
                return ActionResult(success=True, message=f"Deleted: {key}")

            elif operation == "clear":
                count = len(self._cache)
                self._cache.clear()
                return ActionResult(success=True, message=f"Cleared {count} entries")

            elif operation == "stats":
                expired = sum(1 for k, v in self._cache.items() if time.time() - v["timestamp"] > v["ttl"])
                return ActionResult(success=True, message=f"Cache stats", data={"total": len(self._cache), "expired": expired, "active": len(self._cache) - expired})

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Cache persistence error: {e}")


class DataPersistenceCheckpointAction(BaseAction):
    """Checkpoint-based persistence for long-running operations."""
    action_type = "data_persistence_checkpoint"
    display_name = "检查点持久化"
    description = "长时间运行的检查点持久化"

    def __init__(self):
        super().__init__()
        self._checkpoints: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "checkpoint")
            checkpoint_id = params.get("checkpoint_id")
            state_data = params.get("state_data")
            metadata = params.get("metadata", {})
            max_checkpoints = params.get("max_checkpoints", 10)

            if operation == "checkpoint":
                if state_data is None:
                    return ActionResult(success=False, message="state_data required")
                cp_id = checkpoint_id or f"cp_{int(time.time() * 1000)}"
                self._checkpoints[cp_id] = {
                    "state": state_data,
                    "metadata": metadata,
                    "timestamp": time.time(),
                }
                if len(self._checkpoints) > max_checkpoints:
                    oldest = min(self._checkpoints.keys(), key=lambda k: self._checkpoints[k]["timestamp"])
                    del self._checkpoints[oldest]
                return ActionResult(success=True, message=f"Checkpoint saved: {cp_id}", data={"checkpoint_id": cp_id})

            elif operation == "restore":
                if not checkpoint_id or checkpoint_id not in self._checkpoints:
                    return ActionResult(success=False, message=f"Checkpoint not found: {checkpoint_id}")
                cp = self._checkpoints[checkpoint_id]
                return ActionResult(success=True, message=f"Restored: {checkpoint_id}", data={"state": cp["state"], "metadata": cp["metadata"]})

            elif operation == "list":
                return ActionResult(success=True, message=f"{len(self._checkpoints)} checkpoints", data={"checkpoints": {k: {"timestamp": v["timestamp"], "metadata": v["metadata"]} for k, v in self._checkpoints.items()}})

            elif operation == "delete":
                if checkpoint_id and checkpoint_id in self._checkpoints:
                    del self._checkpoints[checkpoint_id]
                    return ActionResult(success=True, message=f"Deleted: {checkpoint_id}")
                return ActionResult(success=False, message="Not found")

            elif operation == "latest":
                if not self._checkpoints:
                    return ActionResult(success=False, message="No checkpoints")
                latest_id = max(self._checkpoints.keys(), key=lambda k: self._checkpoints[k]["timestamp"])
                return ActionResult(success=True, message=f"Latest: {latest_id}", data={"checkpoint_id": latest_id, "state": self._checkpoints[latest_id]["state"]})

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Checkpoint error: {e}")
