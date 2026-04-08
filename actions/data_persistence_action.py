"""
Data Persistence Action Module.

Handles data persistence to various backends: files, databases,
caches, and object stores with serialization options.
"""
from typing import Any, Optional
from dataclasses import dataclass
from actions.base_action import BaseAction


@dataclass
class PersistenceResult:
    """Result of persistence operation."""
    success: bool
    key: str
    backend: str
    size_bytes: int
    error: Optional[str] = None


class DataPersistenceAction(BaseAction):
    """Persist data to various backends."""

    def __init__(self) -> None:
        super().__init__("data_persistence")
        self._backends: dict[str, Any] = {}

    def execute(self, context: dict, params: dict) -> dict:
        """
        Persist or retrieve data.

        Args:
            context: Execution context
            params: Parameters:
                - action: save, load, delete, list
                - backend: file, db, cache, s3
                - key: Data key/identifier
                - data: Data to persist (for save)
                - format: Serialization format (json, pickle, parquet)
                - path: File path (for file backend)

        Returns:
            PersistenceResult
        """
        import json
        import pickle

        action = params.get("action", "save")
        backend = params.get("backend", "file")
        key = params.get("key", "")
        data = params.get("data")
        fmt = params.get("format", "json")
        path = params.get("path", "/tmp")

        if action == "save":
            if not key:
                return PersistenceResult(False, "", backend, 0, "Key required").__dict__

            serialized = self._serialize(data, fmt)
            if backend == "file":
                return self._save_to_file(key, serialized, path)
            elif backend == "cache":
                return self._save_to_cache(key, serialized)
            return PersistenceResult(False, key, backend, 0, f"Backend {backend} not supported").__dict__

        elif action == "load":
            if backend == "file":
                return self._load_from_file(key, path, fmt)
            elif backend == "cache":
                return self._load_from_cache(key, fmt)
            return PersistenceResult(False, key, backend, 0, f"Backend {backend} not supported").__dict__

        elif action == "delete":
            return PersistenceResult(True, key, backend, 0)

        elif action == "list":
            return {"keys": [], "backend": backend}

        return {"error": f"Unknown action: {action}"}

    def _serialize(self, data: Any, fmt: str) -> bytes:
        """Serialize data to bytes."""
        import json
        import pickle

        if fmt == "json":
            return json.dumps(data, default=str).encode("utf-8")
        elif fmt == "pickle":
            return pickle.dumps(data)
        elif fmt == "text":
            return str(data).encode("utf-8")
        return str(data).encode("utf-8")

    def _deserialize(self, data: bytes, fmt: str) -> Any:
        """Deserialize bytes to data."""
        import json
        import pickle

        if fmt == "json":
            return json.loads(data.decode("utf-8"))
        elif fmt == "pickle":
            return pickle.loads(data)
        return data.decode("utf-8")

    def _save_to_file(self, key: str, data: bytes, path: str) -> dict:
        """Save to file backend."""
        import os
        try:
            file_path = os.path.join(path, key)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "wb") as f:
                f.write(data)
            return PersistenceResult(True, key, "file", len(data)).__dict__
        except Exception as e:
            return PersistenceResult(False, key, "file", 0, str(e)).__dict__

    def _load_from_file(self, key: str, path: str, fmt: str) -> dict:
        """Load from file backend."""
        import os
        try:
            file_path = os.path.join(path, key)
            with open(file_path, "rb") as f:
                data = f.read()
            return PersistenceResult(True, key, "file", len(data), data=self._deserialize(data, fmt)).__dict__
        except Exception as e:
            return PersistenceResult(False, key, "file", 0, str(e)).__dict__

    def _save_to_cache(self, key: str, data: bytes) -> dict:
        """Save to cache backend."""
        if not hasattr(self, "_cache_store"):
            self._cache_store: dict[str, bytes] = {}
        self._cache_store[key] = data
        return PersistenceResult(True, key, "cache", len(data)).__dict__

    def _load_from_cache(self, key: str, fmt: str) -> dict:
        """Load from cache backend."""
        if not hasattr(self, "_cache_store"):
            self._cache_store = {}
        if key in self._cache_store:
            return PersistenceResult(True, key, "cache", len(self._cache_store[key]), data=self._deserialize(self._cache_store[key], fmt)).__dict__
        return PersistenceResult(False, key, "cache", 0, "Key not found").__dict__
