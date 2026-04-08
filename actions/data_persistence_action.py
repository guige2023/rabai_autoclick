"""
Data Persistence Action Module.

Provides data persistence to various backends including SQLite,
JSON files, pickle, and in-memory caching with TTL support.

Author: RabAi Team
"""

from __future__ import annotations

import json
import os
import pickle
import sqlite3
import sys
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class StorageBackend(Enum):
    """Supported storage backends."""
    MEMORY = "memory"
    JSON_FILE = "json_file"
    PICKLE_FILE = "pickle_file"
    SQLITE = "sqlite"
    CACHE = "cache"


class EvictionPolicy(Enum):
    """Cache eviction policies."""
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    TTL = "ttl"


@dataclass
class CacheEntry:
    """A cache entry with TTL support."""
    key: str
    value: Any
    created_at: float
    last_accessed: float
    access_count: int = 0
    ttl_seconds: Optional[float] = None

    @property
    def is_expired(self) -> bool:
        if self.ttl_seconds is None:
            return False
        return time.time() - self.created_at > self.ttl_seconds


@dataclass
class PersistenceConfig:
    """Configuration for persistence."""
    backend: StorageBackend = StorageBackend.MEMORY
    path: Optional[str] = None
    max_entries: Optional[int] = None
    ttl_seconds: Optional[float] = None
    eviction_policy: EvictionPolicy = EvictionPolicy.LRU
    auto_commit: bool = True
    commit_interval: float = 5.0


class DataPersistenceAction(BaseAction):
    """Data persistence action.
    
    Stores and retrieves data with various persistence backends,
    caching strategies, and TTL support.
    """
    action_type = "data_persistence"
    display_name = "数据持久化"
    description = "数据存储与缓存"
    
    def __init__(self):
        super().__init__()
        self._memory_store: OrderedDict = OrderedDict()
        self._cache: Dict[str, CacheEntry] = {}
        self._sqlite_conn: Optional[sqlite3.Connection] = None
        self._config = PersistenceConfig()
        self._last_commit = time.time()
        self._lock_count = 0
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Perform persistence operations.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - operation: store/retrieve/delete/list/clear/stats/cache_set/cache_get
                - key: Storage key
                - value: Value to store
                - backend: Storage backend to use
                - path: File path for file-based storage
                - ttl: Time to live in seconds
                - max_entries: Maximum entries for cache
                - table: SQLite table name
                
        Returns:
            ActionResult with operation results.
        """
        start_time = time.time()
        
        operation = params.get("operation", "store")
        backend_str = params.get("backend", "memory")
        
        try:
            backend = StorageBackend(backend_str)
        except ValueError:
            backend = StorageBackend.MEMORY
        
        try:
            if operation == "store":
                result = self._store(params, backend, start_time)
            elif operation == "retrieve":
                result = self._retrieve(params, backend, start_time)
            elif operation == "delete":
                result = self._delete(params, backend, start_time)
            elif operation == "list":
                result = self._list_keys(params, backend, start_time)
            elif operation == "clear":
                result = self._clear(params, backend, start_time)
            elif operation == "stats":
                result = self._stats(params, backend, start_time)
            elif operation == "cache_set":
                result = self._cache_set(params, start_time)
            elif operation == "cache_get":
                result = self._cache_get(params, start_time)
            elif operation == "cache_clear":
                result = self._cache_clear(params, start_time)
            elif operation == "exists":
                result = self._exists(params, backend, start_time)
            elif operation == "batch_store":
                result = self._batch_store(params, backend, start_time)
            elif operation == "batch_retrieve":
                result = self._batch_retrieve(params, backend, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
            
            return result
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Persistence operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _ensure_sqlite(self, path: str) -> sqlite3.Connection:
        """Ensure SQLite connection and table exist."""
        if self._sqlite_conn is None:
            self._sqlite_conn = sqlite3.connect(path, check_same_thread=False)
            self._sqlite_conn.execute("""
                CREATE TABLE IF NOT EXISTS kv_store (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
            """)
            self._sqlite_conn.commit()
        return self._sqlite_conn
    
    def _store(self, params: Dict[str, Any], backend: StorageBackend, start_time: float) -> ActionResult:
        """Store a value."""
        key = params.get("key", "")
        value = params.get("value")
        path = params.get("path", "data_store.json")
        ttl = params.get("ttl")
        
        if not key:
            return ActionResult(
                success=False,
                message="Missing key",
                duration=time.time() - start_time
            )
        
        if backend == StorageBackend.MEMORY:
            self._memory_store[key] = value
            
        elif backend == StorageBackend.JSON_FILE:
            self._ensure_json_store(path)
            data = self._read_json_file(path)
            data[key] = value
            self._write_json_file(path, data)
            
        elif backend == StorageBackend.PICKLE_FILE:
            data = self._read_pickle_file(path)
            data[key] = value
            self._write_pickle_file(path, data)
            
        elif backend == StorageBackend.SQLITE:
            conn = self._ensure_sqlite(path)
            now = time.time()
            value_json = json.dumps(value)
            conn.execute("""
                INSERT OR REPLACE INTO kv_store (key, value, created_at, updated_at)
                VALUES (?, ?, COALESCE((SELECT created_at FROM kv_store WHERE key = ?), ?), ?)
            """, (key, value_json, key, now, now))
            conn.commit()
        
        elif backend == StorageBackend.CACHE:
            self._cache[key] = CacheEntry(
                key=key,
                value=value,
                created_at=time.time(),
                last_accessed=time.time(),
                ttl_seconds=ttl
            )
        
        return ActionResult(
            success=True,
            message=f"Stored: {key}",
            data={"key": key, "backend": backend.value},
            duration=time.time() - start_time
        )
    
    def _retrieve(self, params: Dict[str, Any], backend: StorageBackend, start_time: float) -> ActionResult:
        """Retrieve a value."""
        key = params.get("key", "")
        path = params.get("path", "data_store.json")
        
        if not key:
            return ActionResult(
                success=False,
                message="Missing key",
                duration=time.time() - start_time
            )
        
        value = None
        
        if backend == StorageBackend.MEMORY:
            value = self._memory_store.get(key)
            
        elif backend == StorageBackend.JSON_FILE:
            data = self._read_json_file(path)
            value = data.get(key)
            
        elif backend == StorageBackend.PICKLE_FILE:
            data = self._read_pickle_file(path)
            value = data.get(key)
            
        elif backend == StorageBackend.SQLITE:
            conn = self._ensure_sqlite(path)
            cursor = conn.execute("SELECT value FROM kv_store WHERE key = ?", (key,))
            row = cursor.fetchone()
            if row:
                value = json.loads(row[0])
        
        elif backend == StorageBackend.CACHE:
            entry = self._cache.get(key)
            if entry:
                if entry.is_expired:
                    del self._cache[key]
                    value = None
                else:
                    entry.last_accessed = time.time()
                    entry.access_count += 1
                    value = entry.value
        
        found = value is not None
        
        return ActionResult(
            success=True,
            message=f"{'Found' if found else 'Not found'}: {key}",
            data={"key": key, "value": value, "found": found},
            duration=time.time() - start_time
        )
    
    def _delete(self, params: Dict[str, Any], backend: StorageBackend, start_time: float) -> ActionResult:
        """Delete a key."""
        key = params.get("key", "")
        path = params.get("path", "data_store.json")
        
        if not key:
            return ActionResult(
                success=False,
                message="Missing key",
                duration=time.time() - start_time
            )
        
        deleted = False
        
        if backend == StorageBackend.MEMORY:
            if key in self._memory_store:
                del self._memory_store[key]
                deleted = True
                
        elif backend == StorageBackend.JSON_FILE:
            data = self._read_json_file(path)
            if key in data:
                del data[key]
                self._write_json_file(path, data)
                deleted = True
                
        elif backend == StorageBackend.PICKLE_FILE:
            data = self._read_pickle_file(path)
            if key in data:
                del data[key]
                self._write_pickle_file(path, data)
                deleted = True
                
        elif backend == StorageBackend.SQLITE:
            conn = self._ensure_sqlite(path)
            conn.execute("DELETE FROM kv_store WHERE key = ?", (key,))
            conn.commit()
            deleted = True
            
        elif backend == StorageBackend.CACHE:
            if key in self._cache:
                del self._cache[key]
                deleted = True
        
        return ActionResult(
            success=True,
            message=f"{'Deleted' if deleted else 'Not found'}: {key}",
            data={"key": key, "deleted": deleted},
            duration=time.time() - start_time
        )
    
    def _list_keys(self, params: Dict[str, Any], backend: StorageBackend, start_time: float) -> ActionResult:
        """List all keys."""
        path = params.get("path", "data_store.json")
        pattern = params.get("pattern")
        
        keys = []
        
        if backend == StorageBackend.MEMORY:
            keys = list(self._memory_store.keys())
            
        elif backend == StorageBackend.JSON_FILE:
            data = self._read_json_file(path)
            keys = list(data.keys())
            
        elif backend == StorageBackend.PICKLE_FILE:
            data = self._read_pickle_file(path)
            keys = list(data.keys())
            
        elif backend == StorageBackend.SQLITE:
            conn = self._ensure_sqlite(path)
            cursor = conn.execute("SELECT key FROM kv_store ORDER BY updated_at DESC")
            keys = [row[0] for row in cursor.fetchall()]
        
        elif backend == StorageBackend.CACHE:
            self._evict_expired()
            keys = list(self._cache.keys())
        
        if pattern:
            import fnmatch
            keys = [k for k in keys if fnmatch.fnmatch(k, pattern)]
        
        return ActionResult(
            success=True,
            message=f"Found {len(keys)} keys",
            data={"keys": keys, "count": len(keys)},
            duration=time.time() - start_time
        )
    
    def _clear(self, params: Dict[str, Any], backend: StorageBackend, start_time: float) -> ActionResult:
        """Clear all data."""
        path = params.get("path", "data_store.json")
        
        count = 0
        
        if backend == StorageBackend.MEMORY:
            count = len(self._memory_store)
            self._memory_store.clear()
            
        elif backend == StorageBackend.JSON_FILE:
            self._write_json_file(path, {})
            count = -1
            
        elif backend == StorageBackend.PICKLE_FILE:
            self._write_pickle_file(path, {})
            count = -1
            
        elif backend == StorageBackend.SQLITE:
            conn = self._ensure_sqlite(path)
            conn.execute("DELETE FROM kv_store")
            conn.commit()
            count = -1
            
        elif backend == StorageBackend.CACHE:
            count = len(self._cache)
            self._cache.clear()
        
        return ActionResult(
            success=True,
            message=f"Cleared data",
            data={"count": count},
            duration=time.time() - start_time
        )
    
    def _stats(self, params: Dict[str, Any], backend: StorageBackend, start_time: float) -> ActionResult:
        """Get storage statistics."""
        path = params.get("path", "data_store.json")
        
        stats = {
            "backend": backend.value,
            "memory_size": len(self._memory_store),
            "cache_size": len(self._cache),
            "total_accesses": sum(e.access_count for e in self._cache.values())
        }
        
        if backend == StorageBackend.JSON_FILE:
            if os.path.exists(path):
                stats["file_size"] = os.path.getsize(path)
                data = self._read_json_file(path)
                stats["entry_count"] = len(data)
                
        elif backend == StorageBackend.SQLITE:
            conn = self._ensure_sqlite(path)
            cursor = conn.execute("SELECT COUNT(*) FROM kv_store")
            stats["entry_count"] = cursor.fetchone()[0]
        
        return ActionResult(
            success=True,
            message="Stats retrieved",
            data=stats,
            duration=time.time() - start_time
        )
    
    def _cache_set(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Set a cache entry with TTL."""
        key = params.get("key", "")
        value = params.get("value")
        ttl = params.get("ttl")
        
        if not key:
            return ActionResult(success=False, message="Missing key", duration=time.time() - start_time)
        
        max_entries = params.get("max_entries", 1000)
        if len(self._cache) >= max_entries:
            self._evict_one()
        
        self._cache[key] = CacheEntry(
            key=key,
            value=value,
            created_at=time.time(),
            last_accessed=time.time(),
            ttl_seconds=ttl
        )
        
        return ActionResult(
            success=True,
            message=f"Cached: {key}",
            data={"key": key, "ttl": ttl},
            duration=time.time() - start_time
        )
    
    def _cache_get(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a cache entry."""
        key = params.get("key", "")
        
        entry = self._cache.get(key)
        
        if not entry:
            return ActionResult(
                success=True,
                message=f"Cache miss: {key}",
                data={"key": key, "found": False, "value": None},
                duration=time.time() - start_time
            )
        
        if entry.is_expired:
            del self._cache[key]
            return ActionResult(
                success=True,
                message=f"Cache expired: {key}",
                data={"key": key, "found": False, "value": None, "expired": True},
                duration=time.time() - start_time
            )
        
        entry.last_accessed = time.time()
        entry.access_count += 1
        
        return ActionResult(
            success=True,
            message=f"Cache hit: {key}",
            data={"key": key, "found": True, "value": entry.value},
            duration=time.time() - start_time
        )
    
    def _cache_clear(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Clear all cache entries."""
        count = len(self._cache)
        self._cache.clear()
        
        return ActionResult(
            success=True,
            message="Cache cleared",
            data={"cleared": count},
            duration=time.time() - start_time
        )
    
    def _exists(self, params: Dict[str, Any], backend: StorageBackend, start_time: float) -> ActionResult:
        """Check if key exists."""
        key = params.get("key", "")
        path = params.get("path", "data_store.json")
        
        exists = False
        
        if backend == StorageBackend.MEMORY:
            exists = key in self._memory_store
        elif backend == StorageBackend.CACHE:
            entry = self._cache.get(key)
            exists = entry is not None and not entry.is_expired
        else:
            result = self._retrieve(params, backend, start_time)
            exists = result.data.get("found", False) if result.success else False
        
        return ActionResult(
            success=True,
            message=f"{'Exists' if exists else 'Not found'}: {key}",
            data={"key": key, "exists": exists},
            duration=time.time() - start_time
        )
    
    def _batch_store(self, params: Dict[str, Any], backend: StorageBackend, start_time: float) -> ActionResult:
        """Store multiple key-value pairs."""
        items = params.get("items", {})
        path = params.get("path", "data_store.json")
        ttl = params.get("ttl")
        
        if not items:
            return ActionResult(success=False, message="No items to store", duration=time.time() - start_time)
        
        stored = 0
        for key, value in items.items():
            self._store({"key": key, "value": value, "ttl": ttl}, backend, start_time)
            stored += 1
        
        return ActionResult(
            success=True,
            message=f"Stored {stored} items",
            data={"stored": stored, "total": len(items)},
            duration=time.time() - start_time
        )
    
    def _batch_retrieve(self, params: Dict[str, Any], backend: StorageBackend, start_time: float) -> ActionResult:
        """Retrieve multiple keys."""
        keys = params.get("keys", [])
        path = params.get("path", "data_store.json")
        
        if not keys:
            return ActionResult(success=False, message="No keys provided", duration=time.time() - start_time)
        
        results = {}
        found_count = 0
        
        for key in keys:
            result = self._retrieve({"key": key}, backend, start_time)
            if result.data.get("found"):
                results[key] = result.data.get("value")
                found_count += 1
        
        return ActionResult(
            success=True,
            message=f"Found {found_count}/{len(keys)} keys",
            data={"results": results, "found": found_count, "total": len(keys)},
            duration=time.time() - start_time
        )
    
    def _evict_expired(self) -> None:
        """Evict expired cache entries."""
        expired_keys = [k for k, e in self._cache.items() if e.is_expired]
        for k in expired_keys:
            del self._cache[k]
    
    def _evict_one(self) -> None:
        """Evict one cache entry based on eviction policy."""
        if not self._cache:
            return
        
        if self._config.eviction_policy == EvictionPolicy.LRU:
            lru_key = min(self._cache.keys(), key=lambda k: self._cache[k].last_accessed)
        elif self._config.eviction_policy == EvictionPolicy.LFU:
            lfu_key = min(self._cache.keys(), key=lambda k: self._cache[k].access_count)
        else:
            lfu_key = next(iter(self._cache.keys()))
        
        del self._cache[lfu_key]
    
    def _ensure_json_store(self, path: str) -> None:
        """Ensure JSON file exists."""
        if not os.path.exists(path):
            self._write_json_file(path, {})
    
    def _read_json_file(self, path: str) -> Dict:
        """Read JSON file."""
        if not os.path.exists(path):
            return {}
        with open(path, "r") as f:
            return json.load(f)
    
    def _write_json_file(self, path: str, data: Dict) -> None:
        """Write JSON file."""
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)
    
    def _read_pickle_file(self, path: str) -> Dict:
        """Read pickle file."""
        if not os.path.exists(path):
            return {}
        with open(path, "rb") as f:
            return pickle.load(f)
    
    def _write_pickle_file(self, path: str, data: Dict) -> None:
        """Write pickle file."""
        with open(path, "wb") as f:
            pickle.dump(data, f)
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate persistence parameters."""
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return []
