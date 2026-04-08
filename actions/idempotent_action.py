"""Idempotent action module for RabAI AutoClick.

Provides idempotency utilities:
- IdempotentExecutor: Execute with idempotency
- IdempotencyKey: Manage idempotency keys
- Deduplicator: Deduplicate operations
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass
import threading
import time
import uuid
import hashlib

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class IdempotencyRecord:
    """Idempotency record."""
    key: str
    result: Any
    timestamp: float
    expires_at: float


class IdempotencyStore:
    """Store for idempotency records."""

    def __init__(self, ttl: float = 3600.0):
        self.ttl = ttl
        self._records: Dict[str, IdempotencyRecord] = {}
        self._lock = threading.RLock()

    def has(self, key: str) -> bool:
        """Check if key exists and is valid."""
        with self._lock:
            if key not in self._records:
                return False
            record = self._records[key]
            if time.time() > record.expires_at:
                del self._records[key]
                return False
            return True

    def get(self, key: str) -> Optional[Any]:
        """Get result for key."""
        with self._lock:
            if not self.has(key):
                return None
            return self._records[key].result

    def set(self, key: str, result: Any, ttl: Optional[float] = None) -> None:
        """Set result for key."""
        with self._lock:
            ttl = ttl or self.ttl
            now = time.time()
            self._records[key] = IdempotencyRecord(
                key=key,
                result=result,
                timestamp=now,
                expires_at=now + ttl,
            )

    def delete(self, key: str) -> bool:
        """Delete a key."""
        with self._lock:
            if key in self._records:
                del self._records[key]
                return True
            return False

    def clear_expired(self) -> int:
        """Clear expired records."""
        with self._lock:
            now = time.time()
            expired = [k for k, v in self._records.items() if now > v.expires_at]
            for k in expired:
                del self._records[k]
            return len(expired)

    def clear_all(self) -> int:
        """Clear all records."""
        with self._lock:
            count = len(self._records)
            self._records.clear()
            return count


class IdempotentExecutor:
    """Execute functions idempotently."""

    def __init__(self, store: Optional[IdempotencyStore] = None):
        self.store = store or IdempotencyStore()

    def execute(
        self,
        key: str,
        func: Callable,
        *args,
        **kwargs,
    ) -> Dict[str, Any]:
        """Execute function idempotently."""
        if self.store.has(key):
            cached_result = self.store.get(key)
            return {
                "success": True,
                "result": cached_result,
                "cached": True,
                "key": key,
            }

        result = func(*args, **kwargs)
        self.store.set(key, result)

        return {
            "success": True,
            "result": result,
            "cached": False,
            "key": key,
        }

    def clear(self, key: str) -> bool:
        """Clear idempotency for key."""
        return self.store.delete(key)


class Deduplicator:
    """Deduplicate items."""

    def __init__(self):
        self._seen: Set[str] = set()
        self._lock = threading.Lock()

    def is_duplicate(self, item: Any) -> bool:
        """Check if item is duplicate."""
        key = self._make_key(item)
        with self._lock:
            if key in self._seen:
                return True
            self._seen.add(key)
            return False

    def add(self, item: Any) -> bool:
        """Add item and return True if new."""
        key = self._make_key(item)
        with self._lock:
            if key in self._seen:
                return False
            self._seen.add(key)
            return True

    def clear(self) -> None:
        """Clear deduplicator."""
        with self._lock:
            self._seen.clear()

    def _make_key(self, item: Any) -> str:
        """Make key from item."""
        if isinstance(item, str):
            return item
        if isinstance(item, (int, float)):
            return str(item)
        try:
            import json
            return hashlib.md5(json.dumps(item, sort_keys=True).encode()).hexdigest()
        except Exception:
            return str(hash(str(item)))


class IdempotentAction(BaseAction):
    """Idempotent execution action."""
    action_type = "idempotent"
    display_name = "幂等操作"
    description = "幂等性保障"

    def __init__(self):
        super().__init__()
        self._store = IdempotencyStore()
        self._executor = IdempotentExecutor(self._store)
        self._deduplicator = Deduplicator()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "execute")

            if operation == "execute":
                return self._execute(params)
            elif operation == "has":
                return self._has(params)
            elif operation == "clear":
                return self._clear(params)
            elif operation == "clear_expired":
                return self._clear_expired(params)
            elif operation == "duplicate":
                return self._duplicate(params)
            elif operation == "deduplicate":
                return self._deduplicate(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Idempotent error: {str(e)}")

    def _execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute idempotently."""
        key = params.get("key")
        ttl = params.get("ttl")

        if not key:
            key = str(uuid.uuid4())

        def dummy_func():
            return {"computed": True, "timestamp": time.time()}

        result = self._executor.execute(key, dummy_func, ttl=ttl)

        return ActionResult(
            success=result["success"],
            message="Executed" if not result["cached"] else "Returned cached",
            data={
                "key": key,
                "cached": result["cached"],
                "result": result["result"],
            },
        )

    def _has(self, params: Dict[str, Any]) -> ActionResult:
        """Check if key exists."""
        key = params.get("key")

        if not key:
            return ActionResult(success=False, message="key is required")

        has = self._store.has(key)

        return ActionResult(success=True, message=f"Has key: {has}", data={"has": has, "key": key})

    def _clear(self, params: Dict[str, Any]) -> ActionResult:
        """Clear a key."""
        key = params.get("key")

        if not key:
            return ActionResult(success=False, message="key is required")

        success = self._executor.clear(key)

        return ActionResult(success=success, message="Cleared" if success else "Key not found")

    def _clear_expired(self, params: Dict[str, Any]) -> ActionResult:
        """Clear expired records."""
        count = self._store.clear_expired()

        return ActionResult(success=True, message=f"Cleared {count} expired records")

    def _duplicate(self, params: Dict[str, Any]) -> ActionResult:
        """Check for duplicate."""
        item = params.get("item")

        if item is None:
            return ActionResult(success=False, message="item is required")

        is_dup = self._deduplicator.is_duplicate(item)

        return ActionResult(success=True, message="Duplicate" if is_dup else "New item", data={"is_duplicate": is_dup})

    def _deduplicate(self, params: Dict[str, Any]) -> ActionResult:
        """Deduplicate items."""
        items = params.get("items", [])

        unique = []
        duplicates = []

        for item in items:
            if self._deduplicator.add(item):
                unique.append(item)
            else:
                duplicates.append(item)

        return ActionResult(
            success=True,
            message=f"Unique: {len(unique)}, Duplicates: {len(duplicates)}",
            data={"unique": unique, "duplicates": duplicates, "unique_count": len(unique), "duplicate_count": len(duplicates)},
        )
