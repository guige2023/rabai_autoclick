"""Idempotency Action Module.

Provides idempotency key management for
safe request replay and deduplication.
"""

import time
import threading
from typing import Any, Dict, Optional
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class IdempotencyRecord:
    """Idempotency record."""
    key: str
    result: Any
    created_at: float
    expires_at: float


class IdempotencyManager:
    """Manages idempotency."""

    def __init__(self, default_ttl: float = 86400.0):
        self.default_ttl = default_ttl
        self._records: Dict[str, IdempotencyRecord] = {}
        self._lock = threading.RLock()

    def check(self, key: str) -> tuple[bool, Optional[Any]]:
        """Check if key exists and return result."""
        with self._lock:
            record = self._records.get(key)

            if not record:
                return False, None

            if time.time() > record.expires_at:
                del self._records[key]
                return False, None

            return True, record.result

    def store(
        self,
        key: str,
        result: Any,
        ttl: Optional[float] = None
    ) -> None:
        """Store idempotency record."""
        ttl = ttl or self.default_ttl

        with self._lock:
            self._records[key] = IdempotencyRecord(
                key=key,
                result=result,
                created_at=time.time(),
                expires_at=time.time() + ttl
            )

    def delete(self, key: str) -> bool:
        """Delete idempotency record."""
        with self._lock:
            if key in self._records:
                del self._records[key]
                return True
        return False

    def cleanup(self) -> int:
        """Clean up expired records."""
        with self._lock:
            now = time.time()
            expired = [
                k for k, r in self._records.items()
                if now > r.expires_at
            ]

            for k in expired:
                del self._records[k]

            return len(expired)


class IdempotencyAction(BaseAction):
    """Action for idempotency operations."""

    def __init__(self):
        super().__init__("idempotency")
        self._manager = IdempotencyManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute idempotency action."""
        try:
            operation = params.get("operation", "check")

            if operation == "check":
                return self._check(params)
            elif operation == "store":
                return self._store(params)
            elif operation == "delete":
                return self._delete(params)
            elif operation == "cleanup":
                return self._cleanup(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _check(self, params: Dict) -> ActionResult:
        """Check idempotency key."""
        exists, result = self._manager.check(params.get("key", ""))
        return ActionResult(success=True, data={
            "exists": exists,
            "result": result
        })

    def _store(self, params: Dict) -> ActionResult:
        """Store result."""
        self._manager.store(
            params.get("key", ""),
            params.get("result"),
            params.get("ttl")
        )
        return ActionResult(success=True)

    def _delete(self, params: Dict) -> ActionResult:
        """Delete key."""
        success = self._manager.delete(params.get("key", ""))
        return ActionResult(success=success)

    def _cleanup(self, params: Dict) -> ActionResult:
        """Cleanup expired."""
        count = self._manager.cleanup()
        return ActionResult(success=True, data={"cleaned": count})
