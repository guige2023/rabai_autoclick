"""Idempotency Action Module.

Provides idempotency key management to ensure safe operation retries
and prevent duplicate processing.
"""
from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class IdempotencyStatus(Enum):
    """Idempotency record status."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"


@dataclass
class IdempotencyRecord:
    """Idempotency record."""
    key: str
    status: IdempotencyStatus
    result: Optional[Any] = None
    error: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    expires_at: float = 0.0
    attempts: int = 0
    last_attempt: float = 0.0


class IdempotencyStore:
    """In-memory idempotency store."""

    def __init__(self, default_ttl_seconds: float = 86400):
        self._records: Dict[str, IdempotencyRecord] = {}
        self._default_ttl = default_ttl_seconds

    def create(self, key: str, ttl_seconds: Optional[float] = None) -> Optional[IdempotencyRecord]:
        """Create idempotency record."""
        if key in self._records:
            return None

        ttl = ttl_seconds or self._default_ttl
        record = IdempotencyRecord(
            key=key,
            status=IdempotencyStatus.PENDING,
            expires_at=time.time() + ttl
        )
        self._records[key] = record
        return record

    def get(self, key: str) -> Optional[IdempotencyRecord]:
        """Get idempotency record."""
        record = self._records.get(key)
        if record and time.time() > record.expires_at:
            record.status = IdempotencyStatus.EXPIRED
        return record

    def start_processing(self, key: str) -> Optional[IdempotencyRecord]:
        """Mark as processing."""
        record = self._records.get(key)
        if record and record.status == IdempotencyStatus.PENDING:
            record.status = IdempotencyStatus.PROCESSING
            record.attempts += 1
            record.last_attempt = time.time()
            return record
        return None

    def complete(self, key: str, result: Any) -> Optional[IdempotencyRecord]:
        """Mark as completed."""
        record = self._records.get(key)
        if record:
            record.status = IdempotencyStatus.COMPLETED
            record.result = result
        return record

    def fail(self, key: str, error: str) -> Optional[IdempotencyRecord]:
        """Mark as failed."""
        record = self._records.get(key)
        if record:
            record.status = IdempotencyStatus.FAILED
            record.error = error
        return record

    def delete(self, key: str) -> bool:
        """Delete record."""
        if key in self._records:
            del self._records[key]
            return True
        return False

    def cleanup_expired(self) -> int:
        """Remove expired records."""
        now = time.time()
        expired = [
            k for k, r in self._records.items()
            if now > r.expires_at
        ]
        for k in expired:
            del self._records[k]
        return len(expired)


_global_store = IdempotencyStore()


class IdempotencyAction:
    """Idempotency action.

    Example:
        action = IdempotencyAction()

        result = action.get_or_create("order-123")
        if result.is_new:
            process_order()
            action.complete("order-123", {"order_id": "123"})
    """

    def __init__(self, store: Optional[IdempotencyStore] = None):
        self._store = store or _global_store

    def get_or_create(self, key: str,
                     ttl_seconds: Optional[float] = None) -> Dict[str, Any]:
        """Get existing record or create new one."""
        existing = self._store.get(key)

        if existing:
            if existing.status == IdempotencyStatus.COMPLETED:
                return {
                    "success": True,
                    "is_new": False,
                    "key": key,
                    "status": existing.status.value,
                    "result": existing.result,
                    "message": "Operation already completed"
                }

            if existing.status == IdempotencyStatus.PROCESSING:
                return {
                    "success": True,
                    "is_new": False,
                    "key": key,
                    "status": existing.status.value,
                    "attempts": existing.attempts,
                    "message": "Operation in progress"
                }

            if existing.status == IdempotencyStatus.FAILED:
                return {
                    "success": True,
                    "is_new": False,
                    "key": key,
                    "status": existing.status.value,
                    "error": existing.error,
                    "message": "Operation previously failed"
                }

        record = self._store.create(key, ttl_seconds)
        if record:
            return {
                "success": True,
                "is_new": True,
                "key": key,
                "status": record.status.value,
                "message": "New idempotency key created"
            }

        return {
            "success": False,
            "message": "Failed to create idempotency key"
        }

    def start_processing(self, key: str) -> Dict[str, Any]:
        """Start processing with key."""
        record = self._store.start_processing(key)
        if record:
            return {
                "success": True,
                "key": key,
                "status": record.status.value,
                "attempts": record.attempts,
                "message": "Processing started"
            }
        return {
            "success": False,
            "message": "Cannot start processing (not pending or not found)"
        }

    def complete(self, key: str, result: Any) -> Dict[str, Any]:
        """Complete operation with key."""
        record = self._store.complete(key, result)
        if record:
            return {
                "success": True,
                "key": key,
                "status": record.status.value,
                "message": "Operation completed"
            }
        return {
            "success": False,
            "message": "Key not found"
        }

    def fail(self, key: str, error: str) -> Dict[str, Any]:
        """Fail operation with key."""
        record = self._store.fail(key, error)
        if record:
            return {
                "success": True,
                "key": key,
                "status": record.status.value,
                "error": record.error,
                "message": "Operation marked as failed"
            }
        return {
            "success": False,
            "message": "Key not found"
        }

    def get(self, key: str) -> Dict[str, Any]:
        """Get idempotency record."""
        record = self._store.get(key)
        if record:
            return {
                "success": True,
                "key": key,
                "status": record.status.value,
                "result": record.result,
                "error": record.error,
                "created_at": record.created_at,
                "expires_at": record.expires_at,
                "attempts": record.attempts
            }
        return {
            "success": False,
            "message": "Key not found"
        }

    def delete(self, key: str) -> Dict[str, Any]:
        """Delete idempotency record."""
        if self._store.delete(key):
            return {"success": True, "message": "Deleted"}
        return {"success": False, "message": "Key not found"}

    def cleanup(self) -> Dict[str, Any]:
        """Cleanup expired records."""
        count = self._store.cleanup_expired()
        return {
            "success": True,
            "expired_removed": count,
            "message": f"Removed {count} expired records"
        }


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute idempotency action."""
    operation = params.get("operation", "")
    action = IdempotencyAction()

    try:
        if operation == "get_or_create":
            key = params.get("key", "")
            if not key:
                return {"success": False, "message": "key required"}
            return action.get_or_create(key, params.get("ttl_seconds"))

        elif operation == "start":
            key = params.get("key", "")
            if not key:
                return {"success": False, "message": "key required"}
            return action.start_processing(key)

        elif operation == "complete":
            key = params.get("key", "")
            result = params.get("result")
            if not key:
                return {"success": False, "message": "key required"}
            return action.complete(key, result)

        elif operation == "fail":
            key = params.get("key", "")
            error = params.get("error", "Unknown error")
            if not key:
                return {"success": False, "message": "key required"}
            return action.fail(key, error)

        elif operation == "get":
            key = params.get("key", "")
            if not key:
                return {"success": False, "message": "key required"}
            return action.get(key)

        elif operation == "delete":
            key = params.get("key", "")
            if not key:
                return {"success": False, "message": "key required"}
            return action.delete(key)

        elif operation == "cleanup":
            return action.cleanup()

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"Idempotency error: {str(e)}"}
