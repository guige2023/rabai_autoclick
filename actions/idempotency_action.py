"""Idempotency action module for RabAI AutoClick.

Provides idempotency key management to ensure operations
can be safely retried without duplicate execution.
"""

import sys
import os
import json
import time
import hashlib
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class IdempotencyStatus(Enum):
    """Status of an idempotency record."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"


@dataclass
class IdempotencyRecord:
    """Record for idempotency tracking."""
    key: str
    status: IdempotencyStatus
    created_at: float
    expires_at: float
    result: Optional[Any] = None
    error: Optional[str] = None
    request_body_hash: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class IdempotencyStore:
    """Store for idempotency records."""
    
    def __init__(self, persistence_path: Optional[str] = None):
        self._records: Dict[str, IdempotencyRecord] = {}
        self._default_ttl_seconds: float = 86400.0  # 24 hours
        self._persistence_path = persistence_path
        self._load()
    
    def _load(self) -> None:
        """Load records from persistence."""
        if self._persistence_path and os.path.exists(self._persistence_path):
            try:
                with open(self._persistence_path, 'r') as f:
                    data = json.load(f)
                    for key, rec_data in data.get("records", {}).items():
                        rec_data.pop('status', None)
                        status = IdempotencyStatus(rec_data.get("_status_enum", "pending"))
                        rec = IdempotencyRecord(key=key, status=status, **rec_data)
                        self._records[key] = rec
            except (json.JSONDecodeError, TypeError, KeyError):
                pass
    
    def _persist(self) -> None:
        """Persist records."""
        if self._persistence_path:
            try:
                data = {
                    "records": {
                        k: {
                            "status": v.status.value,
                            "created_at": v.created_at,
                            "expires_at": v.expires_at,
                            "result": v.result,
                            "error": v.error,
                            "request_body_hash": v.request_body_hash,
                            "metadata": v.metadata
                        }
                        for k, v in self._records.items()
                    }
                }
                with open(self._persistence_path, 'w') as f:
                    json.dump(data, f, indent=2, default=str)
            except OSError:
                pass
    
    def _compute_hash(self, data: Any) -> str:
        """Compute hash of request data."""
        data_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(data_str.encode()).hexdigest()[:16]
    
    def check_and_set(
        self,
        key: str,
        request_body: Optional[Any] = None,
        ttl_seconds: Optional[float] = None
    ) -> tuple[bool, Optional[IdempotencyRecord]]:
        """Check if key exists and set if not.
        
        Returns: (is_new, existing_record)
        """
        now = time.time()
        ttl = ttl_seconds or self._default_ttl_seconds
        
        # Check existing
        if key in self._records:
            record = self._records[key]
            # Check expiration
            if record.expires_at < now:
                record.status = IdempotencyStatus.EXPIRED
                del self._records[key]
            else:
                return False, record
        
        # Create new record
        record = IdempotencyRecord(
            key=key,
            status=IdempotencyStatus.PENDING,
            created_at=now,
            expires_at=now + ttl,
            request_body_hash=self._compute_hash(request_body) if request_body else None
        )
        self._records[key] = record
        self._persist()
        return True, record
    
    def mark_processing(self, key: str) -> bool:
        """Mark a key as currently being processed."""
        if key not in self._records:
            return False
        self._records[key].status = IdempotencyStatus.PROCESSING
        self._persist()
        return True
    
    def mark_completed(self, key: str, result: Any) -> bool:
        """Mark a key as successfully completed."""
        if key not in self._records:
            return False
        self._records[key].status = IdempotencyStatus.COMPLETED
        self._records[key].result = result
        self._persist()
        return True
    
    def mark_failed(self, key: str, error: str) -> bool:
        """Mark a key as failed."""
        if key not in self._records:
            return False
        self._records[key].status = IdempotencyStatus.FAILED
        self._records[key].error = error
        self._persist()
        return True
    
    def get(self, key: str) -> Optional[IdempotencyRecord]:
        """Get a record by key."""
        return self._records.get(key)
    
    def delete(self, key: str) -> bool:
        """Delete a record."""
        if key in self._records:
            del self._records[key]
            self._persist()
            return True
        return False
    
    def cleanup_expired(self) -> int:
        """Remove expired records."""
        now = time.time()
        expired = [
            k for k, v in self._records.items()
            if v.expires_at < now
        ]
        for k in expired:
            del self._records[k]
        if expired:
            self._persist()
        return len(expired)
    
    def set_default_ttl(self, ttl_seconds: float) -> None:
        """Set default TTL for new records."""
        self._default_ttl_seconds = ttl_seconds
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics."""
        now = time.time()
        by_status = {}
        for rec in self._records.values():
            status_name = rec.status.value
            by_status[status_name] = by_status.get(status_name, 0) + 1
        
        return {
            "total_records": len(self._records),
            "by_status": by_status,
            "default_ttl_seconds": self._default_ttl_seconds
        }


class IdempotencyAction(BaseAction):
    """Idempotency management for safe retries.
    
    Ensures operations can be safely retried without
    duplicate execution using idempotency keys.
    """
    action_type = "idempotency"
    display_name = "幂等性"
    description = "幂等性管理，确保操作可以安全重试"
    
    def __init__(self):
        super().__init__()
        self._store = IdempotencyStore()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute idempotency operation."""
        operation = params.get("operation", "")
        
        try:
            if operation == "check":
                return self._check(params)
            elif operation == "processing":
                return self._mark_processing(params)
            elif operation == "complete":
                return self._mark_completed(params)
            elif operation == "fail":
                return self._mark_failed(params)
            elif operation == "get":
                return self._get(params)
            elif operation == "delete":
                return self._delete(params)
            elif operation == "cleanup":
                return self._cleanup(params)
            elif operation == "get_stats":
                return self._get_stats(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _check(self, params: Dict[str, Any]) -> ActionResult:
        """Check and set idempotency key."""
        key = params.get("key", "")
        if not key:
            return ActionResult(success=False, message="key is required")
        
        is_new, record = self._store.check_and_set(
            key,
            params.get("request_body"),
            params.get("ttl_seconds")
        )
        
        return ActionResult(
            success=True,
            message="New key" if is_new else f"Existing: {record.status.value}",
            data={
                "is_new": is_new,
                "key": key,
                "status": record.status.value if record else None,
                "result": record.result if record and record.status == IdempotencyStatus.COMPLETED else None
            }
        )
    
    def _mark_processing(self, params: Dict[str, Any]) -> ActionResult:
        """Mark key as processing."""
        key = params.get("key", "")
        marked = self._store.mark_processing(key)
        return ActionResult(success=marked, message="Marked processing" if marked else "Key not found")
    
    def _mark_completed(self, params: Dict[str, Any]) -> ActionResult:
        """Mark key as completed."""
        key = params.get("key", "")
        result = params.get("result")
        marked = self._store.mark_completed(key, result)
        return ActionResult(success=marked, message="Marked completed" if marked else "Key not found")
    
    def _mark_failed(self, params: Dict[str, Any]) -> ActionResult:
        """Mark key as failed."""
        key = params.get("key", "")
        error = params.get("error", "Unknown error")
        marked = self._store.mark_failed(key, error)
        return ActionResult(success=marked, message="Marked failed" if marked else "Key not found")
    
    def _get(self, params: Dict[str, Any]) -> ActionResult:
        """Get idempotency record."""
        key = params.get("key", "")
        record = self._store.get(key)
        if not record:
            return ActionResult(success=False, message="Key not found")
        return ActionResult(success=True, message=f"Status: {record.status.value}",
                         data={"status": record.status.value, "result": record.result, "error": record.error})
    
    def _delete(self, params: Dict[str, Any]) -> ActionResult:
        """Delete idempotency record."""
        key = params.get("key", "")
        deleted = self._store.delete(key)
        return ActionResult(success=deleted, message="Deleted" if deleted else "Not found")
    
    def _cleanup(self, params: Dict[str, Any]) -> ActionResult:
        """Cleanup expired records."""
        count = self._store.cleanup_expired()
        return ActionResult(success=True, message=f"Removed {count} expired records")
    
    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get stats."""
        stats = self._store.get_stats()
        return ActionResult(success=True, message="Stats retrieved", data=stats)
