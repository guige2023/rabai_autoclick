"""Idempotency key management for safe automation retries.

This module provides idempotency guarantees for automation actions,
ensuring that retrying a workflow does not cause duplicate side effects.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import threading


class IdempotencyStatus(Enum):
    """Status of an idempotency key."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"


@dataclass
class IdempotencyRecord:
    """Record of an idempotency key execution."""
    key: str
    status: IdempotencyStatus
    payload_hash: str
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None
    retry_count: int = 0
    max_retries: int = 3


class IdempotencyStore:
    """In-memory idempotency key store with TTL support."""

    def __init__(self, default_ttl_seconds: int = 3600):
        self._store: Dict[str, IdempotencyRecord] = {}
        self._lock = threading.RLock()
        self._default_ttl = default_ttl_seconds
        self._cleanup_interval = 300
        self._last_cleanup = time.time()

    def _cleanup_expired(self) -> None:
        """Remove expired records."""
        now = time.time()
        if now - self._last_cleanup < self._cleanup_interval:
            return
        self._last_cleanup = now
        expired = [
            k for k, v in self._store.items()
            if v.expires_at and v.expires_at < now
        ]
        for k in expired:
            del self._store[k]

    def _generate_key(self, namespace: str, identifier: str) -> str:
        """Generate a deterministic idempotency key."""
        combined = f"{namespace}:{identifier}"
        return hashlib.sha256(combined.encode()).hexdigest()[:32]

    def _compute_payload_hash(self, payload: Dict[str, Any]) -> str:
        """Compute hash of payload for comparison."""
        serialized = json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode()).hexdigest()

    def begin(
        self,
        namespace: str,
        identifier: str,
        payload: Dict[str, Any],
        ttl_seconds: Optional[int] = None,
    ) -> IdempotencyRecord:
        """Begin a new idempotent operation.

        Returns existing record if key exists and payload matches,
        otherwise creates a new pending record.
        """
        self._cleanup_expired()

        key = self._generate_key(namespace, identifier)
        payload_hash = self._compute_payload_hash(payload)
        ttl = ttl_seconds or self._default_ttl
        expires_at = time.time() + ttl

        with self._lock:
            existing = self._store.get(key)

            if existing:
                if existing.payload_hash != payload_hash:
                    raise ValueError(
                        f"Idempotency key '{key}' already exists with different payload"
                    )
                if existing.status == IdempotencyStatus.PROCESSING:
                    if existing.retry_count >= existing.max_retries:
                        raise RuntimeError(
                            f"Idempotency key '{key}' exceeded max retries"
                        )
                    existing.status = IdempotencyStatus.PENDING
                    existing.retry_count += 1
                    existing.updated_at = time.time()
                    return existing
                if existing.status == IdempotencyStatus.COMPLETED:
                    return existing
                if existing.status == IdempotencyStatus.FAILED:
                    if existing.retry_count >= existing.max_retries:
                        return existing
                    existing.status = IdempotencyStatus.PENDING
                    existing.retry_count += 1
                    existing.updated_at = time.time()
                    return existing

            record = IdempotencyRecord(
                key=key,
                status=IdempotencyStatus.PENDING,
                payload_hash=payload_hash,
                expires_at=expires_at,
            )
            self._store[key] = record
            return record

    def start_processing(self, key: str) -> IdempotencyRecord:
        """Mark an idempotency record as processing."""
        with self._lock:
            if key not in self._store:
                raise KeyError(f"Idempotency key '{key}' not found")
            record = self._store[key]
            record.status = IdempotencyStatus.PROCESSING
            record.updated_at = time.time()
            return record

    def complete(self, key: str, result: Dict[str, Any]) -> IdempotencyRecord:
        """Mark an idempotency record as completed with result."""
        with self._lock:
            if key not in self._store:
                raise KeyError(f"Idempotency key '{key}' not found")
            record = self._store[key]
            record.status = IdempotencyStatus.COMPLETED
            record.result = result
            record.updated_at = time.time()
            return record

    def fail(self, key: str, error: str) -> IdempotencyRecord:
        """Mark an idempotency record as failed."""
        with self._lock:
            if key not in self._store:
                raise KeyError(f"Idempotency key '{key}' not found")
            record = self._store[key]
            record.status = IdempotencyStatus.FAILED
            record.error = error
            record.updated_at = time.time()
            return record

    def get(self, namespace: str, identifier: str) -> Optional[IdempotencyRecord]:
        """Get an idempotency record by namespace and identifier."""
        key = self._generate_key(namespace, identifier)
        with self._lock:
            return self._store.get(key)

    def get_by_key(self, key: str) -> Optional[IdempotencyRecord]:
        """Get an idempotency record by key."""
        with self._lock:
            return self._store.get(key)

    def clear(self, key: Optional[str] = None) -> None:
        """Clear idempotency store or specific key."""
        with self._lock:
            if key:
                self._store.pop(key, None)
            else:
                self._store.clear()


class IdempotencyAction:
    """Action that wraps any automation with idempotency guarantees."""

    def __init__(
        self,
        store: Optional[IdempotencyStore] = None,
        default_ttl: int = 3600,
    ):
        self._store = store or IdempotencyStore(default_ttl)

    def execute(
        self,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute an automation action with idempotency protection.

        Required params:
            namespace: str - Logical namespace for the operation
            identifier: str - Unique identifier for the operation
            payload: dict - Operation payload
            operation: callable - The operation to execute

        Optional params:
            ttl_seconds: int - Time-to-live for idempotency key
            max_retries: int - Maximum retry attempts
        """
        namespace = params.get("namespace")
        identifier = params.get("identifier")
        payload = params.get("payload", {})
        operation = params.get("operation")
        ttl_seconds = params.get("ttl_seconds")
        max_retries = params.get("max_retries", 3)

        if not namespace or not identifier:
            raise ValueError("namespace and identifier are required")
        if not callable(operation):
            raise ValueError("operation must be a callable")

        record = self._store.begin(namespace, identifier, payload, ttl_seconds)

        if record.status == IdempotencyStatus.COMPLETED and record.result:
            return {
                "idempotent": True,
                "cached": True,
                "key": record.key,
                "result": record.result,
            }

        self._store.start_processing(record.key)

        try:
            result = operation(context=context, params=payload)
            self._store.complete(record.key, result)
            return {
                "idempotent": True,
                "cached": False,
                "key": record.key,
                "result": result,
            }
        except Exception as e:
            self._store.fail(record.key, str(e))
            return {
                "idempotent": True,
                "cached": False,
                "key": record.key,
                "error": str(e),
                "retry_count": record.retry_count,
                "max_retries": max_retries,
            }

    def get_record(self, namespace: str, identifier: str) -> Optional[Dict[str, Any]]:
        """Get the status of an idempotent operation."""
        record = self._store.get(namespace, identifier)
        if not record:
            return None
        return {
            "key": record.key,
            "status": record.status.value,
            "created_at": datetime.fromtimestamp(record.created_at).isoformat(),
            "updated_at": datetime.fromtimestamp(record.updated_at).isoformat(),
            "retry_count": record.retry_count,
            "result": record.result,
            "error": record.error,
        }
