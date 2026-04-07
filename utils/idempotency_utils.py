"""
Idempotency key utilities for safe request retries.

Provides idempotency key generation, storage, validation,
and automatic cleanup for safe API retries.
"""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class IdempotencyResult(Enum):
    NEW_REQUEST = auto()
    CACHED_RESPONSE = auto()
    STALE_REQUEST = auto()
    CONFLICT = auto()


@dataclass
class IdempotencyRecord:
    """Record of an idempotent operation."""
    key: str
    status: str
    response: Optional[dict[str, Any]] = None
    created_at: float = field(default_factory=time.time)
    expires_at: float = 0.0
    request_hash: Optional[str] = None
    hit_count: int = 0


@dataclass
class IdempotencyConfig:
    """Configuration for idempotency handling."""
    ttl_seconds: float = 86400
    stale_threshold_seconds: float = 3600
    max_request_age_seconds: float = 86400 * 7
    storage_type: str = "memory"  # memory, redis


class IdempotencyStore:
    """In-memory idempotency key storage."""

    def __init__(self, config: Optional[IdempotencyConfig] = None) -> None:
        self.config = config or IdempotencyConfig()
        self._records: dict[str, IdempotencyRecord] = {}

    def get(self, key: str) -> Optional[IdempotencyRecord]:
        """Get a record by idempotency key."""
        return self._records.get(key)

    def set(self, record: IdempotencyRecord) -> None:
        """Store an idempotency record."""
        self._records[record.key] = record

    def delete(self, key: str) -> None:
        """Delete an idempotency record."""
        self._records.pop(key, None)

    def cleanup_expired(self) -> int:
        """Remove expired records. Returns count of removed records."""
        now = time.time()
        expired = [
            key for key, record in self._records.items()
            if record.expires_at > 0 and record.expires_at < now
        ]
        for key in expired:
            del self._records[key]
        return len(expired)


class IdempotencyManager:
    """Manages idempotency for request processing."""

    def __init__(self, store: Optional[IdempotencyStore] = None) -> None:
        self.store = store or IdempotencyStore()

    @staticmethod
    def generate_key(
        method: str,
        path: str,
        body: Optional[dict[str, Any]] = None,
        user_id: Optional[str] = None,
    ) -> str:
        """Generate an idempotency key from request data."""
        import json
        parts = [method.upper(), path]
        if user_id:
            parts.append(user_id)
        if body:
            body_str = json.dumps(body, sort_keys=True)
            parts.append(body_str)
        raw = "|".join(parts)
        return hashlib.sha256(raw.encode()).hexdigest()[:32]

    @staticmethod
    def generate_request_hash(
        method: str,
        path: str,
        body: Optional[dict[str, Any]] = None,
    ) -> str:
        """Generate a hash of the request for conflict detection."""
        import json
        data = {
            "method": method,
            "path": path,
            "body": body,
        }
        raw = json.dumps(data, sort_keys=True)
        return hashlib.sha256(raw.encode()).hexdigest()

    def check(
        self,
        key: str,
        request_hash: Optional[str] = None,
    ) -> tuple[IdempotencyResult, Optional[dict[str, Any]]]:
        """Check idempotency status and return cached response if available."""
        record = self.store.get(key)
        now = time.time()

        if not record:
            return IdempotencyResult.NEW_REQUEST, None

        if record.expires_at > 0 and record.expires_at < now:
            return IdempotencyResult.STALE_REQUEST, None

        if request_hash and record.request_hash and record.request_hash != request_hash:
            return IdempotencyResult.CONFLICT, None

        if record.response:
            record.hit_count += 1
            return IdempotencyResult.CACHED_RESPONSE, record.response

        return IdempotencyResult.NEW_REQUEST, None

    def store_response(
        self,
        key: str,
        response: dict[str, Any],
        request_hash: Optional[str] = None,
    ) -> None:
        """Store a response for an idempotency key."""
        now = time.time()
        record = IdempotencyRecord(
            key=key,
            status="completed",
            response=response,
            created_at=now,
            expires_at=now + self.store.config.ttl_seconds,
            request_hash=request_hash,
        )
        self.store.set(record)

    def begin_request(self, key: str, request_hash: Optional[str] = None) -> None:
        """Mark an idempotency key as being processed."""
        now = time.time()
        record = IdempotencyRecord(
            key=key,
            status="processing",
            created_at=now,
            expires_at=now + self.store.config.max_request_age_seconds,
            request_hash=request_hash,
        )
        self.store.set(record)

    def cleanup(self) -> int:
        """Clean up expired idempotency records."""
        return self.store.cleanup_expired()


def idempotent(
    key_func: Callable[..., str],
    ttl_seconds: float = 86400,
) -> Callable:
    """Decorator to make a function idempotent."""
    manager = IdempotencyManager()

    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            key = key_func(*args, **kwargs)
            result, cached = manager.check(key)
            if result == IdempotencyResult.CACHED_RESPONSE:
                return cached
            if result == IdempotencyResult.CONFLICT:
                raise ValueError("Idempotency conflict: request parameters changed")
            manager.begin_request(key)
            response = func(*args, **kwargs)
            manager.store_response(key, {"result": response} if not isinstance(response, dict) else response)
            return response
        return wrapper
    return decorator
