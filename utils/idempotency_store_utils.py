"""
Idempotency key store utilities.

Provides distributed idempotency checking with TTL support.
"""

from __future__ import annotations

import hashlib
import json
import time
import threading
from typing import Any


class IdempotencyStore:
    """
    In-memory idempotency key store with TTL.

    Tracks processed idempotency keys to prevent duplicate operations.
    """

    def __init__(self, default_ttl_seconds: float = 3600.0):
        self.default_ttl = default_ttl_seconds
        self._lock = threading.Lock()
        self._store: dict[str, tuple[str, float]] = {}

    def _prune_expired(self) -> None:
        now = time.time()
        self._store = {
            k: v for k, v in self._store.items() if v[1] > now
        }

    def check_and_set(
        self,
        key: str,
        ttl: float | None = None,
    ) -> bool:
        """
        Check if key exists and set if not.

        Args:
            key: Idempotency key
            ttl: TTL in seconds (uses default if None)

        Returns:
            True if key was newly set, False if already existed
        """
        with self._lock:
            self._prune_expired()
            if key in self._store:
                return False
            ttl_val = ttl if ttl is not None else self.default_ttl
            self._store[key] = ("processing", time.time() + ttl_val)
            return True

    def mark_complete(
        self,
        key: str,
        result: Any = None,
        ttl: float | None = None,
    ) -> None:
        """
        Mark key as successfully processed.

        Args:
            key: Idempotency key
            result: Optional result to store
            ttl: TTL for result (uses default if None)
        """
        with self._lock:
            ttl_val = ttl if ttl is not None else self.default_ttl
            self._store[key] = (json.dumps(result) if result is not None else "done", time.time() + ttl_val)

    def mark_failed(self, key: str) -> None:
        """
        Mark key as failed so it can be retried.

        Args:
            key: Idempotency key
        """
        with self._lock:
            if key in self._store:
                del self._store[key]

    def get_result(self, key: str) -> Any | None:
        """
        Get stored result for a completed key.

        Args:
            key: Idempotency key

        Returns:
            Stored result or None
        """
        with self._lock:
            self._prune_expired()
            if key in self._store:
                value, _ = self._store[key]
                if value == "done":
                    return None
                try:
                    return json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    return value
            return None

    def is_processing(self, key: str) -> bool:
        """Check if key is currently being processed."""
        with self._lock:
            if key in self._store:
                value, _ = self._store[key]
                return value == "processing"
            return False

    def clear(self) -> None:
        """Clear all keys."""
        with self._lock:
            self._store.clear()


def generate_idempotency_key(
    method: str,
    url: str,
    body: str | None = None,
) -> str:
    """
    Generate idempotency key from request details.

    Args:
        method: HTTP method
        url: Request URL
        body: Request body

    Returns:
        Deterministic hash-based key
    """
    parts = f"{method}:{url}:{body or ''}"
    return hashlib.sha256(parts.encode()).hexdigest()[:32]


class RedisIdempotencyStore:
    """Redis-backed idempotency store for distributed environments."""

    def __init__(
        self,
        redis_client: Any,
        prefix: str = "idempotency:",
        default_ttl: float = 3600.0,
    ):
        self.redis = redis_client
        self.prefix = prefix
        self.default_ttl = default_ttl

    def _make_key(self, key: str) -> str:
        return f"{self.prefix}{key}"

    def check_and_set(
        self,
        key: str,
        ttl: float | None = None,
    ) -> bool:
        """Atomic check-and-set using Redis SET NX."""
        ttl_val = int(ttl if ttl is not None else self.default_ttl)
        result = self.redis.set(
            self._make_key(key),
            "processing",
            ex=ttl_val,
            nx=True,
        )
        return result is True

    def mark_complete(
        self,
        key: str,
        result: Any = None,
        ttl: float | None = None,
    ) -> None:
        ttl_val = int(ttl if ttl is not None else self.default_ttl)
        value = json.dumps(result) if result is not None else "done"
        self.redis.set(self._make_key(key), value, ex=ttl_val)

    def get_result(self, key: str) -> Any | None:
        value = self.redis.get(self._make_key(key))
        if value is None:
            return None
        if value == b"done" or value == "done":
            return None
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value

    def mark_failed(self, key: str) -> None:
        self.redis.delete(self._make_key(key))

    def is_processing(self, key: str) -> bool:
        value = self.redis.get(self._make_key(key))
        return value == b"processing" or value == "processing"
