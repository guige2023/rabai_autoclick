"""
API Snapshot Action Module.

Creates snapshots of API responses for testing,
caching, and replay scenarios.
"""

from __future__ import annotations

from typing import Any, Optional
from dataclasses import dataclass, field
import logging
import json
import hashlib
import time

logger = logging.getLogger(__name__)


@dataclass
class APISnapshot:
    """Stored API response snapshot."""
    request_key: str
    response_data: Any
    status_code: int
    headers: dict[str, str]
    timestamp: float
    size_bytes: int = 0


class APISnapshotAction:
    """
    API response snapshot management.

    Stores and retrieves API responses for testing,
    offline scenarios, and response replay.

    Example:
        snapshots = APISnapshotAction()
        snapshots.save(request, response)
        cached = snapshots.get(request)
    """

    def __init__(
        self,
        max_snapshots: int = 1000,
        ttl_seconds: Optional[float] = None,
    ) -> None:
        self.max_snapshots = max_snapshots
        self.ttl_seconds = ttl_seconds
        self._snapshots: dict[str, APISnapshot] = {}
        self._access_order: list[str] = []

    def save(
        self,
        request: dict[str, Any],
        response_data: Any,
        status_code: int = 200,
        headers: Optional[dict[str, str]] = None,
    ) -> str:
        """Save a snapshot of request/response."""
        key = self._generate_key(request)

        try:
            serialized = json.dumps(response_data, default=str)
            size = len(serialized)
        except Exception:
            size = 0

        snapshot = APISnapshot(
            request_key=key,
            response_data=response_data,
            status_code=status_code,
            headers=headers or {},
            timestamp=time.time(),
            size_bytes=size,
        )

        if key in self._snapshots:
            self._access_order.remove(key)

        self._snapshots[key] = snapshot
        self._access_order.append(key)

        self._enforce_max()

        logger.debug("Saved snapshot for key %s", key[:16])
        return key

    def get(
        self,
        request: dict[str, Any],
        validate_ttl: bool = True,
    ) -> Optional[APISnapshot]:
        """Retrieve a snapshot for request."""
        key = self._generate_key(request)

        if key not in self._snapshots:
            return None

        snapshot = self._snapshots[key]

        if validate_ttl and self.ttl_seconds:
            if time.time() - snapshot.timestamp > self.ttl_seconds:
                logger.debug("Snapshot expired for key %s", key[:16])
                return None

        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)

        return snapshot

    def has(
        self,
        request: dict[str, Any],
    ) -> bool:
        """Check if snapshot exists for request."""
        return self.get(request, validate_ttl=True) is not None

    def delete(self, request: dict[str, Any]) -> bool:
        """Delete snapshot for request."""
        key = self._generate_key(request)

        if key in self._snapshots:
            del self._snapshots[key]
            if key in self._access_order:
                self._access_order.remove(key)
            return True

        return False

    def clear(self) -> None:
        """Clear all snapshots."""
        self._snapshots.clear()
        self._access_order.clear()

    def clear_expired(self) -> int:
        """Remove expired snapshots."""
        if not self.ttl_seconds:
            return 0

        cutoff = time.time() - self.ttl_seconds
        expired = [
            key for key, snap in self._snapshots.items()
            if snap.timestamp < cutoff
        ]

        for key in expired:
            del self._snapshots[key]
            if key in self._access_order:
                self._access_order.remove(key)

        return len(expired)

    def _generate_key(self, request: dict[str, Any]) -> str:
        """Generate cache key from request."""
        parts = [
            request.get("method", "GET"),
            request.get("url", ""),
        ]

        params = request.get("params", {})
        if params:
            parts.append(json.dumps(params, sort_keys=True, default=str))

        body = request.get("body")
        if body:
            parts.append(json.dumps(body, sort_keys=True, default=str))

        key_data = "|".join(str(p) for p in parts)
        return hashlib.sha256(key_data.encode()).hexdigest()

    def _enforce_max(self) -> None:
        """Enforce maximum snapshot count."""
        while len(self._snapshots) > self.max_snapshots:
            oldest_key = self._access_order[0] if self._access_order else None
            if oldest_key:
                del self._snapshots[oldest_key]
                self._access_order.pop(0)

    @property
    def count(self) -> int:
        """Number of stored snapshots."""
        return len(self._snapshots)

    def get_stats(self) -> dict[str, Any]:
        """Get snapshot statistics."""
        total_size = sum(s.size_bytes for s in self._snapshots.values())
        return {
            "count": len(self._snapshots),
            "max": self.max_snapshots,
            "total_size_bytes": total_size,
            "ttl_seconds": self.ttl_seconds,
        }
