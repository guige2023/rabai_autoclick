"""
Data Quota Action Module.

Implements per-client data quotas and fair usage tracking.
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class QuotaLimit:
    """Defines a quota limit."""
    max_bytes: int
    window_seconds: int
    burst_bytes: Optional[int] = None


@dataclass
class QuotaUsage:
    """Tracks quota usage for a client."""
    client_id: str
    total_bytes: int = 0
    window_start: float = field(default_factory=time.time)
    request_count: int = 0
    blocked_count: int = 0


class DataQuotaAction:
    """
    Enforces data quotas per client with sliding window.

    Tracks usage and blocks requests when limits exceeded.
    """

    def __init(
        self,
        default_limit: QuotaLimit,
        per_client_limits: Optional[Dict[str, QuotaLimit]] = None,
    ) -> None:
        self.default_limit = default_limit
        self.per_client_limits = per_client_limits or {}
        self._usage: Dict[str, QuotaUsage] = {}
        self._burst_tokens: Dict[str, float] = defaultdict(float)

    def check(self, client_id: str, data_bytes: int) -> tuple[bool, Dict[str, Any]]:
        """
        Check if request is within quota.

        Args:
            client_id: Client identifier
            data_bytes: Size of data request

        Returns:
            Tuple of (allowed, info_dict)
        """
        limit = self.per_client_limits.get(client_id, self.default_limit)
        usage = self._get_or_create_usage(client_id)

        self._cleanup_window(client_id, limit)

        remaining_bytes = limit.max_bytes - usage.total_bytes
        allowed = usage.total_bytes + data_bytes <= limit.max_bytes

        info = {
            "allowed": allowed,
            "client_id": client_id,
            "limit_bytes": limit.max_bytes,
            "used_bytes": usage.total_bytes,
            "remaining_bytes": max(0, remaining_bytes),
            "window_seconds": limit.window_seconds,
            "blocked": not allowed,
        }

        return allowed, info

    def record(self, client_id: str, data_bytes: int) -> bool:
        """
        Record data usage for a client.

        Args:
            client_id: Client identifier
            data_bytes: Size of data transferred

        Returns:
            True if recorded, False if would exceed quota
        """
        allowed, _ = self.check(client_id, data_bytes)

        if allowed:
            usage = self._get_or_create_usage(client_id)
            usage.total_bytes += data_bytes
            usage.request_count += 1
            return True

        usage = self._get_or_create_usage(client_id)
        usage.blocked_count += 1
        return False

    def _get_or_create_usage(self, client_id: str) -> QuotaUsage:
        """Get or create usage record for client."""
        if client_id not in self._usage:
            self._usage[client_id] = QuotaUsage(client_id=client_id)
        return self._usage[client_id]

    def _cleanup_window(self, client_id: str, limit: QuotaLimit) -> None:
        """Reset window if expired."""
        usage = self._get_or_create_usage(client_id)
        elapsed = time.time() - usage.window_start

        if elapsed >= limit.window_seconds:
            usage.total_bytes = 0
            usage.window_start = time.time()
            usage.request_count = 0

    def get_usage(self, client_id: str) -> Dict[str, Any]:
        """Get current usage for a client."""
        limit = self.per_client_limits.get(client_id, self.default_limit)
        usage = self._get_or_create_usage(client_id)

        self._cleanup_window(client_id, limit)

        return {
            "client_id": client_id,
            "limit_bytes": limit.max_bytes,
            "used_bytes": usage.total_bytes,
            "remaining_bytes": limit.max_bytes - usage.total_bytes,
            "request_count": usage.request_count,
            "blocked_count": usage.blocked_count,
            "window_seconds": limit.window_seconds,
        }

    def reset(self, client_id: str) -> bool:
        """Reset quota for a client."""
        if client_id in self._usage:
            self._usage[client_id].total_bytes = 0
            self._usage[client_id].window_start = time.time()
            self._usage[client_id].request_count = 0
            self._usage[client_id].blocked_count = 0
            return True
        return False

    def reset_all(self) -> int:
        """Reset all quotas."""
        count = len(self._usage)
        for usage in self._usage.values():
            usage.total_bytes = 0
            usage.window_start = time.time()
            usage.request_count = 0
            usage.blocked_count = 0
        return count

    def get_all_usage(self) -> Dict[str, Dict[str, Any]]:
        """Get usage for all clients."""
        return {cid: self.get_usage(cid) for cid in self._usage}

    def set_limit(
        self,
        client_id: str,
        limit: QuotaLimit,
    ) -> None:
        """Set custom limit for a client."""
        self.per_client_limits[client_id] = limit

    def remove_limit(self, client_id: str) -> bool:
        """Remove custom limit, revert to default."""
        if client_id in self.per_client_limits:
            del self.per_client_limits[client_id]
            return True
        return False
