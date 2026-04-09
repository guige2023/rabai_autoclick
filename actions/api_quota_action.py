"""API quota management and tracking.

This module provides API quota management:
- Per-client quota tracking
- Quota allocation and consumption
- Over-quota handling
- Quota reset and rollover

Example:
    >>> from actions.api_quota_action import QuotaManager
    >>> manager = QuotaManager()
    >>> if manager.check_quota("client_123", "api_calls"):
    ...     make_api_call()
"""

from __future__ import annotations

import time
import threading
import logging
from typing import Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class QuotaLimit:
    """Definition of a quota limit."""
    name: str
    limit: int
    window: float
    scope: str = "global"
    block_on_exceeded: bool = True


@dataclass
class QuotaUsage:
    """Current quota usage for a client."""
    client_id: str
    quota_name: str
    consumed: int = 0
    window_start: float = field(default_factory=time.time)
    blocked_until: Optional[float] = None


@dataclass
class QuotaCheckResult:
    """Result of a quota check."""
    allowed: bool
    remaining: int
    reset_at: float
    retry_after: Optional[float] = None


class QuotaManager:
    """Manage API quotas for clients.

    Attributes:
        default_window: Default quota window in seconds.
    """

    def __init__(self, default_window: float = 60.0) -> None:
        self.default_window = default_window
        self._quotas: dict[str, QuotaLimit] = {}
        self._usage: dict[str, dict[str, QuotaUsage]] = defaultdict(dict)
        self._lock = threading.RLock()
        logger.info("QuotaManager initialized")

    def register_quota(
        self,
        name: str,
        limit: int,
        window: Optional[float] = None,
        scope: str = "global",
        block_on_exceeded: bool = True,
    ) -> None:
        """Register a quota limit.

        Args:
            name: Quota name.
            limit: Maximum allowed in window.
            window: Window size in seconds.
            scope: Scope of quota (global, client, endpoint).
            block_on_exceeded: Whether to block when exceeded.
        """
        quota = QuotaLimit(
            name=name,
            limit=limit,
            window=window or self.default_window,
            scope=scope,
            block_on_exceeded=block_on_exceeded,
        )
        self._quotas[name] = quota
        logger.debug(f"Registered quota: {name} ({limit}/{window or self.default_window}s)")

    def check_quota(
        self,
        client_id: str,
        quota_name: str,
        cost: int = 1,
    ) -> QuotaCheckResult:
        """Check if quota is available.

        Args:
            client_id: Client identifier.
            quota_name: Name of quota to check.
            cost: Cost to consume.

        Returns:
            QuotaCheckResult with availability info.
        """
        with self._lock:
            quota = self._quotas.get(quota_name)
            if not quota:
                return QuotaCheckResult(allowed=True, remaining=-1, reset_at=0)

            usage_key = f"{client_id}:{quota_name}"
            usage = self._usage[client_id].get(quota_name)
            if not usage:
                usage = QuotaUsage(client_id=client_id, quota_name=quota_name)
                self._usage[client_id][quota_name] = usage

            now = time.time()
            if now - usage.window_start >= quota.window:
                usage.consumed = 0
                usage.window_start = now
                usage.blocked_until = None

            if usage.blocked_until and now < usage.blocked_until:
                return QuotaCheckResult(
                    allowed=False,
                    remaining=0,
                    reset_at=usage.window_start + quota.window,
                    retry_after=usage.blocked_until - now,
                )

            remaining = quota.limit - usage.consumed
            if remaining >= cost:
                usage.consumed += cost
                return QuotaCheckResult(
                    allowed=True,
                    remaining=remaining - cost,
                    reset_at=usage.window_start + quota.window,
                )
            else:
                if quota.block_on_exceeded:
                    retry_after = quota.window - (now - usage.window_start)
                    return QuotaCheckResult(
                        allowed=False,
                        remaining=0,
                        reset_at=usage.window_start + quota.window,
                        retry_after=retry_after,
                    )
                return QuotaCheckResult(
                    allowed=True,
                    remaining=0,
                    reset_at=usage.window_start + quota.window,
                )

    def consume(
        self,
        client_id: str,
        quota_name: str,
        cost: int = 1,
    ) -> bool:
        """Consume quota units.

        Args:
            client_id: Client identifier.
            quota_name: Quota name.
            cost: Units to consume.

        Returns:
            True if consumed successfully.
        """
        result = self.check_quota(client_id, quota_name, cost)
        if result.allowed:
            return True
        if result.retry_after:
            with self._lock:
                usage = self._usage[client_id].get(quota_name)
                if usage:
                    usage.blocked_until = time.time() + result.retry_after
        return False

    def get_usage(
        self,
        client_id: str,
        quota_name: Optional[str] = None,
    ) -> dict[str, QuotaUsage]:
        """Get current quota usage.

        Args:
            client_id: Client identifier.
            quota_name: Optional specific quota name.

        Returns:
            Dictionary of quota usages.
        """
        with self._lock:
            if quota_name:
                return {quota_name: self._usage[client_id].get(quota_name)}
            return dict(self._usage[client_id])

    def reset_usage(
        self,
        client_id: str,
        quota_name: Optional[str] = None,
    ) -> None:
        """Reset quota usage.

        Args:
            client_id: Client identifier.
            quota_name: Optional specific quota name.
        """
        with self._lock:
            if quota_name:
                if quota_name in self._usage[client_id]:
                    del self._usage[client_id][quota_name]
            else:
                if client_id in self._usage:
                    del self._usage[client_id]

    def get_quota_info(self, quota_name: str) -> Optional[QuotaLimit]:
        """Get quota limit info."""
        return self._quotas.get(quota_name)

    def get_all_quotas(self) -> dict[str, QuotaLimit]:
        """Get all registered quotas."""
        return dict(self._quotas)

    def get_stats(self) -> dict[str, Any]:
        """Get quota manager statistics."""
        with self._lock:
            total_clients = len(self._usage)
            total_quotas = sum(len(usages) for usages in self._usage.values())
            return {
                "registered_quotas": len(self._quotas),
                "active_clients": total_clients,
                "total_usage_records": total_quotas,
            }


class ClientQuotaDecorator:
    """Decorator for adding quota management to functions."""

    def __init__(
        self,
        quota_manager: QuotaManager,
        quota_name: str,
        cost: int = 1,
    ) -> None:
        self.quota_manager = quota_manager
        self.quota_name = quota_name
        self.cost = cost

    def __call__(self, func: callable) -> callable:
        def wrapper(client_id: str, *args: Any, **kwargs: Any) -> Any:
            result = self.quota_manager.check_quota(client_id, self.quota_name, self.cost)
            if not result.allowed:
                raise QuotaExceededError(
                    f"Quota '{self.quota_name}' exceeded for client '{client_id}'",
                    retry_after=result.retry_after,
                )
            return func(client_id, *args, **kwargs)
        return wrapper


class QuotaExceededError(Exception):
    """Raised when quota is exceeded."""

    def __init__(self, message: str, retry_after: Optional[float] = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after
