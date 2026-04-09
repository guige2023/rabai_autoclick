"""API budget and quota tracking action."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Optional


class QuotaType(str, Enum):
    """Type of quota limit."""

    PER_MINUTE = "per_minute"
    PER_HOUR = "per_hour"
    PER_DAY = "per_day"
    PER_MONTH = "per_month"
    TOTAL = "total"


@dataclass
class QuotaLimit:
    """A quota limit configuration."""

    name: str
    quota_type: QuotaType
    limit: int
    window_seconds: float
    scope: str = "global"  # global, client_id, api_key, user
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class QuotaUsage:
    """Current quota usage."""

    quota_name: str
    used: int
    limit: int
    remaining: int
    reset_at: datetime
    window_seconds: float
    scope: str
    is_exhausted: bool = False


@dataclass
class BudgetConfig:
    """Configuration for budget tracking."""

    client_id: str
    quotas: list[QuotaLimit]
    on_quota_exceeded: Optional[Callable[[str, QuotaUsage], None]] = None
    on_warning: Optional[Callable[[str, QuotaUsage, float], None]] = None
    warning_threshold: float = 0.8  # Warn at 80% usage


@dataclass
class BudgetStats:
    """Budget tracking statistics."""

    client_id: str
    total_requests: int = 0
    rejected_requests: int = 0
    total_cost: float = 0.0
    quotas: dict[str, QuotaUsage] = field(default_factory=dict)


class APIBudgetTrackerAction:
    """Tracks API usage against quotas and budgets."""

    def __init__(self):
        """Initialize budget tracker."""
        self._quotas: dict[str, dict[str, QuotaLimit]] = {}  # client_id -> quota_name -> limit
        self._usage: dict[str, dict[str, list[tuple[float, int]]]] = {}  # client_id -> quota_name -> [(timestamp, count)]
        self._stats: dict[str, BudgetStats] = {}
        self._callbacks: dict[str, Callable[[str, QuotaUsage], None]] = {}
        self._warning_callbacks: dict[str, Callable[[str, QuotaUsage, float], None]] = {}

    def register_budget(self, config: BudgetConfig) -> None:
        """Register a budget configuration for a client."""
        if config.client_id not in self._quotas:
            self._quotas[config.client_id] = {}
            self._usage[config.client_id] = {}
            self._stats[config.client_id] = BudgetStats(client_id=config.client_id)

        for quota in config.quotas:
            self._quotas[config.client_id][quota.name] = quota
            self._usage[config.client_id][quota.name] = []

        if config.on_quota_exceeded:
            self._callbacks[config.client_id] = config.on_quota_exceeded
        if config.on_warning:
            self._warning_callbacks[config.client_id] = config.on_warning

    def _clean_expired_usage(
        self,
        client_id: str,
        quota_name: str,
        window_seconds: float,
    ) -> None:
        """Remove expired usage entries."""
        cutoff = time.time() - window_seconds
        usage_list = self._usage.get(client_id, {}).get(quota_name, [])
        self._usage[client_id][quota_name] = [
            (ts, count) for ts, count in usage_list if ts > cutoff
        ]

    def _calculate_usage(
        self,
        client_id: str,
        quota_name: str,
    ) -> int:
        """Calculate current usage for a quota."""
        if client_id not in self._usage or quota_name not in self._usage[client_id]:
            return 0

        self._clean_expired_usage(
            client_id, quota_name, self._quotas[client_id][quota_name].window_seconds
        )

        return sum(count for _, count in self._usage[client_id][quota_name])

    def check_quota(
        self,
        client_id: str,
        quota_name: str,
        increment: int = 1,
    ) -> QuotaUsage:
        """Check if quota is available.

        Returns:
            QuotaUsage with current status.
        """
        if client_id not in self._quotas or quota_name not in self._quotas[client_id]:
            raise ValueError(f"Quota not found: {client_id}/{quota_name}")

        quota = self._quotas[client_id][quota_name]
        current_usage = self._calculate_usage(client_id, quota_name)
        remaining = max(0, quota.limit - current_usage)

        now = time.time()
        reset_at = datetime.fromtimestamp(now + quota.window_seconds)

        if quota.quota_type == QuotaType.PER_MINUTE:
            reset_at = datetime.fromtimestamp(now + 60)
        elif quota.quota_type == QuotaType.PER_HOUR:
            reset_at = datetime.fromtimestamp(now + 3600)
        elif quota.quota_type == QuotaType.PER_DAY:
            reset_at = datetime.fromtimestamp(now + 86400)
        elif quota.quota_type == QuotaType.PER_MONTH:
            reset_at = datetime.fromtimestamp(now + 2592000)

        usage = QuotaUsage(
            quota_name=quota_name,
            used=current_usage,
            limit=quota.limit,
            remaining=remaining,
            reset_at=reset_at,
            window_seconds=quota.window_seconds,
            scope=quota.scope,
            is_exhausted=remaining < increment,
        )

        if usage.is_exhausted and client_id in self._callbacks:
            self._callbacks[client_id](quota_name, usage)

        usage_pct = current_usage / quota.limit if quota.limit > 0 else 0
        if usage_pct >= 0.8 and client_id in self._warning_callbacks:
            self._warning_callbacks[client_id](quota_name, usage, usage_pct)

        return usage

    def record_request(
        self,
        client_id: str,
        quota_name: str,
        count: int = 1,
    ) -> QuotaUsage:
        """Record a request and update usage.

        Returns:
            QuotaUsage with updated status.
        """
        if client_id not in self._usage:
            self._usage[client_id] = {}
        if quota_name not in self._usage[client_id]:
            self._usage[client_id][quota_name] = []

        self._usage[client_id][quota_name].append((time.time(), count))

        if client_id in self._stats:
            self._stats[client_id].total_requests += count

        return self.check_quota(client_id, quota_name)

    def get_usage(
        self,
        client_id: str,
        quota_name: Optional[str] = None,
    ) -> dict[str, QuotaUsage]:
        """Get current usage for quotas."""
        if client_id not in self._quotas:
            return {}

        result = {}
        quotas_to_check = (
            [quota_name] if quota_name else list(self._quotas[client_id].keys())
        )

        for qname in quotas_to_check:
            usage = self.check_quota(client_id, qname)
            result[qname] = usage

        return result

    def get_stats(self, client_id: str) -> Optional[BudgetStats]:
        """Get budget statistics for a client."""
        return self._stats.get(client_id)

    def reset_quota(self, client_id: str, quota_name: str) -> bool:
        """Reset usage for a specific quota."""
        if client_id in self._usage and quota_name in self._usage[client_id]:
            self._usage[client_id][quota_name] = []
            return True
        return False

    def reset_all_quotas(self, client_id: str) -> bool:
        """Reset all quotas for a client."""
        if client_id in self._usage:
            for quota_name in self._usage[client_id]:
                self._usage[client_id][quota_name] = []
            return True
        return False
