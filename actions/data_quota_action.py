"""Data Quota Action Module.

Provides quota management with usage tracking,
thresholds, and notification hooks.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class QuotaScope(Enum):
    """Quota scope."""
    DAILY = "daily"
    HOURLY = "hourly"
    MONTHLY = "monthly"
    TOTAL = "total"


@dataclass
class QuotaLimit:
    """Quota limit configuration."""
    name: str
    limit: int
    scope: QuotaScope = QuotaScope.TOTAL
    window_start: Optional[float] = None


@dataclass
class QuotaUsage:
    """Quota usage tracking."""
    name: str
    used: int
    limit: int
    window_start: float
    window_end: float
    percentage: float


class DataQuotaAction:
    """Quota management with usage tracking.

    Example:
        quota = DataQuotaAction()

        quota.set_limit(QuotaLimit(
            name="api_calls",
            limit=1000,
            scope=QuotaScope.DAILY
        ))

        if quota.check("api_calls"):
            quota.increment("api_calls")
        else:
            print("Quota exceeded")
    """

    def __init__(self) -> None:
        self._limits: Dict[str, QuotaLimit] = {}
        self._usage: Dict[str, List[float]] = defaultdict(list)
        self._hooks: Dict[str, List[Callable]] = {
            "warning": [],
            "exceeded": [],
            "reset": [],
        }

    def set_limit(self, limit: QuotaLimit) -> None:
        """Set quota limit.

        Args:
            limit: QuotaLimit configuration
        """
        self._limits[limit.name] = limit

        if limit.scope != QuotaScope.TOTAL:
            self._usage[limit.name] = []

    def check(self, name: str, amount: int = 1) -> bool:
        """Check if quota allows the request.

        Args:
            name: Quota name
            amount: Amount to check

        Returns:
            True if within quota
        """
        if name not in self._limits:
            return True

        limit = self._limits[name]
        current_usage = self._get_current_usage(name)

        return current_usage + amount <= limit.limit

    def increment(self, name: str, amount: int = 1) -> None:
        """Increment quota usage.

        Args:
            name: Quota name
            amount: Amount to increment
        """
        limit = self._limits.get(name)
        if not limit:
            return

        if limit.scope != QuotaScope.TOTAL:
            now = time.time()
            self._prune_usage(name, limit)
            self._usage[name].append(now)

        self._check_hooks(name)

    def _prune_usage(self, name: str, limit: QuotaLimit) -> None:
        """Prune expired usage entries."""
        now = time.time()
        window_start = self._get_window_start(limit.scope, now)
        self._usage[name] = [
            t for t in self._usage[name]
            if t >= window_start
        ]

    def _get_current_usage(self, name: str) -> int:
        """Get current usage count."""
        limit = self._limits.get(name)
        if not limit:
            return 0

        if limit.scope == QuotaScope.TOTAL:
            return len(self._usage[name])

        self._prune_usage(name, limit)
        return len(self._usage[name])

    def _get_window_start(self, scope: QuotaScope, now: float) -> float:
        """Get window start time."""
        if scope == QuotaScope.HOURLY:
            return now - 3600
        elif scope == QuotaScope.DAILY:
            return now - 86400
        elif scope == QuotaScope.MONTHLY:
            return now - 2592000
        return 0

    def _check_hooks(self, name: str) -> None:
        """Check and trigger hooks."""
        limit = self._limits.get(name)
        if not limit:
            return

        usage = self._get_current_usage(name)
        percentage = (usage / limit.limit * 100) if limit.limit > 0 else 0

        if percentage >= 100:
            for hook in self._hooks["exceeded"]:
                try:
                    hook(name, usage, limit.limit)
                except Exception as e:
                    logger.error(f"Hook error: {e}")

        elif percentage >= 80:
            for hook in self._hooks["warning"]:
                try:
                    hook(name, usage, limit.limit)
                except Exception as e:
                    logger.error(f"Hook error: {e}")

    def add_hook(
        self,
        event: str,
        hook: Callable,
    ) -> "DataQuotaAction":
        """Add quota hook.

        Returns self for chaining.
        """
        if event in self._hooks:
            self._hooks[event].append(hook)
        return self

    def get_usage(self, name: str) -> Optional[QuotaUsage]:
        """Get current quota usage.

        Returns:
            QuotaUsage or None if quota not found
        """
        if name not in self._limits:
            return None

        limit = self._limits[name]
        now = time.time()
        used = self._get_current_usage(name)
        window_start = self._get_window_start(limit.scope, now)

        window_end = now
        if limit.scope == QuotaScope.HOURLY:
            window_end = window_start + 3600
        elif limit.scope == QuotaScope.DAILY:
            window_end = window_start + 86400
        elif limit.scope == QuotaScope.MONTHLY:
            window_end = window_start + 2592000

        percentage = (used / limit.limit * 100) if limit.limit > 0 else 0

        return QuotaUsage(
            name=name,
            used=used,
            limit=limit.limit,
            window_start=window_start,
            window_end=window_end,
            percentage=percentage,
        )

    def get_all_usage(self) -> List[QuotaUsage]:
        """Get usage for all quotas."""
        return [
            usage for name, usage in [
                (name, self.get_usage(name)) for name in self._limits
            ] if usage is not None
        ]

    def reset(self, name: str) -> None:
        """Reset quota usage.

        Args:
            name: Quota name
        """
        if name in self._usage:
            self._usage[name].clear()

        for hook in self._hooks["reset"]:
            try:
                hook(name)
            except Exception as e:
                logger.error(f"Hook error: {e}")

    def reset_all(self) -> None:
        """Reset all quota usage."""
        for name in self._limits:
            self.reset(name)
