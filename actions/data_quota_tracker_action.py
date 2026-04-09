"""Data Quota Tracker Action Module.

Provides quota tracking and enforcement for resource
usage with budget limits and alert thresholds.

Author: RabAi Team
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, List, Optional
from enum import Enum

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class QuotaScope(Enum):
    """Scope of quota limits."""
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    MONTH = "month"


@dataclass
class QuotaLimit:
    """Quota limit configuration."""
    resource: str
    max_usage: float
    scope: QuotaScope
    window_seconds: float


@dataclass
class QuotaUsage:
    """Current quota usage."""
    resource: str
    used: float
    remaining: float
    limit: float
    reset_at: float
    scope: QuotaScope
    percent_used: float


@dataclass
class AlertThreshold:
    """Alert threshold for quota."""
    percent: float
    callback: Optional[Callable] = None
    triggered: bool = False


@dataclass
class UsageRecord:
    """Record of resource usage."""
    timestamp: float
    amount: float
    resource: str


class QuotaTracker:
    """Tracks and enforces resource quotas."""

    def __init__(self):
        self._limits: Dict[str, QuotaLimit] = {}
        self._usage_history: Dict[str, Deque[UsageRecord]] = {}
        self._alerts: Dict[str, List[AlertThreshold]] = {}
        self._total_usage: Dict[str, float] = {}
        self._window_start: Dict[str, float] = {}

    def add_limit(
        self,
        resource: str,
        max_usage: float,
        scope: QuotaScope = QuotaScope.DAY
    ) -> None:
        """Add quota limit for resource."""
        window_seconds = self._scope_to_seconds(scope)

        limit = QuotaLimit(
            resource=resource,
            max_usage=max_usage,
            scope=scope,
            window_seconds=window_seconds
        )

        self._limits[resource] = limit

        if resource not in self._usage_history:
            self._usage_history[resource] = deque(maxlen=10000)
            self._window_start[resource] = time.time()
            self._total_usage[resource] = 0.0

        if resource not in self._alerts:
            self._alerts[resource] = []

    def add_alert(
        self,
        resource: str,
        percent: float,
        callback: Optional[Callable] = None
    ) -> None:
        """Add alert threshold for resource."""
        if resource not in self._alerts:
            self._alerts[resource] = []

        self._alerts[resource].append(
            AlertThreshold(percent=percent, callback=callback)
        )

    def _scope_to_seconds(self, scope: QuotaScope) -> float:
        """Convert scope to seconds."""
        mapping = {
            QuotaScope.SECOND: 1.0,
            QuotaScope.MINUTE: 60.0,
            QuotaScope.HOUR: 3600.0,
            QuotaScope.DAY: 86400.0,
            QuotaScope.MONTH: 2592000.0
        }
        return mapping.get(scope, 86400.0)

    def _evict_expired(self, resource: str) -> float:
        """Remove expired usage records and return current usage."""
        if resource not in self._limits:
            return 0.0

        limit = self._limits[resource]
        cutoff = time.time() - limit.window_seconds

        history = self._usage_history[resource]
        total = 0.0
        new_history: Deque[UsageRecord] = deque(maxlen=10000)

        while history:
            record = history.popleft()
            if record.timestamp >= cutoff:
                total += record.amount
                new_history.append(record)

        for record in new_history:
            history.append(record)

        self._total_usage[resource] = total
        return total

    def check_quota(self, resource: str) -> QuotaUsage:
        """Check current quota usage for resource."""
        if resource not in self._limits:
            return QuotaUsage(
                resource=resource,
                used=0.0,
                remaining=0.0,
                limit=0.0,
                reset_at=0.0,
                scope=QuotaScope.DAY,
                percent_used=0.0
            )

        limit = self._limits[resource]
        current_usage = self._evict_expired(resource)

        used = current_usage
        remaining = max(0.0, limit.max_usage - used)
        percent_used = (used / limit.max_usage * 100) if limit.max_usage > 0 else 0.0

        window_elapsed = time.time() - self._window_start[resource]
        reset_at = time.time() + (limit.window_seconds - window_elapsed)

        return QuotaUsage(
            resource=resource,
            used=used,
            remaining=remaining,
            limit=limit.max_usage,
            reset_at=reset_at,
            scope=limit.scope,
            percent_used=percent_used
        )

    def consume(
        self,
        resource: str,
        amount: float,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Consume quota for resource."""
        if resource not in self._limits:
            return True

        usage = self.check_quota(resource)

        if usage.remaining < amount:
            self._check_alerts(resource, usage)
            return False

        record = UsageRecord(
            timestamp=time.time(),
            amount=amount,
            resource=resource
        )

        self._usage_history[resource].append(record)
        self._total_usage[resource] += amount

        usage = self.check_quota(resource)
        self._check_alerts(resource, usage)

        return True

    def _check_alerts(self, resource: str, usage: QuotaUsage) -> None:
        """Check and trigger alert thresholds."""
        if resource not in self._alerts:
            return

        for alert in self._alerts[resource]:
            if not alert.triggered and usage.percent_used >= alert.percent:
                alert.triggered = True

                if alert.callback:
                    try:
                        alert.callback(resource, usage)
                    except Exception:
                        pass

    def reset_quota(self, resource: str) -> None:
        """Reset quota for resource."""
        if resource in self._usage_history:
            self._usage_history[resource].clear()
        if resource in self._total_usage:
            self._total_usage[resource] = 0.0
        if resource in self._window_start:
            self._window_start[resource] = time.time()

        if resource in self._alerts:
            for alert in self._alerts[resource]:
                alert.triggered = False

    def get_usage_summary(self) -> Dict[str, QuotaUsage]:
        """Get usage summary for all resources."""
        summary = {}

        for resource in self._limits:
            summary[resource] = self.check_quota(resource)

        return summary

    def get_statistics(self) -> Dict[str, Any]:
        """Get quota tracker statistics."""
        resources = list(self._limits.keys())

        return {
            "tracked_resources": len(resources),
            "resources": resources,
            "total_usage": dict(self._total_usage),
            "window_starts": {
                r: self._window_start.get(r, 0)
                for r in resources
            }
        }


class DataQuotaTrackerAction(BaseAction):
    """Action for quota tracking operations."""

    def __init__(self):
        super().__init__("data_quota_tracker")
        self._tracker = QuotaTracker()

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute quota tracker action."""
        try:
            operation = params.get("operation", "consume")

            if operation == "add_limit":
                return self._add_limit(params)
            elif operation == "consume":
                return self._consume(params)
            elif operation == "check":
                return self._check(params)
            elif operation == "reset":
                return self._reset(params)
            elif operation == "summary":
                return self._get_summary(params)
            elif operation == "stats":
                return self._get_stats(params)
            elif operation == "add_alert":
                return self._add_alert(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _add_limit(self, params: Dict[str, Any]) -> ActionResult:
        """Add quota limit."""
        resource = params.get("resource", "")
        max_usage = params.get("max_usage", 0.0)
        scope = QuotaScope(params.get("scope", "day"))

        self._tracker.add_limit(resource, max_usage, scope)

        return ActionResult(
            success=True,
            message=f"Limit added for: {resource}"
        )

    def _consume(self, params: Dict[str, Any]) -> ActionResult:
        """Consume quota."""
        resource = params.get("resource", "")
        amount = params.get("amount", 1.0)

        allowed = self._tracker.consume(resource, amount)

        usage = self._tracker.check_quota(resource)

        return ActionResult(
            success=allowed,
            data={
                "allowed": allowed,
                "used": usage.used,
                "remaining": usage.remaining,
                "percent_used": usage.percent_used
            }
        )

    def _check(self, params: Dict[str, Any]) -> ActionResult:
        """Check quota usage."""
        resource = params.get("resource", "")

        usage = self._tracker.check_quota(resource)

        return ActionResult(
            success=True,
            data={
                "resource": usage.resource,
                "used": usage.used,
                "remaining": usage.remaining,
                "limit": usage.limit,
                "reset_at": usage.reset_at,
                "scope": usage.scope.value,
                "percent_used": usage.percent_used
            }
        )

    def _reset(self, params: Dict[str, Any]) -> ActionResult:
        """Reset quota."""
        resource = params.get("resource", "")

        self._tracker.reset_quota(resource)

        return ActionResult(
            success=True,
            message=f"Quota reset for: {resource}"
        )

    def _get_summary(self, params: Dict[str, Any]) -> ActionResult:
        """Get usage summary."""
        summary = self._tracker.get_usage_summary()

        return ActionResult(
            success=True,
            data={
                "quotas": {
                    r: {
                        "used": q.used,
                        "remaining": q.remaining,
                        "limit": q.limit,
                        "percent_used": q.percent_used
                    }
                    for r, q in summary.items()
                }
            }
        )

    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get statistics."""
        stats = self._tracker.get_statistics()
        return ActionResult(success=True, data=stats)

    def _add_alert(self, params: Dict[str, Any]) -> ActionResult:
        """Add alert threshold."""
        resource = params.get("resource", "")
        percent = params.get("percent", 80.0)

        self._tracker.add_alert(resource, percent)

        return ActionResult(
            success=True,
            message=f"Alert added at {percent}% for {resource}"
        )
