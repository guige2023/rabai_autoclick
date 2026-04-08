"""Quota Action Module.

Provides quota management and enforcement
for resource usage tracking.
"""

import time
import threading
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class QuotaPeriod(Enum):
    """Quota time period."""
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"


@dataclass
class QuotaLimit:
    """A quota limit."""
    quota_id: str
    name: str
    resource: str
    max_amount: float
    period: QuotaPeriod
    window_start: float = field(default_factory=time.time)
    current_usage: float = 0.0
    enabled: bool = True


@dataclass
class QuotaUsage:
    """Quota usage record."""
    quota_id: str
    amount: float
    timestamp: float = field(default_factory=time.time)


class QuotaManager:
    """Manages resource quotas."""

    def __init__(self):
        self._quotas: Dict[str, QuotaLimit] = {}
        self._usage_history: Dict[str, List[QuotaUsage]] = {}
        self._lock = threading.RLock()

    def create_quota(
        self,
        name: str,
        resource: str,
        max_amount: float,
        period: QuotaPeriod
    ) -> str:
        """Create a new quota."""
        quota_id = f"{resource}_{name}_{int(time.time())}"
        quota = QuotaLimit(
            quota_id=quota_id,
            name=name,
            resource=resource,
            max_amount=max_amount,
            period=period
        )
        self._quotas[quota_id] = quota
        self._usage_history[quota_id] = []
        return quota_id

    def check_and_consume(
        self,
        quota_id: str,
        amount: float
    ) -> tuple[bool, float]:
        """Check quota and consume if allowed."""
        with self._lock:
            quota = self._quotas.get(quota_id)
            if not quota or not quota.enabled:
                return True, 0

            self._reset_if_needed(quota)

            available = quota.max_amount - quota.current_usage
            if available >= amount:
                quota.current_usage += amount
                self._usage_history[quota_id].append(QuotaUsage(
                    quota_id=quota_id,
                    amount=amount
                ))
                return True, available - amount
            return False, available

    def _reset_if_needed(self, quota: QuotaLimit) -> None:
        """Reset quota window if period has passed."""
        now = time.time()
        period_seconds = {
            QuotaPeriod.SECOND: 1,
            QuotaPeriod.MINUTE: 60,
            QuotaPeriod.HOUR: 3600,
            QuotaPeriod.DAY: 86400
        }.get(quota.period, 60)

        if now - quota.window_start >= period_seconds:
            quota.current_usage = 0.0
            quota.window_start = now

    def get_usage(self, quota_id: str) -> Optional[Dict]:
        """Get current usage for quota."""
        quota = self._quotas.get(quota_id)
        if not quota:
            return None
        return {
            "quota_id": quota_id,
            "resource": quota.resource,
            "max_amount": quota.max_amount,
            "current_usage": quota.current_usage,
            "available": quota.max_amount - quota.current_usage,
            "period": quota.period.value
        }


class QuotaAction(BaseAction):
    """Action for quota operations."""

    def __init__(self):
        super().__init__("quota")
        self._manager = QuotaManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute quota action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "consume":
                return self._consume(params)
            elif operation == "usage":
                return self._usage(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create a quota."""
        quota_id = self._manager.create_quota(
            name=params.get("name", ""),
            resource=params.get("resource", ""),
            max_amount=params.get("max_amount", 100),
            period=QuotaPeriod(params.get("period", "minute"))
        )
        return ActionResult(success=True, data={"quota_id": quota_id})

    def _consume(self, params: Dict) -> ActionResult:
        """Consume from quota."""
        allowed, remaining = self._manager.check_and_consume(
            params.get("quota_id", ""),
            params.get("amount", 1)
        )
        return ActionResult(success=allowed, data={"allowed": allowed, "remaining": remaining})

    def _usage(self, params: Dict) -> ActionResult:
        """Get quota usage."""
        usage = self._manager.get_usage(params.get("quota_id", ""))
        if not usage:
            return ActionResult(success=False, message="Quota not found")
        return ActionResult(success=True, data=usage)
