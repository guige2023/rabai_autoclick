"""API quota enforcement action module for RabAI AutoClick.

Provides quota management for API usage:
- ApiQuotaManager: Manage API usage quotas
- QuotaEnforcer: Enforce quota limits
- UsageTracker: Track API usage against quotas
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
import time
import threading
import logging
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class QuotaPeriod(Enum):
    """Quota time periods."""
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    MONTH = "month"


@dataclass
class QuotaLimit:
    """Quota limit definition."""
    quota_id: str
    limit: float
    period: QuotaPeriod
    scope: str = "global"
    enabled: bool = True


@dataclass
class QuotaUsage:
    """Current quota usage."""
    quota_id: str
    used: float
    remaining: float
    resets_at: float
    is_exceeded: bool


class ApiQuotaManager:
    """Manage API usage quotas."""
    
    def __init__(self, name: str):
        self.name = name
        self._quotas: Dict[str, QuotaLimit] = {}
        self._usage: Dict[str, List[float]] = defaultdict(list)
        self._reset_times: Dict[str, float] = {}
        self._lock = threading.RLock()
        self._stats = {"total_checks": 0, "allowed": 0, "rejected": 0}
    
    def _get_period_seconds(self, period: QuotaPeriod) -> float:
        """Get seconds for quota period."""
        if period == QuotaPeriod.SECOND:
            return 1.0
        elif period == QuotaPeriod.MINUTE:
            return 60.0
        elif period == QuotaPeriod.HOUR:
            return 3600.0
        elif period == QuotaPeriod.DAY:
            return 86400.0
        elif period == QuotaPeriod.MONTH:
            return 2592000.0
        return 60.0
    
    def _get_reset_time(self, quota: QuotaLimit) -> float:
        """Get reset time for quota."""
        now = time.time()
        period_seconds = self._get_period_seconds(quota.period)
        return now - (now % period_seconds) + period_seconds
    
    def add_quota(self, quota: QuotaLimit):
        """Add quota limit."""
        with self._lock:
            self._quotas[quota.quota_id] = quota
            self._reset_times[quota.quota_id] = self._get_reset_time(quota)
    
    def check_and_consume(self, quota_id: str, amount: float = 1.0, scope: str = "global") -> Tuple[bool, QuotaUsage]:
        """Check quota and consume if allowed."""
        with self._lock:
            self._stats["total_checks"] += 1
            quota = self._quotas.get(quota_id)
            
            if not quota or not quota.enabled:
                self._stats["allowed"] += 1
                return True, QuotaUsage(quota_id=quota_id, used=0, remaining=0, resets_at=0, is_exceeded=False)
            
            if quota.scope != "global" and quota.scope != scope:
                self._stats["allowed"] += 1
                return True, QuotaUsage(quota_id=quota_id, used=0, remaining=0, resets_at=0, is_exceeded=False)
            
            now = time.time()
            
            if now >= self._reset_times.get(quota_id, 0):
                self._usage[quota_id] = []
                self._reset_times[quota_id] = self._get_reset_time(quota)
            
            current_usage = sum(self._usage.get(quota_id, []))
            
            if current_usage + amount > quota.limit:
                self._stats["rejected"] += 1
                return False, QuotaUsage(
                    quota_id=quota_id,
                    used=current_usage,
                    remaining=max(0, quota.limit - current_usage),
                    resets_at=self._reset_times.get(quota_id, 0),
                    is_exceeded=True
                )
            
            self._usage[quota_id].append(amount)
            remaining = quota.limit - (current_usage + amount)
            
            self._stats["allowed"] += 1
            return True, QuotaUsage(
                quota_id=quota_id,
                used=current_usage + amount,
                remaining=remaining,
                resets_at=self._reset_times.get(quota_id, 0),
                is_exceeded=False
            )
    
    def get_usage(self, quota_id: str) -> QuotaUsage:
        """Get current usage for quota."""
        with self._lock:
            quota = self._quotas.get(quota_id)
            if not quota:
                return QuotaUsage(quota_id=quota_id, used=0, remaining=0, resets_at=0, is_exceeded=False)
            
            current_usage = sum(self._usage.get(quota_id, []))
            return QuotaUsage(
                quota_id=quota_id,
                used=current_usage,
                remaining=max(0, quota.limit - current_usage),
                resets_at=self._reset_times.get(quota_id, 0),
                is_exceeded=current_usage >= quota.limit
            )
    
    def reset_quota(self, quota_id: str):
        """Reset quota usage."""
        with self._lock:
            self._usage[quota_id] = []
            quota = self._quotas.get(quota_id)
            if quota:
                self._reset_times[quota_id] = self._get_reset_time(quota)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get quota statistics."""
        with self._lock:
            return {
                "name": self.name,
                "quota_count": len(self._quotas),
                **{k: v for k, v in self._stats.items()},
            }


class ApiQuotaEnforcementAction(BaseAction):
    """API quota enforcement action."""
    action_type = "api_quota_enforcement"
    display_name = "API配额执行"
    description = "API使用配额管理"
    
    def __init__(self):
        super().__init__()
        self._managers: Dict[str, ApiQuotaManager] = {}
        self._lock = threading.Lock()
    
    def _get_manager(self, name: str) -> ApiQuotaManager:
        """Get or create quota manager."""
        with self._lock:
            if name not in self._managers:
                self._managers[name] = ApiQuotaManager(name)
            return self._managers[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute quota operation."""
        try:
            manager_name = params.get("manager", "default")
            command = params.get("command", "check")
            
            manager = self._get_manager(manager_name)
            
            if command == "add_quota":
                quota = QuotaLimit(
                    quota_id=params.get("quota_id"),
                    limit=params.get("limit", 100),
                    period=QuotaPeriod[params.get("period", "minute").upper()],
                    scope=params.get("scope", "global"),
                )
                manager.add_quota(quota)
                return ActionResult(success=True)
            
            elif command == "check":
                quota_id = params.get("quota_id")
                amount = params.get("amount", 1.0)
                scope = params.get("scope", "global")
                allowed, usage = manager.check_and_consume(quota_id, amount, scope)
                return ActionResult(
                    success=allowed,
                    message="Allowed" if allowed else "Quota exceeded",
                    data={"used": usage.used, "remaining": usage.remaining, "resets_at": usage.resets_at}
                )
            
            elif command == "usage":
                quota_id = params.get("quota_id")
                usage = manager.get_usage(quota_id)
                return ActionResult(success=True, data={"used": usage.used, "remaining": usage.remaining, "is_exceeded": usage.is_exceeded})
            
            elif command == "reset":
                quota_id = params.get("quota_id")
                manager.reset_quota(quota_id)
                return ActionResult(success=True)
            
            elif command == "stats":
                stats = manager.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"ApiQuotaEnforcementAction error: {str(e)}")
