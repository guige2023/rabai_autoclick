"""API Rate Budget Action.

Enforces rate limiting budgets with quota management.
"""
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
import time


@dataclass
class BudgetQuota:
    name: str
    limit: int
    window_sec: float
    current: int = 0
    reset_at: float = field(default_factory=lambda: time.time())
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RateDecision:
    allowed: bool
    quota_name: str
    remaining: int
    reset_at: float
    retry_after: Optional[float] = None


class APIRateBudgetAction:
    """Rate limiting with budget and quota management."""

    def __init__(
        self,
        default_window_sec: float = 60.0,
        strict: bool = False,
    ) -> None:
        self.default_window_sec = default_window_sec
        self.strict = strict
        self.quotas: Dict[str, BudgetQuota] = {}

    def add_quota(
        self,
        name: str,
        limit: int,
        window_sec: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> BudgetQuota:
        quota = BudgetQuota(
            name=name,
            limit=limit,
            window_sec=window_sec or self.default_window_sec,
            reset_at=time.time() + (window_sec or self.default_window_sec),
            metadata=metadata or {},
        )
        self.quotas[name] = quota
        return quota

    def check(self, quota_name: str, key: Optional[str] = None) -> RateDecision:
        quota = self.quotas.get(quota_name)
        if not quota:
            return RateDecision(allowed=True, quota_name=quota_name, remaining=-1, reset_at=0)
        now = time.time()
        if now >= quota.reset_at:
            quota.current = 0
            quota.reset_at = now + quota.window_sec
        if quota.current >= quota.limit:
            retry_after = quota.reset_at - now
            return RateDecision(
                allowed=False,
                quota_name=quota_name,
                remaining=0,
                reset_at=quota.reset_at,
                retry_after=retry_after if retry_after > 0 else 0,
            )
        quota.current += 1
        return RateDecision(
            allowed=True,
            quota_name=quota_name,
            remaining=quota.limit - quota.current,
            reset_at=quota.reset_at,
        )

    def acquire(
        self,
        quota_name: str,
        amount: int = 1,
        key: Optional[str] = None,
    ) -> RateDecision:
        decision = self.check(quota_name, key)
        if decision.allowed:
            quota = self.quotas[quota_name]
            quota.current += amount - 1
        return decision

    def reset(self, quota_name: Optional[str] = None) -> None:
        if quota_name:
            if quota_name in self.quotas:
                self.quotas[quota_name].current = 0
                self.quotas[quota_name].reset_at = time.time() + self.quotas[quota_name].window_sec
        else:
            for quota in self.quotas.values():
                quota.current = 0
                quota.reset_at = time.time() + quota.window_sec

    def get_status(self, quota_name: Optional[str] = None) -> Dict[str, Any]:
        if quota_name:
            q = self.quotas.get(quota_name)
            if not q:
                return {}
            return {
                "name": q.name,
                "limit": q.limit,
                "current": q.current,
                "remaining": max(0, q.limit - q.current),
                "window_sec": q.window_sec,
                "reset_at": q.reset_at,
                "reset_in_sec": max(0, q.reset_at - time.time()),
            }
        return {name: self.get_status(name) for name in self.quotas}
