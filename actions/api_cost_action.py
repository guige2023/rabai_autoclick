"""
API Cost Action Module.

Tracks API usage costs with rate limiting,
budget management, and cost allocation.
"""

from __future__ import annotations

from typing import Any, Optional
from dataclasses import dataclass, field
import logging
import time
from enum import Enum

logger = logging.getLogger(__name__)


class CostAllocation(Enum):
    """How costs are allocated."""
    PER_REQUEST = "per_request"
    PER_BYTE = "per_byte"
    PER_MINUTE = "per_minute"


@dataclass
class CostRecord:
    """Single cost record."""
    endpoint: str
    cost: float
    timestamp: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class CostBudget:
    """Budget configuration."""
    name: str
    limit: float
    period_seconds: float
    alert_threshold: float = 0.8


@dataclass
class CostStatus:
    """Current cost status."""
    budget_name: str
    spent: float
    limit: float
    remaining: float
    percent_used: float
    reset_at: float
    is_exceeded: bool


class APICostAction:
    """
    API cost tracking and budget management.

    Tracks costs per endpoint, manages budgets,
    and alerts when thresholds are exceeded.

    Example:
        cost_tracker = APICostAction()
        cost_tracker.set_budget("monthly", limit=1000.0, period=30*24*3600)
        cost_tracker.record_cost("/api/users", cost=0.001)
        status = cost_tracker.get_status("monthly")
    """

    def __init__(
        self,
        default_currency: str = "USD",
        cost_per_request: float = 0.0001,
    ) -> None:
        self.default_currency = default_currency
        self.cost_per_request = cost_per_request
        self._budgets: dict[str, CostBudget] = {}
        self._spending: dict[str, list[CostRecord]] = {}
        self._reset_times: dict[str, float] = {}

    def set_budget(
        self,
        name: str,
        limit: float,
        period_seconds: float,
        alert_threshold: float = 0.8,
    ) -> None:
        """Configure a budget."""
        self._budgets[name] = CostBudget(
            name=name,
            limit=limit,
            period_seconds=period_seconds,
            alert_threshold=alert_threshold,
        )
        self._spending[name] = []
        self._reset_times[name] = time.time() + period_seconds

    def record_cost(
        self,
        endpoint: str,
        cost: float,
        metadata: Optional[dict[str, Any]] = None,
        budget_name: str = "default",
    ) -> None:
        """Record an API cost."""
        self._prune_expired(budget_name)

        record = CostRecord(
            endpoint=endpoint,
            cost=cost,
            timestamp=time.time(),
            metadata=metadata or {},
        )

        if budget_name not in self._spending:
            self._spending[budget_name] = []

        self._spending[budget_name].append(record)

        budget = self._budgets.get(budget_name)
        if budget:
            total_spent = self.get_total_spent(budget_name)
            if total_spent / budget.limit >= budget.alert_threshold:
                logger.warning(
                    "Budget '%s' at %.1f%% threshold",
                    budget_name,
                    (total_spent / budget.limit) * 100
                )

    def get_status(self, budget_name: str) -> Optional[CostStatus]:
        """Get current status for a budget."""
        self._prune_expired(budget_name)

        if budget_name not in self._budgets:
            return None

        budget = self._budgets[budget_name]
        spent = self.get_total_spent(budget_name)

        return CostStatus(
            budget_name=budget_name,
            spent=spent,
            limit=budget.limit,
            remaining=max(0, budget.limit - spent),
            percent_used=(spent / budget.limit * 100) if budget.limit > 0 else 0,
            reset_at=self._reset_times.get(budget_name, 0),
            is_exceeded=spent >= budget.limit,
        )

    def get_total_spent(self, budget_name: str) -> float:
        """Calculate total spent in budget."""
        self._prune_expired(budget_name)

        records = self._spending.get(budget_name, [])
        return sum(r.cost for r in records)

    def get_cost_by_endpoint(
        self,
        budget_name: str = "default",
    ) -> dict[str, float]:
        """Get cost breakdown by endpoint."""
        self._prune_expired(budget_name)

        costs: dict[str, float] = {}
        records = self._spending.get(budget_name, [])

        for record in records:
            costs[record.endpoint] = costs.get(record.endpoint, 0) + record.cost

        return costs

    def check_limit(
        self,
        cost: float,
        budget_name: str = "default",
    ) -> bool:
        """Check if adding cost would exceed budget."""
        status = self.get_status(budget_name)
        if status is None:
            return True

        return (status.spent + cost) <= status.limit

    def reset_budget(self, budget_name: str) -> None:
        """Reset spending for a budget."""
        if budget_name in self._spending:
            self._spending[budget_name] = []

        if budget_name in self._budgets:
            budget = self._budgets[budget_name]
            self._reset_times[budget_name] = time.time() + budget.period_seconds

    def _prune_expired(self, budget_name: str) -> None:
        """Remove expired records."""
        if budget_name not in self._budgets:
            return

        budget = self._budgets[budget_name]
        cutoff = time.time() - budget.period_seconds

        records = self._spending.get(budget_name, [])
        self._spending[budget_name] = [r for r in records if r.timestamp >= cutoff]
