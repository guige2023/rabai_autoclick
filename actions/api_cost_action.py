"""API Cost Action Module.

Provides API cost tracking, budgeting, and optimization
capabilities for metered API usage.
"""

import time
import hashlib
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CostModel(Enum):
    """API cost model type."""
    PER_REQUEST = "per_request"
    PER_BYTE = "per_byte"
    TIERED = "tiered"
    SUBSCRIPTION = "subscription"


class BudgetPeriod(Enum):
    """Budget time period."""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"


@dataclass
class APIPricing:
    """API pricing configuration."""
    base_cost: float = 0.0
    per_request_cost: float = 0.0
    per_kb_cost: float = 0.0
    monthly_subscription: float = 0.0
    free_tier_requests: int = 1000
    free_tier_bytes: int = 1024 * 1024


@dataclass
class APIUsageRecord:
    """Record of API usage."""
    timestamp: float
    endpoint: str
    method: str
    request_bytes: int
    response_bytes: int
    duration_ms: float
    cost: float


@dataclass
class BudgetAlert:
    """Budget alert configuration."""
    threshold_percent: float
    callback: Optional[Callable] = None
    enabled: bool = True


@dataclass
class CostReport:
    """Cost analysis report."""
    period_start: float
    period_end: float
    total_requests: int
    total_bytes: int
    total_cost: float
    cost_by_endpoint: Dict[str, float]
    cost_by_method: Dict[str, float]
    average_cost_per_request: float


class APICostTracker:
    """Tracks and manages API costs."""

    def __init__(self):
        self._usage_records: List[APIUsageRecord] = []
        self._endpoint_costs: Dict[str, APIPricing] = {}
        self._budgets: Dict[str, Dict[BudgetPeriod, float]] = defaultdict(dict)
        self._alerts: Dict[str, List[BudgetAlert]] = defaultdict(list)
        self._default_pricing = APIPricing()

    def set_pricing(self, endpoint: str, pricing: APIPricing) -> None:
        """Set pricing for an endpoint."""
        self._endpoint_costs[endpoint] = pricing

    def get_pricing(self, endpoint: str) -> APIPricing:
        """Get pricing for an endpoint."""
        return self._endpoint_costs.get(endpoint, self._default_pricing)

    def record_usage(
        self,
        endpoint: str,
        method: str,
        request_bytes: int = 0,
        response_bytes: int = 0,
        duration_ms: float = 0.0
    ) -> float:
        """Record API usage and return cost."""
        pricing = self.get_pricing(endpoint)

        cost = self._calculate_cost(pricing, request_bytes, response_bytes)

        record = APIUsageRecord(
            timestamp=time.time(),
            endpoint=endpoint,
            method=method,
            request_bytes=request_bytes,
            response_bytes=response_bytes,
            duration_ms=duration_ms,
            cost=cost
        )

        self._usage_records.append(record)
        self._check_budget_alerts(endpoint, record)

        return cost

    def _calculate_cost(
        self,
        pricing: APIPricing,
        request_bytes: int,
        response_bytes: int
    ) -> float:
        """Calculate cost based on pricing model."""
        total_bytes = request_bytes + response_bytes

        base_cost = pricing.base_cost

        request_cost = pricing.per_request_cost
        if self._get_total_requests() < pricing.free_tier_requests:
            request_cost = 0.0

        byte_cost = pricing.per_kb_cost * (total_bytes / 1024)
        if self._get_total_bytes() < pricing.free_tier_bytes:
            byte_cost = 0.0

        return base_cost + request_cost + byte_cost

    def _get_total_requests(self) -> int:
        """Get total request count."""
        return len(self._usage_records)

    def _get_total_bytes(self) -> int:
        """Get total bytes transferred."""
        return sum(
            r.request_bytes + r.response_bytes
            for r in self._usage_records
        )

    def _check_budget_alerts(self, endpoint: str, record: APIUsageRecord) -> None:
        """Check if any budget alerts should trigger."""
        if endpoint not in self._alerts:
            return

        period_cost = self.get_cost_since(endpoint, time.time() - 86400)

        for alert in self._alerts[endpoint]:
            if not alert.enabled:
                continue

            for period, budget in self._budgets.get(endpoint, {}).items():
                if period_cost >= budget * (alert.threshold_percent / 100):
                    if alert.callback:
                        alert.callback(endpoint, period_cost, budget)

    def set_budget(
        self,
        endpoint: str,
        period: BudgetPeriod,
        amount: float
    ) -> None:
        """Set budget for an endpoint and period."""
        self._budgets[endpoint][period] = amount

    def add_alert(
        self,
        endpoint: str,
        alert: BudgetAlert
    ) -> None:
        """Add a budget alert."""
        self._alerts[endpoint].append(alert)

    def get_cost_since(
        self,
        endpoint: Optional[str] = None,
        since: Optional[float] = None
    ) -> float:
        """Get total cost since timestamp."""
        since = since or 0
        records = self._usage_records

        if endpoint:
            records = [r for r in records if r.endpoint == endpoint]
        if since:
            records = [r for r in records if r.timestamp >= since]

        return sum(r.cost for r in records)

    def get_cost_report(
        self,
        period_start: float,
        period_end: float,
        endpoint: Optional[str] = None
    ) -> CostReport:
        """Generate cost report for period."""
        records = [
            r for r in self._usage_records
            if period_start <= r.timestamp <= period_end
        ]

        if endpoint:
            records = [r for r in records if r.endpoint == endpoint]

        total_requests = len(records)
        total_bytes = sum(r.request_bytes + r.response_bytes for r in records)
        total_cost = sum(r.cost for r in records)

        cost_by_endpoint: Dict[str, float] = defaultdict(float)
        cost_by_method: Dict[str, float] = defaultdict(float)

        for record in records:
            cost_by_endpoint[record.endpoint] += record.cost
            cost_by_method[record.method] += record.cost

        avg_cost = total_cost / total_requests if total_requests > 0 else 0

        return CostReport(
            period_start=period_start,
            period_end=period_end,
            total_requests=total_requests,
            total_bytes=total_bytes,
            total_cost=total_cost,
            cost_by_endpoint=dict(cost_by_endpoint),
            cost_by_method=dict(cost_by_method),
            average_cost_per_request=avg_cost
        )

    def get_usage_history(
        self,
        endpoint: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get usage history."""
        records = self._usage_records

        if endpoint:
            records = [r for r in records if r.endpoint == endpoint]

        records = records[-limit:]

        return [
            {
                "timestamp": r.timestamp,
                "endpoint": r.endpoint,
                "method": r.method,
                "request_bytes": r.request_bytes,
                "response_bytes": r.response_bytes,
                "duration_ms": r.duration_ms,
                "cost": r.cost
            }
            for r in records
        ]

    def optimize_suggestions(self) -> List[str]:
        """Generate cost optimization suggestions."""
        suggestions = []

        total_cost = sum(r.cost for r in self._usage_records)
        if total_cost > 1000:
            suggestions.append("Consider implementing response caching to reduce costs")

        large_responses = [
            r for r in self._usage_records
            if r.response_bytes > 1024 * 1024
        ]
        if large_responses:
            suggestions.append(
                f"Found {len(large_responses)} requests with large responses (>1MB). "
                "Consider compression or pagination."
            )

        endpoint_costs = defaultdict(float)
        for r in self._usage_records:
            endpoint_costs[r.endpoint] += r.cost

        expensive = sorted(
            endpoint_costs.items(),
            key=lambda x: x[1],
            reverse=True
        )[:3]

        if expensive:
            suggestions.append(
                f"Top 3 expensive endpoints: {', '.join(e[0] for e in expensive)}"
            )

        return suggestions


class APICostAction(BaseAction):
    """Action for API cost operations."""

    def __init__(self):
        super().__init__("api_cost")
        self._tracker = APICostTracker()

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute API cost action."""
        try:
            operation = params.get("operation", "record")

            if operation == "record":
                return self._record_usage(params)
            elif operation == "set_pricing":
                return self._set_pricing(params)
            elif operation == "set_budget":
                return self._set_budget(params)
            elif operation == "get_cost":
                return self._get_cost(params)
            elif operation == "report":
                return self._get_report(params)
            elif operation == "history":
                return self._get_history(params)
            elif operation == "optimize":
                return self._get_optimize(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _record_usage(self, params: Dict[str, Any]) -> ActionResult:
        """Record API usage."""
        endpoint = params.get("endpoint", "/")
        method = params.get("method", "GET")
        request_bytes = params.get("request_bytes", 0)
        response_bytes = params.get("response_bytes", 0)
        duration_ms = params.get("duration_ms", 0.0)

        cost = self._tracker.record_usage(
            endpoint=endpoint,
            method=method,
            request_bytes=request_bytes,
            response_bytes=response_bytes,
            duration_ms=duration_ms
        )

        return ActionResult(
            success=True,
            data={"cost": cost}
        )

    def _set_pricing(self, params: Dict[str, Any]) -> ActionResult:
        """Set API pricing."""
        endpoint = params.get("endpoint", "*")
        pricing = APIPricing(
            base_cost=params.get("base_cost", 0.0),
            per_request_cost=params.get("per_request_cost", 0.0),
            per_kb_cost=params.get("per_kb_cost", 0.0),
            monthly_subscription=params.get("monthly_subscription", 0.0),
            free_tier_requests=params.get("free_tier_requests", 1000),
            free_tier_bytes=params.get("free_tier_bytes", 1024 * 1024)
        )

        self._tracker.set_pricing(endpoint, pricing)
        return ActionResult(
            success=True,
            message=f"Pricing set for: {endpoint}"
        )

    def _set_budget(self, params: Dict[str, Any]) -> ActionResult:
        """Set budget for endpoint."""
        endpoint = params.get("endpoint", "*")
        period = BudgetPeriod(params.get("period", "monthly"))
        amount = params.get("amount", 0.0)

        self._tracker.set_budget(endpoint, period, amount)
        return ActionResult(
            success=True,
            message=f"Budget set: {amount} for {period.value} on {endpoint}"
        )

    def _get_cost(self, params: Dict[str, Any]) -> ActionResult:
        """Get cost since timestamp."""
        since = params.get("since")
        endpoint = params.get("endpoint")

        if since:
            since = float(since)

        cost = self._tracker.get_cost_since(endpoint, since)
        return ActionResult(success=True, data={"cost": cost})

    def _get_report(self, params: Dict[str, Any]) -> ActionResult:
        """Get cost report."""
        period_start = params.get("period_start", time.time() - 86400 * 30)
        period_end = params.get("period_end", time.time())
        endpoint = params.get("endpoint")

        report = self._tracker.get_cost_report(
            period_start=float(period_start),
            period_end=float(period_end),
            endpoint=endpoint
        )

        return ActionResult(
            success=True,
            data={
                "period_start": report.period_start,
                "period_end": report.period_end,
                "total_requests": report.total_requests,
                "total_bytes": report.total_bytes,
                "total_cost": report.total_cost,
                "cost_by_endpoint": report.cost_by_endpoint,
                "cost_by_method": report.cost_by_method,
                "average_cost_per_request": report.average_cost_per_request
            }
        )

    def _get_history(self, params: Dict[str, Any]) -> ActionResult:
        """Get usage history."""
        endpoint = params.get("endpoint")
        limit = params.get("limit", 100)

        history = self._tracker.get_usage_history(endpoint, limit)
        return ActionResult(success=True, data={"history": history})

    def _get_optimize(self, params: Dict[str, Any]) -> ActionResult:
        """Get optimization suggestions."""
        suggestions = self._tracker.optimize_suggestions()
        return ActionResult(success=True, data={"suggestions": suggestions})
