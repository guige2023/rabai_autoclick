"""
API Budget Action Module.

Budget tracking and cost management for API usage with quota enforcement,
spending alerts, and usage analytics.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class BudgetPeriod(Enum):
    """Budget tracking periods."""
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"


@dataclass
class Budget:
    """A budget limit for API usage."""
    name: str
    limit: float
    period: BudgetPeriod
    scope: str = "global"  # "global" or API name
    alert_threshold: float = 0.8  # Alert at 80%


@dataclass
class BudgetUsage:
    """Current usage of a budget."""
    budget_name: str
    used: float = 0.0
    remaining: float = 0.0
    percent_used: float = 0.0
    window_start: float = field(default_factory=time.time)
    window_end: float = 0.0


@dataclass
class SpendingAlert:
    """A spending alert triggered when a budget threshold is crossed."""
    budget_name: str
    alert_type: str  # "warning", "critical", "exceeded"
    percent_used: float
    used: float
    limit: float


@dataclass
class BudgetReport:
    """Complete budget status report."""
    budgets: List[BudgetUsage]
    total_spent: float
    active_alerts: List[SpendingAlert]
    timestamp: float = field(default_factory=time.time)


class BudgetEngine:
    """Core budget tracking and enforcement engine."""

    def __init__(self) -> None:
        self._budgets: Dict[str, Budget] = {}
        self._usage: Dict[str, BudgetUsage] = {}
        self._alerts: List[SpendingAlert] = []

    def add_budget(self, budget: Budget) -> None:
        """Add a new budget."""
        self._budgets[budget.name] = budget
        window_end = self._calculate_window_end(budget.period)
        self._usage[budget.name] = BudgetUsage(
            budget_name=budget.name,
            limit=budget.limit,
            window_start=time.time(),
            window_end=window_end,
        )
        logger.info(f"Added budget '{budget.name}': {budget.limit} per {budget.period.value}")

    def remove_budget(self, name: str) -> bool:
        """Remove a budget."""
        if name in self._budgets:
            del self._budgets[name]
            del self._usage[name]
            return True
        return False

    def record_usage(
        self,
        budget_name: str,
        amount: float,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> BudgetUsage:
        """Record API usage against a budget."""
        if budget_name not in self._budgets:
            raise KeyError(f"Budget '{budget_name}' not found")

        budget = self._budgets[budget_name]
        usage = self._usage[budget_name]

        # Check if window has expired
        if time.time() >= usage.window_end:
            # Reset window
            usage = self._reset_window(budget_name)
            self._usage[budget_name] = usage

        # Add usage
        usage.used += amount
        usage.remaining = max(0.0, budget.limit - usage.used)
        usage.percent_used = (usage.used / budget.limit) if budget.limit > 0 else 0.0

        # Check for alerts
        self._check_alerts(budget, usage)

        return usage

    def _calculate_window_end(self, period: BudgetPeriod) -> float:
        """Calculate the end of the current budget window."""
        now = time.time()
        if period == BudgetPeriod.SECOND:
            return now + 1.0
        elif period == BudgetPeriod.MINUTE:
            return now + 60.0
        elif period == BudgetPeriod.HOUR:
            return now + 3600.0
        elif period == BudgetPeriod.DAY:
            return now + 86400.0
        elif period == BudgetPeriod.WEEK:
            return now + 604800.0
        elif period == BudgetPeriod.MONTH:
            return now + 2592000.0  # ~30 days
        return now + 60.0

    def _reset_window(self, budget_name: str) -> BudgetUsage:
        """Reset usage for a new budget window."""
        budget = self._budgets[budget_name]
        window_end = self._calculate_window_end(budget.period)
        return BudgetUsage(
            budget_name=budget_name,
            used=0.0,
            remaining=budget.limit,
            percent_used=0.0,
            window_start=time.time(),
            window_end=window_end,
        )

    def _check_alerts(self, budget: Budget, usage: BudgetUsage) -> None:
        """Check if any alert thresholds have been crossed."""
        if usage.percent_used >= 1.0:
            alert = SpendingAlert(
                budget_name=budget.name,
                alert_type="exceeded",
                percent_used=usage.percent_used,
                used=usage.used,
                limit=budget.limit,
            )
            if not any(a.alert_type == "exceeded" and a.budget_name == budget.name for a in self._alerts):
                self._alerts.append(alert)
                logger.warning(f"Budget '{budget.name}' EXCEEDED: {usage.percent_used:.1%}")

        elif usage.percent_used >= budget.alert_threshold:
            alert = SpendingAlert(
                budget_name=budget.name,
                alert_type="warning",
                percent_used=usage.percent_used,
                used=usage.used,
                limit=budget.limit,
            )
            existing = [a for a in self._alerts if a.budget_name == budget.name and a.alert_type == "warning"]
            if not existing:
                self._alerts.append(alert)
                logger.warning(f"Budget '{budget.name}' warning: {usage.percent_used:.1%}")

    def check_limit(self, budget_name: str, amount: float = 1.0) -> bool:
        """Check if adding amount would exceed budget limit."""
        if budget_name not in self._budgets:
            return True

        budget = self._budgets[budget_name]
        usage = self._usage[budget_name]

        if time.time() >= usage.window_end:
            return True  # Window reset will happen on next record

        return (usage.used + amount) <= budget.limit

    def get_usage(self, budget_name: str) -> Optional[BudgetUsage]:
        """Get current usage for a budget."""
        return self._usage.get(budget_name)

    def get_all_usage(self) -> List[BudgetUsage]:
        """Get all budget usage."""
        return list(self._usage.values())

    def get_alerts(self) -> List[SpendingAlert]:
        """Get current alerts."""
        return self._alerts.copy()

    def clear_alerts(self) -> None:
        """Clear all alerts."""
        self._alerts = []


class APIBudgetAction:
    """
    API budget management and cost control.

    Tracks API usage against configurable budgets, enforces limits,
    and provides spending alerts.

    Example:
        budget_manager = APIBudgetAction()
        budget_manager.add_budget(Budget(
            name="monthly-requests",
            limit=10000,
            period=BudgetPeriod.MONTH,
        ))

        # Before API call
        if not budget_manager.check_limit("monthly-requests"):
            raise RuntimeError("Budget exceeded")

        budget_manager.record_usage("monthly-requests", 1)
    """

    def __init__(self) -> None:
        self.engine = BudgetEngine()
        self._on_alert: Optional[Callable[[SpendingAlert], None]] = None

    def add_budget(
        self,
        name: str,
        limit: float,
        period: BudgetPeriod,
        scope: str = "global",
        alert_threshold: float = 0.8,
    ) -> None:
        """Add a budget."""
        budget = Budget(
            name=name,
            limit=limit,
            period=period,
            scope=scope,
            alert_threshold=alert_threshold,
        )
        self.engine.add_budget(budget)

    def remove_budget(self, name: str) -> bool:
        """Remove a budget."""
        return self.engine.remove_budget(name)

    def record_usage(
        self,
        budget_name: str,
        amount: float = 1.0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> BudgetUsage:
        """Record usage against a budget."""
        return self.engine.record_usage(budget_name, amount, metadata)

    def check_limit(
        self,
        budget_name: str,
        amount: float = 1.0,
    ) -> bool:
        """Check if an amount can be added without exceeding the budget."""
        return self.engine.check_limit(budget_name, amount)

    def get_report(self) -> BudgetReport:
        """Get a complete budget status report."""
        usage = self.engine.get_all_usage()
        total = sum(u.used for u in usage)
        return BudgetReport(
            budgets=usage,
            total_spent=total,
            active_alerts=self.engine.get_alerts(),
        )

    def set_alert_handler(
        self,
        handler: Callable[[SpendingAlert], None],
    ) -> None:
        """Set a handler for spending alerts."""
        self._on_alert = handler

    async def start_monitoring(
        self,
        check_interval: float = 60.0,
    ) -> None:
        """Start periodic budget monitoring."""
        while True:
            report = self.get_report()
            if report.active_alerts and self._on_alert:
                for alert in report.active_alerts:
                    try:
                        self._on_alert(alert)
                    except Exception as e:
                        logger.error(f"Alert handler failed: {e}")
            await asyncio.sleep(check_interval)
