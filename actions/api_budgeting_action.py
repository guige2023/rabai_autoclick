"""
API Budgeting Action Module.

Tracks API usage against budgets per client/endpoint,
enforces quotas, and generates cost reports.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict
from actions.base_action import BaseAction


@dataclass
class BudgetStatus:
    """Budget usage status."""
    client_id: str
    current_usage: float
    budget_limit: float
    budget_percent: float
    within_budget: bool


class APIBudgetingAction(BaseAction):
    """Track and enforce API budgets."""

    def __init__(self) -> None:
        super().__init__("api_budgeting")
        self._budgets: dict[str, tuple[float, str]] = {}  # client -> (limit, period)
        self._usage: dict[str, list[float]] = defaultdict(list)

    def execute(self, context: dict, params: dict) -> dict:
        """
        Manage API budgets.

        Args:
            context: Execution context
            params: Parameters:
                - action: set_budget, check_budget, record_usage, report
                - client_id: Client identifier
                - budget_limit: Monthly/daily budget limit
                - period: monthly or daily
                - amount: Usage amount to record

        Returns:
            Budget status or confirmation
        """
        action = params.get("action", "check_budget")
        client_id = params.get("client_id", "default")

        if action == "set_budget":
            limit = params.get("budget_limit", 10000)
            period = params.get("period", "monthly")
            self._budgets[client_id] = (limit, period)
            return {"budget_set": True, "client_id": client_id, "limit": limit, "period": period}

        elif action == "check_budget":
            if client_id not in self._budgets:
                return {"within_budget": True, "current_usage": 0, "budget_limit": float("inf")}
            limit, period = self._budgets[client_id]
            current = sum(self._usage.get(client_id, []))
            return {
                "within_budget": current < limit,
                "current_usage": current,
                "budget_limit": limit,
                "budget_percent": (current / limit * 100) if limit > 0 else 0
            }

        elif action == "record_usage":
            amount = params.get("amount", 1)
            self._usage[client_id].append(amount)
            return {"recorded": True, "client_id": client_id, "amount": amount}

        elif action == "report":
            report = {}
            for client_id, (limit, period) in self._budgets.items():
                usage = sum(self._usage.get(client_id, []))
                report[client_id] = {
                    "usage": usage,
                    "limit": limit,
                    "percent": (usage / limit * 100) if limit > 0 else 0,
                    "period": period
                }
            return {"report": report}

        return {"error": f"Unknown action: {action}"}
