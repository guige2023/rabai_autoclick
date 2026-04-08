"""API Cost Action Module.

Tracks and allocates API usage costs by client,
endpoint, and time period.
"""

from __future__ import annotations

import sys
import os
import time
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class CostEntry:
    """A cost entry."""
    timestamp: float
    client_id: str
    endpoint: str
    requests: int
    cost_usd: float


class APICostAction(BaseAction):
    """
    API cost tracking and allocation.

    Tracks API usage costs by client, endpoint,
    and time period for billing and optimization.

    Example:
        cost_tracker = APICostAction()
        result = cost_tracker.execute(ctx, {"action": "record", "client_id": "app-1", "requests": 1000})
    """
    action_type = "api_cost"
    display_name = "API成本跟踪"
    description = "API使用成本跟踪和分配"

    def __init__(self) -> None:
        super().__init__()
        self._entries: List[CostEntry] = []
        self._rate_per_request: float = 0.0001

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "record":
                return self._record_cost(params)
            elif action == "get_cost":
                return self._get_cost(params)
            elif action == "set_rate":
                return self._set_rate(params)
            elif action == "get_report":
                return self._get_report(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Cost error: {str(e)}")

    def _record_cost(self, params: Dict[str, Any]) -> ActionResult:
        client_id = params.get("client_id", "")
        endpoint = params.get("endpoint", "/")
        requests = params.get("requests", 1)

        cost = requests * self._rate_per_request

        entry = CostEntry(timestamp=time.time(), client_id=client_id, endpoint=endpoint, requests=requests, cost_usd=cost)
        self._entries.append(entry)

        return ActionResult(success=True, message=f"Recorded: {requests} requests for {client_id}", data={"cost_usd": cost})

    def _get_cost(self, params: Dict[str, Any]) -> ActionResult:
        client_id = params.get("client_id", "")
        start_time = params.get("start_time", time.time() - 86400)
        end_time = params.get("end_time", time.time())

        filtered = [e for e in self._entries if e.client_id == client_id and start_time <= e.timestamp <= end_time]
        total_cost = sum(e.cost_usd for e in filtered)
        total_requests = sum(e.requests for e in filtered)

        return ActionResult(success=True, data={"client_id": client_id, "total_cost_usd": total_cost, "total_requests": total_requests, "period_requests": len(filtered)})

    def _set_rate(self, params: Dict[str, Any]) -> ActionResult:
        rate = params.get("rate_per_request", 0.0001)
        self._rate_per_request = rate
        return ActionResult(success=True, message=f"Rate set: ${rate}/request")

    def _get_report(self, params: Dict[str, Any]) -> ActionResult:
        start_time = params.get("start_time", time.time() - 86400)
        end_time = params.get("end_time", time.time())

        filtered = [e for e in self._entries if start_time <= e.timestamp <= end_time]

        by_client: Dict[str, Dict[str, Any]] = {}
        for entry in filtered:
            if entry.client_id not in by_client:
                by_client[entry.client_id] = {"cost_usd": 0.0, "requests": 0}
            by_client[entry.client_id]["cost_usd"] += entry.cost_usd
            by_client[entry.client_id]["requests"] += entry.requests

        total_cost = sum(e.cost_usd for e in filtered)
        total_requests = sum(e.requests for e in filtered)

        return ActionResult(success=True, data={"total_cost_usd": total_cost, "total_requests": total_requests, "by_client": by_client})
