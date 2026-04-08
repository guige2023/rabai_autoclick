"""API Cost Action Module. Tracks API usage costs."""
import sys, os, time
from typing import Any, Optional
from dataclasses import dataclass, field
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class PricingTier:
    name: str; requests_included: int; cost_per_request: float
    data_included_gb: float; cost_per_gb: float

@dataclass
class CostReport:
    period_start: float; period_end: float; total_requests: int; total_data_gb: float
    estimated_cost_cents: float; by_endpoint: dict = field(default_factory=dict)
    budget_limit_cents: Optional[float] = None; budget_remaining_cents: Optional[float] = None
    over_budget: bool = False

class APICostAction(BaseAction):
    action_type = "api_cost"; display_name = "API成本追踪"
    description = "追踪API使用成本"
    def __init__(self) -> None:
        super().__init__(); self._request_log = []
        self._tiers = [PricingTier("free", 1000, 0, 1.0, 0),
                       PricingTier("tier1", 10000, 0.5, 10.0, 10.0),
                       PricingTier("tier2", 100000, 0.3, 100.0, 5.0)]
        self._budget_cents = None
    def execute(self, context: Any, params: dict) -> ActionResult:
        mode = params.get("mode", "report")
        endpoint = params.get("endpoint", "unknown")
        req_size = params.get("request_size_bytes", 0)
        resp_size = params.get("response_size_bytes", 0)
        tier_name = params.get("tier", "tier1")
        period_days = params.get("period_days", 30)
        if mode == "track":
            self._request_log.append({"timestamp": time.time(), "endpoint": endpoint,
                                      "request_bytes": req_size, "response_bytes": resp_size})
            cutoff = time.time() - period_days * 86400
            self._request_log = [r for r in self._request_log if r["timestamp"] >= cutoff]
            return ActionResult(success=True, message=f"Tracked {endpoint}", data={"total": len(self._request_log)})
        if mode == "set_budget":
            budget = params.get("budget_cents")
            if budget is not None: self._budget_cents = float(budget)
            return ActionResult(success=True, message=f"Budget ${self._budget_cents/100:.2f}" if self._budget_cents else "Budget cleared")
        cutoff = time.time() - period_days * 86400
        window = [r for r in self._request_log if r["timestamp"] >= cutoff]
        tier = next((t for t in self._tiers if t.name == tier_name), self._tiers[1])
        total_req = len(window)
        total_data = sum(r.get("request_bytes",0)+r.get("response_bytes",0) for r in window) / (1024**3)
        incl_req = min(total_req, tier.requests_included)
        over_req = max(0, total_req - tier.requests_included)
        incl_data = min(total_data, tier.data_included_gb)
        over_data = max(0.0, total_data - tier.data_included_gb)
        cost = (incl_req*tier.cost_per_request + over_req*tier.cost_per_request*1.5 +
                incl_data*1000*tier.cost_per_gb + over_data*1000*tier.cost_per_gb*1.5)
        by_endpoint = {}
        for r in window:
            ep = r.get("endpoint","unknown")
            if ep not in by_endpoint: by_endpoint[ep] = {"requests": 0, "bytes": 0}
            by_endpoint[ep]["requests"] += 1
            by_endpoint[ep]["bytes"] += r.get("request_bytes",0)+r.get("response_bytes",0)
        over_budget = False; budget_remaining = None
        if self._budget_cents is not None:
            over_budget = cost > self._budget_cents
            budget_remaining = max(0.0, self._budget_cents - cost)
        report = CostReport(period_start=window[0]["timestamp"] if window else time.time(),
                           period_end=time.time(), total_requests=total_req,
                           total_data_gb=total_data, estimated_cost_cents=cost,
                           by_endpoint=by_endpoint, budget_limit_cents=self._budget_cents,
                           budget_remaining_cents=budget_remaining, over_budget=over_budget)
        return ActionResult(success=True, message=f"Cost: {total_req} reqs, ${cost/100:.4f}", data=report)
