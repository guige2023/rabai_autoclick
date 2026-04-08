"""API SLA Monitor Action Module.

Monitors API Service Level Agreements including
uptime, latency SLOs, and error budgets.
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
class SLOConfig:
    """SLO configuration."""
    name: str
    target: float
    window_seconds: int


class APISLAAction(BaseAction):
    """
    API SLA monitoring.

    Tracks SLOs for uptime, latency, and error rates
    with budget tracking.

    Example:
        sla = APISLAAction()
        result = sla.execute(ctx, {"action": "check_slo", "name": "availability"})
    """
    action_type = "api_sla"
    display_name = "API SLA监控"
    description = "API SLA监控：可用性、延迟和错误预算"

    def __init__(self) -> None:
        super().__init__()
        self._slos: Dict[str, SLOConfig] = {}
        self._measurements: Dict[str, List[Dict[str, Any]]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "define_slo":
                return self._define_slo(params)
            elif action == "record_measurement":
                return self._record_measurement(params)
            elif action == "check_slo":
                return self._check_slo(params)
            elif action == "get_budget":
                return self._get_budget(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"SLA error: {str(e)}")

    def _define_slo(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        target = params.get("target", 99.9)
        window = params.get("window_seconds", 86400 * 30)

        if not name:
            return ActionResult(success=False, message="name is required")

        self._slos[name] = SLOConfig(name=name, target=target, window_seconds=window)

        return ActionResult(success=True, message=f"SLO defined: {name} = {target}%")

    def _record_measurement(self, params: Dict[str, Any]) -> ActionResult:
        slo_name = params.get("slo_name", "")
        value = params.get("value", 0.0)
        timestamp = params.get("timestamp", time.time())

        if slo_name not in self._measurements:
            self._measurements[slo_name] = []

        self._measurements[slo_name].append({"value": value, "timestamp": timestamp})

        window = self._slos[slo_name].window_seconds if slo_name in self._slos else 86400
        cutoff = time.time() - window
        self._measurements[slo_name] = [m for m in self._measurements[slo_name] if m["timestamp"] > cutoff]

        return ActionResult(success=True, message=f"Recorded: {slo_name} = {value}")

    def _check_slo(self, params: Dict[str, Any]) -> ActionResult:
        slo_name = params.get("name", "")

        if slo_name not in self._slos:
            return ActionResult(success=False, message=f"SLO not found: {slo_name}")

        slo = self._slos[slo_name]
        measurements = self._measurements.get(slo_name, [])

        if not measurements:
            return ActionResult(success=True, data={"slo": slo_name, "current": 100.0, "target": slo.target, "met": True})

        current = sum(m["value"] for m in measurements) / len(measurements) * 100
        met = current >= slo.target

        return ActionResult(success=True, data={"slo": slo_name, "current": current, "target": slo.target, "met": met})

    def _get_budget(self, params: Dict[str, Any]) -> ActionResult:
        slo_name = params.get("name", "")

        if slo_name not in self._slos:
            return ActionResult(success=False, message=f"SLO not found: {slo_name}")

        slo = self._slos[slo_name]
        measurements = self._measurements.get(slo_name, [])

        total_budget = 100.0 - slo.target
        consumed = 0.0

        if measurements:
            consumed = (100.0 - sum(m["value"] for m in measurements) / len(measurements) * 100)

        remaining = max(0.0, total_budget - consumed)

        return ActionResult(success=True, data={"slo": slo_name, "total_budget_pct": total_budget, "consumed_pct": consumed, "remaining_pct": remaining})
