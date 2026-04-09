"""API SLO Tracker Action.

Tracks Service Level Objectives (SLOs) for APIs: availability,
latency, error rate targets with burn rate alerts.
"""
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import deque
import math


@dataclass
class SLOConfig:
    name: str
    availability_target: float = 0.995
    latency_target_ms: float = 200.0
    latency_percentile: int = 95
    error_rate_threshold: float = 0.01
    window_days: int = 30


@dataclass
class SLOStatus:
    sli_current: float
    sli_target: float
    budget_remaining: float
    budget_consumed: float
    events_in_window: int
    errors_in_window: int
    is_healthy: bool
    burn_rate: float
    days_until_budget_exhausted: Optional[float] = None


class APISLOTrackerAction:
    """Tracks SLOs and alerts on budget burn rate."""

    def __init__(self) -> None:
        self._slos: Dict[str, SLOConfig] = {}
        self._events: Dict[str, deque] = {}
        self._error_budget: Dict[str, float] = {}

    def register_slo(self, config: SLOConfig) -> None:
        self._slos[config.name] = config
        self._events[config.name] = deque(maxlen=10000)
        total_requests = 1_000_000  # Assuming baseline
        allowed_errors = total_requests * (1 - config.availability_target)
        self._error_budget[config.name] = allowed_errors

    def record(
        self,
        slo_name: str,
        latency_ms: float,
        success: bool,
        status_code: int = 200,
        timestamp: Optional[datetime] = None,
    ) -> None:
        if slo_name not in self._slos:
            return
        ts = timestamp or datetime.now()
        self._events[slo_name].append({
            "timestamp": ts,
            "latency_ms": latency_ms,
            "success": success,
            "status_code": status_code,
        })

    def _get_window(self, slo_name: str, window_days: int) -> List[Dict]:
        config = self._slos[slo_name]
        cutoff = datetime.now() - timedelta(days=window_days)
        return [e for e in self._events.get(slo_name, []) if e["timestamp"] > cutoff]

    def _compute_sli(self, slo_name: str, window_days: int) -> Tuple[float, int, int]:
        events = self._get_window(slo_name, window_days)
        if not events:
            return 1.0, 0, 0
        config = self._slos[slo_name]
        good = sum(1 for e in events
                   if e["success"] and e["latency_ms"] <= config.latency_target_ms)
        total = len(events)
        return good / total if total > 0 else 1.0, good, total - good

    def get_status(self, slo_name: str) -> Optional[SLOStatus]:
        if slo_name not in self._slos:
            return None
        config = self._slos[slo_name]
        sli, good, bad = self._compute_sli(slo_name, config.window_days)
        total_budget = self._error_budget.get(slo_name, 1.0)
        budget_consumed = bad
        budget_remaining = max(0.0, total_budget - budget_consumed)
        # Burn rate: how fast we're consuming budget vs time
        window_hours = config.window_days * 24
        elapsed_hours = window_hours  # simplified
        burn_rate = budget_consumed / total_budget if total_budget > 0 else 0.0
        days_remaining = None
        if burn_rate > 0:
            days_remaining = (budget_remaining / (budget_consumed / elapsed_hours * 24)) if budget_consumed > 0 else config.window_days
        return SLOStatus(
            sli_current=sli,
            sli_target=config.availability_target,
            budget_remaining=budget_remaining,
            budget_consumed=budget_consumed,
            events_in_window=good + bad,
            errors_in_window=bad,
            is_healthy=sli >= config.availability_target,
            burn_rate=burn_rate,
            days_until_budget_exhausted=days_remaining,
        )

    def all_statuses(self) -> Dict[str, Dict[str, Any]]:
        return {
            name: {
                "status": self.get_status(name).__dict__ if self.get_status(name) else {},
                "config": {"availability_target": c.availability_target,
                           "latency_target_ms": c.latency_target_ms,
                           "window_days": c.window_days},
            }
            for name, c in self._slos.items()
        }
