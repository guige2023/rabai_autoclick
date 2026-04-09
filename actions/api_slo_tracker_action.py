"""API SLO Tracker Action.

Tracks Service Level Objectives (SLOs) for API endpoints including
availability, latency, error budget, and alerting thresholds.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional


class SLOStatus(Enum):
    """SLO health status."""
    HEALTHY = "healthy"
    AT_RISK = "at_risk"
    BREACHED = "breached"
    UNKNOWN = "unknown"


@dataclass
class SLOTarget:
    """SLO target specification."""
    name: str
    availability_target: float = 0.995
    latency_p50_target_ms: float = 100.0
    latency_p95_target_ms: float = 500.0
    latency_p99_target_ms: float = 1000.0
    error_budget_percent: float = 1.0
    window_days: int = 30


@dataclass
class SLOsnapshot:
    """Current SLO snapshot."""
    sli_name: str
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    error_rate: float = 0.0
    availability: float = 0.0
    latency_p50_ms: float = 0.0
    latency_p95_ms: float = 0.0
    latency_p99_ms: float = 0.0
    error_budget_remaining: float = 0.0
    error_budget_used: float = 0.0
    status: SLOStatus = SLOStatus.UNKNOWN
    timestamp: float = field(default_factory=time.time)


@dataclass
class ErrorBudgetAlert:
    """Error budget alert threshold."""
    name: str
    threshold_percent: float
    triggered: bool = False
    last_triggered: Optional[float] = None


class APISLOTrackerAction:
    """Tracks and reports SLO compliance for API endpoints."""

    def __init__(self) -> None:
        self._targets: Dict[str, SLOTarget] = {}
        self._snapshots: Dict[str, deque] = {}
        self._current_window: Dict[str, Dict[str, Any]] = {}
        self._alerts: List[ErrorBudgetAlert] = []
        self._max_window_size = 1000

    def register_slo(
        self,
        endpoint: str,
        availability_target: float = 0.995,
        latency_p50_target_ms: float = 100.0,
        latency_p95_target_ms: float = 500.0,
        latency_p99_target_ms: float = 1000.0,
        error_budget_percent: float = 1.0,
        window_days: int = 30,
    ) -> SLOTarget:
        """Register an SLO target for an endpoint."""
        target = SLOTarget(
            name=endpoint,
            availability_target=availability_target,
            latency_p50_target_ms=latency_p50_target_ms,
            latency_p95_target_ms=latency_p95_target_ms,
            latency_p99_target_ms=latency_p99_target_ms,
            error_budget_percent=error_budget_percent,
            window_days=window_days,
        )
        self._targets[endpoint] = target
        self._snapshots[endpoint] = deque(maxlen=self._max_window_size)
        self._init_window(endpoint)
        return target

    def _init_window(self, endpoint: str) -> None:
        """Initialize a tracking window for an endpoint."""
        self._current_window[endpoint] = {
            "requests": 0,
            "errors": 0,
            "latencies": [],
            "window_start": time.time(),
        }

    def record_request(
        self,
        endpoint: str,
        success: bool,
        latency_ms: float,
        timestamp: Optional[float] = None,
    ) -> None:
        """Record a request outcome for SLO tracking."""
        if endpoint not in self._current_window:
            self._init_window(endpoint)

        window = self._current_window[endpoint]
        window["requests"] += 1
        if not success:
            window["errors"] += 1
        window["latencies"].append(latency_ms)

    def record_success(
        self,
        endpoint: str,
        latency_ms: float,
    ) -> None:
        """Record a successful request."""
        self.record_request(endpoint, success=True, latency_ms=latency_ms)

    def record_error(
        self,
        endpoint: str,
        latency_ms: float = 0.0,
    ) -> None:
        """Record a failed request."""
        self.record_request(endpoint, success=False, latency_ms=latency_ms)

    def get_snapshot(self, endpoint: str) -> Optional[SLOsnapshot]:
        """Get current SLO snapshot for an endpoint."""
        if endpoint not in self._targets:
            return None

        target = self._targets[endpoint]
        window = self._current_window.get(endpoint)

        if not window or window["requests"] == 0:
            return SLOsnapshot(sli_name=endpoint)

        requests = window["requests"]
        errors = window["errors"]
        latencies = sorted(window["latencies"])

        error_rate = errors / requests
        availability = 1.0 - error_rate

        error_budget_total = requests * target.error_budget_percent
        error_budget_used = errors
        error_budget_remaining = max(0, error_budget_total - error_budget_used)

        p50_idx = int(len(latencies) * 0.50)
        p95_idx = int(len(latencies) * 0.95)
        p99_idx = int(len(latencies) * 0.99)

        status = self._determine_status(availability, target)

        return SLOsnapshot(
            sli_name=endpoint,
            total_requests=requests,
            successful_requests=requests - errors,
            failed_requests=errors,
            error_rate=error_rate,
            availability=availability,
            latency_p50_ms=latencies[p50_idx] if latencies else 0,
            latency_p95_ms=latencies[p95_idx] if latencies else 0,
            latency_p99_ms=latencies[p99_idx] if latencies else 0,
            error_budget_remaining=error_budget_remaining,
            error_budget_used=error_budget_used,
            status=status,
        )

    def _determine_status(
        self,
        availability: float,
        target: SLOTarget,
    ) -> SLOStatus:
        """Determine SLO health status."""
        if availability >= target.availability_target:
            return SLOStatus.HEALTHY
        elif availability >= target.availability_target * 0.95:
            return SLOStatus.AT_RISK
        else:
            return SLOStatus.BREACHED

    def get_all_snapshots(self) -> Dict[str, SLOsnapshot]:
        """Get SLO snapshots for all registered endpoints."""
        return {
            endpoint: snapshot
            for endpoint in self._targets
            if (snapshot := self.get_snapshot(endpoint)) is not None
        }

    def reset_window(self, endpoint: str) -> None:
        """Reset the tracking window for an endpoint."""
        if endpoint in self._current_window:
            snapshot = self.get_snapshot(endpoint)
            if snapshot:
                self._snapshots[endpoint].append(snapshot)
            self._init_window(endpoint)

    def add_alert(self, name: str, threshold_percent: float) -> None:
        """Add an error budget alert."""
        self._alerts.append(ErrorBudgetAlert(name=name, threshold_percent=threshold_percent))

    def check_alerts(self, endpoint: str) -> List[ErrorBudgetAlert]:
        """Check if any alerts should trigger."""
        snapshot = self.get_snapshot(endpoint)
        if not snapshot:
            return []

        triggered = []
        total_budget = snapshot.total_requests * 0.01
        if total_budget == 0:
            return []

        used_percent = (snapshot.error_budget_used / total_budget) * 100

        for alert in self._alerts:
            if used_percent >= alert.threshold_percent and not alert.triggered:
                alert.triggered = True
                alert.last_triggered = time.time()
                triggered.append(alert)

        return triggered

    def get_compliance_report(self) -> Dict[str, Any]:
        """Generate a compliance report for all SLOs."""
        snapshots = self.get_all_snapshots()
        healthy = sum(1 for s in snapshots.values() if s.status == SLOStatus.HEALTHY)
        at_risk = sum(1 for s in snapshots.values() if s.status == SLOStatus.AT_RISK)
        breached = sum(1 for s in snapshots.values() if s.status == SLOStatus.BREACHED)

        return {
            "generated_at": datetime.now().isoformat(),
            "total_slos": len(snapshots),
            "healthy": healthy,
            "at_risk": at_risk,
            "breached": breached,
            "endpoints": {
                endpoint: {
                    "status": s.status.value,
                    "availability": f"{s.availability * 100:.3f}%",
                    "error_rate": f"{s.error_rate * 100:.3f}%",
                    "p95_latency_ms": s.latency_p95_ms,
                    "error_budget_remaining": s.error_budget_remaining,
                }
                for endpoint, s in snapshots.items()
            },
        }

    def clear(self, endpoint: Optional[str] = None) -> None:
        """Clear tracking data."""
        if endpoint:
            if endpoint in self._current_window:
                self._init_window(endpoint)
            if endpoint in self._snapshots:
                self._snapshots[endpoint].clear()
        else:
            for ep in self._current_window:
                self._init_window(ep)
            for ep in self._snapshots:
                self._snapshots[ep].clear()
