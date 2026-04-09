"""
API SLO (Service Level Objective) Monitor Action Module

Provides SLO and SLI monitoring for API services with budget tracking,
alerting, and burn rate analysis.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class SLOStatus(Enum):
    """SLO health status."""

    HEALTHY = "healthy"
    AT_RISK = "at_risk"
    BREACHED = "breached"


class AlertSeverity(Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class SLI:
    """Service Level Indicator configuration."""

    sli_id: str
    name: str
    metric_type: str
    target_value: float
    current_value: float = 0.0
    window_seconds: float = 86400.0


@dataclass
class SLO:
    """Service Level Objective."""

    slo_id: str
    name: str
    sli_id: str
    target: float
    window_start: float
    window_end: float
    total_requests: int = 0
    successful_requests: int = 0
    budget: float = 1.0
    consumed_budget: float = 0.0


@dataclass
class SLOAlert:
    """An SLO alert."""

    alert_id: str
    slo_id: str
    severity: AlertSeverity
    message: str
    burn_rate: float
    timestamp: float


@dataclass
class SLOConfig:
    """Configuration for SLO monitoring."""

    short_window_seconds: float = 3600.0
    long_window_seconds: float = 86400.0
    alert_burn_rate_threshold: float = 10.0
    critical_burn_rate_threshold: float = 50.0
    check_interval_seconds: float = 60.0
    budget_warning_threshold: float = 0.8


class BurnRateAnalyzer:
    """Analyzes SLO burn rates."""

    def __init__(self, config: Optional[SLOConfig] = None):
        self.config = config or SLOConfig()

    def calculate_burn_rate(
        self,
        slo: SLO,
        window_requests: int,
        window_failures: int,
    ) -> float:
        """Calculate the burn rate for an SLO."""
        if slo.total_requests == 0:
            return 0.0

        error_budget_consumed = window_failures / (slo.target * window_requests)
        time_elapsed = (time.time() - slo.window_start) / slo.window_end
        expected_budget_consumed = time_elapsed

        if expected_budget_consumed <= 0:
            return 0.0

        return error_budget_consumed / expected_budget_consumed


class APISLOAction:
    """
    SLO monitoring action for API services.

    Features:
    - SLO definition and tracking
    - Burn rate analysis (short and long windows)
    - Budget consumption tracking
    - Multi-level alerting (warning, critical)
    - SLI metric collection
    - Historical SLO reporting

    Usage:
        slo_monitor = APISLOAction(config)
        slo_monitor.define_slo("api-availability", target=0.999, window_days=30)
        
        async for alert in slo_monitor.monitor():
            await send_alert(alert)
    """

    def __init__(self, config: Optional[SLOConfig] = None):
        self.config = config or SLOConfig()
        self._burn_rate_analyzer = BurnRateAnalyzer(self.config)
        self._slis: Dict[str, SLI] = {}
        _slos: Dict[str, SLO] = {}
        self._alerts: List[SLOAlert] = []
        self._stats = {
            "slos_defined": 0,
            "checks_performed": 0,
            "alerts_generated": 0,
            "breaches_detected": 0,
        }

    def define_sli(
        self,
        name: str,
        metric_type: str,
        target_value: float,
        window_seconds: float = 86400.0,
    ) -> SLI:
        """Define an SLI."""
        sli_id = f"sli_{uuid.uuid4().hex[:8]}"
        sli = SLI(
            sli_id=sli_id,
            name=name,
            metric_type=metric_type,
            target_value=target_value,
            window_seconds=window_seconds,
        )
        self._slis[sli_id] = sli
        return sli

    def define_slo(
        self,
        name: str,
        sli_id: str,
        target: float,
        window_days: int = 30,
    ) -> SLO:
        """Define an SLO."""
        slo_id = f"slo_{uuid.uuid4().hex[:12]}"
        now = time.time()
        window_seconds = window_days * 86400.0

        slo = SLO(
            slo_id=slo_id,
            name=name,
            sli_id=sli_id,
            target=target,
            window_start=now,
            window_end=now + window_seconds,
        )
        self._slos[slo_id] = slo
        self._stats["slos_defined"] += 1
        return slo

    def record_request(
        self,
        slo_id: str,
        success: bool,
        latency_ms: Optional[float] = None,
    ) -> None:
        """Record a request against an SLO."""
        slo = self._slos.get(slo_id)
        if slo is None:
            return

        slo.total_requests += 1
        if success:
            slo.successful_requests += 1

        # Update budget
        if slo.total_requests > 0:
            current_rate = slo.successful_requests / slo.total_requests
            slo.consumed_budget = max(0, 1 - (current_rate / slo.target))

    def check_slo(self, slo_id: str) -> tuple[SLOStatus, Optional[float]]:
        """Check SLO health status."""
        slo = self._slos.get(slo_id)
        if slo is None:
            return SLOStatus.HEALTHY, None

        if slo.consumed_budget >= 1.0:
            return SLOStatus.BREACHED, None

        if slo.consumed_budget >= self.config.budget_warning_threshold:
            return SLOStatus.AT_RISK, slo.consumed_budget

        return SLOStatus.HEALTHY, slo.consumed_budget

    def calculate_burn_rate(self, slo_id: str) -> float:
        """Calculate burn rate for an SLO."""
        slo = self._slos.get(slo_id)
        if slo is None:
            return 0.0

        if slo.total_requests == 0:
            return 0.0

        window_failures = slo.total_requests - slo.successful_requests
        return self._burn_rate_analyzer.calculate_burn_rate(
            slo, slo.total_requests, window_failures
        )

    def check_alerts(self, slo_id: str) -> List[SLOAlert]:
        """Check if SLO should generate alerts."""
        slo = self._slos.get(slo_id)
        if slo is None:
            return []

        alerts = []
        burn_rate = self.calculate_burn_rate(slo_id)

        if burn_rate >= self.config.critical_burn_rate_threshold:
            alert = SLOAlert(
                alert_id=f"alert_{uuid.uuid4().hex[:8]}",
                slo_id=slo_id,
                severity=AlertSeverity.CRITICAL,
                message=f"Critical burn rate: {burn_rate:.1f}x",
                burn_rate=burn_rate,
                timestamp=time.time(),
            )
            alerts.append(alert)
            self._alerts.append(alert)
            self._stats["alerts_generated"] += 1
        elif burn_rate >= self.config.alert_burn_rate_threshold:
            alert = SLOAlert(
                alert_id=f"alert_{uuid.uuid4().hex[:8]}",
                slo_id=slo_id,
                severity=AlertSeverity.WARNING,
                message=f"High burn rate: {burn_rate:.1f}x",
                burn_rate=burn_rate,
                timestamp=time.time(),
            )
            alerts.append(alert)
            self._alerts.append(alert)
            self._stats["alerts_generated"] += 1

        if slo.consumed_budget >= 1.0:
            self._stats["breaches_detected"] += 1

        self._stats["checks_performed"] += 1
        return alerts

    def get_slo_status(self, slo_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed SLO status."""
        slo = self._slos.get(slo_id)
        if slo is None:
            return None

        status, budget = self.check_slo(slo_id)
        burn_rate = self.calculate_burn_rate(slo_id)

        return {
            "slo_id": slo.slo_id,
            "name": slo.name,
            "status": status.value,
            "target": f"{slo.target * 100:.3f}%",
            "current_rate": f"{(slo.successful_requests / slo.total_requests * 100) if slo.total_requests > 0 else 100:.3f}%",
            "budget_remaining": f"{(1 - slo.consumed_budget) * 100:.1f}%",
            "burn_rate": f"{burn_rate:.1f}x",
            "total_requests": slo.total_requests,
            "window_remaining_hours": max(0, (slo.window_end - time.time()) / 3600),
        }

    def get_all_alerts(self, limit: int = 100) -> List[SLOAlert]:
        """Get recent alerts."""
        return sorted(self._alerts, key=lambda a: -a.timestamp)[:limit]

    def get_stats(self) -> Dict[str, Any]:
        """Get SLO monitoring statistics."""
        return {
            **self._stats.copy(),
            "total_slos": len(self._slos),
            "total_slis": len(self._slis),
            "total_alerts": len(self._alerts),
        }


async def demo_slo():
    """Demonstrate SLO monitoring."""
    config = SLOConfig()
    monitor = APISLOAction(config)

    sli = monitor.define_sli("api-success-rate", "availability", 0.999)
    slo = monitor.define_slo("api-availability", sli.sli_id, 0.999, window_days=30)

    for i in range(1000):
        success = i < 998
        monitor.record_request(slo.slo_id, success)

    alerts = monitor.check_alerts(slo.slo_id)
    status = monitor.get_slo_status(slo.slo_id)

    print(f"SLO Status: {status}")
    print(f"Alerts: {len(alerts)}")
    print(f"Stats: {monitor.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_slo())
