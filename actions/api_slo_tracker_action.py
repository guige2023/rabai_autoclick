"""API SLO Tracker Action Module.

Provides SLO (Service Level Objective) and SLI (Service Level Indicator)
tracking for monitoring API reliability and performance.
"""

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class WindowType(Enum):
    """SLO measurement window types."""
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"


@dataclass
class SLOConfig:
    """SLO configuration."""
    name: str
    target: float  # Target percentage (e.g., 99.9 for 99.9%)
    window: WindowType = WindowType.HOUR
    sli_type: str = "availability"  # availability, latency, throughput
    latency_threshold_ms: float = 1000.0  # For latency SLI


@dataclass
class SLI measurement:
    """A single SLI measurement."""
    timestamp: float
    success: bool
    latency_ms: float
    value: float = 1.0  # For throughput SLI


@dataclass
class SLOStatus:
    """Current SLO status."""
    name: str
    current_value: float
    target: float
    window: WindowType
    breaches: int = 0
    events: int = 0
    good_events: int = 0
    error_budget_remaining: float = 0.0
    time_remaining: float = 0.0


class APISLOTrackerAction(BaseAction):
    """SLO/SLI tracking action.

    Tracks API service level objectives and indicators,
    calculates error budgets, and reports status.

    Args:
        context: Execution context.
        params: Dict with keys:
            - operation: Operation (define_slo, record, get_status, get_budget, report)
            - slo: SLO definition dict
            - measurement: Measurement dict {success, latency_ms, value}
            - slo_name: Name of SLO for status operations
            - long_window: Whether to use day-long window for SLO
    """
    action_type = "api_slo_tracker"
    display_name = "API SLO追踪"
    description = "服务等级目标与指标追踪"

    def get_required_params(self) -> List[str]:
        return ["operation"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "slo": None,
            "measurement": None,
            "slo_name": None,
            "dataset_id": "default",
        }

    def __init__(self) -> None:
        super().__init__()
        self._slos: Dict[str, SLOConfig] = {}
        self._measurements: Dict[str, deque] = {}
        self._window_seconds = {
            WindowType.MINUTE: 60,
            WindowType.HOUR: 3600,
            WindowType.DAY: 86400,
        }

    def _get_window_seconds(self, window: WindowType) -> float:
        return self._window_seconds.get(window, 3600)

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SLO tracking operation."""
        start_time = time.time()

        operation = params.get("operation", "status")
        slo = params.get("slo")
        measurement = params.get("measurement")
        slo_name = params.get("slo_name")
        dataset_id = params.get("dataset_id", "default")

        if operation == "define_slo":
            return self._define_slo(slo, start_time)
        elif operation == "record":
            return self._record_measurement(measurement, slo_name, dataset_id, start_time)
        elif operation == "get_status":
            return self._get_slo_status(slo_name, dataset_id, start_time)
        elif operation == "get_budget":
            return self._get_error_budget(slo_name, dataset_id, start_time)
        elif operation == "burn_rate":
            return self._get_burn_rate(slo_name, dataset_id, start_time)
        elif operation == "report":
            return self._generate_report(dataset_id, start_time)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}",
                duration=time.time() - start_time
            )

    def _define_slo(self, slo: Optional[Dict], start_time: float) -> ActionResult:
        """Define a new SLO."""
        if not slo:
            return ActionResult(success=False, message="SLO definition required", duration=time.time() - start_time)

        name = slo.get("name")
        if not name:
            return ActionResult(success=False, message="SLO name required", duration=time.time() - start_time)

        window_str = slo.get("window", "hour")
        try:
            window = WindowType(window_str)
        except ValueError:
            window = WindowType.HOUR

        config = SLOConfig(
            name=name,
            target=float(slo.get("target", 99.9)),
            window=window,
            sli_type=slo.get("sli_type", "availability"),
            latency_threshold_ms=float(slo.get("latency_threshold_ms", 1000)),
        )

        self._slos[name] = config
        self._measurements[name] = deque(maxlen=10000)

        return ActionResult(
            success=True,
            message=f"SLO '{name}' defined: {config.target}% {config.sli_type} over {config.window.value}",
            data={
                "slo_name": name,
                "target": config.target,
                "window": config.window.value,
                "sli_type": config.sli_type,
            },
            duration=time.time() - start_time
        )

    def _record_measurement(
        self,
        measurement: Optional[Dict],
        slo_name: Optional[str],
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Record an SLI measurement."""
        if not measurement:
            return ActionResult(success=False, message="measurement required", duration=time.time() - start_time)

        # If slo_name not provided, apply to all SLOS
        target_slos = [slo_name] if slo_name else list(self._slos.keys())
        if not target_slos:
            return ActionResult(success=False, message="No SLO defined", duration=time.time() - start_time)

        success = measurement.get("success", True)
        latency_ms = float(measurement.get("latency_ms", 0))
        value = float(measurement.get("value", 1.0))
        timestamp = measurement.get("timestamp", time.time())

        recorded = 0
        for name in target_slos:
            if name in self._slos:
                meas = SLI measurement(
                    timestamp=timestamp,
                    success=success,
                    latency_ms=latency_ms,
                    value=value,
                )
                self._measurements[name].append(meas)
                recorded += 1

        return ActionResult(
            success=True,
            message=f"Recorded measurement to {recorded} SLO(s)",
            data={
                "recorded_count": recorded,
                "success": success,
                "latency_ms": latency_ms,
            },
            duration=time.time() - start_time
        )

    def _get_slo_status(
        self,
        slo_name: Optional[str],
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Get current SLO status."""
        if not slo_name:
            return ActionResult(success=False, message="slo_name required", duration=time.time() - start_time)

        if slo_name not in self._slos:
            return ActionResult(success=False, message=f"SLO '{slo_name}' not defined", duration=time.time() - start_time)

        config = self._slos[slo_name]
        measurements = self._measurements.get(slo_name, deque())
        window_sec = self._get_window_seconds(config.window)

        now = time.time()
        cutoff = now - window_sec
        recent = [m for m in measurements if m.timestamp >= cutoff]

        if not recent:
            return ActionResult(
                success=True,
                message=f"No measurements in window for '{slo_name}'",
                data={"slo_name": slo_name, "has_data": False},
                duration=time.time() - start_time
            )

        total = len(recent)
        good = sum(1 for m in recent if self._is_good(config, m))
        current_value = (good / total * 100) if total > 0 else 0.0
        breached = current_value < config.target

        # Error budget calculation
        error_budget_pct = 100 - config.target
        actual_error_pct = 100 - current_value
        budget_remaining = max(0, error_budget_pct - actual_error_pct)
        time_remaining = window_sec - (now % window_sec)

        return ActionResult(
            success=True,
            message=f"SLO '{slo_name}': {current_value:.3f}% (target: {config.target}%)",
            data={
                "slo_name": slo_name,
                "current_value": current_value,
                "target": config.target,
                "window": config.window.value,
                "breached": breached,
                "total_events": total,
                "good_events": good,
                "bad_events": total - good,
                "error_budget_remaining_pct": budget_remaining,
                "time_remaining_sec": time_remaining,
            },
            duration=time.time() - start_time
        )

    def _is_good(self, config: SLOConfig, m: SLI measurement) -> bool:
        """Determine if a measurement meets the SLO."""
        if config.sli_type == "availability":
            return m.success
        elif config.sli_type == "latency":
            return m.latency_ms <= config.latency_threshold_ms
        elif config.sli_type == "throughput":
            return m.value >= config.latency_threshold_ms  # Using threshold as min throughput
        return m.success

    def _get_error_budget(
        self,
        slo_name: Optional[str],
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Get error budget status."""
        if not slo_name or slo_name not in self._slos:
            return ActionResult(success=False, message=f"SLO '{slo_name}' not found", duration=time.time() - start_time)

        config = self._slos[slo_name]
        measurements = self._measurements.get(slo_name, deque())
        window_sec = self._get_window_seconds(config.window)

        cutoff = time.time() - window_sec
        recent = [m for m in measurements if m.timestamp >= cutoff]

        total = len(recent)
        good = sum(1 for m in recent if self._is_good(config, m))
        current_pct = (good / total * 100) if total > 0 else 100.0

        error_budget_total = 100 - config.target
        error_consumed = 100 - current_pct
        budget_remaining = max(0, error_budget_total - error_consumed)

        # Calculate burn rate
        burn_rate = (error_consumed / error_budget_total) if error_budget_total > 0 else 0
        time_budget_exhausted = (budget_remaining / burn_rate * window_sec) if burn_rate > 0 else float('inf')

        return ActionResult(
            success=True,
            message=f"Error budget for '{slo_name}'",
            data={
                "slo_name": slo_name,
                "target": config.target,
                "error_budget_total_pct": error_budget_total,
                "error_budget_consumed_pct": error_consumed,
                "error_budget_remaining_pct": budget_remaining,
                "burn_rate": burn_rate,
                "time_until_budget_exhausted_sec": time_budget_exhausted if time_budget_exhausted != float('inf') else -1,
                "events_in_window": total,
            },
            duration=time.time() - start_time
        )

    def _get_burn_rate(
        self,
        slo_name: Optional[str],
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Get burn rate for an SLO."""
        if not slo_name or slo_name not in self._slos:
            return ActionResult(success=False, message=f"SLO '{slo_name}' not found", duration=time.time() - start_time)

        config = self._slos[slo_name]
        measurements = self._measurements.get(slo_name, deque())
        window_sec = self._get_window_seconds(config.window)

        cutoff = time.time() - window_sec
        recent = [m for m in measurements if m.timestamp >= cutoff]

        total = len(recent)
        good = sum(1 for m in recent if self._is_good(config, m))
        current_pct = (good / total * 100) if total > 0 else 100.0

        error_budget_total = 100 - config.target
        error_rate = max(0, 100 - current_pct)
        burn_rate = error_rate / error_budget_total if error_budget_total > 0 else 0

        return ActionResult(
            success=True,
            message=f"Burn rate for '{slo_name}': {burn_rate:.2f}x",
            data={
                "slo_name": slo_name,
                "burn_rate": burn_rate,
                "window": config.window.value,
                "events_in_window": total,
                "current_value": current_pct,
                "target": config.target,
            },
            duration=time.time() - start_time
        )

    def _generate_report(self, dataset_id: str, start_time: float) -> ActionResult:
        """Generate SLO report for all SLOS."""
        report = []
        for name, config in self._slos.items():
            measurements = self._measurements.get(name, deque())
            window_sec = self._get_window_seconds(config.window)
            cutoff = time.time() - window_sec
            recent = [m for m in measurements if m.timestamp >= cutoff]

            total = len(recent)
            good = sum(1 for m in recent if self._is_good(config, m))
            current = (good / total * 100) if total > 0 else 100.0

            report.append({
                "slo_name": name,
                "target": config.target,
                "current": current,
                "window": config.window.value,
                "events": total,
                "breached": current < config.target,
            })

        return ActionResult(
            success=True,
            message=f"SLO Report: {len(report)} SLOs tracked",
            data={
                "report": report,
                "total_slos": len(report),
                "slos_met": sum(1 for r in report if not r["breached"]),
                "slos_breached": sum(1 for r in report if r["breached"]),
            },
            duration=time.time() - start_time
        )
