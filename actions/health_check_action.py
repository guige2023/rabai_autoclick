"""Health Check Action Module.

Provides health check system for monitoring
service and component health.
"""

import time
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class HealthStatus(Enum):
    """Health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheck:
    """Health check definition."""
    check_id: str
    name: str
    check_func: Callable
    interval_seconds: float = 60.0
    timeout_seconds: float = 10.0
    enabled: bool = True
    critical: bool = False


@dataclass
class HealthReport:
    """Health report."""
    check_id: str
    name: str
    status: HealthStatus
    message: Optional[str]
    timestamp: float
    latency_ms: Optional[float] = None


class HealthCheckManager:
    """Manages health checks."""

    def __init__(self):
        self._checks: Dict[str, HealthCheck] = {}
        self._reports: Dict[str, HealthReport] = {}
        self._lock = threading.Lock()
        self._last_full_check: Optional[float] = None

    def register(
        self,
        name: str,
        check_func: Callable,
        interval_seconds: float = 60.0,
        critical: bool = False
    ) -> str:
        """Register a health check."""
        check_id = f"check_{name.lower().replace(' ', '_')}"

        check = HealthCheck(
            check_id=check_id,
            name=name,
            check_func=check_func,
            interval_seconds=interval_seconds,
            critical=critical
        )

        with self._lock:
            self._checks[check_id] = check

        return check_id

    def unregister(self, check_id: str) -> bool:
        """Unregister a health check."""
        with self._lock:
            if check_id in self._checks:
                del self._checks[check_id]
                return True
        return False

    def run_check(self, check_id: str) -> HealthReport:
        """Run a single health check."""
        check = self._checks.get(check_id)
        if not check:
            return HealthReport(
                check_id=check_id,
                name=check_id,
                status=HealthStatus.UNKNOWN,
                message="Check not found",
                timestamp=time.time()
            )

        start = time.time()
        try:
            result = check.check_func()
            latency = (time.time() - start) * 1000

            if result is True:
                status = HealthStatus.HEALTHY
                message = "OK"
            elif result is False:
                status = HealthStatus.UNHEALTHY
                message = "Check failed"
            elif isinstance(result, dict):
                status = HealthStatus(result.get("status", "unknown"))
                message = result.get("message")
            else:
                status = HealthStatus.UNKNOWN
                message = str(result)

            report = HealthReport(
                check_id=check_id,
                name=check.name,
                status=status,
                message=message,
                timestamp=time.time(),
                latency_ms=latency
            )

        except Exception as e:
            report = HealthReport(
                check_id=check_id,
                name=check.name,
                status=HealthStatus.UNHEALTHY,
                message=str(e),
                timestamp=time.time(),
                latency_ms=(time.time() - start) * 1000
            )

        with self._lock:
            self._reports[check_id] = report

        return report

    def run_all_checks(self) -> Dict[str, Any]:
        """Run all health checks."""
        results = {}
        overall_status = HealthStatus.HEALTHY

        for check_id in self._checks:
            if not self._checks[check_id].enabled:
                continue

            report = self.run_check(check_id)
            results[check_id] = {
                "name": report.name,
                "status": report.status.value,
                "message": report.message,
                "latency_ms": report.latency_ms
            }

            if report.status == HealthStatus.UNHEALTHY:
                if self._checks[check_id].critical:
                    overall_status = HealthStatus.UNHEALTHY
                elif overall_status != HealthStatus.UNHEALTHY:
                    overall_status = HealthStatus.DEGRADED

        self._last_full_check = time.time()

        return {
            "overall_status": overall_status.value,
            "check_count": len(results),
            "checks": results,
            "timestamp": self._last_full_check
        }

    def get_report(self, check_id: str) -> Optional[HealthReport]:
        """Get last report for check."""
        return self._reports.get(check_id)


class HealthCheckAction(BaseAction):
    """Action for health check operations."""

    def __init__(self):
        super().__init__("health_check")
        self._manager = HealthCheckManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute health check action."""
        try:
            operation = params.get("operation", "register")

            if operation == "register":
                return self._register(params)
            elif operation == "unregister":
                return self._unregister(params)
            elif operation == "run":
                return self._run(params)
            elif operation == "run_all":
                return self._run_all(params)
            elif operation == "report":
                return self._report(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _register(self, params: Dict) -> ActionResult:
        """Register health check."""
        def placeholder():
            return True

        check_id = self._manager.register(
            name=params.get("name", ""),
            check_func=params.get("check_func") or placeholder,
            interval_seconds=params.get("interval_seconds", 60),
            critical=params.get("critical", False)
        )
        return ActionResult(success=True, data={"check_id": check_id})

    def _unregister(self, params: Dict) -> ActionResult:
        """Unregister health check."""
        success = self._manager.unregister(params.get("check_id", ""))
        return ActionResult(success=success)

    def _run(self, params: Dict) -> ActionResult:
        """Run single check."""
        report = self._manager.run_check(params.get("check_id", ""))
        return ActionResult(success=True, data={
            "check_id": report.check_id,
            "status": report.status.value,
            "message": report.message,
            "latency_ms": report.latency_ms
        })

    def _run_all(self, params: Dict) -> ActionResult:
        """Run all checks."""
        result = self._manager.run_all_checks()
        return ActionResult(success=True, data=result)

    def _report(self, params: Dict) -> ActionResult:
        """Get check report."""
        report = self._manager.get_report(params.get("check_id", ""))
        if not report:
            return ActionResult(success=False, message="Report not found")
        return ActionResult(success=True, data={
            "check_id": report.check_id,
            "status": report.status.value,
            "message": report.message
        })
