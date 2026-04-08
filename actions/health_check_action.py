"""Health Check Action Module.

Provides health check for services
and dependencies.
"""

import time
from typing import Any, Callable, Dict, List, Optional
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


@dataclass
class HealthCheck:
    """Health check entry."""
    check_id: str
    name: str
    checker: Callable
    status: HealthStatus = HealthStatus.HEALTHY
    last_check: float = 0
    message: str = ""


class HealthCheckManager:
    """Manages health checks."""

    def __init__(self):
        self._checks: Dict[str, HealthCheck] = {}

    def register(
        self,
        name: str,
        checker: Callable
    ) -> str:
        """Register health check."""
        check_id = f"health_{name.lower().replace(' ', '_')}"

        self._checks[check_id] = HealthCheck(
            check_id=check_id,
            name=name,
            checker=checker
        )

        return check_id

    def run_check(self, check_id: str) -> HealthStatus:
        """Run a health check."""
        check = self._checks.get(check_id)
        if not check:
            return HealthStatus.UNHEALTHY

        try:
            result = check.checker()
            check.status = HealthStatus.HEALTHY if result else HealthStatus.UNHEALTHY
            check.message = "OK" if result else "Failed"
        except Exception as e:
            check.status = HealthStatus.UNHEALTHY
            check.message = str(e)

        check.last_check = time.time()
        return check.status

    def run_all(self) -> Dict[str, Any]:
        """Run all health checks."""
        results = {}
        overall_status = HealthStatus.HEALTHY

        for check_id, check in self._checks.items():
            status = self.run_check(check_id)
            results[check_id] = {
                "name": check.name,
                "status": status.value,
                "message": check.message,
                "last_check": check.last_check
            }

            if status == HealthStatus.UNHEALTHY:
                overall_status = HealthStatus.UNHEALTHY
            elif status == HealthStatus.DEGRADED and overall_status == HealthStatus.HEALTHY:
                overall_status = HealthStatus.DEGRADED

        return {
            "overall": overall_status.value,
            "checks": results
        }


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
            elif operation == "run":
                return self._run(params)
            elif operation == "run_all":
                return self._run_all(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _register(self, params: Dict) -> ActionResult:
        """Register health check."""
        def default_checker():
            return True

        check_id = self._manager.register(
            name=params.get("name", ""),
            checker=params.get("checker") or default_checker
        )
        return ActionResult(success=True, data={"check_id": check_id})

    def _run(self, params: Dict) -> ActionResult:
        """Run single check."""
        status = self._manager.run_check(params.get("check_id", ""))
        return ActionResult(success=True, data={"status": status.value})

    def _run_all(self, params: Dict) -> ActionResult:
        """Run all checks."""
        result = self._manager.run_all()
        return ActionResult(success=True, data=result)
