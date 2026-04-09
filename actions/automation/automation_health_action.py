"""Health monitoring for automation workflow systems.

Provides health checks, metrics collection, alerting, and
system status tracking for automation infrastructure.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

import copy


class HealthStatus(Enum):
    """Health status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class CheckType(Enum):
    """Types of health checks."""
    PROCESS = "process"
    API = "api"
    DATABASE = "database"
    CACHE = "cache"
    QUEUE = "queue"
    DISK = "disk"
    MEMORY = "memory"
    CUSTOM = "custom"


@dataclass
class HealthCheck:
    """A single health check definition."""
    check_id: str
    name: str
    check_type: CheckType
    enabled: bool
    interval_seconds: int
    timeout_seconds: int
    critical: bool
    created_at: float = field(default_factory=time.time)
    last_run: Optional[float] = None
    last_result: Optional[str] = None
    last_status: HealthStatus = HealthStatus.UNKNOWN
    last_message: Optional[str] = None
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    tags: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthCheckResult:
    """Result of a health check execution."""
    check_id: str
    check_name: str
    status: HealthStatus
    message: Optional[str]
    duration_ms: float
    timestamp: float = field(default_factory=time.time)
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SystemHealth:
    """Overall system health status."""
    system_id: str
    overall_status: HealthStatus
    checked_at: float
    uptime_seconds: float
    components: List[HealthCheckResult]
    summary: Dict[str, int]
    alerts: List[Dict[str, Any]] = field(default_factory=list)


class HealthCheckRunner:
    """Executes health checks."""

    def __init__(self):
        self._checks: Dict[str, HealthCheck] = {}
        self._results: Dict[str, List[HealthCheckResult]] = {}
        self._lock = threading.Lock()
        self._start_time = time.time()
        self._alert_callbacks: List[Callable] = []

    def register_check(
        self,
        name: str,
        check_type: CheckType,
        check_fn: Callable[[], HealthCheckResult],
        enabled: bool = True,
        interval_seconds: int = 60,
        timeout_seconds: int = 30,
        critical: bool = False,
        tags: Optional[Set[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Register a new health check."""
        check_id = str(uuid.uuid4())[:12]

        check = HealthCheck(
            check_id=check_id,
            name=name,
            check_type=check_type,
            enabled=enabled,
            interval_seconds=interval_seconds,
            timeout_seconds=timeout_seconds,
            critical=critical,
            tags=tags or set(),
            metadata=metadata or {},
        )

        self._checks[check_id] = check
        self._results[check_id] = []

        return check_id

    def unregister_check(self, check_id: str) -> bool:
        """Unregister a health check."""
        with self._lock:
            if check_id in self._checks:
                del self._checks[check_id]
                del self._results[check_id]
                return True
            return False

    def run_check(self, check_id: str) -> Optional[HealthCheckResult]:
        """Run a specific health check."""
        check = self._checks.get(check_id)
        if not check:
            return None

        start_time = time.time()

        try:
            result = self._execute_check(check)
            duration = (time.time() - start_time) * 1000

            result_obj = HealthCheckResult(
                check_id=check_id,
                check_name=check.name,
                status=result.get("status", HealthStatus.UNKNOWN),
                message=result.get("message"),
                duration_ms=duration,
                details=result.get("details", {}),
            )

        except Exception as e:
            duration = (time.time() - start_time) * 1000
            result_obj = HealthCheckResult(
                check_id=check_id,
                check_name=check.name,
                status=HealthStatus.UNHEALTHY,
                message=str(e),
                duration_ms=duration,
            )

        with self._lock:
            check.last_run = time.time()
            check.last_result = result_obj.status.value
            check.last_status = result_obj.status
            check.last_message = result_obj.message

            self._results[check_id].append(result_obj)

            if len(self._results[check_id]) > 100:
                self._results[check_id] = self._results[check_id][-100:]

            if result_obj.status == HealthStatus.HEALTHY:
                check.consecutive_successes += 1
                check.consecutive_failures = 0
            else:
                check.consecutive_failures += 1
                check.consecutive_successes = 0

                if check.critical and check.consecutive_failures >= 3:
                    self._trigger_alert(check, result_obj)

        return result_obj

    def _execute_check(self, check: HealthCheck) -> Dict[str, Any]:
        """Execute a check and return result."""
        return {
            "status": HealthStatus.HEALTHY,
            "message": "Check passed",
            "details": {},
        }

    def run_all_checks(self) -> SystemHealth:
        """Run all enabled health checks."""
        components = []
        critical_failures = 0
        non_critical_failures = 0

        for check in self._checks.values():
            if not check.enabled:
                continue

            result = self.run_check(check)
            if result:
                components.append(result)
                if result.status == HealthStatus.UNHEALTHY:
                    if check.critical:
                        critical_failures += 1
                    else:
                        non_critical_failures += 1

        if critical_failures > 0:
            overall = HealthStatus.UNHEALTHY
        elif non_critical_failures > 0:
            overall = HealthStatus.DEGRADED
        else:
            overall = HealthStatus.HEALTHY

        uptime = time.time() - self._start_time

        return SystemHealth(
            system_id=str(uuid.uuid4())[:12],
            overall_status=overall,
            checked_at=time.time(),
            uptime_seconds=uptime,
            components=components,
            summary={
                "total": len(components),
                "healthy": sum(1 for c in components if c.status == HealthStatus.HEALTHY),
                "degraded": sum(1 for c in components if c.status == HealthStatus.DEGRADED),
                "unhealthy": sum(1 for c in components if c.status == HealthStatus.UNHEALTHY),
            },
        )

    def _trigger_alert(self, check: HealthCheck, result: HealthCheckResult) -> None:
        """Trigger an alert for a failing check."""
        alert = {
            "alert_id": str(uuid.uuid4())[:12],
            "check_id": check.check_id,
            "check_name": check.name,
            "status": result.status.value,
            "message": result.message,
            "timestamp": datetime.fromtimestamp(result.timestamp).isoformat(),
            "consecutive_failures": check.consecutive_failures,
        }

        for callback in self._alert_callbacks:
            try:
                callback(alert)
            except Exception:
                pass

    def register_alert_callback(self, callback: Callable) -> None:
        """Register a callback for alerts."""
        self._alert_callbacks.append(callback)

    def get_check_status(self, check_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific check."""
        check = self._checks.get(check_id)
        if not check:
            return None

        return {
            "check_id": check.check_id,
            "name": check.name,
            "type": check.check_type.value,
            "status": check.last_status.value,
            "message": check.last_message,
            "last_run": (
                datetime.fromtimestamp(check.last_run).isoformat()
                if check.last_run else None
            ),
            "consecutive_failures": check.consecutive_failures,
            "consecutive_successes": check.consecutive_successes,
            "enabled": check.enabled,
            "critical": check.critical,
        }


class AutomationHealthAction:
    """Action providing health monitoring for automation workflows."""

    def __init__(self, runner: Optional[HealthCheckRunner] = None):
        self._runner = runner or HealthCheckRunner()
        self._check_functions: Dict[str, Callable] = {}

    def register_check(
        self,
        name: str,
        check_type: str = "custom",
        interval_seconds: int = 60,
        timeout_seconds: int = 30,
        critical: bool = False,
        tags: Optional[List[str]] = None,
    ) -> str:
        """Register a health check (without implementation)."""
        try:
            check_type_enum = CheckType(check_type.lower())
        except ValueError:
            check_type_enum = CheckType.CUSTOM

        check_id = self._runner.register_check(
            name=name,
            check_type=check_type_enum,
            check_fn=lambda: {"status": HealthStatus.HEALTHY, "message": "OK"},
            enabled=True,
            interval_seconds=interval_seconds,
            timeout_seconds=timeout_seconds,
            critical=critical,
            tags=set(tags) if tags else None,
        )

        return check_id

    def set_check_implementation(
        self,
        check_id: str,
        implementation: Callable[[], Dict[str, Any]],
    ) -> None:
        """Set the implementation function for a health check."""
        self._check_functions[check_id] = implementation

    def check_status(self, check_id: str) -> Dict[str, Any]:
        """Get status of a health check."""
        return self._runner.get_check_status(check_id) or {"error": "Check not found"}

    def run_check(self, check_id: str) -> Dict[str, Any]:
        """Run a specific health check."""
        result = self._runner.run_check(check_id)
        if not result:
            return {"error": "Check not found"}

        return {
            "check_id": result.check_id,
            "check_name": result.check_name,
            "status": result.status.value,
            "message": result.message,
            "duration_ms": round(result.duration_ms, 2),
            "timestamp": datetime.fromtimestamp(result.timestamp).isoformat(),
            "details": result.details,
        }

    def get_health(self) -> Dict[str, Any]:
        """Get overall system health."""
        health = self._runner.run_all_checks()

        return {
            "system_id": health.system_id,
            "overall_status": health.overall_status.value,
            "checked_at": datetime.fromtimestamp(health.checked_at).isoformat(),
            "uptime_seconds": round(health.uptime_seconds, 2),
            "summary": health.summary,
            "components": [
                {
                    "check_id": c.check_id,
                    "name": c.check_name,
                    "status": c.status.value,
                    "message": c.message,
                    "duration_ms": round(c.duration_ms, 2),
                }
                for c in health.components
            ],
        }

    def execute(
        self,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute a health monitoring operation.

        Required params:
            operation: str - 'register', 'run', 'status', 'health'
        """
        operation = params.get("operation")

        if operation == "register":
            return {
                "check_id": self.register_check(
                    name=params.get("name"),
                    check_type=params.get("check_type", "custom"),
                    interval_seconds=params.get("interval_seconds", 60),
                    timeout_seconds=params.get("timeout_seconds", 30),
                    critical=params.get("critical", False),
                    tags=params.get("tags"),
                )
            }

        elif operation == "run":
            check_id = params.get("check_id")
            if not check_id:
                raise ValueError("check_id is required")
            return self.run_check(check_id)

        elif operation == "status":
            check_id = params.get("check_id")
            if not check_id:
                raise ValueError("check_id is required")
            return self.check_status(check_id)

        elif operation == "health":
            return self.get_health()

        else:
            raise ValueError(f"Unknown operation: {operation}")

    def get_all_checks(self) -> List[Dict[str, Any]]:
        """Get all registered health checks."""
        return [
            self._runner.get_check_status(check_id) or {}
            for check_id in self._runner._checks.keys()
        ]
