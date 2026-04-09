"""
Automation Health Check Action Module.

Comprehensive health checks for automation systems
with dependency tracking and alerting.
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health check status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheck:
    """
    Health check definition.

    Attributes:
        name: Check name.
        check_func: Async function that returns True if healthy.
        timeout: Check timeout in seconds.
        critical: Whether check is critical.
        dependencies: List of dependency check names.
    """
    name: str
    check_func: Callable
    timeout: float = 10.0
    critical: bool = True
    dependencies: list = field(default_factory=list)


@dataclass
class HealthCheckResult:
    """Result of a health check."""
    name: str
    status: HealthStatus
    latency_ms: float
    message: str = ""
    error: Optional[str] = None


@dataclass
class SystemHealth:
    """Overall system health."""
    status: HealthStatus
    checks: list[HealthCheckResult]
    timestamp: float
    uptime_seconds: float


class AutomationHealthCheckAction:
    """
    Comprehensive health checking for automation systems.

    Example:
        health = AutomationHealthCheckAction()
        health.add_check("database", db_health_func, critical=True)
        health.add_check("api", api_health_func, dependencies=["database"])
        result = await health.check_all()
    """

    def __init__(self):
        """Initialize health check action."""
        self._checks: dict[str, HealthCheck] = {}
        self._check_results: dict[str, HealthCheckResult] = {}
        self._start_time = time.time()
        self._alert_handlers: list[Callable] = []

    def add_check(
        self,
        name: str,
        check_func: Callable,
        timeout: float = 10.0,
        critical: bool = True,
        dependencies: Optional[list] = None
    ) -> HealthCheck:
        """
        Add a health check.

        Args:
            name: Check identifier.
            check_func: Async function returning bool.
            timeout: Check timeout in seconds.
            critical: Whether critical to system health.
            dependencies: Names of checks that must pass first.

        Returns:
            Created HealthCheck.
        """
        check = HealthCheck(
            name=name,
            check_func=check_func,
            timeout=timeout,
            critical=critical,
            dependencies=dependencies or []
        )

        self._checks[name] = check
        logger.debug(f"Added health check: {name}")
        return check

    def register_alert_handler(self, handler: Callable[[str, HealthStatus, str], None]) -> None:
        """
        Register alert handler for unhealthy states.

        Args:
            handler: Function called with (check_name, status, message).
        """
        self._alert_handlers.append(handler)

    async def check_one(self, name: str) -> HealthCheckResult:
        """
        Run single health check.

        Args:
            name: Check name.

        Returns:
            HealthCheckResult.
        """
        if name not in self._checks:
            return HealthCheckResult(
                name=name,
                status=HealthStatus.UNKNOWN,
                latency_ms=0.0,
                message="Check not found"
            )

        check = self._checks[name]
        start = time.time()

        for dep in check.dependencies:
            if dep in self._check_results:
                dep_result = self._check_results[dep]
                if dep_result.status != HealthStatus.HEALTHY:
                    latency = (time.time() - start) * 1000
                    return HealthCheckResult(
                        name=name,
                        status=HealthStatus.UNHEALTHY,
                        latency_ms=latency,
                        message=f"Dependency {dep} is {dep_result.status.value}"
                    )

        try:
            if asyncio.iscoroutinefunction(check.check_func):
                result = await asyncio.wait_for(check.check_func(), timeout=check.timeout)
            else:
                result = check.check_func()

            latency = (time.time() - start) * 1000

            if result:
                status = HealthStatus.HEALTHY
                message = "OK"
            else:
                status = HealthStatus.UNHEALTHY
                message = "Check returned False"

            return HealthCheckResult(
                name=name,
                status=status,
                latency_ms=latency,
                message=message
            )

        except asyncio.TimeoutError:
            latency = (time.time() - start) * 1000
            return HealthCheckResult(
                name=name,
                status=HealthStatus.UNHEALTHY,
                latency_ms=latency,
                message=f"Timeout after {check.timeout}s"
            )

        except Exception as e:
            latency = (time.time() - start) * 1000
            return HealthCheckResult(
                name=name,
                status=HealthStatus.UNHEALTHY,
                latency_ms=latency,
                message=str(e),
                error=str(e)
            )

    async def check_all(self) -> SystemHealth:
        """
        Run all health checks.

        Returns:
            SystemHealth with overall status.
        """
        self._check_results.clear()

        check_tasks = [
            self.check_one(name)
            for name in self._checks
        ]

        results = await asyncio.gather(*check_tasks)

        for result in results:
            self._check_results[result.name] = result

        critical_unhealthy = [
            r for r in results
            if r.status != HealthStatus.HEALTHY and self._checks[r.name].critical
        ]

        any_unhealthy = [r for r in results if r.status != HealthStatus.HEALTHY]

        if not critical_unhealthy and not any_unhealthy:
            status = HealthStatus.HEALTHY
        elif critical_unhealthy:
            status = HealthStatus.UNHEALTHY
            for result in critical_unhealthy:
                self._send_alert(result.name, result.status, result.message)
        else:
            status = HealthStatus.DEGRADED

        return SystemHealth(
            status=status,
            checks=list(results),
            timestamp=time.time(),
            uptime_seconds=time.time() - self._start_time
        )

    def _send_alert(self, name: str, status: HealthStatus, message: str) -> None:
        """Send alert to registered handlers."""
        for handler in self._alert_handlers:
            try:
                handler(name, status, message)
            except Exception as e:
                logger.error(f"Alert handler failed: {e}")

    def get_last_result(self, name: str) -> Optional[HealthCheckResult]:
        """Get last result for a check."""
        return self._check_results.get(name)

    def get_critical_failures(self) -> list[HealthCheckResult]:
        """Get critical checks that failed."""
        return [
            r for r in self._check_results.values()
            if r.status != HealthStatus.HEALTHY and self._checks[r.name].critical
        ]

    def get_stats(self) -> dict:
        """Get health check statistics."""
        total = len(self._checks)
        healthy = sum(1 for r in self._check_results.values() if r.status == HealthStatus.HEALTHY)
        unhealthy = sum(1 for r in self._check_results.values() if r.status == HealthStatus.UNHEALTHY)

        return {
            "total_checks": total,
            "healthy": healthy,
            "unhealthy": unhealthy,
            "uptime_seconds": time.time() - self._start_time
        }

    def get_dependency_graph(self) -> dict:
        """Get check dependency graph."""
        return {
            name: check.dependencies
            for name, check in self._checks.items()
        }
