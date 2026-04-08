"""
API Health Check Action Module.

Monitors API health with configurable checks,
status aggregation, and alerting hooks.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Protocol
from dataclasses import dataclass, field
from enum import Enum
import logging
import time
import asyncio
import httpx
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    """Result of a single health check."""
    name: str
    status: HealthStatus
    latency_ms: float = 0.0
    message: str = ""
    timestamp: datetime = field(default_factory=datetime.now)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthReport:
    """Aggregated health report for all checks."""
    overall_status: HealthStatus
    checks: list[HealthCheckResult]
    timestamp: datetime = field(default_factory=datetime.now)
    duration_ms: float = 0.0


class HealthCheckFunc(Protocol):
    """Protocol for health check functions."""
    def __call__(self) -> HealthCheckResult: ...


class APIHealthCheckAction:
    """
    Multi-check health monitoring system.

    Runs multiple health checks and aggregates results.
    Supports thresholds, timeouts, and alerting callbacks.

    Example:
        checker = APIHealthCheckAction()
        checker.register("auth", auth_health_check)
        checker.register("database", db_health_check)
        report = checker.run_all()
        print(report.overall_status)
    """

    def __init__(
        self,
        timeout: float = 5.0,
        degraded_threshold_ms: float = 1000.0,
        unhealthy_threshold_ms: float = 3000.0,
    ) -> None:
        self.timeout = timeout
        self.degraded_threshold_ms = degraded_threshold_ms
        self.unhealthy_threshold_ms = unhealthy_threshold_ms
        self._checks: dict[str, HealthCheckFunc] = {}
        self._history: list[HealthReport] = []
        self._max_history: int = 100

    def register(self, name: str, check_func: HealthCheckFunc) -> None:
        """Register a health check function."""
        self._checks[name] = check_func

    def register_http(
        self,
        name: str,
        url: str,
        method: str = "GET",
        expected_status: int = 200,
    ) -> None:
        """Register an HTTP endpoint as a health check."""
        async def http_check() -> HealthCheckResult:
            start = time.perf_counter()
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.request(method, url)
                    latency_ms = (time.perf_counter() - start) * 1000

                    if response.status_code == expected_status:
                        status = HealthStatus.HEALTHY
                        message = "OK"
                    else:
                        status = HealthStatus.UNHEALTHY
                        message = f"Status {response.status_code}"

                    return HealthCheckResult(
                        name=name,
                        status=status,
                        latency_ms=latency_ms,
                        message=message,
                        details={"status_code": response.status_code},
                    )
            except Exception as e:
                latency_ms = (time.perf_counter() - start) * 1000
                return HealthCheckResult(
                    name=name,
                    status=HealthStatus.UNHEALTHY,
                    latency_ms=latency_ms,
                    message=str(e),
                )

        self._checks[name] = http_check  # type: ignore

    def run_all(self) -> HealthReport:
        """Run all registered health checks synchronously."""
        start = time.perf_counter()
        results: list[HealthCheckResult] = []

        for name, check in self._checks.items():
            try:
                if asyncio.iscoroutinefunction(check):
                    result = asyncio.run(check())
                else:
                    result = check()
            except Exception as e:
                logger.error("Health check '%s' failed: %s", name, e)
                result = HealthCheckResult(
                    name=name,
                    status=HealthStatus.UNKNOWN,
                    message=str(e),
                )
            results.append(result)

        duration_ms = (time.perf_counter() - start) * 1000
        overall = self._aggregate_status(results)

        report = HealthReport(
            overall_status=overall,
            checks=results,
            duration_ms=duration_ms,
        )
        self._add_to_history(report)
        return report

    async def run_all_async(self) -> HealthReport:
        """Run all health checks concurrently."""
        start = time.perf_counter()

        async def run_check(name: str, check: HealthCheckFunc) -> HealthCheckResult:
            try:
                if asyncio.iscoroutinefunction(check):
                    return await check()
                return check()
            except Exception as e:
                logger.error("Health check '%s' failed: %s", name, e)
                return HealthCheckResult(
                    name=name,
                    status=HealthStatus.UNKNOWN,
                    message=str(e),
                )

        tasks = [run_check(name, check) for name, check in self._checks.items()]
        results = await asyncio.gather(*tasks)

        duration_ms = (time.perf_counter() - start) * 1000
        overall = self._aggregate_status(list(results))

        report = HealthReport(
            overall_status=overall,
            checks=list(results),
            duration_ms=duration_ms,
        )
        self._add_to_history(report)
        return report

    def _aggregate_status(self, results: list[HealthCheckResult]) -> HealthStatus:
        """Determine overall status from individual check results."""
        if not results:
            return HealthStatus.UNKNOWN

        statuses = [r.status for r in results]

        if HealthStatus.UNHEALTHY in statuses:
            return HealthStatus.UNHEALTHY
        if HealthStatus.DEGRADED in statuses or HealthStatus.UNKNOWN in statuses:
            return HealthStatus.DEGRADED
        if all(s == HealthStatus.HEALTHY for s in statuses):
            return HealthStatus.HEALTHY

        return HealthStatus.DEGRADED

    def _add_to_history(self, report: HealthReport) -> None:
        """Add report to history, maintaining max size."""
        self._history.append(report)
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history:]

    def get_history(
        self,
        since: Optional[datetime] = None,
    ) -> list[HealthReport]:
        """Get historical reports, optionally filtered by time."""
        if since is None:
            return list(self._history)
        return [r for r in self._history if r.timestamp >= since]

    def get_trend(self, check_name: str) -> list[HealthCheckResult]:
        """Get trend for a specific check."""
        return [r for r in self._history for r in r.checks if r.name == check_name]
