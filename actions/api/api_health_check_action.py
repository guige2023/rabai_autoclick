"""
API Health Check Action Module.

Provides comprehensive health check capabilities for API services,
including endpoint monitoring, dependency checks, and status reporting.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

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
    message: str = ""
    latency_ms: float = 0.0
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthReport:
    """Aggregated health report for a service."""
    service_name: str
    overall_status: HealthStatus
    checks: List[HealthCheckResult] = field(default_factory=list)
    uptime_seconds: float = 0.0
    timestamp: float = field(default_factory=time.time)


class HealthCheck:
    """
    A single health check that can be registered with the health check service.

    A health check is a callable that returns a HealthCheckResult.
    """

    def __init__(
        self,
        name: str,
        check_fn: Callable[..., HealthCheckResult],
        timeout_seconds: float = 5.0,
        critical: bool = True,
    ) -> None:
        self.name = name
        self.check_fn = check_fn
        self.timeout_seconds = timeout_seconds
        self.critical = critical
        self._last_result: Optional[HealthCheckResult] = None

    async def execute(self) -> HealthCheckResult:
        """Execute the health check with timeout."""
        start = time.time()
        try:
            if asyncio.iscoroutinefunction(self.check_fn):
                result = await asyncio.wait_for(
                    self.check_fn(), timeout=self.timeout_seconds
                )
            else:
                result = self.check_fn()
            result.latency_ms = (time.time() - start) * 1000
            self._last_result = result
            return result
        except asyncio.TimeoutError:
            latency = (time.time() - start) * 1000
            result = HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check timed out after {self.timeout_seconds}s",
                latency_ms=latency,
            )
            self._last_result = result
            return result
        except Exception as e:
            latency = (time.time() - start) * 1000
            result = HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check failed: {e}",
                latency_ms=latency,
            )
            self._last_result = result
            return result

    @property
    def last_result(self) -> Optional[HealthCheckResult]:
        """Get the last execution result."""
        return self._last_result


class APIHealthCheckAction:
    """
    Comprehensive health check service for API monitoring.

    Manages multiple health checks, executes them periodically,
    and provides aggregated health reports.

    Example:
        service = APIHealthCheckAction(service_name="user-api")
        service.register_check("database", db_health_check, critical=True)
        service.register_check("cache", cache_health_check, critical=False)

        report = await service.run_all_checks()
        print(f"Status: {report.overall_status}")
    """

    def __init__(
        self,
        service_name: str = "api-service",
        check_interval_seconds: float = 30.0,
    ) -> None:
        self.service_name = service_name
        self.check_interval_seconds = check_interval_seconds
        self._checks: Dict[str, HealthCheck] = {}
        self._start_time = time.time()
        self._last_report: Optional[HealthReport] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def register_check(
        self,
        name: str,
        check_fn: Callable[..., HealthCheckResult],
        timeout_seconds: float = 5.0,
        critical: bool = True,
    ) -> None:
        """Register a health check."""
        if name in self._checks:
            logger.warning(f"Health check '{name}' already registered, overwriting")

        self._checks[name] = HealthCheck(
            name=name,
            check_fn=check_fn,
            timeout_seconds=timeout_seconds,
            critical=critical,
        )
        logger.info(f"Registered health check: {name} (critical={critical})")

    def unregister_check(self, name: str) -> bool:
        """Unregister a health check."""
        if name in self._checks:
            del self._checks[name]
            logger.info(f"Unregistered health check: {name}")
            return True
        return False

    async def run_check(self, name: str) -> Optional[HealthCheckResult]:
        """Run a single health check by name."""
        check = self._checks.get(name)
        if not check:
            return None
        return await check.execute()

    async def run_all_checks(self) -> HealthReport:
        """Run all registered health checks in parallel."""
        if not self._checks:
            return HealthReport(
                service_name=self.service_name,
                overall_status=HealthStatus.UNKNOWN,
                message="No health checks registered",
            )

        tasks = [check.execute() for check in self._checks.values()]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        check_results: List[HealthCheckResult] = []
        for result in results:
            if isinstance(result, HealthCheckResult):
                check_results.append(result)
            elif isinstance(result, Exception):
                check_results.append(HealthCheckResult(
                    name="unknown",
                    status=HealthStatus.UNHEALTHY,
                    message=f"Check raised exception: {result}",
                ))

        # Determine overall status
        critical_failures = [
            r for r in check_results
            if r.status == HealthStatus.UNHEALTHY and self._checks.get(r.name, HealthCheck(r.name, lambda: HealthCheckResult(r.name, HealthStatus.UNKNOWN))).critical
        ]
        non_critical_failures = [
            r for r in check_results
            if r.status == HealthStatus.UNHEALTHY and not self._checks.get(r.name, HealthCheck(r.name, lambda: HealthCheckResult(r.name, HealthStatus.UNKNOWN))).critical
        ]

        if critical_failures:
            overall = HealthStatus.UNHEALTHY
        elif non_critical_failures:
            overall = HealthStatus.DEGRADED
        elif any(r.status == HealthStatus.DEGRADED for r in check_results):
            overall = HealthStatus.DEGRADED
        else:
            overall = HealthStatus.HEALTHY

        report = HealthReport(
            service_name=self.service_name,
            overall_status=overall,
            checks=check_results,
            uptime_seconds=time.time() - self._start_time,
        )
        self._last_report = report
        return report

    async def _periodic_check_loop(self) -> None:
        """Internal loop for periodic health checks."""
        while self._running:
            try:
                await self.run_all_checks()
            except Exception as e:
                logger.error(f"Periodic health check failed: {e}")
            await asyncio.sleep(self.check_interval_seconds)

    def start_periodic_checks(self) -> None:
        """Start periodic health check execution."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._periodic_check_loop())
        logger.info(f"Started periodic health checks every {self.check_interval_seconds}s")

    async def stop_periodic_checks(self) -> None:
        """Stop periodic health check execution."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Stopped periodic health checks")

    @property
    def last_report(self) -> Optional[HealthReport]:
        """Get the last health report."""
        return self._last_report

    def get_check_status(self, name: str) -> Optional[HealthStatus]:
        """Get the current status of a specific check."""
        check = self._checks.get(name)
        if check and check.last_result:
            return check.last_result.status
        return None


# --- Built-in health check factories ---

def http_health_check(url: str, expected_status: int = 200) -> Callable[[], HealthCheckResult]:
    """Create an HTTP health check function."""
    async def check() -> HealthCheckResult:
        import aiohttp
        start = time.time()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    latency = (time.time() - start) * 1000
                    if resp.status == expected_status:
                        return HealthCheckResult(
                            name="http_check",
                            status=HealthStatus.HEALTHY,
                            message=f"HTTP {resp.status}",
                            latency_ms=latency,
                            metadata={"status_code": resp.status, "url": url},
                        )
                    return HealthCheckResult(
                        name="http_check",
                        status=HealthStatus.UNHEALTHY,
                        message=f"Unexpected status {resp.status}, expected {expected_status}",
                        latency_ms=latency,
                        metadata={"status_code": resp.status, "url": url},
                    )
        except Exception as e:
            latency = (time.time() - start) * 1000
            return HealthCheckResult(
                name="http_check",
                status=HealthStatus.UNHEALTHY,
                message=f"HTTP check failed: {e}",
                latency_ms=latency,
            )
    return check


def dependency_health_check(
    name: str,
    check_fn: Callable[[], bool],
    healthy_message: str = "Dependency is healthy",
) -> Callable[[], HealthCheckResult]:
    """Create a dependency health check function."""
    def check() -> HealthCheckResult:
        start = time.time()
        try:
            result = check_fn()
            latency = (time.time() - start) * 1000
            if result:
                return HealthCheckResult(
                    name=name,
                    status=HealthStatus.HEALTHY,
                    message=healthy_message,
                    latency_ms=latency,
                )
            return HealthCheckResult(
                name=name,
                status=HealthStatus.UNHEALTHY,
                message="Dependency check returned False",
                latency_ms=latency,
            )
        except Exception as e:
            latency = (time.time() - start) * 1000
            return HealthCheckResult(
                name=name,
                status=HealthStatus.UNHEALTHY,
                message=f"Dependency check failed: {e}",
                latency_ms=latency,
            )
    return check
