"""Health Check Action Module.

Comprehensive health checking for services and dependencies.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from .service_discovery_action import ServiceStatus


class HealthStatus(Enum):
    """Overall health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


class CheckType(Enum):
    """Health check types."""
    HTTP = "http"
    TCP = "tcp"
    PROCESS = "process"
    DATABASE = "database"
    CACHE = "cache"
    CUSTOM = "custom"


@dataclass
class HealthCheckResult:
    """Result of a single health check."""
    name: str
    check_type: CheckType
    status: ServiceStatus
    latency_ms: float | None = None
    message: str | None = None
    details: dict = field(default_factory=dict)
    timestamp: float = 0.0


@dataclass
class HealthReport:
    """Overall health report."""
    status: HealthStatus
    checks: list[HealthCheckResult]
    overall_latency_ms: float
    timestamp: float
    version: str = "1.0"


class HealthChecker:
    """Comprehensive health checker."""

    def __init__(self, service_name: str) -> None:
        self.service_name = service_name
        self._checks: dict[str, Callable] = {}
        self._check_types: dict[str, CheckType] = {}

    def register_check(
        self,
        name: str,
        check_fn: Callable,
        check_type: CheckType = CheckType.CUSTOM
    ) -> None:
        """Register a health check."""
        self._checks[name] = check_fn
        self._check_types[name] = check_type

    async def check_health(self) -> HealthReport:
        """Run all health checks."""
        start = time.monotonic()
        results = []
        for name, check_fn in self._checks.items():
            result = await self._run_check(name, check_fn)
            results.append(result)
        overall_status = self._compute_overall_status(results)
        return HealthReport(
            status=overall_status,
            checks=results,
            overall_latency_ms=(time.monotonic() - start) * 1000,
            timestamp=time.time()
        )

    async def _run_check(self, name: str, check_fn: Callable) -> HealthCheckResult:
        """Run single health check."""
        start = time.monotonic()
        try:
            result = check_fn()
            if asyncio.iscoroutine(result):
                result = await result
            latency = (time.monotonic() - start) * 1000
            if isinstance(result, dict):
                return HealthCheckResult(
                    name=name,
                    check_type=self._check_types.get(name, CheckType.CUSTOM),
                    status=ServiceStatus.HEALTHY if result.get("healthy") else ServiceStatus.UNHEALTHY,
                    latency_ms=latency,
                    message=result.get("message"),
                    details=result.get("details", {}),
                    timestamp=time.time()
                )
            return HealthCheckResult(
                name=name,
                check_type=self._check_types.get(name, CheckType.CUSTOM),
                status=ServiceStatus.HEALTHY,
                latency_ms=latency,
                timestamp=time.time()
            )
        except Exception as e:
            return HealthCheckResult(
                name=name,
                check_type=self._check_types.get(name, CheckType.CUSTOM),
                status=ServiceStatus.UNHEALTHY,
                latency_ms=(time.monotonic() - start) * 1000,
                message=str(e),
                timestamp=time.time()
            )

    def _compute_overall_status(self, results: list[HealthCheckResult]) -> HealthStatus:
        """Compute overall health status."""
        if all(r.status == ServiceStatus.HEALTHY for r in results):
            return HealthStatus.HEALTHY
        if any(r.status == ServiceStatus.UNHEALTHY for r in results):
            return HealthStatus.UNHEALTHY
        return HealthStatus.DEGRADED

    def register_http_check(self, name: str, url: str, expected_status: int = 200) -> None:
        """Register HTTP health check."""
        async def check() -> dict:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    return {
                        "healthy": resp.status == expected_status,
                        "message": f"HTTP {resp.status}",
                        "details": {"status_code": resp.status}
                    }
        self.register_check(name, check, CheckType.HTTP)

    def register_tcp_check(self, name: str, host: str, port: int) -> None:
        """Register TCP health check."""
        async def check() -> dict:
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=5.0
                )
                writer.close()
                await writer.wait_closed()
                return {"healthy": True, "message": "TCP connection successful"}
            except Exception as e:
                return {"healthy": False, "message": str(e)}
        self.register_check(name, check, CheckType.TCP)
