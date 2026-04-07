"""
Health check utilities for monitoring service and dependency status.

Provides health check registry, liveness/readiness probes,
dependency health aggregation, and reporting.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
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
    timestamp: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None

    @property
    def is_healthy(self) -> bool:
        return self.status == HealthStatus.HEALTHY


@dataclass
class HealthReport:
    """Aggregated health report for a service."""
    service_name: str
    overall_status: HealthStatus
    checks: list[HealthCheckResult]
    timestamp: float = field(default_factory=time.time)
    version: str = "1.0.0"
    uptime_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "service": self.service_name,
            "status": self.overall_status.value,
            "version": self.version,
            "uptime_seconds": self.uptime_seconds,
            "timestamp": self.timestamp,
            "checks": [
                {
                    "name": c.name,
                    "status": c.status.value,
                    "latency_ms": c.latency_ms,
                    "message": c.message,
                    "error": c.error,
                    "metadata": c.metadata,
                }
                for c in self.checks
            ],
        }


class HealthCheck:
    """Base health check class."""

    def __init__(self, name: str, timeout: float = 5.0, critical: bool = True) -> None:
        self.name = name
        self.timeout = timeout
        self.critical = critical

    async def check(self) -> HealthCheckResult:
        """Perform the health check. Override in subclass."""
        raise NotImplementedError


class HTTPHealthCheck(HealthCheck):
    """Health check that makes an HTTP request."""

    def __init__(
        self,
        name: str,
        url: str,
        expected_status: int = 200,
        timeout: float = 5.0,
        critical: bool = True,
    ) -> None:
        super().__init__(name, timeout, critical)
        self.url = url
        self.expected_status = expected_status

    async def check(self) -> HealthCheckResult:
        import httpx
        start = time.perf_counter()
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(self.url, timeout=self.timeout)
                latency = (time.perf_counter() - start) * 1000
                if response.status_code == self.expected_status:
                    return HealthCheckResult(
                        name=self.name,
                        status=HealthStatus.HEALTHY,
                        latency_ms=latency,
                        message=f"HTTP {response.status_code}",
                    )
                return HealthCheckResult(
                    name=self.name,
                    status=HealthStatus.UNHEALTHY,
                    latency_ms=latency,
                    message=f"Unexpected status: {response.status_code}",
                )
        except httpx.TimeoutException:
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                latency_ms=(time.perf_counter() - start) * 1000,
                error="Request timeout",
            )
        except Exception as e:
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                latency_ms=(time.perf_counter() - start) * 1000,
                error=str(e),
            )


class TCPHealthCheck(HealthCheck):
    """Health check that tests TCP connectivity."""

    def __init__(
        self,
        name: str,
        host: str,
        port: int,
        timeout: float = 5.0,
        critical: bool = True,
    ) -> None:
        super().__init__(name, timeout, critical)
        self.host = host
        self.port = port

    async def check(self) -> HealthCheckResult:
        import socket
        start = time.perf_counter()
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.timeout)
            result = sock.connect_ex((self.host, self.port))
            sock.close()
            latency = (time.perf_counter() - start) * 1000
            if result == 0:
                return HealthCheckResult(
                    name=self.name,
                    status=HealthStatus.HEALTHY,
                    latency_ms=latency,
                    message=f"TCP connection to {self.host}:{self.port} successful",
                )
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                latency_ms=latency,
                error=f"TCP connection failed (code {result})",
            )
        except Exception as e:
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                latency_ms=(time.perf_counter() - start) * 1000,
                error=str(e),
            )


class DatabaseHealthCheck(HealthCheck):
    """Health check for database connectivity."""

    def __init__(
        self,
        name: str,
        dsn: str,
        timeout: float = 5.0,
        critical: bool = True,
    ) -> None:
        super().__init__(name, timeout, critical)
        self.dsn = dsn

    async def check(self) -> HealthCheckResult:
        import asyncio
        start = time.perf_counter()
        try:
            if self.dsn.startswith("postgresql"):
                import asyncpg
                conn = await asyncio.wait_for(
                    asyncpg.connect(self.dsn, timeout=self.timeout),
                    timeout=self.timeout,
                )
                await conn.close()
            elif self.dsn.startswith("mysql"):
                import aiomysql
                conn = await asyncio.wait_for(
                    aiomysql.connect(dsn=self.dsn, timeout=self.timeout),
                    timeout=self.timeout,
                )
                conn.close()
            elif self.dsn.startswith("redis"):
                import aioredis
                conn = await aioredis.from_url(self.dsn, timeout=self.timeout)
                await conn.ping()
                conn.close()
            else:
                raise ValueError(f"Unsupported DSN type: {self.dsn}")

            latency = (time.perf_counter() - start) * 1000
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.HEALTHY,
                latency_ms=latency,
                message="Database connection successful",
            )
        except Exception as e:
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                latency_ms=(time.perf_counter() - start) * 1000,
                error=str(e),
            )


class HealthCheckRegistry:
    """Registry for managing multiple health checks."""

    def __init__(self, service_name: str = "unknown") -> None:
        self.service_name = service_name
        self._checks: dict[str, HealthCheck] = {}
        self._start_time = time.time()

    def register(self, check: HealthCheck) -> None:
        """Register a health check."""
        self._checks[check.name] = check
        logger.info("Registered health check: %s", check.name)

    def unregister(self, name: str) -> None:
        """Unregister a health check."""
        self._checks.pop(name, None)

    async def run_all(self) -> HealthReport:
        """Run all registered health checks."""
        tasks = [check.check() for check in self._checks.values()]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        check_results: list[HealthCheckResult] = []
        overall = HealthStatus.HEALTHY

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                check_name = list(self._checks.keys())[i]
                check_results.append(HealthCheckResult(
                    name=check_name,
                    status=HealthStatus.UNHEALTHY,
                    error=str(result),
                ))
            else:
                check_results.append(result)
                if result.status == HealthStatus.UNHEALTHY:
                    overall = HealthStatus.UNHEALTHY
                elif result.status == HealthStatus.DEGRADED and overall == HealthStatus.HEALTHY:
                    overall = HealthStatus.DEGRADED

        uptime = time.time() - self._start_time
        return HealthReport(
            service_name=self.service_name,
            overall_status=overall,
            checks=check_results,
            uptime_seconds=uptime,
        )

    async def run_liveness(self) -> HealthCheckResult:
        """Run liveness probe (critical checks only)."""
        critical_checks = [c for c in self._checks.values() if c.critical]
        if not critical_checks:
            return HealthCheckResult(name="liveness", status=HealthStatus.HEALTHY, message="No critical checks")
        tasks = [c.check() for c in critical_checks]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_healthy = all(
            isinstance(r, HealthCheckResult) and r.status == HealthStatus.HEALTHY
            for r in results
        )
        return HealthCheckResult(
            name="liveness",
            status=HealthStatus.HEALTHY if all_healthy else HealthStatus.UNHEALTHY,
        )

    async def run_readiness(self) -> HealthCheckResult:
        """Run readiness probe (all checks)."""
        report = await self.run_all()
        return HealthCheckResult(
            name="readiness",
            status=report.overall_status,
            message=f"{sum(1 for c in report.checks if c.is_healthy)}/{len(report.checks)} checks passed",
        )
