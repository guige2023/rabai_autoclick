"""API health check and status monitoring action."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

import httpx


class HealthStatus(str, Enum):
    """Health status levels."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class CheckType(str, Enum):
    """Type of health check."""

    HTTP = "http"
    TCP = "tcp"
    PROCESS = "process"


@dataclass
class HealthCheckResult:
    """Result of a health check."""

    endpoint: str
    status: HealthStatus
    latency_ms: float
    timestamp: float
    message: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthCheckConfig:
    """Configuration for a health check."""

    name: str
    endpoint: str
    check_type: CheckType = CheckType.HTTP
    timeout_seconds: float = 5.0
    expected_status: int = 200
    headers: Optional[dict[str, str]] = None
    interval_seconds: float = 60.0


class APIHealthCheckAction:
    """Monitors API health and reports status changes."""

    def __init__(
        self,
        checks: Optional[list[HealthCheckConfig]] = None,
        on_status_change: Optional[Callable[[str, HealthStatus, HealthStatus], None]] = None,
    ):
        """Initialize the health check action.

        Args:
            checks: List of health check configurations.
            on_status_change: Callback when status changes (name, old, new).
        """
        self._checks = checks or []
        self._results: dict[str, HealthCheckResult] = {}
        self._on_status_change = on_status_change
        self._last_status: dict[str, HealthStatus] = {}

    def add_check(self, config: HealthCheckConfig) -> None:
        """Add a health check configuration."""
        self._checks.append(config)

    async def check_http(self, config: HealthCheckConfig) -> HealthCheckResult:
        """Perform HTTP health check."""
        start = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=config.timeout_seconds) as client:
                response = await client.get(
                    config.endpoint,
                    headers=config.headers or {},
                )
                latency_ms = (time.monotonic() - start) * 1000

                if response.status_code == config.expected_status:
                    status = HealthStatus.HEALTHY
                elif 400 <= response.status_code < 500:
                    status = HealthStatus.DEGRADED
                else:
                    status = HealthStatus.UNHEALTHY

                return HealthCheckResult(
                    endpoint=config.endpoint,
                    status=status,
                    latency_ms=latency_ms,
                    timestamp=time.time(),
                    message=f"HTTP {response.status_code}",
                    metadata={"status_code": response.status_code},
                )
        except httpx.TimeoutException:
            return HealthCheckResult(
                endpoint=config.endpoint,
                status=HealthStatus.UNHEALTHY,
                latency_ms=(time.monotonic() - start) * 1000,
                timestamp=time.time(),
                message="Request timeout",
            )
        except Exception as e:
            return HealthCheckResult(
                endpoint=config.endpoint,
                status=HealthStatus.UNHEALTHY,
                latency_ms=(time.monotonic() - start) * 1000,
                timestamp=time.time(),
                message=str(e),
            )

    async def check_tcp(self, config: HealthCheckConfig) -> HealthCheckResult:
        """Perform TCP health check."""
        start = time.monotonic()
        host = config.endpoint.replace("tcp://", "")
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host.split(":")[0], int(host.split(":")[1])),
                timeout=config.timeout_seconds,
            )
            writer.close()
            await writer.wait_closed()
            latency_ms = (time.monotonic() - start) * 1000
            return HealthCheckResult(
                endpoint=config.endpoint,
                status=HealthStatus.HEALTHY,
                latency_ms=latency_ms,
                timestamp=time.time(),
                message="TCP connection successful",
            )
        except Exception as e:
            return HealthCheckResult(
                endpoint=config.endpoint,
                status=HealthStatus.UNHEALTHY,
                latency_ms=(time.monotonic() - start) * 1000,
                timestamp=time.time(),
                message=str(e),
            )

    async def run_check(self, config: HealthCheckConfig) -> HealthCheckResult:
        """Run a single health check based on type."""
        if config.check_type == CheckType.HTTP:
            return await self.check_http(config)
        elif config.check_type == CheckType.TCP:
            return await self.check_tcp(config)
        else:
            return HealthCheckResult(
                endpoint=config.endpoint,
                status=HealthStatus.UNKNOWN,
                latency_ms=0,
                timestamp=time.time(),
                message=f"Unsupported check type: {config.check_type}",
            )

    async def run_all_checks(self) -> dict[str, HealthCheckResult]:
        """Run all configured health checks."""
        tasks = [self.run_check(check) for check in self._checks]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for check, result in zip(self._checks, results):
            if isinstance(result, Exception):
                self._results[check.name] = HealthCheckResult(
                    endpoint=check.endpoint,
                    status=HealthStatus.UNHEALTHY,
                    latency_ms=0,
                    timestamp=time.time(),
                    message=str(result),
                )
            else:
                self._notify_status_change(check.name, result.status)
                self._results[check.name] = result

        return self._results

    def _notify_status_change(self, name: str, new_status: HealthStatus) -> None:
        """Notify if status changed."""
        old_status = self._last_status.get(name, HealthStatus.UNKNOWN)
        if old_status != new_status and self._on_status_change:
            self._on_status_change(name, old_status, new_status)
        self._last_status[name] = new_status

    def get_aggregate_status(self) -> HealthStatus:
        """Get aggregate health status across all checks."""
        if not self._results:
            return HealthStatus.UNKNOWN

        statuses = [r.status for r in self._results.values()]
        if all(s == HealthStatus.HEALTHY for s in statuses):
            return HealthStatus.HEALTHY
        elif any(s == HealthStatus.UNHEALTHY for s in statuses):
            return HealthStatus.UNHEALTHY
        return HealthStatus.DEGRADED

    def get_results(self) -> dict[str, HealthCheckResult]:
        """Get all health check results."""
        return self._results.copy()
