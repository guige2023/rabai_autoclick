"""API Health Check Action module.

Provides health check capabilities for API monitoring
with support for dependency checks, latency thresholds,
and composite health evaluations.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

import aiohttp


class HealthStatus(Enum):
    """Health check status levels."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    """Result of a health check."""

    name: str
    status: HealthStatus
    message: str = ""
    latency_ms: float = 0.0
    timestamp: float = field(default_factory=time.time)
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def is_healthy(self) -> bool:
        """Check if status is healthy."""
        return self.status == HealthStatus.HEALTHY

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "status": self.status.value,
            "message": self.message,
            "latency_ms": self.latency_ms,
            "timestamp": self.timestamp,
            "details": self.details,
        }


@dataclass
class HealthCheckConfig:
    """Configuration for health checks."""

    timeout: float = 5.0
    max_retries: int = 2
    retry_delay: float = 0.5
    latency_threshold_ms: float = 1000.0
    failure_threshold: int = 3


class HealthCheck:
    """Base health check."""

    def __init__(
        self,
        name: str,
        config: Optional[HealthCheckConfig] = None,
    ):
        self.name = name
        self.config = config or HealthCheckConfig()
        self._last_result: Optional[HealthCheckResult] = None
        self._failure_count = 0

    async def check(self) -> HealthCheckResult:
        """Perform health check."""
        raise NotImplementedError

    async def check_with_retry(self) -> HealthCheckResult:
        """Perform health check with retry logic."""
        last_error: Optional[Exception] = None

        for attempt in range(self.config.max_retries + 1):
            try:
                result = await asyncio.wait_for(
                    self.check(),
                    timeout=self.config.timeout,
                )
                self._failure_count = 0
                self._last_result = result
                return result
            except asyncio.TimeoutError:
                last_error = TimeoutError(f"Health check timed out after {self.config.timeout}s")
                if attempt < self.config.max_retries:
                    await asyncio.sleep(self.config.retry_delay)
            except Exception as e:
                last_error = e
                if attempt < self.config.max_retries:
                    await asyncio.sleep(self.config.retry_delay)

        self._failure_count += 1

        return HealthCheckResult(
            name=self.name,
            status=HealthStatus.UNHEALTHY if self._failure_count >= self.config.failure_threshold else HealthStatus.DEGRADED,
            message=str(last_error),
            latency_ms=self.config.timeout * 1000,
        )

    @property
    def last_result(self) -> Optional[HealthCheckResult]:
        """Get last check result."""
        return self._last_result

    @property
    def is_healthy(self) -> bool:
        """Check if last result was healthy."""
        return self._last_result is not None and self._last_result.is_healthy


class HttpHealthCheck(HealthCheck):
    """HTTP endpoint health check."""

    def __init__(
        self,
        name: str,
        url: str,
        expected_status: int = 200,
        check_json: Optional[dict] = None,
        headers: Optional[dict[str, str]] = None,
        config: Optional[HealthCheckConfig] = None,
    ):
        super().__init__(name, config)
        self.url = url
        self.expected_status = expected_status
        self.check_json = check_json
        self.headers = headers or {}

    async def check(self) -> HealthCheckResult:
        """Perform HTTP health check."""
        start = time.monotonic()

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.url,
                    headers=self.headers,
                    timeout=aiohttp.ClientTimeout(total=self.config.timeout),
                ) as response:
                    latency_ms = (time.monotonic() - start) * 1000

                    if response.status != self.expected_status:
                        return HealthCheckResult(
                            name=self.name,
                            status=HealthStatus.UNHEALTHY,
                            message=f"Unexpected status: {response.status}",
                            latency_ms=latency_ms,
                        )

                    if self.check_json:
                        try:
                            data = await response.json()
                            for key, expected in self.check_json.items():
                                if data.get(key) != expected:
                                    return HealthCheckResult(
                                        name=self.name,
                                        status=HealthStatus.UNHEALTHY,
                                        message=f"JSON check failed for key '{key}'",
                                        latency_ms=latency_ms,
                                    )
                        except Exception as e:
                            return HealthCheckResult(
                                name=self.name,
                                status=HealthStatus.UNHEALTHY,
                                message=f"JSON parse error: {e}",
                                latency_ms=latency_ms,
                            )

                    status = HealthStatus.HEALTHY
                    if latency_ms > self.config.latency_threshold_ms:
                        status = HealthStatus.DEGRADED

                    return HealthCheckResult(
                        name=self.name,
                        status=status,
                        message="OK",
                        latency_ms=latency_ms,
                        details={"status_code": response.status},
                    )

        except Exception as e:
            latency_ms = (time.monotonic() - start) * 1000
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message=str(e),
                latency_ms=latency_ms,
            )


class TcpHealthCheck(HealthCheck):
    """TCP port health check."""

    def __init__(
        self,
        name: str,
        host: str,
        port: int,
        config: Optional[HealthCheckConfig] = None,
    ):
        super().__init__(name, config)
        self.host = host
        self.port = port

    async def check(self) -> HealthCheckResult:
        """Perform TCP health check."""
        start = time.monotonic()

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=self.config.timeout,
            )
            writer.close()
            await writer.wait_closed()
            latency_ms = (time.monotonic() - start) * 1000

            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.HEALTHY,
                message="TCP connection successful",
                latency_ms=latency_ms,
            )

        except Exception as e:
            latency_ms = (time.monotonic() - start) * 1000
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message=str(e),
                latency_ms=latency_ms,
            )


@dataclass
class CompositeHealthCheck:
    """Composite health check combining multiple checks."""

    name: str
    checks: list[HealthCheck] = field(default_factory=list)
    strategy: str = "all"
    unhealthy_threshold: float = 0.5

    async def check_all(self) -> HealthCheckResult:
        """Run all health checks."""
        results = []
        for check in self.checks:
            result = await check.check_with_retry()
            results.append(result)

        statuses = [r.status for r in results]

        if self.strategy == "all":
            if all(s == HealthStatus.HEALTHY for s in statuses):
                overall = HealthStatus.HEALTHY
            elif any(s == HealthStatus.UNHEALTHY for s in statuses):
                overall = HealthStatus.UNHEALTHY
            else:
                overall = HealthStatus.DEGRADED
        elif self.strategy == "any":
            if any(s == HealthStatus.HEALTHY for s in statuses):
                overall = HealthStatus.HEALTHY
            else:
                overall = HealthStatus.UNHEALTHY
        elif self.strategy == "majority":
            healthy_count = sum(1 for s in statuses if s == HealthStatus.HEALTHY)
            if healthy_count / len(statuses) >= self.unhealthy_threshold:
                overall = HealthStatus.HEALTHY
            elif any(s == HealthStatus.UNHEALTHY for s in statuses):
                overall = HealthStatus.UNHEALTHY
            else:
                overall = HealthStatus.DEGRADED
        else:
            overall = HealthStatus.UNKNOWN

        unhealthy = [r for r in results if r.status == HealthStatus.UNHEALTHY]
        message = f"{len([r for r in results if r.is_healthy])}/{len(results)} checks healthy"

        return HealthCheckResult(
            name=self.name,
            status=overall,
            message=message,
            details={"checks": [r.to_dict() for r in results]},
        )


class HealthCheckRegistry:
    """Registry for managing health checks."""

    def __init__(self):
        self._checks: dict[str, HealthCheck] = {}

    def register(self, check: HealthCheck) -> None:
        """Register a health check."""
        self._checks[check.name] = check

    def unregister(self, name: str) -> bool:
        """Unregister a health check."""
        if name in self._checks:
            del self._checks[name]
            return True
        return False

    def get(self, name: str) -> Optional[HealthCheck]:
        """Get a health check by name."""
        return self._checks.get(name)

    async def check_all(self) -> dict[str, HealthCheckResult]:
        """Run all registered health checks."""
        results = {}
        for name, check in self._checks.items():
            results[name] = await check.check_with_retry()
        return results

    def get_status(self) -> dict[str, str]:
        """Get status of all checks."""
        return {
            name: check.last_result.status.value if check.last_result else "unknown"
            for name, check in self._checks.items()
        }
