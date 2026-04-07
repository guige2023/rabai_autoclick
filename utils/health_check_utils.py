"""Health check system for services, dependencies, and readiness probes."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

__all__ = ["HealthStatus", "HealthCheck", "HealthCheckResult", "HealthMonitor"]


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
    message: str = ""
    latency_ms: float = 0.0
    timestamp: float = field(default_factory=time.time)
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def is_healthy(self) -> bool:
        return self.status == HealthStatus.HEALTHY


class HealthCheck:
    """A single health check that can be registered with the monitor."""

    def __init__(
        self,
        name: str,
        check_fn: Callable[[], bool | HealthCheckResult],
        timeout: float = 5.0,
        critical: bool = True,
    ) -> None:
        self.name = name
        self.check_fn = check_fn
        self.timeout = timeout
        self.critical = critical

    def run(self) -> HealthCheckResult:
        start = time.perf_counter()
        try:
            if asyncio.iscoroutinefunction(self.check_fn):
                result = asyncio.run(self.check_fn())
            else:
                result = self.check_fn()
            latency = (time.perf_counter() - start) * 1000

            if isinstance(result, HealthCheckResult):
                return result
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.HEALTHY if result else HealthStatus.UNHEALTHY,
                message="Check passed" if result else "Check failed",
                latency_ms=latency,
            )
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Exception: {e}",
                latency_ms=latency,
            )

    def __repr__(self) -> str:
        return f"HealthCheck(name={self.name!r}, critical={self.critical})"


class HealthMonitor:
    """Aggregates multiple health checks and provides overall status."""

    def __init__(self) -> None:
        self._checks: dict[str, HealthCheck] = {}

    def register(self, check: HealthCheck) -> None:
        self._checks[check.name] = check

    def register_simple(
        self,
        name: str,
        fn: Callable[[], bool],
        **kwargs: Any,
    ) -> None:
        self.register(HealthCheck(name, fn, **kwargs))

    def run_all(self) -> dict[str, HealthCheckResult]:
        return {name: check.run() for name, check in self._checks.items()}

    def status(self) -> tuple[HealthStatus, list[HealthCheckResult]]:
        """Run all checks and return overall status + individual results."""
        results = self.run_all()
        critical_failures = [
            r for r in results.values() if r.status == HealthStatus.UNHEALTHY and self._checks[r.name].critical
        ]
        non_critical_failures = [
            r for r in results.values() if r.status == HealthStatus.UNHEALTHY and not self._checks[r.name].critical
        ]

        if critical_failures:
            status = HealthStatus.UNHEALTHY
        elif non_critical_failures:
            status = HealthStatus.DEGRADED
        else:
            status = HealthStatus.HEALTHY

        return status, list(results.values())

    def is_healthy(self) -> bool:
        s, _ = self.status()
        return s == HealthStatus.HEALTHY

    def summary(self) -> dict[str, Any]:
        """Return a dict summary suitable for JSON serialization."""
        status, results = self.status()
        return {
            "status": status.value,
            "is_healthy": status == HealthStatus.HEALTHY,
            "timestamp": time.time(),
            "checks": {r.name: {"status": r.status.value, "message": r.message, "latency_ms": r.latency_ms} for r in results},
        }

    def liveness_probe(self) -> dict[str, Any]:
        """Kubernetes liveness probe format."""
        return {"status": "ok" if self.is_healthy() else "fail"}

    def readiness_probe(self) -> dict[str, Any]:
        """Kubernetes readiness probe format."""
        status, results = self.status()
        return {
            "ready": status in (HealthStatus.HEALTHY, HealthStatus.DEGRADED),
            "status": status.value,
        }
