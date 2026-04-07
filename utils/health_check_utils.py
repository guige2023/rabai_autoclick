"""Health check utilities: liveness, readiness, and dependency checks."""

from __future__ import annotations

import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

__all__ = [
    "HealthStatus",
    "HealthCheck",
    "HealthCheckResult",
    "HealthCheckRegistry",
    "LivenessCheck",
    "ReadinessCheck",
]


class HealthStatus(Enum):
    """Health status values."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass
class HealthCheckResult:
    """Result of a single health check."""

    name: str
    status: HealthStatus
    message: str = ""
    latency_ms: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthCheck:
    """A health check definition."""

    name: str
    check_fn: Callable[[], bool]
    critical: bool = True
    timeout_seconds: float = 5.0


class HealthCheckRegistry:
    """Registry and runner for application health checks."""

    def __init__(self) -> None:
        self._checks: list[HealthCheck] = []

    def register(
        self,
        name: str,
        check_fn: Callable[[], bool],
        critical: bool = True,
        timeout_seconds: float = 5.0,
    ) -> None:
        self._checks.append(
            HealthCheck(name, check_fn, critical, timeout_seconds)
        )

    def check_all(self) -> tuple[HealthStatus, list[HealthCheckResult]]:
        """Run all registered health checks."""
        results: list[HealthCheckResult] = []
        overall_status = HealthStatus.HEALTHY

        for check in self._checks:
            start = time.perf_counter()
            try:
                healthy = check.check_fn()
                latency = (time.perf_counter() - start) * 1000
                result = HealthCheckResult(
                    name=check.name,
                    status=HealthStatus.HEALTHY if healthy else HealthStatus.UNHEALTHY,
                    latency_ms=latency,
                )
            except Exception as e:
                latency = (time.perf_counter() - start) * 1000
                result = HealthCheckResult(
                    name=check.name,
                    status=HealthStatus.UNHEALTHY,
                    message=str(e),
                    latency_ms=latency,
                )

            results.append(result)

            if result.status == HealthStatus.UNHEALTHY and check.critical:
                overall_status = HealthStatus.UNHEALTHY
            elif result.status == HealthStatus.UNHEALTHY and overall_status == HealthStatus.HEALTHY:
                overall_status = HealthStatus.DEGRADED

        return overall_status, results


class LivenessCheck:
    """Simple liveness check - is the process running?"""

    @staticmethod
    def process_alive() -> bool:
        return True


class ReadinessCheck:
    """Readiness checks for external dependencies."""

    @staticmethod
    def http_endpoint(url: str, expected_status: int = 200) -> Callable[[], bool]:
        def check() -> bool:
            try:
                req = urllib.request.Request(url, method="GET")
                with urllib.request.urlopen(req, timeout=5) as resp:
                    return resp.status == expected_status
            except Exception:
                return False
        return check

    @staticmethod
    def tcp_port(host: str, port: int) -> Callable[[], bool]:
        def check() -> bool:
            import socket
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex((host, port))
                sock.close()
                return result == 0
            except Exception:
                return False
        return check
