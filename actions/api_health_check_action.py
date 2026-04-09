"""
API Health Check Action Module.

Monitors API health and availability.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class HealthStatus(Enum):
    """Health status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    """Result of a health check."""
    status: HealthStatus
    latency_ms: float
    message: str = ""
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthCheckConfig:
    """Configuration for health checks."""
    timeout_seconds: float = 5.0
    interval_seconds: float = 30.0
    failure_threshold: int = 3
    success_threshold: int = 2


class ApiHealthCheckAction:
    """
    Health check monitor for APIs and services.

    Tracks latency, failures, and overall health status.
    """

    def __init__(
        self,
        config: Optional[HealthCheckConfig] = None,
    ) -> None:
        self.config = config or HealthCheckConfig()
        self._checks: Dict[str, Callable[[], Any]] = {}
        self._results: Dict[str, HealthCheckResult] = {}
        self._failure_counts: Dict[str, int] = {}
        self._success_counts: Dict[str, int] = {}
        self._last_check_time: Dict[str, float] = {}

    def register(
        self,
        name: str,
        check_func: Callable[[], Any],
    ) -> None:
        """
        Register a health check.

        Args:
            name: Check name
            check_func: Function that returns True if healthy
        """
        self._checks[name] = check_func

    async def check(self, name: str) -> HealthCheckResult:
        """
        Run a specific health check.

        Args:
            name: Check name

        Returns:
            HealthCheckResult
        """
        if name not in self._checks:
            return HealthCheckResult(
                status=HealthStatus.UNKNOWN,
                latency_ms=0,
                message=f"Unknown check: {name}",
            )

        start = time.time()
        check_func = self._checks[name]

        try:
            if asyncio.iscoroutinefunction(check_func):
                result = await asyncio.wait_for(
                    check_func(),
                    timeout=self.config.timeout_seconds,
                )
            else:
                result = check_func()

            latency = (time.time() - start) * 1000

            if result:
                self._success_counts[name] = self._success_counts.get(name, 0) + 1
                self._failure_counts[name] = 0
                status = self._determine_status(name, success=True)
            else:
                self._failure_counts[name] = self._failure_counts.get(name, 0) + 1
                self._success_counts[name] = 0
                status = self._determine_status(name, success=False)

            self._results[name] = HealthCheckResult(
                status=status,
                latency_ms=latency,
                message="OK" if result else "Check failed",
            )

        except asyncio.TimeoutError:
            latency = (time.time() - start) * 1000
            self._failure_counts[name] = self._failure_counts.get(name, 0) + 1
            self._success_counts[name] = 0
            self._results[name] = HealthCheckResult(
                status=HealthStatus.UNHEALTHY,
                latency_ms=latency,
                message="Timeout",
            )

        except Exception as e:
            latency = (time.time() - start) * 1000
            self._failure_counts[name] = self._failure_counts.get(name, 0) + 1
            self._success_counts[name] = 0
            self._results[name] = HealthCheckResult(
                status=HealthStatus.UNHEALTHY,
                latency_ms=latency,
                message=str(e),
            )

        self._last_check_time[name] = time.time()
        return self._results[name]

    async def check_all(self) -> Dict[str, HealthCheckResult]:
        """Run all health checks."""
        tasks = [self.check(name) for name in self._checks]
        results = await asyncio.gather(*tasks)
        return dict(zip(self._checks.keys(), results))

    def _determine_status(
        self,
        name: str,
        success: bool,
    ) -> HealthStatus:
        """Determine health status based on consecutive results."""
        failures = self._failure_counts.get(name, 0)
        successes = self._success_counts.get(name, 0)

        if failures >= self.config.failure_threshold:
            return HealthStatus.UNHEALTHY
        if successes >= self.config.success_threshold:
            return HealthStatus.HEALTHY
        if failures > 0:
            return HealthStatus.DEGRADED
        return HealthStatus.HEALTHY

    def get_overall_status(self) -> HealthStatus:
        """Get overall system health status."""
        if not self._results:
            return HealthStatus.UNKNOWN

        statuses = [r.status for r in self._results.values()]

        if any(s == HealthStatus.UNHEALTHY for s in statuses):
            return HealthStatus.UNHEALTHY
        if any(s == HealthStatus.DEGRADED for s in statuses):
            return HealthStatus.DEGRADED
        if all(s == HealthStatus.HEALTHY for s in statuses):
            return HealthStatus.HEALTHY
        return HealthStatus.UNKNOWN

    def get_stats(self) -> Dict[str, Any]:
        """Get health check statistics."""
        return {
            "overall_status": self.get_overall_status().value,
            "checks": {
                name: {
                    "status": result.status.value,
                    "latency_ms": result.latency_ms,
                    "message": result.message,
                    "last_check": self._last_check_time.get(name),
                    "failure_count": self._failure_counts.get(name, 0),
                    "success_count": self._success_counts.get(name, 0),
                }
                for name, result in self._results.items()
            },
        }
