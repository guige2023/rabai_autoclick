"""API Health Check Action Module.

Provides health checking with endpoint probing,
status aggregation, and alerting on degraded services.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass
class HealthCheck:
    """Single health check."""
    name: str
    check_fn: Callable[[], bool]
    timeout: float = 5.0
    critical: bool = False


@dataclass
class HealthCheckResult:
    """Health check result."""
    name: str
    status: HealthStatus
    latency_ms: float
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class AggregateHealth:
    """Aggregated health status."""
    overall_status: HealthStatus
    checks: List[HealthCheckResult]
    timestamp: float = field(default_factory=time.time)


class APIHealthCheckAction:
    """API health checker.

    Example:
        health = APIHealthCheckAction()

        health.add_check(HealthCheck(
            name="database",
            check_fn=lambda: db.ping()
        ))

        health.add_check(HealthCheck(
            name="api",
            check_fn=lambda: requests.get("/health").ok
        ))

        status = await health.check()
        print(status.overall_status)
    """

    def __init__(self) -> None:
        self._checks: List[HealthCheck] = []
        self._history: Dict[str, List[HealthCheckResult]] = defaultdict(list)
        self._max_history = 100

    def add_check(self, check: HealthCheck) -> "APIHealthCheckAction":
        """Add health check.

        Returns self for chaining.
        """
        self._checks.append(check)
        return self

    async def check(self) -> AggregateHealth:
        """Run all health checks.

        Returns:
            AggregateHealth with results
        """
        tasks = [
            self._run_check(check)
            for check in self._checks
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        valid_results = []
        for check, result in zip(self._checks, results):
            if isinstance(result, Exception):
                valid_results.append(HealthCheckResult(
                    name=check.name,
                    status=HealthStatus.UNHEALTHY,
                    latency_ms=0,
                    error=str(result),
                ))
            else:
                valid_results.append(result)
                self._record_result(check.name, result)

        overall = self._determine_overall_status(valid_results)

        return AggregateHealth(
            overall_status=overall,
            checks=valid_results,
        )

    async def _run_check(self, check: HealthCheck) -> HealthCheckResult:
        """Run single health check."""
        start = time.time()

        try:
            if asyncio.iscoroutinefunction(check.check_fn):
                result = await asyncio.wait_for(
                    check.check_fn(),
                    timeout=check.timeout
                )
            else:
                result = check.check_fn()

            latency = (time.time() - start) * 1000
            status = HealthStatus.HEALTHY if result else HealthStatus.UNHEALTHY

            return HealthCheckResult(
                name=check.name,
                status=status,
                latency_ms=latency,
            )

        except asyncio.TimeoutError:
            return HealthCheckResult(
                name=check.name,
                status=HealthStatus.UNHEALTHY,
                latency_ms=check.timeout * 1000,
                error="Timeout",
            )

        except Exception as e:
            return HealthCheckResult(
                name=check.name,
                status=HealthStatus.UNHEALTHY,
                latency_ms=(time.time() - start) * 1000,
                error=str(e),
            )

    def _record_result(self, name: str, result: HealthCheckResult) -> None:
        """Record result in history."""
        self._history[name].append(result)
        if len(self._history[name]) > self._max_history:
            self._history[name].pop(0)

    def _determine_overall_status(
        self,
        results: List[HealthCheckResult],
    ) -> HealthStatus:
        """Determine overall health status."""
        critical_unhealthy = any(
            r.status == HealthStatus.UNHEALTHY and
            any(c.name == r.name and c.critical for c in self._checks)
            for r in results
        )

        if critical_unhealthy:
            return HealthStatus.UNHEALTHY

        unhealthy_count = sum(
            1 for r in results
            if r.status == HealthStatus.UNHEALTHY
        )

        if unhealthy_count > len(results) / 2:
            return HealthStatus.UNHEALTHY

        degraded_count = sum(
            1 for r in results
            if r.status == HealthStatus.DEGRADED
        )

        if unhealthy_count > 0 or degraded_count > len(results) / 3:
            return HealthStatus.DEGRADED

        return HealthStatus.HEALTHY

    def get_history(
        self,
        check_name: str,
        limit: int = 10,
    ) -> List[HealthCheckResult]:
        """Get history for specific check."""
        return self._history.get(check_name, [])[-limit:]

    def get_stats(self) -> Dict[str, Any]:
        """Get health check statistics."""
        stats = {}
        for name, history in self._history.items():
            if history:
                recent = history[-10:]
                healthy_count = sum(
                    1 for r in recent if r.status == HealthStatus.HEALTHY
                )
                stats[name] = {
                    "uptime": healthy_count / len(recent) if recent else 0,
                    "avg_latency_ms": sum(r.latency_ms for r in recent) / len(recent),
                }
        return stats
