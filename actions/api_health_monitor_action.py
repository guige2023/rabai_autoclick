# Copyright (c) 2024. coded by claude
"""API Health Monitor Action Module.

Monitors API health status, tracks latency metrics, and triggers alerts
when services become unhealthy.
"""
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    endpoint: str
    status: HealthStatus
    latency_ms: float
    timestamp: datetime
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_healthy(self) -> bool:
        return self.status == HealthStatus.HEALTHY


class HealthMonitor:
    def __init__(
        self,
        check_interval: float = 30.0,
        timeout: float = 5.0,
        degraded_threshold: float = 1000.0,
        unhealthy_threshold: float = 5000.0,
    ):
        self.check_interval = check_interval
        self.timeout = timeout
        self.degraded_threshold = degraded_threshold
        self.unhealthy_threshold = unhealthy_threshold
        self._health_checks: Dict[str, Callable] = {}
        self._last_results: Dict[str, HealthCheckResult] = {}
        self._alert_callbacks: List[Callable] = []
        self._monitoring_task: Optional[asyncio.Task] = None

    def register_check(self, name: str, check_fn: Callable) -> None:
        self._health_checks[name] = check_fn

    def register_alert_callback(self, callback: Callable) -> None:
        self._alert_callbacks.append(callback)

    async def check_health(self, name: str) -> HealthCheckResult:
        if name not in self._health_checks:
            return HealthCheckResult(
                endpoint=name,
                status=HealthStatus.UNKNOWN,
                latency_ms=0.0,
                timestamp=datetime.now(),
                error_message="No health check registered",
            )
        try:
            start = datetime.now()
            result = await asyncio.wait_for(
                self._health_checks[name](),
                timeout=self.timeout,
            )
            latency = (datetime.now() - start).total_seconds() * 1000
            status = self._determine_status(latency)
            return HealthCheckResult(
                endpoint=name,
                status=status,
                latency_ms=latency,
                timestamp=datetime.now(),
            )
        except asyncio.TimeoutError:
            return HealthCheckResult(
                endpoint=name,
                status=HealthStatus.UNHEALTHY,
                latency_ms=self.timeout * 1000,
                timestamp=datetime.now(),
                error_message="Health check timed out",
            )
        except Exception as e:
            return HealthCheckResult(
                endpoint=name,
                status=HealthStatus.UNHEALTHY,
                latency_ms=0.0,
                timestamp=datetime.now(),
                error_message=str(e),
            )

    def _determine_status(self, latency_ms: float) -> HealthStatus:
        if latency_ms >= self.unhealthy_threshold:
            return HealthStatus.UNHEALTHY
        if latency_ms >= self.degraded_threshold:
            return HealthStatus.DEGRADED
        return HealthStatus.HEALTHY

    async def run_all_checks(self) -> Dict[str, HealthCheckResult]:
        tasks = [self.check_health(name) for name in self._health_checks]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        result_map = {}
        for name, result in zip(self._health_checks.keys(), results):
            if isinstance(result, Exception):
                result_map[name] = HealthCheckResult(
                    endpoint=name,
                    status=HealthStatus.UNHEALTHY,
                    latency_ms=0.0,
                    timestamp=datetime.now(),
                    error_message=str(result),
                )
            else:
                result_map[name] = result
                self._last_results[name] = result
        return result_map

    async def start_monitoring(self) -> None:
        async def monitor_loop():
            while True:
                results = await self.run_all_checks()
                await self._process_alerts(results)
                await asyncio.sleep(self.check_interval)

        self._monitoring_task = asyncio.create_task(monitor_loop())

    async def stop_monitoring(self) -> None:
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass

    async def _process_alerts(self, results: Dict[str, HealthCheckResult]) -> None:
        for result in results.values():
            if result.status == HealthStatus.UNHEALTHY:
                for callback in self._alert_callbacks:
                    try:
                        await callback(result)
                    except Exception as e:
                        logger.error(f"Alert callback failed: {e}")

    def get_health_summary(self) -> Dict[str, Any]:
        if not self._last_results:
            return {"status": HealthStatus.UNKNOWN.value, "services": {}}
        unhealthy = sum(1 for r in self._last_results.values() if r.status == HealthStatus.UNHEALTHY)
        degraded = sum(1 for r in self._last_results.values() if r.status == HealthStatus.DEGRADED)
        overall = HealthStatus.UNHEALTHY if unhealthy > 0 else HealthStatus.DEGRADED if degraded > 0 else HealthStatus.HEALTHY
        return {
            "status": overall.value,
            "summary": {
                "healthy": len(self._last_results) - unhealthy - degraded,
                "degraded": degraded,
                "unhealthy": unhealthy,
            },
            "services": {name: r.status.value for name, r in self._last_results.items()},
        }
