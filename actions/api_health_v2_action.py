"""API Health Check v2 Action.

Advanced health checking with dependency checks and composite status.
"""
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import time


class HealthStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass
class ComponentHealth:
    name: str
    status: HealthStatus
    latency_ms: float
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)


class APIHealthV2Action:
    """Advanced health checking with dependencies."""

    def __init__(
        self,
        latency_threshold_ms: float = 1000.0,
        failure_threshold: int = 3,
    ) -> None:
        self.latency_threshold_ms = latency_threshold_ms
        self.failure_threshold = failure_threshold
        self._checks: Dict[str, Callable[[], ComponentHealth]] = {}
        self._failure_counts: Dict[str, int] = {}

    def register_check(
        self,
        name: str,
        check_fn: Callable[[], ComponentHealth],
    ) -> None:
        self._checks[name] = check_fn

    def run_check(self, name: str) -> ComponentHealth:
        check_fn = self._checks.get(name)
        if not check_fn:
            return ComponentHealth(name=name, status=HealthStatus.UNHEALTHY, latency_ms=0, message="No check registered")
        start = time.time()
        try:
            result = check_fn()
            return result
        except Exception as e:
            return ComponentHealth(name=name, status=HealthStatus.UNHEALTHY, latency_ms=0, message=str(e))

    def check_all(self) -> tuple[HealthStatus, List[ComponentHealth]]:
        components = []
        for name in self._checks:
            health = self.run_check(name)
            components.append(health)
        unhealthy = [c for c in components if c.status == HealthStatus.UNHEALTHY]
        degraded = [c for c in components if c.status == HealthStatus.DEGRADED]
        if unhealthy:
            return HealthStatus.UNHEALTHY, components
        elif degraded:
            return HealthStatus.DEGRADED, components
        return HealthStatus.HEALTHY, components

    def get_status(self) -> Dict[str, Any]:
        status, components = self.check_all()
        return {
            "overall_status": status.value,
            "components": [
                {"name": c.name, "status": c.status.value, "latency_ms": c.latency_ms, "message": c.message}
                for c in components
            ],
        }
