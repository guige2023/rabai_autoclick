"""API Health Check Action Module.

Provides health check functionality for API endpoints and services,
including dependency checking, status aggregation, and alerting.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health check status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    """Result of a single health check."""
    component: str
    status: HealthStatus
    latency_ms: Optional[float] = None
    message: Optional[str] = None
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "component": self.component,
            "status": self.status.value,
            "latency_ms": self.latency_ms,
            "message": self.message,
            "error": self.error,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
        }


@dataclass
class HealthCheckConfig:
    """Configuration for health checks."""
    timeout_seconds: float = 5.0
    retry_count: int = 2
    retry_delay_seconds: float = 1.0
    healthy_threshold: int = 2
    unhealthy_threshold: int = 3
    enabled: bool = True


class HealthCheckRegistry:
    """Registry for health check functions."""

    def __init__(self):
        self._checks: Dict[str, Callable[[], Any]] = {}
        self._configs: Dict[str, HealthCheckConfig] = {}
        self._status_cache: Dict[str, HealthCheckResult] = {}
        self._cache_ttl: timedelta = timedelta(seconds=30)

    def register(
        self,
        name: str,
        check_fn: Callable[[], Any],
        config: Optional[HealthCheckConfig] = None,
    ) -> None:
        self._checks[name] = check_fn
        self._configs[name] = config or HealthCheckConfig()

    def unregister(self, name: str) -> None:
        self._checks.pop(name, None)
        self._configs.pop(name, None)
        self._status_cache.pop(name, None)

    async def check(self, name: str) -> HealthCheckResult:
        if name not in self._checks:
            return HealthCheckResult(
                component=name,
                status=HealthStatus.UNKNOWN,
                error=f"Health check '{name}' not registered",
            )

        config = self._configs.get(name, HealthCheckConfig())
        if not config.enabled:
            return HealthCheckResult(
                component=name,
                status=HealthStatus.UNKNOWN,
                message="Health check disabled",
            )

        cached = self._status_cache.get(name)
        if cached and datetime.now() - cached.timestamp < self._cache_ttl:
            return cached

        for attempt in range(config.retry_count + 1):
            try:
                start = datetime.now()
                result = await asyncio.wait_for(
                    asyncio.to_thread(self._checks[name]),
                    timeout=config.timeout_seconds,
                )
                latency = (datetime.now() - start).total_seconds() * 1000

                if isinstance(result, HealthCheckResult):
                    return result

                status = HealthStatus.HEALTHY if result else HealthStatus.DEGRADED
                health_result = HealthCheckResult(
                    component=name,
                    status=status,
                    latency_ms=latency,
                    message=str(result) if result else None,
                )
                self._status_cache[name] = health_result
                return health_result

            except asyncio.TimeoutError:
                health_result = HealthCheckResult(
                    component=name,
                    status=HealthStatus.UNHEALTHY,
                    error=f"Timeout after {config.timeout_seconds}s",
                )
            except Exception as e:
                health_result = HealthCheckResult(
                    component=name,
                    status=HealthStatus.UNHEALTHY,
                    error=str(e),
                )

            if attempt < config.retry_count:
                await asyncio.sleep(config.retry_delay_seconds)

        self._status_cache[name] = health_result
        return health_result

    async def check_all(self) -> Dict[str, HealthCheckResult]:
        tasks = [self.check(name) for name in self._checks]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return dict(zip(self._checks.keys(), results))

    def get_overall_status(self, results: Dict[str, HealthCheckResult]) -> HealthStatus:
        statuses = [r.status for r in results.values() if isinstance(r, HealthCheckResult)]
        if not statuses:
            return HealthStatus.UNKNOWN
        if any(s == HealthStatus.UNHEALTHY for s in statuses):
            return HealthStatus.UNHEALTHY
        if any(s == HealthStatus.DEGRADED for s in statuses):
            return HealthStatus.DEGRADED
        return HealthStatus.HEALTHY


_global_registry: Optional[HealthCheckRegistry] = None


def get_health_registry() -> HealthCheckRegistry:
    global _global_registry
    if _global_registry is None:
        _global_registry = HealthCheckRegistry()
    return _global_registry


def register_health_check(
    name: str,
    config: Optional[HealthCheckConfig] = None,
) -> Callable:
    def decorator(fn: Callable[[], Any]) -> Callable:
        get_health_registry().register(name, fn, config)
        return fn
    return decorator


async def check_api_health(endpoints: List[str]) -> Dict[str, HealthCheckResult]:
    results = {}
    for endpoint in endpoints:
        parts = endpoint.split("://", 1)
        scheme = parts[0] if len(parts) > 1 else "http"
        host = parts[1] if len(parts) > 1 else parts[0]

        async def check_endpoint() -> bool:
            try:
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"{scheme}://{host}", timeout=5) as resp:
                        return resp.status < 500
            except Exception:
                return False

        registry = get_health_registry()
        registry.register(endpoint, check_endpoint)
        results[endpoint] = await registry.check(endpoint)
    return results
