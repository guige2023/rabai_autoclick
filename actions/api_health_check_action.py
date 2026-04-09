"""API Health Check Monitor.

This module provides health check capabilities:
- Component-level health checks
- Dependency monitoring
- Health status aggregation
- Automatic recovery detection

Example:
    >>> from actions.api_health_check_action import HealthCheckMonitor
    >>> monitor = HealthCheckMonitor()
    >>> monitor.register_component("database", check_db)
    >>> status = monitor.get_health_status()
"""

from __future__ import annotations

import time
import logging
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class ComponentHealth:
    """Health status of a single component."""
    name: str
    status: HealthStatus
    message: str = ""
    last_check: float = field(default_factory=time.time)
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    latency_ms: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


class HealthCheckMonitor:
    """Monitors health of API components and dependencies."""

    def __init__(
        self,
        failure_threshold: int = 3,
        recovery_threshold: int = 2,
        check_timeout: float = 5.0,
    ) -> None:
        """Initialize the health monitor.

        Args:
            failure_threshold: Consecutive failures before unhealthy.
            recovery_threshold: Consecutive successes before healthy.
            check_timeout: Timeout for each health check.
        """
        self._components: dict[str, Callable[[], tuple[bool, str]]] = {}
        self._component_health: dict[str, ComponentHealth] = {}
        self._failure_threshold = failure_threshold
        self._recovery_threshold = recovery_threshold
        self._check_timeout = check_timeout
        self._lock = threading.RLock()
        self._stats = {"checks": 0, "healthy": 0, "unhealthy": 0}

    def register_component(
        self,
        name: str,
        check_func: Callable[[], tuple[bool, str]],
        description: str = "",
    ) -> None:
        """Register a component for health monitoring.

        Args:
            name: Component name.
            check_func: Function returning (is_healthy, message).
            description: Human-readable description.
        """
        with self._lock:
            self._components[name] = check_func
            self._component_health[name] = ComponentHealth(
                name=name,
                status=HealthStatus.UNKNOWN,
                message=f"Registered: {description}" if description else "Registered",
            )
            logger.info("Registered health check component: %s", name)

    def unregister_component(self, name: str) -> None:
        """Unregister a component.

        Args:
            name: Component name.
        """
        with self._lock:
            self._components.pop(name, None)
            self._component_health.pop(name, None)
            logger.info("Unregistered health check component: %s", name)

    def check_component(self, name: str) -> ComponentHealth:
        """Manually trigger a health check for a component.

        Args:
            name: Component name.

        Returns:
            The ComponentHealth result.
        """
        check_func = self._components.get(name)
        if check_func is None:
            return ComponentHealth(name=name, status=HealthStatus.UNKNOWN, message="Component not registered")

        start = time.time()
        try:
            is_healthy, message = check_func()
            latency = (time.time() - start) * 1000
        except Exception as e:
            is_healthy = False
            message = f"Exception: {type(e).__name__}: {e}"
            latency = (time.time() - start) * 1000

        health = self._component_health.get(name)
        if health is None:
            return ComponentHealth(name=name, status=HealthStatus.UNKNOWN, message=message)

        return self._update_health(health, is_healthy, message, latency)

    def check_all(self) -> dict[str, ComponentHealth]:
        """Run health checks on all components.

        Returns:
            Dict mapping component name to health status.
        """
        with self._lock:
            results = {}
            for name in list(self._components.keys()):
                results[name] = self.check_component(name)
        return results

    def _update_health(
        self,
        health: ComponentHealth,
        is_healthy: bool,
        message: str,
        latency_ms: float,
    ) -> ComponentHealth:
        """Update component health based on check result."""
        health.last_check = time.time()
        health.message = message
        health.latency_ms = latency_ms

        if is_healthy:
            health.consecutive_successes += 1
            health.consecutive_failures = 0

            if health.consecutive_successes >= self._recovery_threshold:
                health.status = HealthStatus.HEALTHY
        else:
            health.consecutive_failures += 1
            health.consecutive_successes = 0

            if health.consecutive_failures >= self._failure_threshold:
                health.status = HealthStatus.UNHEALTHY

        self._stats["checks"] += 1
        if health.status == HealthStatus.HEALTHY:
            self._stats["healthy"] += 1
        elif health.status == HealthStatus.UNHEALTHY:
            self._stats["unhealthy"] += 1

        return health

    def get_health_status(self) -> dict[str, Any]:
        """Get overall health status.

        Returns:
            Dict with overall status and component details.
        """
        with self._lock:
            components = dict(self._component_health)
            statuses = [c.status for c in components.values()]

            if not statuses:
                overall = HealthStatus.UNKNOWN
            elif HealthStatus.UNHEALTHY in statuses:
                overall = HealthStatus.UNHEALTHY
            elif HealthStatus.DEGRADED in statuses or HealthStatus.UNKNOWN in statuses:
                overall = HealthStatus.DEGRADED
            else:
                overall = HealthStatus.HEALTHY

            return {
                "status": overall.value,
                "components": {name: {"status": c.status.value, "message": c.message} for name, c in components.items()},
                "timestamp": time.time(),
            }

    def is_healthy(self) -> bool:
        """Quick check if overall system is healthy."""
        return self.get_health_status()["status"] == HealthStatus.HEALTHY.value

    def get_stats(self) -> dict[str, int]:
        """Get health check statistics."""
        with self._lock:
            return dict(self._stats)
