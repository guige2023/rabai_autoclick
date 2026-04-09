"""Health check action module.

Provides health check functionality for monitoring
service and dependency health status.
"""

from __future__ import annotations

import time
import logging
from typing import Any, Optional, Callable, TypeVar
from dataclasses import dataclass, field
from enum import Enum
from dataclasses import dataclass
import threading

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health check status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    """Result of a health check."""
    name: str
    status: HealthStatus
    message: Optional[str] = None
    duration_ms: float = 0.0
    timestamp: float = field(default_factory=time.time)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthCheckConfig:
    """Configuration for health check."""
    name: str
    check_func: Callable[[], bool]
    timeout: float = 5.0
    critical: bool = True
    tags: list[str] = field(default_factory=list)


class HealthCheck:
    """Individual health check."""

    def __init__(self, config: HealthCheckConfig):
        """Initialize health check.

        Args:
            config: Health check configuration
        """
        self.config = config
        self._last_result: Optional[HealthCheckResult] = None
        self._lock = threading.Lock()

    def execute(self) -> HealthCheckResult:
        """Execute health check.

        Returns:
            HealthCheckResult
        """
        start = time.time()
        status = HealthStatus.UNHEALTHY
        message = None
        details = {}

        try:
            result = self.config.check_func()
            if result:
                status = HealthStatus.HEALTHY
                message = "OK"
            else:
                status = HealthStatus.UNHEALTHY
                message = "Check failed"

        except Exception as e:
            status = HealthStatus.UNHEALTHY
            message = str(e)
            logger.error(f"Health check {self.config.name} failed: {e}")

        duration = (time.time() - start) * 1000
        result = HealthCheckResult(
            name=self.config.name,
            status=status,
            message=message,
            duration_ms=duration,
            details=details,
        )

        with self._lock:
            self._last_result = result

        return result

    def get_last_result(self) -> Optional[HealthCheckResult]:
        """Get last check result."""
        with self._lock:
            return self._last_result


class HealthCheckRegistry:
    """Registry for health checks."""

    def __init__(self):
        """Initialize registry."""
        self._checks: dict[str, HealthCheck] = {}
        self._lock = threading.Lock()

    def register(self, config: HealthCheckConfig) -> HealthCheck:
        """Register a health check.

        Args:
            config: Health check configuration

        Returns:
            HealthCheck instance
        """
        with self._lock:
            check = HealthCheck(config)
            self._checks[config.name] = check
            return check

    def unregister(self, name: str) -> None:
        """Unregister a health check.

        Args:
            name: Check name
        """
        with self._lock:
            self._checks.pop(name, None)

    def get_check(self, name: str) -> Optional[HealthCheck]:
        """Get health check by name."""
        with self._lock:
            return self._checks.get(name)

    def list_checks(self) -> list[str]:
        """List all registered check names."""
        with self._lock:
            return list(self._checks.keys())

    def execute_all(self) -> list[HealthCheckResult]:
        """Execute all health checks.

        Returns:
            List of results
        """
        with self._lock:
            checks = list(self._checks.values())

        return [check.execute() for check in checks]


class HealthMonitor:
    """Health monitoring service."""

    def __init__(self, registry: HealthCheckRegistry):
        """Initialize monitor.

        Args:
            registry: Health check registry
        """
        self.registry = registry
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._last_overall_status = HealthStatus.UNKNOWN

    def start(self, interval: float = 30.0) -> None:
        """Start health monitoring.

        Args:
            interval: Check interval in seconds
        """
        with self._lock:
            if self._running:
                return
            self._running = True

        def run():
            while self._running:
                try:
                    self._check_and_alert()
                except Exception as e:
                    logger.error(f"Health monitor error: {e}")
                time.sleep(interval)

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()
        logger.info(f"Health monitor started (interval={interval}s)")

    def stop(self) -> None:
        """Stop health monitoring."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        logger.info("Health monitor stopped")

    def get_status(self) -> dict[str, Any]:
        """Get overall health status.

        Returns:
            Dictionary with health status info
        """
        results = self.registry.execute_all()
        healthy = sum(1 for r in results if r.status == HealthStatus.HEALTHY)
        degraded = sum(1 for r in results if r.status == HealthStatus.DEGRADED)
        unhealthy = sum(1 for r in results if r.status == HealthStatus.UNHEALTHY)

        if unhealthy > 0:
            overall = HealthStatus.UNHEALTHY
        elif degraded > 0:
            overall = HealthStatus.DEGRADED
        else:
            overall = HealthStatus.HEALTHY

        return {
            "status": overall.value,
            "healthy_count": healthy,
            "degraded_count": degraded,
            "unhealthy_count": unhealthy,
            "total_count": len(results),
            "checks": [
                {
                    "name": r.name,
                    "status": r.status.value,
                    "message": r.message,
                    "duration_ms": r.duration_ms,
                }
                for r in results
            ],
        }

    def _check_and_alert(self) -> None:
        """Check health and send alerts if needed."""
        results = self.registry.execute_all()
        unhealthy = [r for r in results if r.status == HealthStatus.UNHEALTHY]

        if unhealthy and self._last_overall_status != HealthStatus.UNHEALTHY:
            logger.warning(
                f"Health alert: {len(unhealthy)} unhealthy checks: "
                f"{[r.name for r in unhealthy]}"
            )

        critical = [
            r for r in unhealthy
            if self.registry.get_check(r.name).config.critical
        ]

        if critical:
            logger.error(f"Critical health checks failed: {[r.name for r in critical]}")

        self._last_overall_status = (
            HealthStatus.UNHEALTHY if unhealthy else HealthStatus.HEALTHY
        )


def create_health_check(
    name: str,
    check_func: Callable[[], bool],
    timeout: float = 5.0,
    critical: bool = True,
) -> HealthCheckConfig:
    """Create health check configuration.

    Args:
        name: Check name
        check_func: Check function
        timeout: Check timeout
        critical: Is critical check

    Returns:
        HealthCheckConfig
    """
    return HealthCheckConfig(
        name=name,
        check_func=check_func,
        timeout=timeout,
        critical=critical,
    )


def ping_check(host: str, port: int) -> Callable[[], bool]:
    """Create ping-based health check.

    Args:
        host: Host to ping
        port: Port to check

    Returns:
        Health check function
    """
    def check() -> bool:
        try:
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            sock.close()
            return result == 0
        except Exception:
            return False
    return check


def http_check(url: str, expected_status: int = 200) -> Callable[[], bool]:
    """Create HTTP-based health check.

    Args:
        url: URL to check
        expected_status: Expected status code

    Returns:
        Health check function
    """
    def check() -> bool:
        try:
            import urllib.request
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=5) as response:
                return response.status == expected_status
        except Exception:
            return False
    return check
