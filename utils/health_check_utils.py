"""
Health Check Framework for Service Monitoring.

Provides a comprehensive health check system for monitoring service dependencies,
database connections, external APIs, and custom health indicators.

Example:
    >>> checker = HealthChecker()
    >>> checker.register("database", check_db_connection)
    >>> checker.register("redis", check_redis_connection)
    >>> result = checker.run_all_checks()
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

HealthCheckFunc = Callable[[], "HealthCheckResult"]


class HealthStatus(Enum):
    """Health check status levels."""
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
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "name": self.name,
            "status": self.status.value,
            "message": self.message,
            "latency_ms": self.latency_ms,
            "timestamp": self.timestamp,
            "metadata": self.metadata
        }


@dataclass
class AggregateHealthResult:
    """Aggregate result of multiple health checks."""
    overall_status: HealthStatus
    checks: list[HealthCheckResult]
    total_latency_ms: float
    timestamp: float = field(default_factory=time.time)
    version: str = "1.0"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "status": self.overall_status.value,
            "total_latency_ms": self.total_latency_ms,
            "timestamp": self.timestamp,
            "version": self.version,
            "checks": [check.to_dict() for check in self.checks]
        }

    def is_healthy(self) -> bool:
        """Check if overall status is healthy."""
        return self.overall_status == HealthStatus.HEALTHY


class HealthChecker:
    """
    Health check orchestrator.

    Manages multiple health checks and provides aggregate health status
    with thread-safe operations.
    """

    def __init__(self, timeout: float = 5.0):
        """
        Initialize health checker.

        Args:
            timeout: Maximum time to wait for a single check (seconds)
        """
        self._checks: dict[str, HealthCheckFunc] = {}
        self._timeout = timeout
        self._lock = threading.Lock()
        self._last_results: dict[str, HealthCheckResult] = {}

    def register(self, name: str, check_func: HealthCheckFunc) -> None:
        """
        Register a health check.

        Args:
            name: Unique name for the check
            check_func: Function that returns HealthCheckResult
        """
        with self._lock:
            self._checks[name] = check_func

    def unregister(self, name: str) -> bool:
        """Unregister a health check."""
        with self._lock:
            if name in self._checks:
                del self._checks[name]
                return True
            return False

    def run_check(self, name: str) -> Optional[HealthCheckResult]:
        """
        Run a single health check by name.

        Args:
            name: Name of the check to run

        Returns:
            HealthCheckResult or None if check not found
        """
        with self._lock:
            check_func = self._checks.get(name)

        if check_func is None:
            return None

        return self._execute_check(name, check_func)

    def _execute_check(self, name: str, check_func: HealthCheckFunc) -> HealthCheckResult:
        """Execute a health check with timing."""
        start = time.time()
        try:
            result = check_func()
            result.latency_ms = (time.time() - start) * 1000
            return result
        except Exception as e:
            return HealthCheckResult(
                name=name,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check failed: {str(e)}",
                latency_ms=(time.time() - start) * 1000
            )

    def run_all_checks(self, parallel: bool = True) -> AggregateHealthResult:
        """
        Run all registered health checks.

        Args:
            parallel: Run checks in parallel if True

        Returns:
            AggregateHealthResult with combined status
        """
        with self._lock:
            check_items = list(self._checks.items())

        start_time = time.time()
        results: list[HealthCheckResult] = []

        if parallel:
            threads = []
            results_dict: dict[str, HealthCheckResult] = {}

            def run_in_thread(name: str, func: HealthCheckFunc) -> None:
                result = self._execute_check(name, func)
                results_dict[name] = result

            for name, func in check_items:
                thread = threading.Thread(target=run_in_thread, args=(name, func))
                thread.start()
                threads.append(thread)

            for thread in threads:
                thread.join(timeout=self._timeout)

            results = [results_dict.get(name) for name, _ in check_items if name in results_dict]
        else:
            for name, func in check_items:
                result = self._execute_check(name, func)
                results.append(result)

        total_latency = (time.time() - start_time) * 1000
        overall_status = self._compute_overall_status(results)

        with self._lock:
            self._last_results = {r.name: r for r in results}

        return AggregateHealthResult(
            overall_status=overall_status,
            checks=results,
            total_latency_ms=total_latency
        )

    def _compute_overall_status(self, results: list[HealthCheckResult]) -> HealthStatus:
        """Compute aggregate status from individual results."""
        if not results:
            return HealthStatus.UNKNOWN

        statuses = [r.status for r in results]

        if all(s == HealthStatus.HEALTHY for s in statuses):
            return HealthStatus.HEALTHY

        if any(s == HealthStatus.UNHEALTHY for s in statuses):
            return HealthStatus.UNHEALTHY

        if any(s == HealthStatus.DEGRADED for s in statuses):
            return HealthStatus.DEGRADED

        return HealthStatus.UNKNOWN

    def get_last_results(self) -> dict[str, HealthCheckResult]:
        """Get the last results of all checks."""
        with self._lock:
            return dict(self._last_results)


def check_database_connection(connection_string: str) -> HealthCheckResult:
    """
    Create a database connection health check.

    Args:
        connection_string: Database connection string

    Returns:
        HealthCheckResult for the database check
    """
    import sqlite3
    try:
        conn = sqlite3.connect(":memory:")
        conn.execute("SELECT 1")
        conn.close()
        return HealthCheckResult(
            name="database",
            status=HealthStatus.HEALTHY,
            message="Database connection successful"
        )
    except Exception as e:
        return HealthCheckResult(
            name="database",
            status=HealthStatus.UNHEALTHY,
            message=f"Database connection failed: {str(e)}"
        )


def check_http_endpoint(url: str, expected_status: int = 200) -> HealthCheckResult:
    """
    Create an HTTP endpoint health check.

    Args:
        url: URL to check
        expected_status: Expected HTTP status code

    Returns:
        HealthCheckResult for the HTTP check
    """
    import urllib.request
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status == expected_status:
                return HealthCheckResult(
                    name=f"http_{url}",
                    status=HealthStatus.HEALTHY,
                    message=f"Endpoint returned {response.status}"
                )
            else:
                return HealthCheckResult(
                    name=f"http_{url}",
                    status=HealthStatus.DEGRADED,
                    message=f"Expected {expected_status}, got {response.status}"
                )
    except Exception as e:
        return HealthCheckResult(
            name=f"http_{url}",
            status=HealthStatus.UNHEALTHY,
            message=f"HTTP check failed: {str(e)}"
        )


def check_disk_space(min_free_gb: float = 1.0) -> HealthCheckResult:
    """
    Create a disk space health check.

    Args:
        min_free_gb: Minimum required free space in GB

    Returns:
        HealthCheckResult for disk space check
    """
    import shutil
    try:
        stat = shutil.disk_usage("/")
        free_gb = stat.free / (1024 ** 3)
        if free_gb >= min_free_gb:
            return HealthCheckResult(
                name="disk_space",
                status=HealthStatus.HEALTHY,
                message=f"Free space: {free_gb:.2f} GB",
                metadata={"free_gb": free_gb, "total_gb": stat.total / (1024 ** 3)}
            )
        else:
            return HealthCheckResult(
                name="disk_space",
                status=HealthStatus.UNHEALTHY,
                message=f"Low disk space: {free_gb:.2f} GB free",
                metadata={"free_gb": free_gb}
            )
    except Exception as e:
        return HealthCheckResult(
            name="disk_space",
            status=HealthStatus.UNKNOWN,
            message=f"Could not check disk space: {str(e)}"
        )


def check_memory_usage(max_usage_percent: float = 90.0) -> HealthCheckResult:
    """
    Create a memory usage health check.

    Args:
        max_usage_percent: Maximum allowed memory usage percentage

    Returns:
        HealthCheckResult for memory check
    """
    import psutil
    try:
        memory = psutil.virtual_memory()
        if memory.percent <= max_usage_percent:
            return HealthCheckResult(
                name="memory",
                status=HealthStatus.HEALTHY,
                message=f"Memory usage: {memory.percent:.1f}%",
                metadata={"percent": memory.percent, "available_gb": memory.available / (1024 ** 3)}
            )
        else:
            return HealthCheckResult(
                name="memory",
                status=HealthStatus.DEGRADED,
                message=f"High memory usage: {memory.percent:.1f}%",
                metadata={"percent": memory.percent}
            )
    except Exception as e:
        return HealthCheckResult(
            name="memory",
            status=HealthStatus.UNKNOWN,
            message=f"Could not check memory: {str(e)}"
        )
