"""
Health Check and Readiness Probe Utilities.

Provides utilities for implementing health checks, readiness probes,
and dependency monitoring for microservices.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import json
import socket
import sqlite3
import threading
import time
import urllib.request
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional


class HealthStatus(Enum):
    """Health check status values."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class CheckType(Enum):
    """Types of health checks."""
    HTTP = "http"
    TCP = "tcp"
    DATABASE = "database"
    REDIS = "redis"
    PROCESS = "process"
    CUSTOM = "custom"


@dataclass
class HealthCheckResult:
    """Result of a single health check."""
    name: str
    status: HealthStatus
    check_type: CheckType
    response_time_ms: float
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    consecutive_failures: int = 0


@dataclass
class ComponentHealth:
    """Health status of a single component."""
    name: str
    status: HealthStatus
    checks: list[HealthCheckResult] = field(default_factory=list)
    last_check: Optional[datetime] = None
    uptime_seconds: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SystemHealth:
    """Overall system health status."""
    status: HealthStatus
    timestamp: datetime = field(default_factory=datetime.now)
    version: str = "1.0.0"
    environment: str = "production"
    components: list[ComponentHealth] = field(default_factory=list)
    total_response_time_ms: float = 0.0
    healthy_components: int = 0
    unhealthy_components: int = 0
    degraded_components: int = 0


class HealthCheck:
    """Base class for health checks."""

    def __init__(
        self,
        name: str,
        check_type: CheckType,
        timeout_seconds: float = 5.0,
        critical: bool = True,
    ) -> None:
        self.name = name
        self.check_type = check_type
        self.timeout_seconds = timeout_seconds
        self.critical = critical
        self._consecutive_failures = 0

    def check(self) -> HealthCheckResult:
        """Perform the health check."""
        raise NotImplementedError

    def _create_result(
        self,
        status: HealthStatus,
        response_time_ms: float,
        message: str = "",
        details: Optional[dict[str, Any]] = None,
    ) -> HealthCheckResult:
        """Create a health check result."""
        if status == HealthStatus.UNHEALTHY:
            self._consecutive_failures += 1
        else:
            self._consecutive_failures = 0

        return HealthCheckResult(
            name=self.name,
            status=status,
            check_type=self.check_type,
            response_time_ms=response_time_ms,
            message=message,
            details=details or {},
            consecutive_failures=self._consecutive_failures,
        )


class HTTPHealthCheck(HealthCheck):
    """HTTP endpoint health check."""

    def __init__(
        self,
        name: str,
        url: str,
        expected_status: int = 200,
        expected_content: Optional[str] = None,
        headers: Optional[dict[str, str]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(name, CheckType.HTTP, **kwargs)
        self.url = url
        self.expected_status = expected_status
        self.expected_content = expected_content
        self.headers = headers or {}

    def check(self) -> HealthCheckResult:
        """Perform HTTP health check."""
        start_time = time.time()

        try:
            request = urllib.request.Request(self.url)
            for key, value in self.headers.items():
                request.add_header(key, value)

            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                response_time_ms = (time.time() - start_time) * 1000
                content = response.read().decode("utf-8", errors="ignore")

                if response.status != self.expected_status:
                    return self._create_result(
                        HealthStatus.UNHEALTHY,
                        response_time_ms,
                        f"Expected status {self.expected_status}, got {response.status}",
                    )

                if self.expected_content and self.expected_content not in content:
                    return self._create_result(
                        HealthStatus.UNHEALTHY,
                        response_time_ms,
                        f"Expected content not found in response",
                    )

                return self._create_result(
                    HealthStatus.HEALTHY,
                    response_time_ms,
                    "HTTP check passed",
                )

        except urllib.error.HTTPError as e:
            response_time_ms = (time.time() - start_time) * 1000
            return self._create_result(
                HealthStatus.UNHEALTHY,
                response_time_ms,
                f"HTTP error: {e.code} {e.reason}",
            )
        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            return self._create_result(
                HealthStatus.UNHEALTHY,
                response_time_ms,
                f"Check failed: {str(e)}",
            )


class TCPHealthCheck(HealthCheck):
    """TCP port health check."""

    def __init__(
        self,
        name: str,
        host: str,
        port: int,
        **kwargs: Any,
    ) -> None:
        super().__init__(name, CheckType.TCP, **kwargs)
        self.host = host
        self.port = port

    def check(self) -> HealthCheckResult:
        """Perform TCP health check."""
        start_time = time.time()

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.timeout_seconds)
            result = sock.connect_ex((self.host, self.port))
            sock.close()

            response_time_ms = (time.time() - start_time) * 1000

            if result == 0:
                return self._create_result(
                    HealthStatus.HEALTHY,
                    response_time_ms,
                    f"TCP connection to {self.host}:{self.port} successful",
                )
            else:
                return self._create_result(
                    HealthStatus.UNHEALTHY,
                    response_time_ms,
                    f"TCP connection to {self.host}:{self.port} failed",
                )

        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            return self._create_result(
                HealthStatus.UNHEALTHY,
                response_time_ms,
                f"TCP check failed: {str(e)}",
            )


class DatabaseHealthCheck(HealthCheck):
    """Database connectivity health check."""

    def __init__(
        self,
        name: str,
        db_path: Path,
        **kwargs: Any,
    ) -> None:
        super().__init__(name, CheckType.DATABASE, **kwargs)
        self.db_path = db_path

    def check(self) -> HealthCheckResult:
        """Perform database health check."""
        start_time = time.time()

        try:
            conn = sqlite3.connect(str(self.db_path), timeout=1)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            conn.close()

            response_time_ms = (time.time() - start_time) * 1000

            return self._create_result(
                HealthStatus.HEALTHY,
                response_time_ms,
                "Database connection successful",
            )

        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            return self._create_result(
                HealthStatus.UNHEALTHY,
                response_time_ms,
                f"Database check failed: {str(e)}",
            )


class ProcessHealthCheck(HealthCheck):
    """Process existence health check."""

    def __init__(
        self,
        name: str,
        process_name: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(name, CheckType.PROCESS, **kwargs)
        self.process_name = process_name

    def check(self) -> HealthCheckResult:
        """Perform process health check."""
        start_time = time.time()

        try:
            import subprocess
            result = subprocess.run(
                ["pgrep", "-x", self.process_name],
                capture_output=True,
                timeout=self.timeout_seconds,
            )

            response_time_ms = (time.time() - start_time) * 1000

            if result.returncode == 0:
                return self._create_result(
                    HealthStatus.HEALTHY,
                    response_time_ms,
                    f"Process {self.process_name} is running",
                )
            else:
                return self._create_result(
                    HealthStatus.UNHEALTHY,
                    response_time_ms,
                    f"Process {self.process_name} is not running",
                )

        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            return self._create_result(
                HealthStatus.UNHEALTHY,
                response_time_ms,
                f"Process check failed: {str(e)}",
            )


class CustomHealthCheck(HealthCheck):
    """Custom health check with user-defined function."""

    def __init__(
        self,
        name: str,
        check_function: Callable[[], tuple[bool, str, dict[str, Any]]],
        **kwargs: Any,
    ) -> None:
        super().__init__(name, CheckType.CUSTOM, **kwargs)
        self.check_function = check_function

    def check(self) -> HealthCheckResult:
        """Perform custom health check."""
        start_time = time.time()

        try:
            success, message, details = self.check_function()
            response_time_ms = (time.time() - start_time) * 1000

            if success:
                return self._create_result(
                    HealthStatus.HEALTHY,
                    response_time_ms,
                    message,
                    details,
                )
            else:
                return self._create_result(
                    HealthStatus.UNHEALTHY,
                    response_time_ms,
                    message,
                    details,
                )

        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            return self._create_result(
                HealthStatus.UNHEALTHY,
                response_time_ms,
                f"Custom check failed: {str(e)}",
            )


class HealthMonitor:
    """Monitors health of multiple components."""

    def __init__(
        self,
        component_name: str,
        interval_seconds: int = 30,
        failure_threshold: int = 3,
    ) -> None:
        self.component_name = component_name
        self.interval_seconds = interval_seconds
        self.failure_threshold = failure_threshold

        self._checks: dict[str, HealthCheck] = {}
        self._lock = threading.Lock()
        self._start_time = datetime.now()
        self._last_results: dict[str, HealthCheckResult] = {}
        self._running = False
        self._monitor_thread: Optional[threading.Thread] = None

    def register_check(self, check: HealthCheck) -> None:
        """Register a health check."""
        with self._lock:
            self._checks[check.name] = check

    def unregister_check(self, name: str) -> bool:
        """Unregister a health check."""
        with self._lock:
            if name in self._checks:
                del self._checks[name]
                return True
        return False

    def run_checks(self) -> ComponentHealth:
        """Run all health checks."""
        results: list[HealthCheckResult] = []
        total_response_time = 0.0

        with self._lock:
            checks = list(self._checks.values())

        for check in checks:
            result = check.check()
            results.append(result)
            total_response_time += result.response_time_ms

        overall_status = self._determine_overall_status(results)

        return ComponentHealth(
            name=self.component_name,
            status=overall_status,
            checks=results,
            last_check=datetime.now(),
            uptime_seconds=(datetime.now() - self._start_time).total_seconds(),
        )

    def _determine_overall_status(
        self,
        results: list[HealthCheckResult],
    ) -> HealthStatus:
        """Determine overall component status from check results."""
        if not results:
            return HealthStatus.UNKNOWN

        critical_failures = [r for r in results if r.critical and r.status == HealthStatus.UNHEALTHY]
        if critical_failures:
            return HealthStatus.UNHEALTHY

        non_critical_failures = [r for r in results if not r.critical and r.status == HealthStatus.UNHEALTHY]
        if non_critical_failures:
            return HealthStatus.DEGRADED

        degraded = [r for r in results if r.status == HealthStatus.DEGRADED]
        if degraded:
            return HealthStatus.DEGRADED

        return HealthStatus.HEALTHY

    def get_last_results(self) -> dict[str, HealthCheckResult]:
        """Get the last check results."""
        with self._lock:
            return dict(self._last_results)

    def start_monitoring(self) -> None:
        """Start background health monitoring."""
        self._running = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop)
        self._monitor_thread.daemon = True
        self._monitor_thread.start()

    def stop_monitoring(self) -> None:
        """Stop background health monitoring."""
        self._running = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)

    def _monitor_loop(self) -> None:
        """Background monitoring loop."""
        while self._running:
            health = self.run_checks()

            with self._lock:
                for result in health.checks:
                    self._last_results[result.name] = result

            time.sleep(self.interval_seconds)

    def to_system_health(self) -> SystemHealth:
        """Convert to system health format."""
        component_health = self.run_checks()

        healthy = sum(1 for c in [component_health] if c.status == HealthStatus.HEALTHY)
        unhealthy = sum(1 for c in [component_health] if c.status == HealthStatus.UNHEALTHY)
        degraded = sum(1 for c in [component_health] if c.status == HealthStatus.DEGRADED)

        return SystemHealth(
            status=component_health.status,
            components=[component_health],
            total_response_time_ms=sum(r.response_time_ms for r in component_health.checks),
            healthy_components=healthy,
            unhealthy_components=unhealthy,
            degraded_components=degraded,
        )


class ReadinessProbe:
    """Readiness probe for Kubernetes-style readiness checks."""

    def __init__(
        self,
        checks: list[HealthCheck],
        failure_threshold: int = 3,
    ) -> None:
        self.checks = checks
        self.failure_threshold = failure_threshold
        self._failure_count = 0
        self._is_ready = False
        self._lock = threading.Lock()

    def check_readiness(self) -> bool:
        """Check if the service is ready."""
        with self._lock:
            all_passed = True

            for check in self.checks:
                result = check.check()
                if result.status != HealthStatus.HEALTHY:
                    all_passed = False
                    break

            if all_passed:
                self._failure_count = 0
                self._is_ready = True
            else:
                self._failure_count += 1
                if self._failure_count >= self.failure_threshold:
                    self._is_ready = False

            return self._is_ready

    def is_ready(self) -> bool:
        """Check if service is currently ready."""
        with self._lock:
            return self._is_ready

    def reset(self) -> None:
        """Reset readiness state."""
        with self._lock:
            self._failure_count = 0
            self._is_ready = True


class LivenessProbe:
    """Liveness probe for Kubernetes-style liveness checks."""

    def __init__(
        self,
        check: HealthCheck,
        initial_delay_seconds: int = 0,
    ) -> None:
        self.check = check
        self.initial_delay_seconds = initial_delay_seconds
        self._start_time = datetime.now()
        self._is_alive = True
        self._lock = threading.Lock()

    def check_liveness(self) -> bool:
        """Check if the service is alive."""
        with self._lock:
            elapsed = (datetime.now() - self._start_time).total_seconds()
            if elapsed < self.initial_delay_seconds:
                return True

            result = self.check.check()

            if result.status == HealthStatus.HEALTHY:
                self._is_alive = True
            else:
                self._is_alive = False

            return self._is_alive

    def is_alive(self) -> bool:
        """Check if service is currently alive."""
        with self._lock:
            return self._is_alive
