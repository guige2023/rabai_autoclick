"""
Health monitor utilities for tracking system and service health.

Provides health check aggregation, status reporting, and alerting
for automation workflow monitoring.

Example:
    >>> from health_monitor_utils import HealthMonitor, HealthStatus
    >>> monitor = HealthMonitor()
    >>> monitor.register("api", lambda: check_api_health())
    >>> status = monitor.get_overall_status()
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


# =============================================================================
# Types
# =============================================================================


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
    latency_ms: float = 0.0
    last_check: float = 0.0
    consecutive_failures: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthReport:
    """Overall health report."""
    overall_status: HealthStatus
    components: List[ComponentHealth]
    timestamp: float
    duration_ms: float
    message: str = ""


# =============================================================================
# Health Check
# =============================================================================


class HealthCheck:
    """
    A health check for a component or service.

    Example:
        >>> check = HealthCheck("database", lambda: db.ping())
        >>> check.execute()
    """

    def __init__(
        self,
        name: str,
        check_fn: Callable[[], bool],
        timeout: float = 5.0,
        critical: bool = True,
    ):
        self.name = name
        self.check_fn = check_fn
        self.timeout = timeout
        self.critical = critical
        self._last_result: Optional[ComponentHealth] = None

    def execute(self) -> ComponentHealth:
        """
        Execute the health check.

        Returns:
            ComponentHealth with the result.
        """
        start = time.monotonic()

        try:
            if self.timeout > 0:
                result = self._execute_with_timeout()
            else:
                result = self.check_fn()

            latency = (time.monotonic() - start) * 1000

            if result:
                return ComponentHealth(
                    name=self.name,
                    status=HealthStatus.HEALTHY,
                    latency_ms=latency,
                    last_check=time.time(),
                )
            else:
                return ComponentHealth(
                    name=self.name,
                    status=HealthStatus.UNHEALTHY,
                    message="Check returned False",
                    latency_ms=latency,
                    last_check=time.time(),
                )

        except Exception as e:
            latency = (time.monotonic() - start) * 1000
            return ComponentHealth(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message=str(e),
                latency_ms=latency,
                last_check=time.time(),
            )

    def _execute_with_timeout(self) -> bool:
        """Execute with timeout using threading."""
        result = {"value": None, "error": None, "done": False}

        def target():
            try:
                result["value"] = self.check_fn()
            except Exception as e:
                result["error"] = e
            finally:
                result["done"] = True

        thread = threading.Thread(target=target)
        thread.start()
        thread.join(timeout=self.timeout)

        if thread.is_alive():
            raise TimeoutError(f"Health check timed out after {self.timeout}s")

        if result["error"]:
            raise result["error"]

        return result["value"] if result["value"] is not None else False

    @property
    def last_result(self) -> Optional[ComponentHealth]:
        """Get last execution result."""
        return self._last_result


# =============================================================================
# Health Monitor
# =============================================================================


class HealthMonitor:
    """
    Aggregates health checks and provides overall status.

    Example:
        >>> monitor = HealthMonitor()
        >>> monitor.register("api", lambda: check_api())
        >>> monitor.register("db", lambda: check_db(), critical=True)
        >>> report = monitor.get_report()
        >>> print(report.overall_status)
    """

    def __init__(
        self,
        check_interval: float = 30.0,
        degradation_threshold: int = 3,
    ):
        self.check_interval = check_interval
        self.degradation_threshold = degradation_threshold

        self._checks: Dict[str, HealthCheck] = {}
        self._results: Dict[str, ComponentHealth] = {}
        self._lock = threading.RLock()
        self._running = False
        self._monitor_thread: Optional[threading.Thread] = None

    def register(
        self,
        name: str,
        check_fn: Callable[[], bool],
        critical: bool = True,
        timeout: float = 5.0,
    ) -> None:
        """
        Register a health check.

        Args:
            name: Component name.
            check_fn: Function returning True if healthy.
            critical: If True, affects overall status.
            timeout: Check timeout in seconds.
        """
        with self._lock:
            self._checks[name] = HealthCheck(
                name=name,
                check_fn=check_fn,
                timeout=timeout,
                critical=critical,
            )

    def unregister(self, name: str) -> None:
        """Unregister a health check."""
        with self._lock:
            self._checks.pop(name, None)
            self._results.pop(name, None)

    def check(self, name: str) -> Optional[ComponentHealth]:
        """
        Execute a specific health check.

        Args:
            name: Component name.

        Returns:
            ComponentHealth result.
        """
        with self._lock:
            check = self._checks.get(name)

        if not check:
            return None

        result = check.execute()

        with self._lock:
            self._results[name] = result

        return result

    def check_all(self) -> HealthReport:
        """
        Execute all health checks.

        Returns:
            HealthReport with overall status.
        """
        start = time.monotonic()
        results: List[ComponentHealth] = []

        with self._lock:
            checks = list(self._checks.items())

        for name, check in checks:
            result = check.execute()
            results.append(result)

            with self._lock:
                self._results[name] = result

        duration = (time.monotonic() - start) * 1000

        # Determine overall status
        overall = self._compute_overall_status(results)

        return HealthReport(
            overall_status=overall,
            components=results,
            timestamp=time.time(),
            duration_ms=duration,
        )

    def _compute_overall_status(self, results: List[ComponentHealth]) -> HealthStatus:
        """Compute overall status from component results."""
        if not results:
            return HealthStatus.UNKNOWN

        critical_unhealthy = False
        non_critical_unhealthy = False
        degraded = False

        for result in results:
            check = self._checks.get(result.name)

            if result.status == HealthStatus.UNHEALTHY:
                if check and check.critical:
                    critical_unhealthy = True
                else:
                    non_critical_unhealthy = True

            elif result.status == HealthStatus.DEGRADED:
                degraded = True

        if critical_unhealthy:
            return HealthStatus.UNHEALTHY
        elif degraded or non_critical_unhealthy:
            return HealthStatus.DEGRADED

        return HealthStatus.HEALTHY

    def get_overall_status(self) -> HealthStatus:
        """Get current overall status without running checks."""
        with self._lock:
            results = list(self._results.values())

        return self._compute_overall_status(results)

    def get_component_status(self, name: str) -> Optional[HealthStatus]:
        """Get status of a specific component."""
        with self._lock:
            result = self._results.get(name)
        return result.status if result else None

    def start_monitoring(self) -> None:
        """Start background monitoring."""
        self._running = True
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop, daemon=True
        )
        self._monitor_thread.start()

    def stop_monitoring(self) -> None:
        """Stop background monitoring."""
        self._running = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=2.0)

    def _monitor_loop(self) -> None:
        """Background monitoring loop."""
        while self._running:
            self.check_all()
            time.sleep(self.check_interval)

    def __enter__(self) -> "HealthMonitor":
        self.start_monitoring()
        return self

    def __exit__(self, *args: Any) -> None:
        self.stop_monitoring()


# =============================================================================
# HTTP Health Endpoint
# =============================================================================


class HTTPHealthServer:
    """
    Simple HTTP server that serves health status.

    Example:
        >>> server = HTTPHealthServer(monitor, port=8080)
        >>> server.start()
        >>> # GET http://localhost:8080/health
    """

    def __init__(
        self,
        monitor: HealthMonitor,
        port: int = 8080,
    ):
        self.monitor = monitor
        self.port = port
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start health server."""
        self._running = True
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop health server."""
        self._running = False

    def _serve(self) -> None:
        """Serve health endpoint (simplified)."""
        import http.server
        import json

        class Handler(http.server.BaseHTTPRequestHandler):
            monitor = self.monitor

            def do_GET(self):
                if self.path == "/health" or self.path == "/":
                    report = self.monitor.check_all()
                    self.send_response(200 if report.overall_status == HealthStatus.HEALTHY else 503)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()

                    data = {
                        "status": report.overall_status.value,
                        "timestamp": report.timestamp,
                        "components": {
                            c.name: {
                                "status": c.status.value,
                                "message": c.message,
                                "latency_ms": c.latency_ms,
                            }
                            for c in report.components
                        },
                    }
                    self.wfile.write(json.dumps(data, indent=2).encode())

            def log_message(self, format, *args):
                pass  # Suppress logging

        server = http.server.HTTPServer(("", self.port), Handler)
        while self._running:
            server.serve_forever(timeout=1.0)
        server.shutdown()


# =============================================================================
# Alert Handler
# =============================================================================


class HealthAlertHandler:
    """
    Handles health status change alerts.

    Example:
        >>> def send_alert(status, message):
        ...     send_email("ops@example.com", message)
        >>> handler = HealthAlertHandler()
        >>> handler.on_unhealthy(lambda s, m: send_alert(s, m))
        >>> handler.check_status_change(monitor.get_report())
    """

    def __init__(self):
        self._callbacks: Dict[HealthStatus, List[Callable]] = {
            status: [] for status in HealthStatus
        }
        self._previous_status: Optional[HealthStatus] = None

    def on_status_change(
        self,
        callback: Callable[[HealthStatus, HealthStatus], None],
    ) -> None:
        """Register callback for any status change."""
        for status in HealthStatus:
            self._callbacks[status].append(
                lambda s, cb=callback: cb(self._previous_status, s)
            )

    def on_healthy(self, callback: Callable[[], None]) -> None:
        """Register callback for healthy status."""
        self._callbacks[HealthStatus.HEALTHY].append(lambda: callback())

    def on_degraded(self, callback: Callable[[], None]) -> None:
        """Register callback for degraded status."""
        self._callbacks[HealthStatus.DEGRADED].append(lambda: callback())

    def on_unhealthy(self, callback: Callable[[], None]) -> None:
        """Register callback for unhealthy status."""
        self._callbacks[HealthStatus.UNHEALTHY].append(lambda: callback())

    def check_status_change(self, report: HealthReport) -> None:
        """
        Check for status changes and fire alerts.

        Args:
            report: Current health report.
        """
        if report.overall_status == self._previous_status:
            return

        previous = self._previous_status
        self._previous_status = report.overall_status

        # Fire callbacks for new status
        callbacks = self._callbacks.get(report.overall_status, [])
        for callback in callbacks:
            try:
                callback()
            except Exception:
                pass
