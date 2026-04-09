"""Automation health check action for system monitoring.

Performs periodic health checks on automation systems
and reports status with alerting capabilities.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health check status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheck:
    """A single health check configuration."""
    name: str
    check_fn: Callable[[], bool]
    interval_seconds: float = 60.0
    timeout_seconds: float = 10.0
    critical: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthReport:
    """Result of a health check run."""
    status: HealthStatus
    checks: dict[str, bool]
    last_check: float
    uptime_seconds: float
    metadata: dict[str, Any] = field(default_factory=dict)


class AutomationHealthCheckAction:
    """Monitor automation system health with checks and alerts.

    Args:
        check_interval: Seconds between health check cycles.

    Example:
        >>> health = AutomationHealthCheckAction()
        >>> health.add_check("api", api_health_fn, critical=True)
        >>> await health.start()
    """

    def __init__(self, check_interval: float = 60.0) -> None:
        self.check_interval = check_interval
        self._checks: dict[str, HealthCheck] = {}
        self._running = False
        self._last_report: Optional[HealthReport] = None
        self._start_time = time.time()
        self._alert_handlers: list[Callable[[HealthStatus, HealthReport], None]] = []

    def add_check(
        self,
        name: str,
        check_fn: Callable[[], bool],
        critical: bool = False,
        interval_seconds: float = 60.0,
    ) -> "AutomationHealthCheckAction":
        """Add a health check.

        Args:
            name: Unique name for this check.
            check_fn: Function returning True if healthy.
            critical: If True, failures trigger alerts.
            interval_seconds: How often to run this check.

        Returns:
            Self for method chaining.
        """
        self._checks[name] = HealthCheck(
            name=name,
            check_fn=check_fn,
            critical=critical,
            interval_seconds=interval_seconds,
        )
        return self

    def add_alert_handler(
        self,
        handler: Callable[[HealthStatus, HealthReport], None],
    ) -> None:
        """Add an alert handler for health status changes.

        Args:
            handler: Function to call when status changes.
        """
        self._alert_handlers.append(handler)

    async def start(self) -> None:
        """Start the health check monitoring loop."""
        self._running = True
        self._start_time = time.time()
        logger.info(f"Health check monitor started with {len(self._checks)} checks")

        while self._running:
            await self._run_checks()
            await asyncio.sleep(self.check_interval)

    def stop(self) -> None:
        """Stop the health check monitoring loop."""
        self._running = False
        logger.info("Health check monitor stopped")

    async def _run_checks(self) -> None:
        """Execute all health checks."""
        results: dict[str, bool] = {}
        all_healthy = True
        any_critical_failed = False

        for name, check in self._checks.items():
            try:
                result = await self._run_single_check(check)
                results[name] = result

                if not result:
                    all_healthy = False
                    if check.critical:
                        any_critical_failed = True
                    logger.warning(f"Health check failed: {name}")
            except Exception as e:
                logger.error(f"Health check error for {name}: {e}")
                results[name] = False
                all_healthy = False

        if all_healthy:
            status = HealthStatus.HEALTHY
        elif any_critical_failed:
            status = HealthStatus.UNHEALTHY
        else:
            status = HealthStatus.DEGRADED

        report = HealthReport(
            status=status,
            checks=results,
            last_check=time.time(),
            uptime_seconds=time.time() - self._start_time,
        )

        if self._last_report and self._last_report.status != status:
            self._trigger_alerts(status, report)

        self._last_report = report

    async def _run_single_check(self, check: HealthCheck) -> bool:
        """Execute a single health check.

        Args:
            check: Health check configuration.

        Returns:
            True if check passed.
        """
        try:
            if asyncio.iscoroutinefunction(check.check_fn):
                return await asyncio.wait_for(
                    check.check_fn(),
                    timeout=check.timeout_seconds,
                )
            return await asyncio.get_event_loop().run_in_executor(
                None, check.check_fn
            )
        except asyncio.TimeoutError:
            logger.error(f"Health check {check.name} timed out")
            return False
        except Exception as e:
            logger.error(f"Health check {check.name} raised: {e}")
            return False

    def _trigger_alerts(self, status: HealthStatus, report: HealthReport) -> None:
        """Trigger alert handlers on status change.

        Args:
            status: New health status.
            report: Full health report.
        """
        for handler in self._alert_handlers:
            try:
                handler(status, report)
            except Exception as e:
                logger.error(f"Alert handler failed: {e}")

    async def check_now(self) -> HealthReport:
        """Run all health checks immediately.

        Returns:
            Health report from the check.
        """
        await self._run_checks()
        return self._last_report or HealthReport(
            status=HealthStatus.UNKNOWN,
            checks={},
            last_check=time.time(),
            uptime_seconds=0.0,
        )

    def get_report(self) -> Optional[HealthReport]:
        """Get the last health report.

        Returns:
            Last health report or None.
        """
        return self._last_report

    def is_healthy(self) -> bool:
        """Check if system is healthy.

        Returns:
            True if last status was HEALTHY.
        """
        return self._last_report is not None and self._last_report.status == HealthStatus.HEALTHY
