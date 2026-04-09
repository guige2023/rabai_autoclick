"""
System Diagnostics and Health Monitoring Module.

Monitors system resources, detects anomalies, and provides
health reporting for automation workflows.

Author: AutoGen
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import platform
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Tuple

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    HEALTHY = auto()
    WARNING = auto()
    CRITICAL = auto()
    UNKNOWN = auto()


@dataclass
class SystemMetrics:
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    memory_used_mb: float = 0.0
    memory_total_mb: float = 0.0
    disk_percent: float = 0.0
    disk_free_gb: float = 0.0
    network_bytes_sent: int = 0
    network_bytes_recv: int = 0
    process_count: int = 0
    thread_count: int = 0
    open_files: int = 0
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class HealthCheckResult:
    component: str
    status: HealthStatus
    message: str = ""
    metrics: Dict[str, Any] = field(default_factory=dict)
    duration_ms: float = 0.0


@dataclass
class DiagnosticReport:
    timestamp: datetime
    hostname: str
    platform: str
    overall_status: HealthStatus
    checks: List[HealthCheckResult] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)


class SystemMetricsCollector:
    """Collects system-level metrics."""

    def __init__(self):
        self._baseline: Optional[SystemMetrics] = None
        self._history: List[SystemMetrics] = []

    def collect(self) -> SystemMetrics:
        import psutil

        try:
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage("/")
            net_io = psutil.net_io_counters()

            process = psutil.Process()
            num_fds = 0
            try:
                num_fds = process.num_fds()
            except AttributeError:
                num_fds = len(process.open_files())

            metrics = SystemMetrics(
                cpu_percent=psutil.cpu_percent(interval=0.1),
                memory_percent=memory.percent,
                memory_used_mb=memory.used / (1024 * 1024),
                memory_total_mb=memory.total / (1024 * 1024),
                disk_percent=disk.percent,
                disk_free_gb=disk.free / (1024 * 1024 * 1024),
                network_bytes_sent=net_io.bytes_sent,
                network_bytes_recv=net_io.bytes_recv,
                process_count=len(psutil.pids()),
                thread_count=sum(p.num_threads() for p in psutil.process_iter(attrs=["num_threads"])),
                open_files=num_fds,
            )

            self._history.append(metrics)
            if len(self._history) > 1000:
                self._history = self._history[-500:]

            return metrics

        except Exception as exc:
            logger.error("Failed to collect system metrics: %s", exc)
            return SystemMetrics()

    def set_baseline(self) -> None:
        self._baseline = self.collect()

    def get_baseline(self) -> Optional[SystemMetrics]:
        return self._baseline

    def get_history(self, duration_seconds: Optional[float] = None) -> List[SystemMetrics]:
        if duration_seconds is None:
            return list(self._history)
        cutoff = datetime.utcnow() - timedelta(seconds=duration_seconds)
        return [m for m in self._history if m.timestamp > cutoff]

    def detect_anomalies(
        self, current: SystemMetrics, threshold_multiplier: float = 2.0
    ) -> List[str]:
        anomalies = []
        if not self._baseline:
            return anomalies

        if current.cpu_percent > self._baseline.cpu_percent * threshold_multiplier:
            anomalies.append(
                f"CPU usage spike: {current.cpu_percent:.1f}% "
                f"(baseline: {self._baseline.cpu_percent:.1f}%)"
            )

        if current.memory_percent > self._baseline.memory_percent * threshold_multiplier:
            anomalies.append(
                f"Memory usage spike: {current.memory_percent:.1f}% "
                f"(baseline: {self._baseline.memory_percent:.1f}%)"
            )

        return anomalies


class HealthChecker:
    """Performs health checks on system components."""

    def __init__(self, metrics_collector: SystemMetricsCollector):
        self.metrics_collector = metrics_collector
        self._check_registry: Dict[str, Callable[[], HealthCheckResult]] = {}

    def register_check(self, name: str, handler: Callable[[], HealthCheckResult]) -> None:
        self._check_registry[name] = handler

    async def run_check(self, name: str) -> HealthCheckResult:
        start = time.time()
        handler = self._check_registry.get(name)
        if not handler:
            return HealthCheckResult(
                component=name,
                status=HealthStatus.UNKNOWN,
                message=f"No handler registered for '{name}'",
                duration_ms=(time.time() - start) * 1000,
            )

        try:
            result = handler()
            result.duration_ms = (time.time() - start) * 1000
            return result
        except Exception as exc:
            return HealthCheckResult(
                component=name,
                status=HealthStatus.CRITICAL,
                message=f"Check failed: {exc}",
                duration_ms=(time.time() - start) * 1000,
            )

    async def run_all_checks(self) -> List[HealthCheckResult]:
        results = await asyncio.gather(*[
            self.run_check(name) for name in self._check_registry
        ])
        return list(results)

    def check_cpu(self) -> HealthCheckResult:
        metrics = self.metrics_collector.collect()
        status = HealthStatus.HEALTHY
        message = f"CPU: {metrics.cpu_percent:.1f}%"

        if metrics.cpu_percent > 90:
            status = HealthStatus.CRITICAL
            message = f"CPU critical: {metrics.cpu_percent:.1f}%"
        elif metrics.cpu_percent > 75:
            status = HealthStatus.WARNING
            message = f"CPU high: {metrics.cpu_percent:.1f}%"

        return HealthCheckResult(
            component="cpu",
            status=status,
            message=message,
            metrics={"cpu_percent": metrics.cpu_percent},
        )

    def check_memory(self) -> HealthCheckResult:
        metrics = self.metrics_collector.collect()
        status = HealthStatus.HEALTHY
        message = f"Memory: {metrics.memory_percent:.1f}%"

        if metrics.memory_percent > 90:
            status = HealthStatus.CRITICAL
            message = f"Memory critical: {metrics.memory_percent:.1f}%"
        elif metrics.memory_percent > 75:
            status = HealthStatus.WARNING
            message = f"Memory high: {metrics.memory_percent:.1f}%"

        return HealthCheckResult(
            component="memory",
            status=status,
            message=message,
            metrics={"memory_percent": metrics.memory_percent, "used_mb": metrics.memory_used_mb},
        )

    def check_disk(self) -> HealthCheckResult:
        metrics = self.metrics_collector.collect()
        status = HealthStatus.HEALTHY
        message = f"Disk: {metrics.disk_percent:.1f}% used"

        if metrics.disk_percent > 95:
            status = HealthStatus.CRITICAL
            message = f"Disk critical: {metrics.disk_percent:.1f}% used"
        elif metrics.disk_percent > 85:
            status = HealthStatus.WARNING
            message = f"Disk warning: {metrics.disk_percent:.1f}% used"

        return HealthCheckResult(
            component="disk",
            status=status,
            message=message,
            metrics={"disk_percent": metrics.disk_percent, "free_gb": metrics.disk_free_gb},
        )


class SystemDiagnostics:
    """
    Main diagnostics orchestrator for system health monitoring.
    """

    def __init__(self):
        self.metrics_collector = SystemMetricsCollector()
        self.health_checker = HealthChecker(self.metrics_collector)
        self._alert_handlers: List[Callable[[HealthStatus, str], None]] = []
        self._last_report: Optional[DiagnosticReport] = None

        self.health_checker.register_check("cpu", self.health_checker.check_cpu)
        self.health_checker.register_check("memory", self.health_checker.check_memory)
        self.health_checker.register_check("disk", self.health_checker.check_disk)

    def register_alert_handler(
        self, handler: Callable[[HealthStatus, str], None]
    ) -> None:
        self._alert_handlers.append(handler)

    async def generate_report(self) -> DiagnosticReport:
        """Generate a full diagnostic report."""
        import platform

        results = await self.health_checker.run_all_checks()

        overall_status = HealthStatus.HEALTHY
        for result in results:
            if result.status == HealthStatus.CRITICAL:
                overall_status = HealthStatus.CRITICAL
                break
            elif result.status == HealthStatus.WARNING and overall_status != HealthStatus.CRITICAL:
                overall_status = HealthStatus.WARNING

        report = DiagnosticReport(
            timestamp=datetime.utcnow(),
            hostname=platform.node(),
            platform=platform.platform(),
            overall_status=overall_status,
            checks=results,
            summary={
                "total_checks": len(results),
                "healthy": sum(1 for r in results if r.status == HealthStatus.HEALTHY),
                "warnings": sum(1 for r in results if r.status == HealthStatus.WARNING),
                "critical": sum(1 for r in results if r.status == HealthStatus.CRITICAL),
            },
        )

        self._last_report = report

        for result in results:
            if result.status == HealthStatus.CRITICAL:
                for handler in self._alert_handlers:
                    try:
                        handler(result.status, f"[CRITICAL] {result.component}: {result.message}")
                    except Exception:
                        pass

        return report

    def get_current_metrics(self) -> SystemMetrics:
        return self.metrics_collector.collect()

    def get_trend(
        self, metric_name: str, duration_seconds: float = 300
    ) -> List[Tuple[datetime, float]]:
        history = self.metrics_collector.get_history(duration_seconds)
        result: List[Tuple[datetime, float]] = []

        for m in history:
            value: Optional[float] = None
            if metric_name == "cpu_percent":
                value = m.cpu_percent
            elif metric_name == "memory_percent":
                value = m.memory_percent
            elif metric_name == "disk_percent":
                value = m.disk_percent
            if value is not None:
                result.append((m.timestamp, value))

        return result

    def to_json(self, report: DiagnosticReport) -> str:
        return json.dumps(
            {
                "timestamp": report.timestamp.isoformat(),
                "hostname": report.hostname,
                "platform": report.platform,
                "overall_status": report.overall_status.name,
                "checks": [
                    {
                        "component": c.component,
                        "status": c.status.name,
                        "message": c.message,
                        "metrics": c.metrics,
                        "duration_ms": c.duration_ms,
                    }
                    for c in report.checks
                ],
                "summary": report.summary,
            },
            indent=2,
        )
