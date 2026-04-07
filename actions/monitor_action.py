"""monitor action module for rabai_autoclick.

Provides system monitoring utilities: CPU, memory, disk, network,
process monitoring, and health check functionality.
"""

from __future__ import annotations

import os
import psutil
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Sequence

__all__ = [
    "SystemMonitor",
    "ProcessMonitor",
    "HealthChecker",
    "AlertRule",
    "Alert",
    "AlertLevel",
    "MonitorMetric",
    "get_cpu_percent",
    "get_memory_info",
    "get_disk_usage",
    "get_network_io",
    "get_process_info",
    "system_health_check",
    "MonitorBackend",
]


class AlertLevel(Enum):
    """Alert severity levels."""
    INFO = auto()
    WARNING = auto()
    CRITICAL = auto()
    EMERGENCY = auto()


@dataclass
class Alert:
    """System alert."""
    level: AlertLevel
    message: str
    metric: str
    value: float
    threshold: float
    timestamp: float = field(default_factory=time.time)


@dataclass
class AlertRule:
    """Rule for triggering alerts."""
    name: str
    metric: str
    condition: str
    threshold: float
    level: AlertLevel = AlertLevel.WARNING
    cooldown_seconds: float = 60.0

    def evaluate(self, value: float) -> bool:
        """Evaluate if rule triggers."""
        if self.condition == ">":
            return value > self.threshold
        elif self.condition == "<":
            return value < self.threshold
        elif self.condition == ">=":
            return value >= self.threshold
        elif self.condition == "<=":
            return value <= self.threshold
        elif self.condition == "==":
            return value == self.threshold
        return False


@dataclass
class MonitorMetric:
    """A single metric value with metadata."""
    name: str
    value: float
    unit: str = ""
    timestamp: float = field(default_factory=time.time)
    tags: Dict[str, str] = field(default_factory=dict)


class MonitorBackend:
    """Backend storage for metrics."""

    def __init__(self, max_points: int = 1000) -> None:
        self.max_points = max_points
        self._metrics: Dict[str, deque] = {}

    def record(self, metric: MonitorMetric) -> None:
        """Record a metric value."""
        if metric.name not in self._metrics:
            self._metrics[metric.name] = deque(maxlen=self.max_points)
        self._metrics[metric.name].append(metric)

    def get(self, name: str, limit: Optional[int] = None) -> List[MonitorMetric]:
        """Get metric values."""
        if name not in self._metrics:
            return []
        data = list(self._metrics[name])
        if limit:
            return data[-limit:]
        return data

    def latest(self, name: str) -> Optional[MonitorMetric]:
        """Get most recent value for metric."""
        if name not in self._metrics or not self._metrics[name]:
            return None
        return self._metrics[name][-1]


class SystemMonitor:
    """System-wide resource monitoring."""

    def __init__(self, interval: float = 1.0) -> None:
        self.interval = interval
        self._backend = MonitorBackend()
        self._running = False
        self._alerts: List[Alert] = []
        self._alert_rules: List[AlertRule] = []
        self._last_alert_time: Dict[str, float] = {}

    def record_cpu(self) -> float:
        """Record CPU usage."""
        value = psutil.cpu_percent(interval=0.1)
        self._backend.record(MonitorMetric(name="cpu_percent", value=value, unit="%"))
        return value

    def record_memory(self) -> Dict[str, float]:
        """Record memory usage."""
        mem = psutil.virtual_memory()
        metrics = {
            "memory_percent": mem.percent,
            "memory_used_gb": mem.used / (1024 ** 3),
            "memory_available_gb": mem.available / (1024 ** 3),
            "memory_total_gb": mem.total / (1024 ** 3),
        }
        for name, value in metrics.items():
            self._backend.record(MonitorMetric(name=name, value=value, unit="GB" if "gb" in name else "%"))
        return metrics

    def record_disk(self, path: str = "/") -> Dict[str, float]:
        """Record disk usage for path."""
        disk = psutil.disk_usage(path)
        metrics = {
            "disk_percent": disk.percent,
            "disk_used_gb": disk.used / (1024 ** 3),
            "disk_free_gb": disk.free / (1024 ** 3),
            "disk_total_gb": disk.total / (1024 ** 3),
        }
        for name, value in metrics.items():
            self._backend.record(MonitorMetric(name=name, value=value, unit="GB" if "gb" in name else "%"))
        return metrics

    def record_network(self) -> Dict[str, float]:
        """Record network I/O."""
        net = psutil.net_io_counters()
        metrics = {
            "net_bytes_sent": net.bytes_sent,
            "net_bytes_recv": net.bytes_recv,
            "net_packets_sent": net.packets_sent,
            "net_packets_recv": net.packets_recv,
        }
        for name, value in metrics.items():
            unit = "bytes"
            self._backend.record(MonitorMetric(name=name, value=value, unit=unit))
        return metrics

    def record_all(self) -> Dict[str, float]:
        """Record all metrics."""
        self.record_cpu()
        self.record_memory()
        self.record_disk()
        self.record_network()
        return self.snapshot()

    def snapshot(self) -> Dict[str, float]:
        """Get current metric values."""
        result = {}
        for name in self._backend._metrics:
            latest = self._backend.latest(name)
            if latest:
                result[name] = latest.value
        return result

    def add_alert_rule(self, rule: AlertRule) -> None:
        """Add an alert rule."""
        self._alert_rules.append(rule)

    def check_alerts(self) -> List[Alert]:
        """Evaluate alert rules and return triggered alerts."""
        triggered = []
        now = time.time()
        snapshot = self.snapshot()

        for rule in self._alert_rules:
            if rule.metric not in snapshot:
                continue
            value = snapshot[rule.metric]
            if rule.evaluate(value):
                last_time = self._last_alert_time.get(rule.name, 0)
                if now - last_time >= rule.cooldown_seconds:
                    alert = Alert(
                        level=rule.level,
                        message=f"{rule.name}: {value:.2f} {rule.condition} {rule.threshold}",
                        metric=rule.metric,
                        value=value,
                        threshold=rule.threshold,
                    )
                    triggered.append(alert)
                    self._alerts.append(alert)
                    self._last_alert_time[rule.name] = now

        return triggered

    def get_alerts(self, level: Optional[AlertLevel] = None, limit: int = 100) -> List[Alert]:
        """Get recent alerts."""
        alerts = self._alerts
        if level:
            alerts = [a for a in alerts if a.level == level]
        return alerts[-limit:]


class ProcessMonitor:
    """Monitor a specific process."""

    def __init__(self, pid: Optional[int] = None) -> None:
        self.pid = pid or os.getpid()
        try:
            self._process = psutil.Process(self.pid)
        except psutil.NoSuchProcess:
            self._process = None

    def cpu_percent(self, interval: float = 0.1) -> float:
        """Get CPU usage percent."""
        if self._process is None:
            return 0.0
        return self._process.cpu_percent(interval=interval)

    def memory_info(self) -> Dict[str, float]:
        """Get memory info in GB."""
        if self._process is None:
            return {}
        mem = self._process.memory_info()
        return {
            "rss_gb": mem.rss / (1024 ** 3),
            "vms_gb": mem.vms / (1024 ** 3),
            "percent": self._process.memory_percent(),
        }

    def num_threads(self) -> int:
        """Get number of threads."""
        if self._process is None:
            return 0
        return self._process.num_threads()

    def open_files(self) -> List[str]:
        """Get list of open files."""
        if self._process is None:
            return []
        try:
            return [f.path for f in self._process.open_files()]
        except psutil.AccessDenied:
            return []

    def connections(self) -> int:
        """Get number of network connections."""
        if self._process is None:
            return 0
        try:
            return len(self._process.connections())
        except psutil.AccessDenied:
            return 0

    def cmdline(self) -> List[str]:
        """Get command line arguments."""
        if self._process is None:
            return []
        return self._process.cmdline()

    def status(self) -> str:
        """Get process status."""
        if self._process is None:
            return "unknown"
        return self._process.status()

    def create_time(self) -> float:
        """Get process creation time."""
        if self._process is None:
            return 0.0
        return self._process.create_time()

    def is_running(self) -> bool:
        """Check if process is still running."""
        if self._process is None:
            return False
        try:
            self._process.status()
            return True
        except psutil.NoSuchProcess:
            return False

    def kill(self) -> bool:
        """Terminate the process."""
        if self._process is None:
            return False
        try:
            self._process.kill()
            return True
        except psutil.NoSuchProcess:
            return False


class HealthChecker:
    """Health check coordinator."""

    def __init__(self) -> None:
        self._checks: Dict[str, Callable[[], bool]] = {}

    def register(self, name: str, check: Callable[[], bool]) -> None:
        """Register a health check function.

        Args:
            name: Check name.
            check: Function returning True if healthy.
        """
        self._checks[name] = check

    def check(self) -> Dict[str, bool]:
        """Run all health checks."""
        return {name: check() for name, check in self._checks.items()}

    def is_healthy(self) -> bool:
        """Return True if all checks pass."""
        results = self.check()
        return all(results.values())

    def unhealthy(self) -> List[str]:
        """Return list of failing check names."""
        results = self.check()
        return [name for name, ok in results.items() if not ok]


def get_cpu_percent(interval: float = 0.1, per_cpu: bool = False) -> Any:
    """Get CPU usage percent."""
    if per_cpu:
        return psutil.cpu_percent(interval=interval, percpu=True)
    return psutil.cpu_percent(interval=interval)


def get_memory_info() -> Dict[str, Any]:
    """Get detailed memory information."""
    mem = psutil.virtual_memory()
    swap = psutil.swap_memory()
    return {
        "virtual": {
            "total_gb": mem.total / (1024 ** 3),
            "available_gb": mem.available / (1024 ** 3),
            "used_gb": mem.used / (1024 ** 3),
            "percent": mem.percent,
        },
        "swap": {
            "total_gb": swap.total / (1024 ** 3),
            "used_gb": swap.used / (1024 ** 3),
            "percent": swap.percent,
        },
    }


def get_disk_usage(path: str = "/") -> Dict[str, Any]:
    """Get disk usage for path."""
    disk = psutil.disk_usage(path)
    return {
        "total_gb": disk.total / (1024 ** 3),
        "used_gb": disk.used / (1024 ** 3),
        "free_gb": disk.free / (1024 ** 3),
        "percent": disk.percent,
    }


def get_network_io() -> Dict[str, int]:
    """Get network I/O counters."""
    net = psutil.net_io_counters()
    return {
        "bytes_sent": net.bytes_sent,
        "bytes_recv": net.bytes_recv,
        "packets_sent": net.packets_sent,
        "packets_recv": net.packets_recv,
        "errin": net.errin,
        "errout": net.errout,
        "dropin": net.dropin,
        "dropout": net.dropout,
    }


def get_process_info(pid: Optional[int] = None) -> Dict[str, Any]:
    """Get process information."""
    try:
        proc = psutil.Process(pid or os.getpid())
        return {
            "pid": proc.pid,
            "name": proc.name(),
            "status": proc.status(),
            "cpu_percent": proc.cpu_percent(),
            "memory_percent": proc.memory_percent(),
            "num_threads": proc.num_threads(),
            "cmdline": proc.cmdline(),
            "create_time": proc.create_time(),
        }
    except psutil.NoSuchProcess:
        return {}


def system_health_check() -> Dict[str, Any]:
    """Perform comprehensive system health check."""
    cpu = get_cpu_percent()
    mem = get_memory_info()
    disk = get_disk_usage()
    results = {
        "healthy": True,
        "checks": {
            "cpu": {"ok": cpu < 90, "value": cpu},
            "memory": {"ok": mem["virtual"]["percent"] < 90, "value": mem["virtual"]["percent"]},
            "disk": {"ok": disk["percent"] < 90, "value": disk["percent"]},
        },
    }
    results["healthy"] = all(c["ok"] for c in results["checks"].values())
    return results
