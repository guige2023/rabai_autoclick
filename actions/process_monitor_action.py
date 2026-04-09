"""Process monitor action for tracking system processes.

Monitors process CPU, memory, and status changes with
configurable thresholds and alerting.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class ProcessStatus(Enum):
    RUNNING = "running"
    SLEEPING = "sleeping"
    STOPPED = "stopped"
    ZOMBIE = "zombie"


@dataclass
class ProcessInfo:
    pid: int
    name: str
    status: ProcessStatus
    cpu_percent: float = 0.0
    memory_mb: float = 0.0
    start_time: float = field(default_factory=time.time)
    parent_pid: Optional[int] = None
    num_threads: int = 1


@dataclass
class ProcessAlert:
    alert_type: str
    message: str
    timestamp: float = field(default_factory=time.time)
    pid: Optional[int] = None


class ProcessMonitorAction:
    """Monitor system processes with threshold alerts.

    Args:
        check_interval: Interval between process checks in seconds.
        cpu_threshold: CPU percentage threshold for alerts.
        memory_threshold_mb: Memory threshold in MB for alerts.
    """

    def __init__(
        self,
        check_interval: float = 5.0,
        cpu_threshold: float = 90.0,
        memory_threshold_mb: float = 2000.0,
    ) -> None:
        self._check_interval = check_interval
        self._cpu_threshold = cpu_threshold
        self._memory_threshold_mb = memory_threshold_mb
        self._alert_handlers: list[Callable[[ProcessAlert], None]] = []
        self._process_cache: dict[int, ProcessInfo] = {}
        self._alert_history: list[ProcessAlert] = []
        self._monitored_pids: set[int] = set()
        self._running = False

    def start_monitoring(self) -> None:
        """Start the process monitoring loop."""
        self._running = True
        logger.info("Process monitor started")

    def stop_monitoring(self) -> None:
        """Stop the process monitoring loop."""
        self._running = False
        logger.info("Process monitor stopped")

    def is_running(self) -> bool:
        """Check if monitoring is active.

        Returns:
            True if monitoring is active.
        """
        return self._running

    def watch_pid(self, pid: int) -> None:
        """Add a process ID to watch list.

        Args:
            pid: Process ID to monitor.
        """
        self._monitored_pids.add(pid)
        logger.debug(f"Now monitoring PID {pid}")

    def unwatch_pid(self, pid: int) -> None:
        """Remove a process ID from watch list.

        Args:
            pid: Process ID to stop monitoring.
        """
        self._monitored_pids.discard(pid)

    def register_alert_handler(self, handler: Callable[[ProcessAlert], None]) -> None:
        """Register a handler for process alerts.

        Args:
            handler: Callback function for alerts.
        """
        self._alert_handlers.append(handler)

    def _trigger_alert(self, alert: ProcessAlert) -> None:
        """Trigger an alert to all registered handlers.

        Args:
            alert: Alert to trigger.
        """
        self._alert_history.append(alert)
        for handler in self._alert_handlers:
            try:
                handler(alert)
            except Exception as e:
                logger.error(f"Alert handler error: {e}")
        logger.warning(f"Process alert: {alert.message}")

    def check_process(self, pid: int) -> Optional[ProcessInfo]:
        """Get information about a specific process.

        Args:
            pid: Process ID to check.

        Returns:
            Process information or None if not found.
        """
        if pid in self._process_cache:
            return self._process_cache[pid]
        return None

    def get_top_processes(self, limit: int = 10, sort_by: str = "cpu") -> list[ProcessInfo]:
        """Get top processes by CPU or memory usage.

        Args:
            limit: Maximum number of processes to return.
            sort_by: Sort by 'cpu' or 'memory'.

        Returns:
            List of top processes.
        """
        processes = list(self._process_cache.values())
        if sort_by == "cpu":
            processes.sort(key=lambda p: p.cpu_percent, reverse=True)
        else:
            processes.sort(key=lambda p: p.memory_mb, reverse=True)
        return processes[:limit]

    def find_process_by_name(self, name: str) -> list[ProcessInfo]:
        """Find processes by name (partial match).

        Args:
            name: Process name to search for.

        Returns:
            List of matching processes.
        """
        name_lower = name.lower()
        return [
            p for p in self._process_cache.values()
            if name_lower in p.name.lower()
        ]

    def get_alert_history(self, limit: int = 50) -> list[ProcessAlert]:
        """Get recent alert history.

        Args:
            limit: Maximum number of alerts to return.

        Returns:
            List of alerts (newest first).
        """
        return self._alert_history[-limit:][::-1]

    def clear_alert_history(self) -> int:
        """Clear alert history.

        Returns:
            Number of alerts cleared.
        """
        count = len(self._alert_history)
        self._alert_history.clear()
        return count

    def get_stats(self) -> dict[str, Any]:
        """Get process monitor statistics.

        Returns:
            Dictionary with monitor stats.
        """
        total_cpu = sum(p.cpu_percent for p in self._process_cache.values())
        total_memory = sum(p.memory_mb for p in self._process_cache.values())
        return {
            "monitored_pids": len(self._monitored_pids),
            "cached_processes": len(self._process_cache),
            "total_cpu_percent": total_cpu,
            "total_memory_mb": total_memory,
            "total_alerts": len(self._alert_history),
            "is_running": self._running,
            "thresholds": {
                "cpu": self._cpu_threshold,
                "memory_mb": self._memory_threshold_mb,
            },
        }

    def update_thresholds(
        self,
        cpu_threshold: Optional[float] = None,
        memory_threshold_mb: Optional[float] = None,
    ) -> None:
        """Update alert thresholds.

        Args:
            cpu_threshold: New CPU threshold percentage.
            memory_threshold_mb: New memory threshold in MB.
        """
        if cpu_threshold is not None:
            self._cpu_threshold = cpu_threshold
        if memory_threshold_mb is not None:
            self._memory_threshold_mb = memory_threshold_mb
        logger.debug(
            f"Updated thresholds: cpu={self._cpu_threshold}, "
            f"memory={self._memory_threshold_mb}MB"
        )
