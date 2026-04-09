"""
System Health Utilities

Monitor and report on system health metrics relevant to automation:
CPU, memory, I/O, and process health indicators.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import os
import time
import resource
from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class SystemHealthSnapshot:
    """Snapshot of system health metrics."""
    cpu_percent: float
    memory_used_mb: float
    memory_available_mb: float
    memory_percent: float
    timestamp_ms: float
    is_healthy: bool
    warnings: List[str] = field(default_factory=list)


class SystemHealthMonitor:
    """Monitor system health over time with configurable thresholds."""

    def __init__(
        self,
        cpu_warning_threshold: float = 80.0,
        memory_warning_threshold: float = 85.0,
    ):
        self.cpu_warning_threshold = cpu_warning_threshold
        self.memory_warning_threshold = memory_warning_threshold
        self._snapshots: List[SystemHealthSnapshot] = []
        self._max_snapshots = 100

    def take_snapshot(self) -> SystemHealthSnapshot:
        """Take a current snapshot of system health."""
        import psutil
        cpu = psutil.cpu_percent(interval=0.1)
        mem = psutil.virtual_memory()
        warnings = []

        if cpu > self.cpu_warning_threshold:
            warnings.append(f"CPU usage high: {cpu:.1f}%")
        if mem.percent > self.memory_warning_threshold:
            warnings.append(f"Memory usage high: {mem.percent:.1f}%")

        snapshot = SystemHealthSnapshot(
            cpu_percent=cpu,
            memory_used_mb=mem.used / (1024 * 1024),
            memory_available_mb=mem.available / (1024 * 1024),
            memory_percent=mem.percent,
            timestamp_ms=time.time() * 1000,
            is_healthy=len(warnings) == 0,
            warnings=warnings,
        )

        self._snapshots.append(snapshot)
        if len(self._snapshots) > self._max_snapshots:
            self._snapshots.pop(0)

        return snapshot

    def get_trend(self) -> dict:
        """Compute trend from recorded snapshots."""
        if not self._snapshots:
            return {}
        cpu_values = [s.cpu_percent for s in self._snapshots]
        mem_values = [s.memory_percent for s in self._snapshots]
        return {
            "avg_cpu": sum(cpu_values) / len(cpu_values),
            "max_cpu": max(cpu_values),
            "avg_memory": sum(mem_values) / len(mem_values),
            "max_memory": max(mem_values),
            "snapshot_count": len(self._snapshots),
        }


def get_process_cpu_time() -> float:
    """Get the CPU time used by the current process in seconds."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime + usage.ru_stime


def get_open_fd_count() -> int:
    """Get the number of open file descriptors for this process."""
    try:
        return len(os.listdir(f"/proc/{os.getpid()}/fd"))
    except (FileNotFoundError, PermissionError):
        return -1
