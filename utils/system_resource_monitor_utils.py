"""
System Resource Monitor Utilities

Monitor system resource usage relevant to automation tasks:
CPU, memory, disk I/O, and network activity.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import time
import os
from dataclasses import dataclass, field
from typing import Optional, List, Dict


@dataclass
class ResourceSnapshot:
    """A snapshot of system resource usage."""
    timestamp_ms: float
    cpu_user_percent: float
    cpu_system_percent: float
    memory_rss_mb: float
    memory_vms_mb: float
    open_fd_count: int
    thread_count: int
    io_read_bytes: int
    io_write_bytes: int


class SystemResourceMonitor:
    """Monitor system resources used by the automation process."""

    def __init__(self):
        self._pid = os.getpid()
        self._snapshots: List[ResourceSnapshot] = []
        self._last_io: Optional[tuple[int, int]] = None  # (read, write)

    def take_snapshot(self) -> ResourceSnapshot:
        """Take a snapshot of current resource usage."""
        import psutil
        proc = psutil.Process(self._pid)

        cpu = proc.cpu_times()
        mem = proc.memory_info()
        num_fds = proc.num_fds() if hasattr(proc, 'num_fds') else -1

        try:
            io = proc.io_counters()
            read_bytes = io.read_bytes
            write_bytes = io.write_bytes
        except (AttributeError, PermissionError):
            read_bytes = write_bytes = 0

        snapshot = ResourceSnapshot(
            timestamp_ms=time.time() * 1000,
            cpu_user_percent=cpu.user * 100,
            cpu_system_percent=cpu.system * 100,
            memory_rss_mb=mem.rss / (1024 * 1024),
            memory_vms_mb=mem.vms / (1024 * 1024),
            open_fd_count=num_fds,
            thread_count=proc.num_threads(),
            io_read_bytes=read_bytes,
            io_write_bytes=write_bytes,
        )
        self._snapshots.append(snapshot)
        return snapshot

    def get_latest(self) -> Optional[ResourceSnapshot]:
        """Get the most recent resource snapshot."""
        return self._snapshots[-1] if self._snapshots else None

    def get_peak_memory_mb(self) -> float:
        """Get peak RSS memory usage from recorded snapshots."""
        if not self._snapshots:
            return 0.0
        return max(s.memory_rss_mb for s in self._snapshots)

    def get_average_cpu_percent(self) -> float:
        """Get average CPU usage across all snapshots."""
        if not self._snapshots:
            return 0.0
        total = sum(s.cpu_user_percent + s.cpu_system_percent for s in self._snapshots)
        return total / len(self._snapshots)
