"""
Resource Monitor Action Module.

Monitors system resource usage including CPU, memory,
and network for automation workflow management.
"""

import time
from collections import deque
from dataclasses import dataclass
from typing import Optional


@dataclass
class ResourceSnapshot:
    """A snapshot of resource usage."""
    timestamp: float
    cpu_percent: float
    memory_percent: float
    memory_used_mb: float
    memory_available_mb: float


class ResourceMonitor:
    """Monitors system resource usage."""

    def __init__(self, history_size: int = 100):
        """
        Initialize resource monitor.

        Args:
            history_size: Number of snapshots to retain.
        """
        self.history_size = history_size
        self._history: deque[ResourceSnapshot] = deque(maxlen=history_size)
        self._last_check: Optional[float] = None

    def capture(self) -> ResourceSnapshot:
        """
        Capture current resource usage.

        Returns:
            ResourceSnapshot with current usage.
        """
        import psutil

        memory = psutil.virtual_memory()
        cpu = psutil.cpu_percent(interval=0.1)

        snapshot = ResourceSnapshot(
            timestamp=time.time(),
            cpu_percent=cpu,
            memory_percent=memory.percent,
            memory_used_mb=memory.used / (1024 * 1024),
            memory_available_mb=memory.available / (1024 * 1024),
        )

        self._history.append(snapshot)
        self._last_check = time.time()

        return snapshot

    def get_current(self) -> Optional[ResourceSnapshot]:
        """Get the most recent snapshot."""
        if self._history:
            return self._history[-1]
        return None

    def get_history(self, limit: int = 10) -> list[ResourceSnapshot]:
        """
        Get recent resource history.

        Args:
            limit: Maximum snapshots to return.

        Returns:
            List of ResourceSnapshot objects.
        """
        return list(self._history)[-limit:]

    def get_average_cpu(self, seconds: float = 60.0) -> float:
        """Get average CPU usage over time window."""
        cutoff = time.time() - seconds
        recent = [s for s in self._history if s.timestamp >= cutoff]

        if not recent:
            return 0.0

        return sum(s.cpu_percent for s in recent) / len(recent)

    def get_average_memory(self, seconds: float = 60.0) -> float:
        """Get average memory usage over time window."""
        cutoff = time.time() - seconds
        recent = [s for s in self._history if s.timestamp >= cutoff]

        if not recent:
            return 0.0

        return sum(s.memory_percent for s in recent) / len(recent)

    def is_cpu_high(self, threshold: float = 80.0) -> bool:
        """Check if CPU usage is above threshold."""
        current = self.get_current()
        if current:
            return current.cpu_percent >= threshold
        return False

    def is_memory_high(self, threshold: float = 80.0) -> bool:
        """Check if memory usage is above threshold."""
        current = self.get_current()
        if current:
            return current.memory_percent >= threshold
        return False
