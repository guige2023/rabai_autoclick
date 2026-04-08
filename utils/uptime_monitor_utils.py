"""Uptime monitor utilities for RabAI AutoClick.

Provides:
- Uptime tracking
- Availability monitoring
- Alert on downtime
"""

from __future__ import annotations

import time
from typing import (
    Callable,
    Dict,
    List,
    Optional,
)


class UptimeMonitor:
    """Monitor uptime and availability."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._start_time: Optional[float] = None
        self._down_events: List[Dict] = []
        self._up = True
        self._callbacks: List[Callable[[str], None]] = []

    def start(self) -> None:
        """Start monitoring."""
        self._start_time = time.time()
        self._up = True

    def record_up(self) -> None:
        """Record an up event."""
        if not self._up:
            self._down_events[-1]["duration"] = time.time() - self._down_events[-1]["start"]
            self._up = True

    def record_down(self) -> None:
        """Record a down event."""
        if self._up:
            self._down_events.append({"start": time.time(), "duration": None})
            self._up = False
            for cb in self._callbacks:
                cb(f"{self.name} is down")

    def on_down(self, callback: Callable[[str], None]) -> None:
        """Register a down callback."""
        self._callbacks.append(callback)

    @property
    def uptime_seconds(self) -> float:
        """Get uptime in seconds."""
        if self._start_time is None:
            return 0.0
        return time.time() - self._start_time

    @property
    def downtime_seconds(self) -> float:
        """Get total downtime in seconds."""
        total = 0.0
        for event in self._down_events:
            if event["duration"] is not None:
                total += event["duration"]
            elif self._up:
                total += time.time() - event["start"]
        return total

    @property
    def availability(self) -> float:
        """Get availability percentage (0-100)."""
        total = self.uptime_seconds + self.downtime_seconds
        if total == 0:
            return 100.0
        uptime = self.uptime_seconds - self.downtime_seconds
        return max(0.0, min(100.0, (uptime / total) * 100))


__all__ = [
    "UptimeMonitor",
]
