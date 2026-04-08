"""
Monitor Utilities

Provides utilities for monitoring system and
application state in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import time


@dataclass
class MonitorMetric:
    """Represents a monitored metric."""
    name: str
    value: float
    timestamp: float
    unit: str = ""


class SystemMonitor:
    """
    Monitors system and application metrics.
    
    Tracks CPU, memory, and custom metrics
    over time.
    """

    def __init__(self) -> None:
        self._metrics: dict[str, list[MonitorMetric]] = {}
        self._max_history = 1000

    def record_metric(
        self,
        name: str,
        value: float,
        unit: str = "",
    ) -> MonitorMetric:
        """
        Record a metric value.
        
        Args:
            name: Metric name.
            value: Metric value.
            unit: Optional unit.
            
        Returns:
            Created MonitorMetric.
        """
        metric = MonitorMetric(
            name=name,
            value=value,
            timestamp=time.time(),
            unit=unit,
        )
        if name not in self._metrics:
            self._metrics[name] = []
        self._metrics[name].append(metric)
        if len(self._metrics[name]) > self._max_history:
            self._metrics[name].pop(0)
        return metric

    def get_metric_history(
        self,
        name: str,
        limit: int | None = None,
    ) -> list[MonitorMetric]:
        """Get metric history."""
        history = self._metrics.get(name, [])
        if limit:
            history = history[-limit:]
        return list(history)

    def get_latest(self, name: str) -> MonitorMetric | None:
        """Get latest value of a metric."""
        history = self._metrics.get(name)
        if history:
            return history[-1]
        return None

    def get_average(self, name: str, last_n: int | None = None) -> float | None:
        """Get average value of a metric."""
        history = self._metrics.get(name)
        if not history:
            return None
        values = [m.value for m in history[-last_n:]] if last_n else [m.value for m in history]
        return sum(values) / len(values) if values else None

    def get_metric_names(self) -> list[str]:
        """Get all tracked metric names."""
        return list(self._metrics.keys())
