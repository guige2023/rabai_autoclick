"""
Gauge metric action for tracking current values that can go up and down.

This module provides actions for recording and analyzing gauge metrics,
supporting value transformations, history tracking, and alerting thresholds.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


class GaugeDirection(Enum):
    """Direction indicators for gauge value changes."""
    UP = "up"
    DOWN = "down"
    STABLE = "stable"
    UNKNOWN = "unknown"


@dataclass
class GaugeSnapshot:
    """A point-in-time snapshot of a gauge."""
    name: str
    value: float
    timestamp: datetime
    direction: GaugeDirection
    delta: Optional[float] = None
    history_size: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert snapshot to dictionary."""
        return {
            "name": self.name,
            "value": self.value,
            "timestamp": self.timestamp.isoformat(),
            "direction": self.direction.value,
            "delta": self.delta,
            "history_size": self.history_size,
        }


@dataclass
class GaugeConfig:
    """Configuration for gauge metrics."""
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    default_value: float = 0.0
    wrap: bool = False
    history_size: int = 100
    history_ttl_seconds: Optional[int] = None
    alert_threshold_high: Optional[float] = None
    alert_threshold_low: Optional[float] = None
    transformation: Optional[Callable[[float], float]] = None


class Gauge:
    """
    Gauge metric for tracking values that can go up and down.

    Supports value clamping, history tracking, direction detection,
    and threshold-based alerting.
    """

    def __init__(
        self,
        name: str,
        config: Optional[GaugeConfig] = None,
    ):
        """
        Initialize a gauge.

        Args:
            name: Name of the gauge metric.
            config: Optional gauge configuration.
        """
        self.name = name
        self.config = config or GaugeConfig()
        self._value = self.config.default_value
        self._last_value = self.config.default_value
        self._last_update_time = time.time()
        self._history: deque = deque(maxlen=self.config.history_size)
        self._lock = threading.RLock()
        self._alert_callbacks: List[Callable[[str, float, Optional[str]], None]] = []

        if self.config.history_ttl_seconds:
            self._history_ttl = self.config.history_ttl_seconds
        else:
            self._history_ttl = None

    def set(self, value: float) -> Tuple[float, Optional[float]]:
        """
        Set the gauge to a specific value.

        Args:
            value: The new value to set.

        Returns:
            Tuple of (new_value, delta).
        """
        with self._lock:
            old_value = self._value
            delta = value - old_value

            self._last_value = old_value
            self._value = self._apply_constraints(value)
            self._last_update_time = time.time()

            self._history.append({
                "value": self._value,
                "timestamp": datetime.now(),
                "delta": delta,
            })

            self._check_alerts(self._value, old_value)

            if self._value != old_value:
                return self._value, delta
            return self._value, None

    def increment(self, amount: float = 1.0) -> Tuple[float, float]:
        """
        Increment the gauge by a given amount.

        Args:
            amount: Amount to increment by.

        Returns:
            Tuple of (new_value, delta).
        """
        with self._lock:
            return self.set(self._value + amount)

    def decrement(self, amount: float = 1.0) -> Tuple[float, float]:
        """
        Decrement the gauge by a given amount.

        Args:
            amount: Amount to decrement by.

        Returns:
            Tuple of (new_value, delta).
        """
        with self._lock:
            return self.set(self._value - amount)

    def get(self) -> float:
        """Get the current value of the gauge."""
        with self._lock:
            return self._value

    def get_with_direction(self) -> Tuple[float, GaugeDirection]:
        """
        Get the current value and direction of change.

        Returns:
            Tuple of (value, direction).
        """
        with self._lock:
            if self._value > self._last_value:
                return self._value, GaugeDirection.UP
            elif self._value < self._last_value:
                return self._value, GaugeDirection.DOWN
            else:
                return self._value, GaugeDirection.STABLE

    def get_snapshot(self) -> GaugeSnapshot:
        """
        Get a point-in-time snapshot of the gauge.

        Returns:
            GaugeSnapshot with current state.
        """
        with self._lock:
            value, direction = self.get_with_direction()
            delta = value - self._last_value if direction != GaugeDirection.STABLE else None

            return GaugeSnapshot(
                name=self.name,
                value=value,
                timestamp=datetime.now(),
                direction=direction,
                delta=delta,
                history_size=len(self._history),
            )

    def get_history(
        self,
        limit: Optional[int] = None,
        since: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get the history of gauge values.

        Args:
            limit: Maximum number of entries to return.
            since: Only return entries since this time.

        Returns:
            List of history entries.
        """
        with self._lock:
            history = list(self._history)

            if since:
                history = [
                    h for h in history
                    if h["timestamp"] >= since
                ]

            if limit:
                history = history[-limit:]

            return [
                {
                    "value": h["value"],
                    "timestamp": h["timestamp"].isoformat(),
                    "delta": h["delta"],
                }
                for h in history
            ]

    def get_rate(self, window_seconds: float = 60.0) -> float:
        """
        Calculate the rate of change over a time window.

        Args:
            window_seconds: Time window in seconds.

        Returns:
            Rate of change (units per second).
        """
        with self._lock:
            if len(self._history) < 2:
                return 0.0

            cutoff = time.time() - window_seconds
            recent = [
                h for h in self._history
                if h["timestamp"].timestamp() >= cutoff
            ]

            if len(recent) < 2:
                return 0.0

            first = recent[0]
            last = recent[-1]
            time_diff = (last["timestamp"] - first["timestamp"]).total_seconds()

            if time_diff == 0:
                return 0.0

            return (last["value"] - first["value"]) / time_diff

    def _apply_constraints(self, value: float) -> float:
        """Apply value constraints (min, max, wrap)."""
        if self.config.transformation:
            value = self.config.transformation(value)

        if self.config.wrap:
            if self.config.min_value is not None and self.config.max_value is not None:
                range_size = self.config.max_value - self.config.min_value
                if range_size > 0:
                    value = self.config.min_value + (
                        (value - self.config.min_value) % range_size
                    )
        else:
            if self.config.min_value is not None:
                value = max(value, self.config.min_value)
            if self.config.max_value is not None:
                value = min(value, self.config.max_value)

        return value

    def _check_alerts(self, value: float, old_value: float) -> None:
        """Check if value triggers any alerts."""
        for callback in self._alert_callbacks:
            if self.config.alert_threshold_high is not None:
                if old_value <= self.config.alert_threshold_high and value > self.config.alert_threshold_high:
                    try:
                        callback(self.name, value, "high")
                    except Exception:
                        pass

            if self.config.alert_threshold_low is not None:
                if old_value >= self.config.alert_threshold_low and value < self.config.alert_threshold_low:
                    try:
                        callback(self.name, value, "low")
                    except Exception:
                        pass

    def on_alert(
        self,
        callback: Callable[[str, float, Optional[str]], None],
    ) -> None:
        """
        Register an alert callback.

        Args:
            callback: Function to call when alert triggers.
        """
        self._alert_callbacks.append(callback)

    def reset(self) -> float:
        """
        Reset the gauge to its default value.

        Returns:
            The value before reset.
        """
        with self._lock:
            old_value = self._value
            self._value = self.config.default_value
            self._last_value = self.config.default_value
            self._history.clear()
            return old_value

    def time_since_update(self) -> float:
        """Get seconds since last update."""
        return time.time() - self._last_update_time


class GaugeRegistry:
    """Thread-safe registry of gauge metrics."""

    def __init__(self):
        """Initialize the gauge registry."""
        self._gauges: Dict[str, Gauge] = {}
        self._lock = threading.RLock()

    def get_or_create(
        self,
        name: str,
        config: Optional[GaugeConfig] = None,
    ) -> Gauge:
        """
        Get an existing gauge or create a new one.

        Args:
            name: Name of the gauge.
            config: Optional gauge configuration.

        Returns:
            The gauge metric.
        """
        with self._lock:
            if name not in self._gauges:
                self._gauges[name] = Gauge(name, config)
            return self._gauges[name]

    def get(self, name: str) -> Optional[Gauge]:
        """Get a gauge by name."""
        with self._lock:
            return self._gauges.get(name)

    def list_gauges(self) -> List[str]:
        """List all gauge names."""
        with self._lock:
            return list(self._gauges.keys())

    def set_value(self, name: str, value: float) -> Tuple[float, Optional[float]]:
        """Set a gauge value by name."""
        gauge = self.get_or_create(name)
        return gauge.set(value)

    def get_value(self, name: str) -> Optional[float]:
        """Get a gauge value by name."""
        gauge = self.get(name)
        if gauge:
            return gauge.get()
        return None

    def increment(self, name: str, amount: float = 1.0) -> Optional[Tuple[float, float]]:
        """Increment a gauge by name."""
        gauge = self.get_or_create(name)
        return gauge.increment(amount)

    def decrement(self, name: str, amount: float = 1.0) -> Optional[Tuple[float, float]]:
        """Decrement a gauge by name."""
        gauge = self.get_or_create(name)
        return gauge.decrement(amount)

    def reset_all(self) -> None:
        """Reset all gauges in the registry."""
        with self._lock:
            for g in self._gauges.values():
                g.reset()

    def get_snapshots(self) -> List[GaugeSnapshot]:
        """Get snapshots of all gauges."""
        with self._lock:
            return [g.get_snapshot() for g in self._gauges.values()]


_default_registry = GaugeRegistry()


def gauge_action(
    value: Optional[float],
    name: str,
    operation: str = "set",
    amount: float = 1.0,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Action function for gauge metric operations.

    Args:
        value: Value to set (for 'set' operation).
        name: Name of the gauge.
        operation: Operation to perform (set, increment, decrement, get).
        amount: Amount for increment/decrement.
        min_value: Minimum value constraint.
        max_value: Maximum value constraint.

    Returns:
        Dictionary with gauge result.
    """
    config = GaugeConfig(min_value=min_value, max_value=max_value)
    registry = GaugeRegistry()
    gauge = registry.get_or_create(name, config)

    if operation == "set":
        if value is None:
            raise ValueError("value is required for 'set' operation")
        new_value, delta = gauge.set(value)
        snapshot = gauge.get_snapshot()
        return snapshot.to_dict()

    elif operation == "increment":
        new_value, delta = gauge.increment(amount)
        snapshot = gauge.get_snapshot()
        return snapshot.to_dict()

    elif operation == "decrement":
        new_value, delta = gauge.decrement(amount)
        snapshot = gauge.get_snapshot()
        return snapshot.to_dict()

    elif operation == "get":
        snapshot = gauge.get_snapshot()
        return snapshot.to_dict()

    else:
        raise ValueError(f"Unknown operation: {operation}")


def get_gauge_value(name: str) -> Optional[float]:
    """Get the current value of a gauge."""
    registry = GaugeRegistry()
    return registry.get_value(name)


def set_gauge_value(name: str, value: float) -> Dict[str, Any]:
    """Set a gauge value."""
    registry = GaugeRegistry()
    gauge = registry.get_or_create(name)
    gauge.set(value)
    return gauge.get_snapshot().to_dict()
