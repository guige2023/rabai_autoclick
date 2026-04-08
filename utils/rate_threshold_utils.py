"""
Rate threshold alerting utilities.

Provides threshold-based alerting for metrics
with cooldown and auto-recovery support.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Literal


AlertState = Literal["ok", "warning", "critical"]


@dataclass
class Alert:
    """Alert event."""
    state: AlertState
    metric_name: str
    value: float
    threshold: float
    message: str
    timestamp: float = field(default_factory=time.time)


@dataclass
class MetricSnapshot:
    """Point-in-time metric value."""
    name: str
    value: float
    timestamp: float = field(default_factory=time.time)


class RateThresholdChecker:
    """
    Monitor metrics against thresholds and trigger alerts.

    Supports warning/critical dual thresholds with cooldown periods.
    """

    def __init__(
        self,
        metric_name: str,
        warning_threshold: float,
        critical_threshold: float,
        comparison: Literal["gt", "lt", "gte", "lte", "eq"] = "gt",
        cooldown_seconds: float = 60.0,
    ):
        self.metric_name = metric_name
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
        self.comparison = comparison
        self.cooldown = cooldown_seconds
        self._last_alert_time: dict[AlertState, float] = {
            "ok": 0.0,
            "warning": 0.0,
            "critical": 0.0,
        }
        self._last_state: AlertState = "ok"
        self._lock = threading.Lock()

    def _compare(self, value: float) -> bool:
        if self.comparison == "gt":
            return value > self.warning_threshold
        if self.comparison == "lt":
            return value < self.warning_threshold
        if self.comparison == "gte":
            return value >= self.warning_threshold
        if self.comparison == "lte":
            return value <= self.warning_threshold
        return value == self.warning_threshold

    def _get_state(self, value: float) -> AlertState:
        if self._compare_critical(value):
            return "critical"
        if self._compare_warning(value):
            return "warning"
        return "ok"

    def _compare_critical(self, value: float) -> bool:
        if self.comparison == "gt":
            return value > self.critical_threshold
        if self.comparison == "lt":
            return value < self.critical_threshold
        if self.comparison == "gte":
            return value >= self.critical_threshold
        if self.comparison == "lte":
            return value <= self.critical_threshold
        return value == self.critical_threshold

    def _compare_warning(self, value: float) -> bool:
        return self._compare(value)

    def check(
        self,
        value: float,
        on_alert: Callable[[Alert], None] | None = None,
    ) -> AlertState:
        """
        Check a metric value against thresholds.

        Args:
            value: Current metric value
            on_alert: Optional callback for alerts

        Returns:
            Current alert state
        """
        state = self._get_state(value)
        now = time.time()

        with self._lock:
            should_alert = False
            if state != self._last_state:
                last_alert = self._last_alert_time.get(state, 0)
                if now - last_alert >= self.cooldown:
                    should_alert = True
                    self._last_alert_time[state] = now
                    self._last_state = state

        if should_alert and on_alert:
            threshold = (self.critical_threshold if state == "critical"
                        else self.warning_threshold)
            alert = Alert(
                state=state,
                metric_name=self.metric_name,
                value=value,
                threshold=threshold,
                message=f"{self.metric_name} is {state}: {value}",
            )
            on_alert(alert)

        return state


class CompositeRateChecker:
    """Monitor multiple metrics with rate threshold checking."""

    def __init__(self):
        self._checkers: dict[str, RateThresholdChecker] = {}
        self._lock = threading.Lock()

    def add_metric(
        self,
        name: str,
        warning_threshold: float,
        critical_threshold: float,
        comparison: Literal["gt", "lt", "gte", "lte", "eq"] = "gt",
        cooldown_seconds: float = 60.0,
    ) -> RateThresholdChecker:
        """Add a metric to monitor."""
        checker = RateThresholdChecker(
            metric_name=name,
            warning_threshold=warning_threshold,
            critical_threshold=critical_threshold,
            comparison=comparison,
            cooldown_seconds=cooldown_seconds,
        )
        with self._lock:
            self._checkers[name] = checker
        return checker

    def check_value(
        self,
        name: str,
        value: float,
        on_alert: Callable[[Alert], None] | None = None,
    ) -> AlertState | None:
        """Check a specific metric."""
        with self._lock:
            checker = self._checkers.get(name)
        if checker:
            return checker.check(value, on_alert)
        return None

    def check_all(
        self,
        values: dict[str, float],
        on_alert: Callable[[Alert], None] | None = None,
    ) -> dict[str, AlertState]:
        """Check all provided metric values."""
        results = {}
        for name, value in values.items():
            state = self.check_value(name, value, on_alert)
            if state:
                results[name] = state
        return results

    def get_states(self) -> dict[str, AlertState]:
        """Get current states of all monitored metrics."""
        with self._lock:
            return {name: c._last_state for name, c in self._checkers.items()}
