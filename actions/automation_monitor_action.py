"""Automation Monitor Action.

Monitors automation execution health and emits alerts.
"""
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import time


class AlertLevel(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class AutomationAlert:
    alert_id: str
    level: AlertLevel
    message: str
    automation_id: Optional[str]
    timestamp: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MonitoredMetric:
    name: str
    value: float
    timestamp: float
    tags: Dict[str, str] = field(default_factory=dict)


class AutomationMonitorAction:
    """Monitors automation health and triggers alerts."""

    def __init__(
        self,
        error_threshold: float = 0.1,
        latency_threshold_ms: float = 5000.0,
        alert_handlers: Optional[List[Callable[[AutomationAlert], None]]] = None,
    ) -> None:
        self.error_threshold = error_threshold
        self.latency_threshold_ms = latency_threshold_ms
        self.alert_handlers = alert_handlers or []
        self.metrics: List[MonitoredMetric] = []
        self.alerts: List[AutomationAlert] = []
        self._counters: Dict[str, int] = {}
        self._alert_id = 0

    def record_metric(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        self.metrics.append(MonitoredMetric(
            name=name,
            value=value,
            timestamp=time.time(),
            tags=tags or {},
        ))

    def increment(self, counter: str, value: int = 1) -> None:
        self._counters[counter] = self._counters.get(counter, 0) + value

    def check_thresholds(self, automation_id: Optional[str] = None) -> List[AutomationAlert]:
        new_alerts = []
        error_count = self._counters.get(f"{automation_id}_errors", 0)
        total_count = self._counters.get(f"{automation_id}_total", 1)
        error_rate = error_count / max(total_count, 1)
        if error_rate > self.error_threshold:
            alert = self._create_alert(
                AlertLevel.ERROR,
                f"Error rate {error_rate:.2%} exceeds threshold {self.error_threshold:.2%}",
                automation_id,
            )
            new_alerts.append(alert)
        return new_alerts

    def _create_alert(self, level: AlertLevel, message: str, automation_id: Optional[str]) -> AutomationAlert:
        self._alert_id += 1
        alert = AutomationAlert(
            alert_id=f"alert_{self._alert_id}",
            level=level,
            message=message,
            automation_id=automation_id,
            timestamp=time.time(),
        )
        self.alerts.append(alert)
        for handler in self.alert_handlers:
            try:
                handler(alert)
            except Exception:
                pass
        return alert

    def get_recent_alerts(self, level: Optional[AlertLevel] = None, limit: int = 50) -> List[AutomationAlert]:
        result = self.alerts
        if level:
            result = [a for a in result if a.level == level]
        return result[-limit:]

    def get_stats(self) -> Dict[str, Any]:
        return {
            "total_alerts": len(self.alerts),
            "counters": dict(self._counters),
            "recent_metrics": len(self.metrics),
        }
