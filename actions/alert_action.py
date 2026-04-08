"""Alert Action Module.

Provides alerting system with configurable channels,
severity levels, and alert routing.
"""

import time
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AlertSeverity(Enum):
    """Alert severity level."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertChannel(Enum):
    """Alert notification channel."""
    LOG = "log"
    EMAIL = "email"
    WEBHOOK = "webhook"
    SMS = "sms"


@dataclass
class Alert:
    """An alert."""
    alert_id: str
    title: str
    message: str
    severity: AlertSeverity
    channel: AlertChannel
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    acknowledged: bool = False
    resolved: bool = False


@dataclass
class AlertRule:
    """Rule for triggering alerts."""
    rule_id: str
    name: str
    condition: Callable
    severity: AlertSeverity
    channel: AlertChannel
    enabled: bool = True


class AlertManager:
    """Manages alerts and notifications."""

    def __init__(self):
        self._alerts: List[Alert] = []
        self._rules: Dict[str, AlertRule] = {}
        self._handlers: Dict[AlertChannel, Callable] = {
            AlertChannel.LOG: self._log_handler
        }
        self._lock = threading.Lock()
        self._max_alerts = 1000

    def create_alert(
        self,
        title: str,
        message: str,
        severity: AlertSeverity,
        channel: AlertChannel = AlertChannel.LOG,
        metadata: Optional[Dict] = None
    ) -> str:
        """Create an alert."""
        alert_id = f"alert_{int(time.time() * 1000)}"

        alert = Alert(
            alert_id=alert_id,
            title=title,
            message=message,
            severity=severity,
            channel=channel,
            metadata=metadata or {}
        )

        with self._lock:
            self._alerts.append(alert)
            if len(self._alerts) > self._max_alerts:
                self._alerts = self._alerts[-self._max_alerts // 2:]

        handler = self._handlers.get(channel)
        if handler:
            handler(alert)

        return alert_id

    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert."""
        for alert in self._alerts:
            if alert.alert_id == alert_id:
                alert.acknowledged = True
                return True
        return False

    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an alert."""
        for alert in self._alerts:
            if alert.alert_id == alert_id:
                alert.resolved = True
                return True
        return False

    def get_alerts(
        self,
        severity: Optional[AlertSeverity] = None,
        unresolved_only: bool = False,
        limit: int = 100
    ) -> List[Dict]:
        """Get alerts."""
        alerts = self._alerts

        if severity:
            alerts = [a for a in alerts if a.severity == severity]

        if unresolved_only:
            alerts = [a for a in alerts if not a.resolved]

        alerts = alerts[-limit:]

        return [
            {
                "alert_id": a.alert_id,
                "title": a.title,
                "severity": a.severity.value,
                "timestamp": a.timestamp,
                "acknowledged": a.acknowledged,
                "resolved": a.resolved
            }
            for a in alerts
        ]

    def register_handler(
        self,
        channel: AlertChannel,
        handler: Callable[[Alert], None]
    ) -> None:
        """Register alert handler."""
        self._handlers[channel] = handler

    def _log_handler(self, alert: Alert) -> None:
        """Handle alert via logging."""
        print(f"[{alert.severity.value.upper()}] {alert.title}: {alert.message}")


class AlertAction(BaseAction):
    """Action for alert operations."""

    def __init__(self):
        super().__init__("alert")
        self._manager = AlertManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute alert action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "acknowledge":
                return self._acknowledge(params)
            elif operation == "resolve":
                return self._resolve(params)
            elif operation == "list":
                return self._list(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create an alert."""
        alert_id = self._manager.create_alert(
            title=params.get("title", ""),
            message=params.get("message", ""),
            severity=AlertSeverity(params.get("severity", "info")),
            channel=AlertChannel(params.get("channel", "log")),
            metadata=params.get("metadata")
        )
        return ActionResult(success=True, data={"alert_id": alert_id})

    def _acknowledge(self, params: Dict) -> ActionResult:
        """Acknowledge an alert."""
        success = self._manager.acknowledge_alert(params.get("alert_id", ""))
        return ActionResult(success=success)

    def _resolve(self, params: Dict) -> ActionResult:
        """Resolve an alert."""
        success = self._manager.resolve_alert(params.get("alert_id", ""))
        return ActionResult(success=success)

    def _list(self, params: Dict) -> ActionResult:
        """List alerts."""
        alerts = self._manager.get_alerts(
            severity=AlertSeverity(params.get("severity")) if params.get("severity") else None,
            unresolved_only=params.get("unresolved_only", False),
            limit=params.get("limit", 100)
        )
        return ActionResult(success=True, data={"alerts": alerts})
