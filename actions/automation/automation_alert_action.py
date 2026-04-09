"""
Automation Alert Action Module.

Alert management for automation systems with configurable channels,
severity levels, aggregation, and deduplication.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertStatus(Enum):
    """Alert status values."""
    FIRING = "firing"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SILENCED = "silenced"


@dataclass
class Alert:
    """A single alert instance."""
    id: str
    name: str
    severity: AlertSeverity
    message: str
    status: AlertStatus = AlertStatus.FIRING
    source: str = ""
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)
    fired_at: float = field(default_factory=time.time)
    acknowledged_at: Optional[float] = None
    resolved_at: Optional[float] = None
    count: int = 1


@dataclass
class AlertChannel:
    """A notification channel for alerts."""
    name: str
    channel_type: str  # "webhook", "email", "slack", "pagerduty"
    enabled: bool = True
    min_severity: AlertSeverity = AlertSeverity.WARNING
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AlertRule:
    """A rule that triggers alerts."""
    name: str
    condition_fn: Callable[[], bool]
    severity: AlertSeverity
    message_template: str
    cooldown_seconds: float = 60.0
    enabled: bool = True


class AlertAggregator:
    """
    Aggregates similar alerts to prevent alert storms.

    Groups alerts by name/source and tracks occurrences.
    """

    def __init__(self, dedup_window_seconds: float = 300.0) -> None:
        self.dedup_window_seconds = dedup_window_seconds
        self._groups: Dict[str, List[Alert]] = {}

    def add(self, alert: Alert) -> Optional[Alert]:
        """
        Add an alert to the aggregator.

        Returns the aggregated alert or None if deduplicated.
        """
        key = self._make_key(alert)

        if key in self._groups:
            # Update existing group
            existing = self._groups[key][-1]
            if time.time() - existing.fired_at < self.dedup_window_seconds:
                existing.count += alert.count
                existing.annotations.update(alert.annotations)
                return None
            else:
                # Window expired, start new group
                self._groups[key] = [alert]
                return alert
        else:
            self._groups[key] = [alert]
            return alert

    def _make_key(self, alert: Alert) -> str:
        """Generate aggregation key for an alert."""
        labels_str = ",".join(f"{k}={v}" for k, v in sorted(alert.labels.items()))
        return f"{alert.name}:{alert.source}:{labels_str}"

    def get_active_groups(self) -> Dict[str, List[Alert]]:
        """Get all active alert groups."""
        now = time.time()
        active = {}
        for key, alerts in self._groups.items():
            recent = [a for a in alerts if now - a.fired_at < self.dedup_window_seconds]
            if recent:
                active[key] = recent
        return active


class AutomationAlertAction:
    """
    Alert management system for automation.

    Manages alert lifecycle, routing to channels, aggregation,
    and rule-based alert generation.

    Example:
        alert_manager = AutomationAlertAction()
        alert_manager.add_channel(AlertChannel(
            name="slack-alerts",
            channel_type="slack",
            config={"webhook_url": "https://..."},
        ))

        alert_manager.fire_alert(
            name="high-cpu",
            severity=AlertSeverity.WARNING,
            message="CPU usage above 80%",
            source="system-monitor",
        )
    """

    def __init__(
        self,
        dedup_window_seconds: float = 300.0,
    ) -> None:
        self.aggregator = AlertAggregator(dedup_window_seconds)
        self._channels: Dict[str, AlertChannel] = {}
        self._rules: List[AlertRule] = []
        self._active_alerts: Dict[str, Alert] = {}
        self._history: List[Alert] = []
        self._handlers: Dict[str, List[Callable[[Alert], None]]] = {
            "fired": [],
            "acknowledged": [],
            "resolved": [],
        }
        self._id_counter = 0

    def add_channel(self, channel: AlertChannel) -> None:
        """Add a notification channel."""
        self._channels[channel.name] = channel
        logger.info(f"Added alert channel: {channel.name} ({channel.channel_type})")

    def remove_channel(self, name: str) -> bool:
        """Remove a notification channel."""
        if name in self._channels:
            del self._channels[name]
            return True
        return False

    def register_handler(
        self,
        event: str,
        handler: Callable[[Alert], None],
    ) -> None:
        """Register a handler for alert events."""
        if event not in self._handlers:
            self._handlers[event] = []
        self._handlers[event].append(handler)

    def fire_alert(
        self,
        name: str,
        severity: AlertSeverity,
        message: str,
        source: str = "",
        labels: Optional[Dict[str, str]] = None,
        annotations: Optional[Dict[str, str]] = None,
    ) -> Optional[Alert]:
        """
        Fire a new alert.

        Returns the alert if fired, None if deduplicated.
        """
        self._id_counter += 1
        alert = Alert(
            id=f"alert-{self._id_counter}",
            name=name,
            severity=severity,
            message=message,
            source=source,
            labels=labels or {},
            annotations=annotations or {},
        )

        # Aggregate
        aggregated = self.aggregator.add(alert)
        if aggregated is None:
            logger.debug(f"Alert '{name}' deduplicated")
            return None

        # Store
        self._active_alerts[alert.id] = alert
        self._history.append(alert)

        # Notify handlers
        self._emit("fired", alert)

        # Route to channels
        asyncio.create_task(self._route_to_channels(alert))

        logger.info(f"Alert fired: [{alert.severity.value}] {alert.name}: {alert.message}")
        return alert

    async def _route_to_channels(self, alert: Alert) -> None:
        """Route alert to appropriate channels."""
        for channel in self._channels.values():
            if not channel.enabled:
                continue
            if alert.severity.value not in [s.value for s in AlertSeverity] if channel.min_severity else True:
                if alert.severity.value < channel.min_severity.value:
                    continue

            try:
                await self._send_to_channel(channel, alert)
            except Exception as e:
                logger.error(f"Failed to send alert to channel '{channel.name}': {e}")

    async def _send_to_channel(
        self,
        channel: AlertChannel,
        alert: Alert,
    ) -> None:
        """Send alert to a specific channel."""
        if channel.channel_type == "webhook":
            import aiohttp
            url = channel.config.get("webhook_url")
            if url:
                payload = {
                    "alert_id": alert.id,
                    "name": alert.name,
                    "severity": alert.severity.value,
                    "message": alert.message,
                    "source": alert.source,
                    "fired_at": alert.fired_at,
                }
                async with aiohttp.ClientSession() as session:
                    await session.post(url, json=payload)

        elif channel.channel_type == "log":
            severity_str = alert.severity.value.upper()
            logger.log(
                logging.ERROR if alert.severity in (AlertSeverity.ERROR, AlertSeverity.CRITICAL) else logging.WARNING,
                f"[ALERT:{severity_str}] {alert.name}: {alert.message}"
            )

    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert."""
        alert = self._active_alerts.get(alert_id)
        if not alert or alert.status != AlertStatus.FIRING:
            return False

        alert.status = AlertStatus.ACKNOWLEDGED
        alert.acknowledged_at = time.time()
        self._emit("acknowledged", alert)
        logger.info(f"Alert acknowledged: {alert_id}")
        return True

    def resolve_alert(
        self,
        alert_id: str,
        message: Optional[str] = None,
    ) -> bool:
        """Resolve an alert."""
        alert = self._active_alerts.get(alert_id)
        if not alert:
            return False

        alert.status = AlertStatus.RESOLVED
        alert.resolved_at = time.time()
        if message:
            alert.annotations["resolution"] = message

        if alert_id in self._active_alerts:
            del self._active_alerts[alert_id]

        self._emit("resolved", alert)
        logger.info(f"Alert resolved: {alert_id}")
        return True

    def silence_alert(self, alert_id: str, duration_seconds: float) -> bool:
        """Silence an alert temporarily."""
        alert = self._active_alerts.get(alert_id)
        if not alert:
            return False

        alert.status = AlertStatus.SILENCED
        self._emit("resolved", alert)
        logger.info(f"Alert silenced: {alert_id} for {duration_seconds}s")
        return True

    def get_active_alerts(
        self,
        severity: Optional[AlertSeverity] = None,
        source: Optional[str] = None,
    ) -> List[Alert]:
        """Get active alerts, optionally filtered."""
        alerts = list(self._active_alerts.values())

        if severity:
            alerts = [a for a in alerts if a.severity == severity]
        if source:
            alerts = [a for a in alerts if a.source == source]

        return alerts

    def get_history(
        self,
        limit: int = 100,
        since: Optional[float] = None,
    ) -> List[Alert]:
        """Get alert history."""
        history = self._history
        if since:
            history = [a for a in history if a.fired_at >= since]
        return history[-limit:]

    def _emit(self, event: str, alert: Alert) -> None:
        """Emit an alert event to handlers."""
        handlers = self._handlers.get(event, [])
        for handler in handlers:
            try:
                handler(alert)
            except Exception as e:
                logger.error(f"Alert handler error: {e}")
