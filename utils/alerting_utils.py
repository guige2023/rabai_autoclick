"""Alerting utilities with severity levels, routing, and deduplication."""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

__all__ = ["AlertSeverity", "Alert", "AlertManager", "AlertChannel", "PagerDutyChannel"]


class AlertSeverity(Enum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Alert:
    """An alert notification."""
    title: str
    message: str
    severity: AlertSeverity = AlertSeverity.WARNING
    source: str = ""
    tags: dict[str, str] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    fingerprint: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.fingerprint:
            fp_data = f"{self.source}:{self.title}"
            import hashlib
            self.fingerprint = hashlib.md5(fp_data.encode()).hexdigest()[:16]

    def to_dict(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "message": self.message,
            "severity": self.severity.value,
            "source": self.source,
            "tags": self.tags,
            "created_at": self.created_at,
            "fingerprint": self.fingerprint,
            "metadata": self.metadata,
        }


class AlertChannel(ABC):
    """Abstract alert channel."""

    @abstractmethod
    def send(self, alert: Alert) -> bool:
        pass


class LogChannel(AlertChannel):
    """Log-based alert channel."""

    def __init__(self, logger: Callable[[str], None] | None = None) -> None:
        self._logger = logger or print

    def send(self, alert: Alert) -> bool:
        level_map = {
            AlertSeverity.DEBUG: "[DEBUG]",
            AlertSeverity.INFO: "[INFO]",
            AlertSeverity.WARNING: "[WARN]",
            AlertSeverity.ERROR: "[ERROR]",
            AlertSeverity.CRITICAL: "[CRITICAL]",
        }
        prefix = level_map.get(alert.severity, "[INFO]")
        self._logger(f"{prefix} [{alert.source}] {alert.title}: {alert.message}")
        return True


class WebhookChannel(AlertChannel):
    """Webhook alert channel."""

    def __init__(self, url: str, headers: dict[str, str] | None = None) -> None:
        import urllib.request
        self.url = url
        self.headers = headers or {"Content-Type": "application/json"}
        self._urlib = urllib.request

    def send(self, alert: Alert) -> bool:
        data = json.dumps(alert.to_dict()).encode("utf-8")
        req = self._urlib.Request(
            self.url,
            data=data,
            headers=self.headers,
            method="POST",
        )
        try:
            with self._urlib.urlopen(req, timeout=5) as resp:
                return resp.status in (200, 201, 202)
        except Exception:
            return False


class PagerDutyChannel(AlertChannel):
    """PagerDuty Events API v2 alert channel."""

    def __init__(self, routing_key: str) -> None:
        self.routing_key = routing_key
        self._pagerduty_url = "https://events.pagerduty.com/v2/enqueue"

    def send(self, alert: Alert) -> bool:
        payload = {
            "routing_key": self.routing_key,
            "event_action": "trigger",
            "payload": {
                "summary": alert.title,
                "source": alert.source,
                "severity": alert.severity.value,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(alert.created_at)),
                "custom_details": {
                    "message": alert.message,
                    **alert.metadata,
                },
            },
            "dedup_key": alert.fingerprint,
        }
        try:
            import urllib.request
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                self._pagerduty_url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status in (200, 201, 202)
        except Exception:
            return False


class AlertManager:
    """Central alert management with routing and deduplication."""

    def __init__(self, dedup_window_seconds: float = 300.0) -> None:
        self._channels: list[AlertChannel] = []
        self._dedup: dict[str, float] = {}
        self._dedup_window = dedup_window_seconds
        self._lock = threading.Lock()
        self._handlers: dict[AlertSeverity, list[Callable[[Alert], None]]] = {
            sev: [] for sev in AlertSeverity
        }

    def add_channel(self, channel: AlertChannel) -> None:
        self._channels.append(channel)

    def on(self, severity: AlertSeverity, handler: Callable[[Alert], None]) -> None:
        self._handlers[severity].append(handler)

    def fire(self, alert: Alert) -> list[bool]:
        """Fire an alert to all channels. Returns list of success flags."""
        now = time.time()
        with self._lock:
            last = self._dedup.get(alert.fingerprint, 0)
            if now - last < self._dedup_window:
                return []
            self._dedup[alert.fingerprint] = now

        results: list[bool] = []
        for channel in self._channels:
            try:
                results.append(channel.send(alert))
            except Exception:
                results.append(False)

        for handler in self._handlers.get(alert.severity, []):
            try:
                handler(alert)
            except Exception:
                pass

        return results

    def fire_if_healthy(
        self,
        condition: bool,
        title: str,
        message: str,
        severity: AlertSeverity = AlertSeverity.WARNING,
        **kwargs: Any,
    ) -> None:
        """Fire only if condition indicates unhealthy state."""
        if not condition:
            alert = Alert(title=title, message=message, severity=severity, **kwargs)
            self.fire(alert)

    def clear_dedup(self, fingerprint: str | None = None) -> None:
        with self._lock:
            if fingerprint:
                self._dedup.pop(fingerprint, None)
            else:
                self._dedup.clear()

    def get_active_count(self) -> int:
        now = time.time()
        with self._lock:
            return sum(1 for t in self._dedup.values() if now - t < self._dedup_window)
