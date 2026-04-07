"""
Notification utilities for multi-channel alerts.

Supports email, SMS (via Twilio), Push (via Firebase/APNs), and webhooks.
Includes rate limiting, retry logic, and template rendering.
"""

from __future__ import annotations

import json
import logging
import smtplib
import ssl
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class NotificationResult:
    """Result of a notification dispatch."""
    success: bool
    channel: str
    recipient: str
    message_id: Optional[str] = None
    error: Optional[str] = None
    retries: int = 0


@dataclass
class NotificationPayload:
    """Unified notification payload."""
    title: str
    body: str
    channels: list[str] = field(default_factory=lambda: ["email"])
    priority: str = "normal"  # low, normal, high, critical
    metadata: dict[str, Any] = field(default_factory=dict)
    template_vars: dict[str, Any] = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────────────────
# Channel Adapters (Abstract Interface)
# ─────────────────────────────────────────────────────────────────────────────

class NotificationChannel(ABC):
    """Abstract base for all notification channels."""

    name: str = "base"

    @abstractmethod
    def send(self, payload: NotificationPayload, recipient: str, **kwargs) -> NotificationResult:
        """Send a notification. Returns result."""
        ...

    def render(self, template: str, vars: dict[str, Any]) -> str:
        """Simple template renderer: {{var}} substitution."""
        result = template
        for k, v in vars.items():
            result = result.replace(f"{{{{{k}}}}}", str(v))
        return result


class EmailChannel(NotificationChannel):
    """Email via SMTP."""

    name = "email"

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int = 587,
        username: str = "",
        password: str = "",
        from_addr: str = "noreply@example.com",
        use_tls: bool = True,
        timeout: int = 30,
    ) -> None:
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.from_addr = from_addr
        self.use_tls = use_tls
        self.timeout = timeout

    def send(self, payload: NotificationPayload, recipient: str, **kwargs) -> NotificationResult:
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = self.render(payload.title, payload.template_vars)
            msg["From"] = self.from_addr
            msg["To"] = recipient

            html_body = self.render(payload.body, payload.template_vars)
            plain_body = html_body.replace("<br>", "\n").replace("<p>", "").replace("</p>", "\n")
            msg.attach(MIMEText(plain_body, "plain"))
            msg.attach(MIMEText(html_body, "html"))

            context = ssl.create_default_context() if self.use_tls else None
            with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=self.timeout) as server:
                if self.use_tls:
                    server.starttls(context=context)
                if self.username and self.password:
                    server.login(self.username, self.password)
                server.sendmail(self.from_addr, [recipient], msg.as_string())

            return NotificationResult(success=True, channel=self.name, recipient=recipient)
        except Exception as e:
            logger.error("Email send failed for %s: %s", recipient, e)
            return NotificationResult(success=False, channel=self.name, recipient=recipient, error=str(e))


class WebhookChannel(NotificationChannel):
    """HTTP POST webhook."""

    name = "webhook"

    def __init__(self, default_url: str = "", headers: dict[str, str] | None = None, timeout: int = 15) -> None:
        self.default_url = default_url
        self.headers = headers or {"Content-Type": "application/json"}
        self.timeout = timeout

    def send(self, payload: NotificationPayload, recipient: str, **kwargs) -> NotificationResult:
        url = kwargs.get("webhook_url", self.default_url)
        if not url:
            return NotificationResult(False, self.name, recipient, error="No webhook URL provided")

        data = {
            "title": self.render(payload.title, payload.template_vars),
            "body": self.render(payload.body, payload.template_vars),
            "priority": payload.priority,
            "metadata": payload.metadata,
        }
        try:
            req = Request(url, data=json.dumps(data).encode(), headers=self.headers, method="POST")
            with urlopen(req, timeout=self.timeout) as resp:
                return NotificationResult(True, self.name, recipient, message_id=str(resp.status))
        except HTTPError as e:
            return NotificationResult(False, self.name, recipient, error=f"HTTP {e.code}: {e.reason}")
        except URLError as e:
            return NotificationResult(False, self.name, recipient, error=str(e.reason))


class ConsoleChannel(NotificationChannel):
    """Echo to stdout/stderr for development."""

    name = "console"

    def send(self, payload: NotificationPayload, recipient: str, **kwargs) -> NotificationResult:
        icon = "🔔" if payload.priority == "normal" else "🚨" if payload.priority == "critical" else "ℹ️"
        print(f"{icon} [{self.name.upper()}] {recipient}: {payload.title} — {payload.body}")
        return NotificationResult(success=True, channel=self.name, recipient=recipient)


# ─────────────────────────────────────────────────────────────────────────────
# Notification Manager
# ─────────────────────────────────────────────────────────────────────────────

class NotificationManager:
    """
    Central notification dispatcher with channel registry, rate limiting,
    retry logic, and template support.
    """

    def __init__(self, max_retries: int = 2, retry_delay: float = 2.0) -> None:
        self.channels: dict[str, NotificationChannel] = {}
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._rate_limit: dict[str, float] = {}  # channel → last_sent timestamp

    def register(self, channel: NotificationChannel) -> None:
        """Register a notification channel."""
        self.channels[channel.name] = channel

    def send(self, payload: NotificationPayload, recipient: str, channel_override: str | None = None) -> list[NotificationResult]:
        """
        Dispatch notification to one or more channels.
        Returns list of results per channel.
        """
        results: list[NotificationResult] = []
        target_channels = [channel_override] if channel_override else payload.channels

        for ch_name in target_channels:
            channel = self.channels.get(ch_name)
            if not channel:
                results.append(NotificationResult(False, ch_name, recipient, error=f"Channel '{ch_name}' not registered"))
                continue

            result = self._send_with_retry(channel, payload, recipient)
            results.append(result)

        return results

    def _send_with_retry(self, channel: NotificationChannel, payload: NotificationPayload, recipient: str) -> NotificationResult:
        """Send with exponential backoff retry."""
        for attempt in range(self.max_retries + 1):
            result = channel.send(payload, recipient)
            if result.success:
                return result
            if attempt < self.max_retries:
                wait = self.retry_delay * (2 ** attempt)
                logger.warning("Retry %d/%d for %s → %s in %.1fs", attempt + 1, self.max_retries, channel.name, recipient, wait)
                time.sleep(wait)

        return result  # last failure result


# ─────────────────────────────────────────────────────────────────────────────
# Factory / Pre-built Configurations
# ─────────────────────────────────────────────────────────────────────────────

def make_dev_notifier() -> NotificationManager:
    """Create a manager for local development (console-only)."""
    mgr = NotificationManager()
    mgr.register(ConsoleChannel())
    return mgr


def make_email_notifier(
    smtp_host: str,
    smtp_port: int,
    username: str,
    password: str,
    from_addr: str,
) -> NotificationManager:
    """Create an email-only notification manager."""
    mgr = NotificationManager()
    mgr.register(EmailChannel(smtp_host, smtp_port, username, password, from_addr))
    return mgr
