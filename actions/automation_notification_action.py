"""
Automation Notification Action Module.

Sends notifications via multiple channels including email,
webhook, SMS, Slack, Discord, and system notifications.

Author: RabAi Team
"""

from __future__ import annotations

import json
import sys
import os
import time
import smtplib
import ssl
from dataclasses import dataclass, field
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class NotificationChannel(Enum):
    """Supported notification channels."""
    EMAIL = "email"
    WEBHOOK = "webhook"
    SLACK = "slack"
    DISCORD = "discord"
    SYSTEM = "system"
    SMS = "sms"


class NotificationPriority(Enum):
    """Notification priority levels."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


@dataclass
class NotificationConfig:
    """Configuration for notification channels."""
    email_host: str = "smtp.gmail.com"
    email_port: int = 587
    email_use_tls: bool = True
    email_username: Optional[str] = None
    email_password: Optional[str] = None
    email_from: Optional[str] = None
    webhook_timeout: float = 10.0
    slack_webhook_url: Optional[str] = None
    discord_webhook_url: Optional[str] = None


@dataclass
class Notification:
    """A notification message."""
    id: str
    channel: NotificationChannel
    priority: NotificationPriority
    title: str
    body: str
    recipients: List[str]
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    sent_at: Optional[float] = None
    status: str = "pending"


@dataclass
class NotificationResult:
    """Result of sending a notification."""
    success: bool
    channel: NotificationChannel
    message_id: Optional[str]
    error: Optional[str] = None


class AutomationNotificationAction(BaseAction):
    """Automation notification action.
    
    Sends notifications across multiple channels with
    templating, batching, and delivery confirmation.
    """
    action_type = "automation_notification"
    display_name = "自动化通知"
    description = "多渠道通知发送"
    
    def __init__(self):
        super().__init__()
        self._config = NotificationConfig()
        self._history: List[Notification] = []
        self._max_history = 1000
        self._delivery_callbacks: Dict[str, callable] = {}
    
    def configure(self, config: NotificationConfig) -> None:
        """Configure notification settings."""
        self._config = config
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Send a notification.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - channel: Notification channel (email/webhook/slack/discord/system)
                - title: Notification title
                - body: Notification body
                - recipients: List of recipients
                - priority: Priority level (low/normal/high/urgent)
                - metadata: Additional metadata
                - template: Template name for formatted notifications
                - template_data: Data for template rendering
                
        Returns:
            ActionResult with notification delivery results.
        """
        start_time = time.time()
        
        channel_str = params.get("channel", "webhook")
        title = params.get("title", "")
        body = params.get("body", "")
        recipients = params.get("recipients", [])
        priority_str = params.get("priority", "normal")
        metadata = params.get("metadata", {})
        template = params.get("template")
        template_data = params.get("template_data", {})
        
        if not title and not body:
            return ActionResult(
                success=False,
                message="Must provide title or body",
                duration=time.time() - start_time
            )
        
        try:
            channel = NotificationChannel(channel_str)
        except ValueError:
            return ActionResult(
                success=False,
                message=f"Unknown channel: {channel_str}",
                duration=time.time() - start_time
            )
        
        priority = NotificationPriority(priority_str)
        
        if template:
            title, body = self._render_template(template, template_data, title, body)
        
        notification = Notification(
            id=str(hash(f"{title}{body}{time.time()}")),
            channel=channel,
            priority=priority,
            title=title,
            body=body,
            recipients=recipients,
            metadata=metadata
        )
        
        try:
            if channel == NotificationChannel.EMAIL:
                result = self._send_email(notification)
            elif channel == NotificationChannel.WEBHOOK:
                result = self._send_webhook(notification)
            elif channel == NotificationChannel.SLACK:
                result = self._send_slack(notification)
            elif channel == NotificationChannel.DISCORD:
                result = self._send_discord(notification)
            elif channel == NotificationChannel.SYSTEM:
                result = self._send_system(notification)
            elif channel == NotificationChannel.SMS:
                result = self._send_sms(notification)
            else:
                return ActionResult(
                    success=False,
                    message=f"Channel not implemented: {channel}",
                    duration=time.time() - start_time
                )
            
            notification.status = "sent" if result.success else "failed"
            notification.sent_at = time.time()
            
            self._add_to_history(notification)
            
            if result.success and notification.id in self._delivery_callbacks:
                try:
                    self._delivery_callbacks[notification.id](notification)
                except Exception:
                    pass
            
            return ActionResult(
                success=result.success,
                message=f"Notification {'sent' if result.success else 'failed'}: {result.error or 'OK'}",
                data={
                    "notification_id": notification.id,
                    "channel": channel.value,
                    "message_id": result.message_id,
                    "sent_at": notification.sent_at
                },
                duration=time.time() - start_time
            )
            
        except Exception as e:
            notification.status = "error"
            notification.sent_at = time.time()
            self._add_to_history(notification)
            
            return ActionResult(
                success=False,
                message=f"Notification failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _send_email(self, notification: Notification) -> NotificationResult:
        """Send email notification."""
        try:
            if not self._config.email_username or not self._config.email_password:
                return NotificationResult(
                    success=False,
                    channel=notification.channel,
                    message_id=None,
                    error="Email not configured"
                )
            
            msg = MIMEMultipart("alternative")
            msg["Subject"] = notification.title
            msg["From"] = self._config.email_from or self._config.email_username
            msg["To"] = ", ".join(notification.recipients)
            
            text_part = MIMEText(notification.body, "plain")
            html_part = MIMEText(
                f"<html><body><h1>{notification.title}</h1><p>{notification.body}</p></body></html>",
                "html"
            )
            msg.attach(text_part)
            msg.attach(html_part)
            
            context = ssl.create_default_context() if self._config.email_use_tls else None
            
            with smtplib.SMTP(self._config.email_host, self._config.email_port) as server:
                if self._config.email_use_tls:
                    server.starttls(context=context)
                server.login(self._config.email_username, self._config.email_password)
                server.sendmail(msg["From"], notification.recipients, msg.as_string())
            
            return NotificationResult(
                success=True,
                channel=notification.channel,
                message_id=notification.id
            )
            
        except Exception as e:
            return NotificationResult(
                success=False,
                channel=notification.channel,
                message_id=None,
                error=str(e)
            )
    
    def _send_webhook(self, notification: Notification) -> NotificationResult:
        """Send webhook notification."""
        try:
            payload = {
                "title": notification.title,
                "body": notification.body,
                "priority": notification.priority.value,
                "metadata": notification.metadata,
                "timestamp": notification.created_at
            }
            
            url = notification.recipients[0] if notification.recipients else ""
            if not url:
                return NotificationResult(
                    success=False,
                    channel=notification.channel,
                    message_id=None,
                    error="No webhook URL provided"
                )
            
            data = json.dumps(payload).encode("utf-8")
            req = Request(
                url,
                data=data,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "RabAi-AutoClick/1.0"
                },
                method="POST"
            )
            
            with urlopen(req, timeout=self._config.webhook_timeout) as response:
                return NotificationResult(
                    success=response.status < 400,
                    channel=notification.channel,
                    message_id=notification.id,
                    error=None if response.status < 400 else f"HTTP {response.status}"
                )
                
        except HTTPError as e:
            return NotificationResult(
                success=False,
                channel=notification.channel,
                message_id=None,
                error=f"HTTP {e.code}: {str(e)}"
            )
        except Exception as e:
            return NotificationResult(
                success=False,
                channel=notification.channel,
                message_id=None,
                error=str(e)
            )
    
    def _send_slack(self, notification: Notification) -> NotificationResult:
        """Send Slack notification."""
        try:
            webhook_url = self._config.slack_webhook_url or notification.recipients[0]
            if not webhook_url:
                return NotificationResult(
                    success=False,
                    channel=notification.channel,
                    message_id=None,
                    error="No Slack webhook URL"
                )
            
            priority_emoji = {
                NotificationPriority.LOW: ":information_source:",
                NotificationPriority.NORMAL: ":bell:",
                NotificationPriority.HIGH: ":warning:",
                NotificationPriority.URGENT: ":rotating_light:"
            }
            
            payload = {
                "text": f"{priority_emoji.get(notification.priority, ':bell:')} *{notification.title}*",
                "attachments": [{
                    "text": notification.body,
                    "color": "#36a64f" if notification.priority == NotificationPriority.LOW else
                             "#0074D9" if notification.priority == NotificationPriority.NORMAL else
                             "#FF851B" if notification.priority == NotificationPriority.HIGH else "#FF4136"
                }]
            }
            
            data = json.dumps(payload).encode("utf-8")
            req = Request(webhook_url, data=data, headers={"Content-Type": "application/json"}, method="POST")
            
            with urlopen(req, timeout=self._config.webhook_timeout) as response:
                return NotificationResult(
                    success=response.status < 400,
                    channel=notification.channel,
                    message_id=notification.id
                )
                
        except Exception as e:
            return NotificationResult(
                success=False,
                channel=notification.channel,
                message_id=None,
                error=str(e)
            )
    
    def _send_discord(self, notification: Notification) -> NotificationResult:
        """Send Discord notification."""
        try:
            webhook_url = self._config.discord_webhook_url or notification.recipients[0]
            if not webhook_url:
                return NotificationResult(
                    success=False,
                    channel=notification.channel,
                    message_id=None,
                    error="No Discord webhook URL"
                )
            
            priority_color = {
                NotificationPriority.LOW: 3447003,
                NotificationPriority.NORMAL: 2591575,
                NotificationPriority.HIGH: 15105570,
                NotificationPriority.URGENT: 15158332
            }
            
            payload = {
                "embeds": [{
                    "title": notification.title,
                    "description": notification.body,
                    "color": priority_color.get(notification.priority, 2591575)
                }]
            }
            
            data = json.dumps(payload).encode("utf-8")
            req = Request(webhook_url, data=data, headers={"Content-Type": "application/json"}, method="POST")
            
            with urlopen(req, timeout=self._config.webhook_timeout) as response:
                return NotificationResult(
                    success=response.status < 400,
                    channel=notification.channel,
                    message_id=notification.id
                )
                
        except Exception as e:
            return NotificationResult(
                success=False,
                channel=notification.channel,
                message_id=None,
                error=str(e)
            )
    
    def _send_system(self, notification: Notification) -> NotificationResult:
        """Send system notification."""
        try:
            os.system(f"""
                osascript -e 'display notification "{notification.body}" with title "{notification.title}"'
            """)
            return NotificationResult(
                success=True,
                channel=notification.channel,
                message_id=notification.id
            )
        except Exception as e:
            return NotificationResult(
                success=False,
                channel=notification.channel,
                message_id=None,
                error=str(e)
            )
    
    def _send_sms(self, notification: Notification) -> NotificationResult:
        """Send SMS notification (stub - requires SMS gateway)."""
        return NotificationResult(
            success=False,
            channel=notification.channel,
            message_id=None,
            error="SMS not configured - requires SMS gateway"
        )
    
    def _render_template(
        self, template: str, data: Dict[str, Any], default_title: str, default_body: str
    ) -> Tuple[str, str]:
        """Render a notification template."""
        templates = {
            "alert": ("Alert: {title}", "{body}\n\nTimestamp: {timestamp}"),
            "success": ("Success: {title}", "{body}\n\nCompleted at: {timestamp}"),
            "error": ("Error: {title}", "{body}\n\nError time: {timestamp}"),
            "summary": ("Execution Summary", "{body}")
        }
        
        if template not in templates:
            return default_title, default_body
        
        title_tmpl, body_tmpl = templates[template]
        data.setdefault("timestamp", time.strftime("%Y-%m-%d %H:%M:%S"))
        data.setdefault("title", default_title)
        data.setdefault("body", default_body)
        
        try:
            title = title_tmpl.format(**data)
            body = body_tmpl.format(**data)
        except (KeyError, ValueError):
            title = default_title
            body = default_body
        
        return title, body
    
    def _add_to_history(self, notification: Notification) -> None:
        """Add notification to history."""
        self._history.append(notification)
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history:]
    
    def get_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get notification history."""
        recent = self._history[-limit:]
        return [
            {
                "id": n.id,
                "channel": n.channel.value,
                "priority": n.priority.value,
                "title": n.title,
                "status": n.status,
                "sent_at": n.sent_at
            }
            for n in recent
        ]
    
    def register_callback(self, notification_id: str, callback: callable) -> None:
        """Register a callback for delivery confirmation."""
        self._delivery_callbacks[notification_id] = callback
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate notification parameters."""
        if not params.get("title") and not params.get("body"):
            return False, "Must provide title or body"
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return []
