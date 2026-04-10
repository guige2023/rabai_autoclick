"""
Multi-channel notification system for RAbAI AutoClick.
Supports desktop, email, Telegram, Discord, Slack, webhooks, SMS, and more.
"""

import os
import sys
import smtplib
import subprocess
import time
import logging
from datetime import datetime, time as dt_time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from enum import Enum
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import json

sys.path.insert(0, '/Users/guige/my_project')

logger = logging.getLogger(__name__)


class NotificationPriority(Enum):
    """Notification priority levels."""
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


class NotificationChannel(Enum):
    """Available notification channels."""
    DESKTOP = "desktop"
    EMAIL = "email"
    TELEGRAM = "telegram"
    DISCORD = "discord"
    SLACK = "slack"
    WEBHOOK = "webhook"
    SMS = "sms"


@dataclass
class NotificationTemplate:
    """Template for notification messages with variable substitution."""
    name: str
    subject: str = ""
    body: str = ""
    variables: List[str] = field(default_factory=list)

    def render(self, **kwargs) -> Dict[str, str]:
        """Render template with provided variables."""
        result = {"subject": self.subject, "body": self.body}
        for key, value in kwargs.items():
            result["subject"] = result["subject"].replace(f"{{{key}}}", str(value))
            result["body"] = result["body"].replace(f"{{{key}}}", str(value))
        return result


@dataclass
class DoNotDisturbSchedule:
    """Do-not-disturb schedule configuration."""
    enabled: bool = False
    start_time: str = "22:00"  # HH:MM format
    end_time: str = "08:00"
    timezone: str = "local"
    exempt_channels: List[str] = field(default_factory=list)  # Channels exempt from DND

    def is_active(self) -> bool:
        """Check if DND is currently active."""
        if not self.enabled:
            return False

        now = datetime.now()
        start_h, start_m = map(int, self.start_time.split(":"))
        end_h, end_m = map(int, self.end_time.split(":"))

        start = now.replace(hour=start_h, minute=start_m, second=0, microsecond=0)
        end = now.replace(hour=end_h, minute=end_m, second=0, microsecond=0)

        if start <= end:
            # Same day range (e.g., 08:00 to 18:00)
            return start <= now <= end
        else:
            # Overnight range (e.g., 22:00 to 08:00)
            return now >= start or now <= end


@dataclass
class NotificationRule:
    """Rule for conditional notification delivery."""
    name: str
    condition: Callable[[Dict[str, Any]], bool]
    channels: List[NotificationChannel]
    priority: NotificationPriority = NotificationPriority.NORMAL
    template_name: Optional[str] = None
    enabled: bool = True


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    base_delay: float = 1.0  # seconds
    max_delay: float = 60.0  # seconds
    exponential_base: float = 2.0


class NotificationSystem:
    """
    Multi-channel notification system supporting various delivery methods.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the notification system.

        Args:
            config: Configuration dictionary with channel settings
        """
        self.config = config or {}
        self.templates: Dict[str, NotificationTemplate] = {}
        self.rules: List[NotificationRule] = []
        self.dnd_schedule = DoNotDisturbSchedule()
        self.retry_config = RetryConfig()
        self.delivery_history: List[Dict[str, Any]] = []

        self._init_default_templates()
        self._init_default_rules()

    def _init_default_templates(self):
        """Initialize default notification templates."""
        self.add_template(NotificationTemplate(
            name="workflow_complete",
            subject="Workflow Completed: {workflow_name}",
            body="Your workflow '{workflow_name}' has completed successfully at {timestamp}.",
            variables=["workflow_name", "timestamp"]
        ))
        self.add_template(NotificationTemplate(
            name="workflow_failed",
            subject="Workflow Failed: {workflow_name}",
            body="Workflow '{workflow_name}' failed at {timestamp}.\nError: {error_message}",
            variables=["workflow_name", "timestamp", "error_message"]
        ))
        self.add_template(NotificationTemplate(
            name="workflow_started",
            subject="Workflow Started: {workflow_name}",
            body="Workflow '{workflow_name}' has started execution.",
            variables=["workflow_name"]
        ))
        self.add_template(NotificationTemplate(
            name="alert",
            subject="Alert: {alert_type}",
            body="{alert_message}\n\nPriority: {priority}\nTime: {timestamp}",
            variables=["alert_type", "alert_message", "priority", "timestamp"]
        ))

    def _init_default_rules(self):
        """Initialize default notification rules."""
        pass

    def add_template(self, template: NotificationTemplate):
        """Add a notification template."""
        self.templates[template.name] = template

    def add_rule(self, rule: NotificationRule):
        """Add a notification rule."""
        self.rules.append(rule)

    def set_dnd_schedule(self, schedule: DoNotDisturbSchedule):
        """Set the do-not-disturb schedule."""
        self.dnd_schedule = schedule

    def set_retry_config(self, config: RetryConfig):
        """Set the retry configuration."""
        self.retry_config = config

    def _is_dnd_active(self, channel: NotificationChannel) -> bool:
        """Check if DND is active for a given channel."""
        if channel.value in self.dnd_schedule.exempt_channels:
            return False
        return self.dnd_schedule.is_active()

    def _should_deliver(self, rule: Optional[NotificationRule], context: Dict[str, Any]) -> bool:
        """Check if notification should be delivered based on rules."""
        if rule and rule.enabled:
            try:
                return rule.condition(context)
            except Exception:
                return False
        return True

    def _get_retry_delay(self, attempt: int) -> float:
        """Calculate exponential backoff delay."""
        delay = self.retry_config.base_delay * (self.retry_config.exponential_base ** attempt)
        return min(delay, self.retry_config.max_delay)

    def _retry_with_backoff(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with exponential backoff retry."""
        last_exception = None
        for attempt in range(self.retry_config.max_attempts):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt < self.retry_config.max_attempts - 1:
                    delay = self._get_retry_delay(attempt)
                    logger.warning(f"Notification attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
        raise last_exception

    # --- Desktop Notifications (macOS) ---
    def send_desktop_notification(self, title: str, message: str, sound: bool = True) -> bool:
        """
        Send desktop notification using macOS Notification Center via osascript.

        Args:
            title: Notification title
            message: Notification body
            sound: Whether to play a sound

        Returns:
            True if successful, False otherwise
        """
        if self._is_dnd_active(NotificationChannel.DESKTOP):
            logger.info("Desktop notification blocked by DND")
            return False

        try:
            script = f'display notification "{message}" with title "{title}"'
            if sound:
                script += ' sound name "default"'

            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=10
            )
            success = result.returncode == 0
            self._log_delivery("desktop", title, success)
            return success
        except Exception as e:
            logger.error(f"Desktop notification failed: {e}")
            self._log_delivery("desktop", title, False, str(e))
            return False

    # --- Email Notifications ---
    def _get_email_config(self) -> Dict[str, Any]:
        """Get email configuration from config or environment."""
        return {
            "smtp_host": self.config.get("email", {}).get("smtp_host", os.environ.get("SMTP_HOST", "localhost")),
            "smtp_port": self.config.get("email", {}).get("smtp_port", int(os.environ.get("SMTP_PORT", "587"))),
            "smtp_user": self.config.get("email", {}).get("smtp_user", os.environ.get("SMTP_USER", "")),
            "smtp_password": self.config.get("email", {}).get("smtp_password", os.environ.get("SMTP_PASSWORD", "")),
            "from_addr": self.config.get("email", {}).get("from_addr", os.environ.get("EMAIL_FROM", "noreply@localhost")),
            "use_tls": self.config.get("email", {}).get("use_tls", True),
        }

    def send_email(self, to_addr: str, subject: str, body: str, html: bool = False) -> bool:
        """
        Send email notification via SMTP.

        Args:
            to_addr: Recipient email address
            subject: Email subject
            body: Email body
            html: Whether body is HTML

        Returns:
            True if successful, False otherwise
        """
        if self._is_dnd_active(NotificationChannel.EMAIL):
            logger.info("Email notification blocked by DND")
            return False

        email_config = self._get_email_config()
        if not email_config["smtp_host"]:
            logger.warning("Email notification skipped: no SMTP host configured")
            return False

        def _send():
            msg = MIMEMultipart()
            msg["From"] = email_config["from_addr"]
            msg["To"] = to_addr
            msg["Subject"] = subject
            msg["Date"] = datetime.now().strftime("%a, %d %b %Y %H:%M:%S %z")

            msg.attach(MIMEText(body, "html" if html else "plain"))

            with smtplib.SMTP(email_config["smtp_host"], email_config["smtp_port"]) as server:
                if email_config["use_tls"]:
                    server.starttls()
                if email_config["smtp_user"] and email_config["smtp_password"]:
                    server.login(email_config["smtp_user"], email_config["smtp_password"])
                server.send_message(msg)

        try:
            self._retry_with_backoff(_send)
            self._log_delivery("email", subject, True)
            return True
        except Exception as e:
            logger.error(f"Email notification failed: {e}")
            self._log_delivery("email", subject, False, str(e))
            return False

    # --- Telegram Bot ---
    def _get_telegram_config(self) -> Dict[str, Any]:
        """Get Telegram configuration."""
        return {
            "bot_token": self.config.get("telegram", {}).get("bot_token", os.environ.get("TELEGRAM_BOT_TOKEN", "")),
            "chat_id": self.config.get("telegram", {}).get("chat_id", os.environ.get("TELEGRAM_CHAT_ID", "")),
        }

    def send_telegram(self, message: str, parse_mode: str = "Markdown") -> bool:
        """
        Send message via Telegram Bot API.

        Args:
            message: Message text (supports Markdown/HTML formatting)
            parse_mode: 'Markdown' or 'HTML'

        Returns:
            True if successful, False otherwise
        """
        if self._is_dnd_active(NotificationChannel.TELEGRAM):
            logger.info("Telegram notification blocked by DND")
            return False

        telegram_config = self._get_telegram_config()
        bot_token = telegram_config["bot_token"]
        chat_id = telegram_config["chat_id"]

        if not bot_token or not chat_id:
            logger.warning("Telegram notification skipped: bot token or chat ID not configured")
            return False

        def _send():
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": parse_mode
            }
            req = Request(url, json.dumps(payload).encode(), {"Content-Type": "application/json"})
            with urlopen(req, timeout=30) as response:
                return json.loads(response.read().decode())

        try:
            self._retry_with_backoff(_send)
            self._log_delivery("telegram", message[:50], True)
            return True
        except Exception as e:
            logger.error(f"Telegram notification failed: {e}")
            self._log_delivery("telegram", message[:50], False, str(e))
            return False

    # --- Discord Webhook ---
    def _get_discord_config(self) -> Dict[str, Any]:
        """Get Discord configuration."""
        return {
            "webhook_url": self.config.get("discord", {}).get("webhook_url", os.environ.get("DISCORD_WEBHOOK_URL", "")),
            "username": self.config.get("discord", {}).get("username", "RAbAI Bot"),
            "avatar_url": self.config.get("discord", {}).get("avatar_url", ""),
        }

    def send_discord(self, message: str, embed: Optional[Dict] = None) -> bool:
        """
        Send message to Discord channel via webhook.

        Args:
            message: Message text
            embed: Optional Discord embed object

        Returns:
            True if successful, False otherwise
        """
        if self._is_dnd_active(NotificationChannel.DISCORD):
            logger.info("Discord notification blocked by DND")
            return False

        discord_config = self._get_discord_config()
        webhook_url = discord_config["webhook_url"]

        if not webhook_url:
            logger.warning("Discord notification skipped: webhook URL not configured")
            return False

        def _send():
            payload = {"content": message}
            if discord_config["username"]:
                payload["username"] = discord_config["username"]
            if discord_config["avatar_url"]:
                payload["avatar_url"] = discord_config["avatar_url"]
            if embed:
                payload["embeds"] = [embed]

            req = Request(
                webhook_url,
                json.dumps(payload).encode(),
                {"Content-Type": "application/json"}
            )
            with urlopen(req, timeout=30) as response:
                return response.read().decode()

        try:
            self._retry_with_backoff(_send)
            self._log_delivery("discord", message[:50], True)
            return True
        except Exception as e:
            logger.error(f"Discord notification failed: {e}")
            self._log_delivery("discord", message[:50], False, str(e))
            return False

    # --- Slack Webhook ---
    def _get_slack_config(self) -> Dict[str, Any]:
        """Get Slack configuration."""
        return {
            "webhook_url": self.config.get("slack", {}).get("webhook_url", os.environ.get("SLACK_WEBHOOK_URL", "")),
            "channel": self.config.get("slack", {}).get("channel", ""),
            "username": self.config.get("slack", {}).get("username", "RAbAI Bot"),
        }

    def send_slack(self, message: str, blocks: Optional[List[Dict]] = None) -> bool:
        """
        Send message to Slack channel via webhook.

        Args:
            message: Message text (supports Slack Markdown)
            blocks: Optional Slack block kit blocks

        Returns:
            True if successful, False otherwise
        """
        if self._is_dnd_active(NotificationChannel.SLACK):
            logger.info("Slack notification blocked by DND")
            return False

        slack_config = self._get_slack_config()
        webhook_url = slack_config["webhook_url"]

        if not webhook_url:
            logger.warning("Slack notification skipped: webhook URL not configured")
            return False

        def _send():
            payload = {"text": message}
            if slack_config["channel"]:
                payload["channel"] = slack_config["channel"]
            if slack_config["username"]:
                payload["username"] = slack_config["username"]
            if blocks:
                payload["blocks"] = blocks

            req = Request(
                webhook_url,
                json.dumps(payload).encode(),
                {"Content-Type": "application/json"}
            )
            with urlopen(req, timeout=30) as response:
                return response.read().decode()

        try:
            self._retry_with_backoff(_send)
            self._log_delivery("slack", message[:50], True)
            return True
        except Exception as e:
            logger.error(f"Slack notification failed: {e}")
            self._log_delivery("slack", message[:50], False, str(e))
            return False

    # --- Generic Webhook ---
    def send_webhook(self, url: str, payload: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> bool:
        """
        Send notification to a generic HTTP webhook.

        Args:
            url: Webhook URL
            payload: JSON payload to send
            headers: Optional additional headers

        Returns:
            True if successful, False otherwise
        """
        if self._is_dnd_active(NotificationChannel.WEBHOOK):
            logger.info("Webhook notification blocked by DND")
            return False

        def _send():
            request_headers = {"Content-Type": "application/json"}
            if headers:
                request_headers.update(headers)

            req = Request(
                url,
                json.dumps(payload).encode(),
                request_headers
            )
            with urlopen(req, timeout=30) as response:
                return response.read().decode()

        try:
            self._retry_with_backoff(_send)
            self._log_delivery("webhook", str(payload)[:50], True)
            return True
        except Exception as e:
            logger.error(f"Webhook notification failed: {e}")
            self._log_delivery("webhook", str(payload)[:50], False, str(e))
            return False

    # --- Twilio SMS ---
    def _get_twilio_config(self) -> Dict[str, Any]:
        """Get Twilio configuration."""
        return {
            "account_sid": self.config.get("twilio", {}).get("account_sid", os.environ.get("TWILIO_ACCOUNT_SID", "")),
            "auth_token": self.config.get("twilio", {}).get("auth_token", os.environ.get("TWILIO_AUTH_TOKEN", "")),
            "from_number": self.config.get("twilio", {}).get("from_number", os.environ.get("TWILIO_FROM_NUMBER", "")),
        }

    def send_sms(self, to_number: str, message: str) -> bool:
        """
        Send SMS via Twilio API.

        Args:
            to_number: Recipient phone number (E.164 format)
            message: SMS body (max 160 characters for single SMS)

        Returns:
            True if successful, False otherwise
        """
        if self._is_dnd_active(NotificationChannel.SMS):
            logger.info("SMS notification blocked by DND")
            return False

        twilio_config = self._get_twilio_config()
        account_sid = twilio_config["account_sid"]
        auth_token = twilio_config["auth_token"]
        from_number = twilio_config["from_number"]

        if not all([account_sid, auth_token, from_number]):
            logger.warning("SMS notification skipped: Twilio credentials not configured")
            return False

        def _send():
            url = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json"
            data = f"To={to_number}&From={from_number}&Body={message}"
            credentials = f"{account_sid}:{auth_token}"

            import base64
            auth_header = base64.b64encode(credentials.encode()).decode()

            req = Request(
                url,
                data.encode(),
                {
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Authorization": f"Basic {auth_header}"
                },
                method="POST"
            )
            with urlopen(req, timeout=30) as response:
                return json.loads(response.read().decode())

        try:
            self._retry_with_backoff(_send)
            self._log_delivery("sms", message[:50], True)
            return True
        except Exception as e:
            logger.error(f"SMS notification failed: {e}")
            self._log_delivery("sms", message[:50], False, str(e))
            return False

    # --- Unified Send Method ---
    def _get_channels_for_priority(self, priority: NotificationPriority) -> List[NotificationChannel]:
        """Get default channels for a priority level."""
        if priority == NotificationPriority.HIGH:
            return [
                NotificationChannel.DESKTOP,
                NotificationChannel.EMAIL,
                NotificationChannel.TELEGRAM,
                NotificationChannel.SMS
            ]
        elif priority == NotificationPriority.NORMAL:
            return [
                NotificationChannel.DESKTOP,
                NotificationChannel.EMAIL,
                NotificationChannel.TELEGRAM
            ]
        else:  # LOW
            return [NotificationChannel.EMAIL]

    def send(
        self,
        message: str,
        title: Optional[str] = None,
        priority: NotificationPriority = NotificationPriority.NORMAL,
        channels: Optional[List[NotificationChannel]] = None,
        context: Optional[Dict[str, Any]] = None,
        rule_name: Optional[str] = None,
        **kwargs
    ) -> Dict[str, bool]:
        """
        Send notification through specified channels or based on priority/rules.

        Args:
            message: Notification message
            title: Notification title (for desktop, etc.)
            priority: Notification priority
            channels: Specific channels to use (overrides priority-based defaults)
            context: Context data for template rendering and rule evaluation
            rule_name: Name of a specific rule to use
            **kwargs: Additional arguments passed to channels

        Returns:
            Dictionary mapping channel names to success status
        """
        results = {}
        context = context or {}
        context["message"] = message
        context["timestamp"] = datetime.now().isoformat()
        context["priority"] = priority.value

        # Determine channels to use
        if channels:
            target_channels = channels
        elif rule_name:
            matched_rule = next((r for r in self.rules if r.name == rule_name), None)
            if matched_rule:
                target_channels = matched_rule.channels
            else:
                target_channels = self._get_channels_for_priority(priority)
        else:
            target_channels = self._get_channels_for_priority(priority)

        # Apply rules and check conditions
        for channel in target_channels:
            applicable_rule = None
            for rule in self.rules:
                if rule.enabled and channel in rule.channels:
                    if self._should_deliver(rule, context):
                        applicable_rule = rule
                        break

            if not self._should_deliver(applicable_rule, context):
                logger.info(f"Notification to {channel.value} skipped due to rule conditions")
                results[channel.value] = False
                continue

            # Send to channel
            if channel == NotificationChannel.DESKTOP:
                results[channel.value] = self.send_desktop_notification(
                    title or "RAbAI Notification", message
                )
            elif channel == NotificationChannel.EMAIL:
                subject = title or kwargs.get("subject", "RAbAI Notification")
                results[channel.value] = self.send_email(
                    kwargs.get("to_email", "recipient@example.com"),
                    subject,
                    message,
                    kwargs.get("html", False)
                )
            elif channel == NotificationChannel.TELEGRAM:
                results[channel.value] = self.send_telegram(message)
            elif channel == NotificationChannel.DISCORD:
                results[channel.value] = self.send_discord(message)
            elif channel == NotificationChannel.SLACK:
                results[channel.value] = self.send_slack(message)
            elif channel == NotificationChannel.WEBHOOK:
                webhook_url = kwargs.get("webhook_url", "")
                webhook_payload = kwargs.get("webhook_payload", {"message": message})
                if webhook_url:
                    results[channel.value] = self.send_webhook(webhook_url, webhook_payload)
                else:
                    results[channel.value] = False
            elif channel == NotificationChannel.SMS:
                results[channel.value] = self.send_sms(
                    kwargs.get("to_phone", ""),
                    message[:160]
                )

        return results

    def send_from_template(
        self,
        template_name: str,
        priority: NotificationPriority = NotificationPriority.NORMAL,
        channels: Optional[List[NotificationChannel]] = None,
        **template_vars
    ) -> Dict[str, bool]:
        """
        Send notification using a named template.

        Args:
            template_name: Name of the template to use
            priority: Notification priority
            channels: Channels to deliver to
            **template_vars: Variables to render in the template

        Returns:
            Dictionary mapping channel names to success status
        """
        template = self.templates.get(template_name)
        if not template:
            logger.error(f"Template '{template_name}' not found")
            return {ch.value: False for ch in (channels or [])}

        rendered = template.render(**template_vars)
        return self.send(
            message=rendered["body"],
            title=rendered["subject"],
            priority=priority,
            channels=channels,
            context=template_vars
        )

    def _log_delivery(self, channel: str, title: str, success: bool, error: Optional[str] = None):
        """Log notification delivery attempt."""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "channel": channel,
            "title": title,
            "success": success,
            "error": error
        }
        self.delivery_history.append(entry)

    def get_delivery_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent delivery history."""
        return self.delivery_history[-limit:]

    def clear_history(self):
        """Clear delivery history."""
        self.delivery_history.clear()
