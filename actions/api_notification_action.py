"""API notification action module for RabAI AutoClick.

Provides push notification operations:
- NotificationSendAction: Send push notification
- NotificationChannelAction: Configure notification channel
- NotificationTemplateAction: Use notification template
- NotificationBatchAction: Batch send notifications
- NotificationPrefsAction: Manage user notification preferences
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class NotificationSendAction(BaseAction):
    """Send a push notification."""
    action_type = "notification_send"
    display_name = "发送通知"
    description = "发送推送通知"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            recipient = params.get("recipient", "")
            title = params.get("title", "")
            body = params.get("body", "")
            channel = params.get("channel", "default")
            priority = params.get("priority", "normal")
            data = params.get("data", {})

            if not recipient or not body:
                return ActionResult(success=False, message="recipient and body are required")

            notification_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "notifications"):
                context.notifications = []
            context.notifications.append({
                "id": notification_id,
                "recipient": recipient,
                "title": title,
                "body": body,
                "channel": channel,
                "priority": priority,
                "sent_at": time.time(),
            })

            return ActionResult(
                success=True,
                data={"notification_id": notification_id, "recipient": recipient, "channel": channel},
                message=f"Notification {notification_id} sent to {recipient}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Notification send failed: {e}")


class NotificationChannelAction(BaseAction):
    """Configure notification channel."""
    action_type = "notification_channel"
    display_name = "配置通知渠道"
    description = "配置通知渠道"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            channel_name = params.get("channel_name", "")
            enabled = params.get("enabled", True)
            settings = params.get("settings", {})

            if not channel_name:
                return ActionResult(success=False, message="channel_name is required")

            if not hasattr(context, "notification_channels"):
                context.notification_channels = {}
            context.notification_channels[channel_name] = {
                "channel_name": channel_name,
                "enabled": enabled,
                "settings": settings,
                "configured_at": time.time(),
            }

            return ActionResult(
                success=True,
                data={"channel_name": channel_name, "enabled": enabled},
                message=f"Channel {channel_name} {'enabled' if enabled else 'disabled'}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Notification channel config failed: {e}")


class NotificationTemplateAction(BaseAction):
    """Use a notification template."""
    action_type = "notification_template"
    display_name = "通知模板"
    description = "使用通知模板"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            template_name = params.get("template_name", "")
            variables = params.get("variables", {})

            if not template_name:
                return ActionResult(success=False, message="template_name is required")

            templates = {
                "welcome": {"title": "Welcome {name}!", "body": "Hello {name}, welcome to our platform."},
                "alert": {"title": "Alert: {alert_type}", "body": "{message}"},
                "reminder": {"title": "Reminder", "body": "Don't forget: {task}"},
            }

            template = templates.get(template_name, {"title": "Notification", "body": "Template not found"})
            rendered_title = template["title"].format(**variables)
            rendered_body = template["body"].format(**variables)

            return ActionResult(
                success=True,
                data={"template_name": template_name, "title": rendered_title, "body": rendered_body},
                message=f"Template {template_name} rendered",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Notification template failed: {e}")


class NotificationBatchAction(BaseAction):
    """Batch send notifications."""
    action_type = "notification_batch"
    display_name = "批量通知"
    description = "批量发送通知"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            notifications = params.get("notifications", [])
            if not notifications:
                return ActionResult(success=False, message="notifications list is required")

            sent_ids = []
            for n in notifications:
                nid = str(uuid.uuid4())[:8]
                sent_ids.append(nid)

            return ActionResult(
                success=True,
                data={"sent_count": len(sent_ids), "notification_ids": sent_ids},
                message=f"Batch sent {len(sent_ids)} notifications",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Notification batch failed: {e}")


class NotificationPrefsAction(BaseAction):
    """Manage user notification preferences."""
    action_type = "notification_prefs"
    display_name = "通知偏好"
    description = "管理用户通知偏好"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            user_id = params.get("user_id", "")
            preferences = params.get("preferences", {})

            if not user_id:
                return ActionResult(success=False, message="user_id is required")

            if not hasattr(context, "notification_prefs"):
                context.notification_prefs = {}
            context.notification_prefs[user_id] = {
                "user_id": user_id,
                "preferences": preferences,
                "updated_at": time.time(),
            }

            return ActionResult(
                success=True,
                data={"user_id": user_id, "preferences": preferences},
                message=f"Preferences updated for user {user_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Notification prefs failed: {e}")
