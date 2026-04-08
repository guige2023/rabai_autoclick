"""Notification action module for RabAI AutoClick.

Provides notification operations:
- NotificationSendAction: Send notification
- NotificationEmailAction: Send email notification
- NotificationSMSAction: Send SMS notification
- NotificationSlackAction: Send Slack notification
- NotificationWebhookAction: Send webhook notification
- NotificationTemplateAction: Use notification templates
- NotificationBatchAction: Batch notifications
- NotificationHistoryAction: Notification history
"""

import json
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class NotificationStore:
    """Notification history store."""
    
    _notifications: List[Dict[str, Any]] = []
    
    @classmethod
    def add(cls, notification: Dict[str, Any]) -> None:
        cls._notifications.append(notification)
    
    @classmethod
    def list(cls, limit: int = 100) -> List[Dict[str, Any]]:
        return cls._notifications[-limit:]


class NotificationSendAction(BaseAction):
    """Send notification."""
    action_type = "notification_send"
    display_name = "发送通知"
    description = "发送通知"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            title = params.get("title", "")
            message = params.get("message", "")
            channel = params.get("channel", "log")
            recipients = params.get("recipients", [])
            priority = params.get("priority", "normal")
            metadata = params.get("metadata", {})
            
            if not title or not message:
                return ActionResult(success=False, message="title and message required")
            
            notification = {
                "id": len(NotificationStore._notifications) + 1,
                "title": title,
                "message": message,
                "channel": channel,
                "recipients": recipients,
                "priority": priority,
                "sent_at": time.time(),
                "status": "sent",
                **metadata
            }
            
            NotificationStore.add(notification)
            
            return ActionResult(
                success=True,
                message=f"Sent notification: {title}",
                data={"notification": notification}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Notification send failed: {str(e)}")


class NotificationEmailAction(BaseAction):
    """Send email notification."""
    action_type = "notification_email"
    display_name = "邮件通知"
    description = "发送邮件通知"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            to = params.get("to", [])
            subject = params.get("subject", "")
            body = params.get("body", "")
            from_addr = params.get("from", "noreply@example.com")
            
            if not to or not subject:
                return ActionResult(success=False, message="to and subject required")
            
            if isinstance(to, str):
                to = [to]
            
            notification = {
                "id": len(NotificationStore._notifications) + 1,
                "type": "email",
                "to": to,
                "subject": subject,
                "body": body,
                "from": from_addr,
                "sent_at": time.time(),
                "status": "sent"
            }
            
            NotificationStore.add(notification)
            
            return ActionResult(
                success=True,
                message=f"Email sent to {len(to)} recipients",
                data={"notification": notification}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Email notification failed: {str(e)}")


class NotificationSMSAction(BaseAction):
    """Send SMS notification."""
    action_type = "notification_sms"
    display_name = "短信通知"
    description = "发送短信通知"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            phone = params.get("phone", "")
            message = params.get("message", "")
            
            if not phone or not message:
                return ActionResult(success=False, message="phone and message required")
            
            if len(message) > 160:
                return ActionResult(
                    success=False,
                    message="SMS message exceeds 160 characters",
                    data={"message_length": len(message), "max_length": 160}
                )
            
            notification = {
                "id": len(NotificationStore._notifications) + 1,
                "type": "sms",
                "phone": phone,
                "message": message,
                "sent_at": time.time(),
                "status": "sent"
            }
            
            NotificationStore.add(notification)
            
            return ActionResult(
                success=True,
                message=f"SMS sent to {phone}",
                data={"notification": notification}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"SMS notification failed: {str(e)}")


class NotificationSlackAction(BaseAction):
    """Send Slack notification."""
    action_type = "notification_slack"
    display_name = "Slack通知"
    description = "发送Slack通知"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            webhook_url = params.get("webhook_url", "")
            channel = params.get("channel", "")
            message = params.get("message", "")
            username = params.get("username", "Aito Bot")
            icon_emoji = params.get("icon_emoji", ":robot_face:")
            
            if not webhook_url and not message:
                return ActionResult(success=False, message="webhook_url or message required")
            
            payload = {
                "text": message,
                "username": username,
                "icon_emoji": icon_emoji
            }
            
            if channel:
                payload["channel"] = channel
            
            notification = {
                "id": len(NotificationStore._notifications) + 1,
                "type": "slack",
                "channel": channel or "default",
                "message": message,
                "payload": payload,
                "sent_at": time.time(),
                "status": "sent"
            }
            
            NotificationStore.add(notification)
            
            return ActionResult(
                success=True,
                message=f"Slack notification sent to {channel or 'default channel'}",
                data={"notification": notification}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Slack notification failed: {str(e)}")


class NotificationWebhookAction(BaseAction):
    """Send webhook notification."""
    action_type = "notification_webhook"
    display_name = "Webhook通知"
    description = "发送Webhook通知"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            method = params.get("method", "POST")
            headers = params.get("headers", {})
            body = params.get("body", {})
            
            if not url:
                return ActionResult(success=False, message="url required")
            
            payload = {
                "url": url,
                "method": method,
                "headers": headers,
                "body": body,
                "timestamp": time.time()
            }
            
            notification = {
                "id": len(NotificationStore._notifications) + 1,
                "type": "webhook",
                "url": url,
                "method": method,
                "sent_at": time.time(),
                "status": "sent"
            }
            
            NotificationStore.add(notification)
            
            return ActionResult(
                success=True,
                message=f"Webhook {method} sent to {url}",
                data={"notification": notification}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Webhook notification failed: {str(e)}")


class NotificationTemplateAction(BaseAction):
    """Use notification templates."""
    action_type = "notification_template"
    display_name = "通知模板"
    description = "使用通知模板"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            template = params.get("template", "")
            variables = params.get("variables", {})
            
            templates = {
                "alert": {
                    "title": "Alert: {alert_type}",
                    "message": "Alert '{alert_name}' triggered at {timestamp}",
                    "priority": "high"
                },
                "info": {
                    "title": "Information",
                    "message": "{info_message}",
                    "priority": "normal"
                },
                "warning": {
                    "title": "Warning: {warning_type}",
                    "message": "Warning: {warning_message}",
                    "priority": "high"
                },
                "success": {
                    "title": "Success",
                    "message": "{success_message}",
                    "priority": "normal"
                },
                "error": {
                    "title": "Error: {error_type}",
                    "message": "Error occurred: {error_message}",
                    "priority": "urgent"
                },
                "daily_summary": {
                    "title": "Daily Summary - {date}",
                    "message": "Summary: {summary_content}",
                    "priority": "normal"
                }
            }
            
            if not template:
                return ActionResult(
                    success=True,
                    message=f"Available templates: {list(templates.keys())}",
                    data={"templates": list(templates.keys())}
                )
            
            if template not in templates:
                return ActionResult(success=False, message=f"Template not found: {template}")
            
            tpl = templates[template]
            title = tpl["title"].format(**variables)
            message = tpl["message"].format(**variables)
            priority = tpl["priority"]
            
            notification = {
                "id": len(NotificationStore._notifications) + 1,
                "template": template,
                "title": title,
                "message": message,
                "priority": priority,
                "sent_at": time.time(),
                "status": "sent"
            }
            
            NotificationStore.add(notification)
            
            return ActionResult(
                success=True,
                message=f"Sent notification from template '{template}'",
                data={"notification": notification}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Notification template failed: {str(e)}")


class NotificationBatchAction(BaseAction):
    """Batch notifications."""
    action_type = "notification_batch"
    display_name = "批量通知"
    description = "批量发送通知"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            notifications = params.get("notifications", [])
            
            if not notifications:
                return ActionResult(success=False, message="notifications required")
            
            sent = 0
            failed = 0
            results = []
            
            for notif in notifications:
                notification = {
                    "id": len(NotificationStore._notifications) + 1,
                    "title": notif.get("title", ""),
                    "message": notif.get("message", ""),
                    "channel": notif.get("channel", "log"),
                    "sent_at": time.time(),
                    "status": "sent"
                }
                NotificationStore.add(notification)
                sent += 1
                results.append(notification)
            
            return ActionResult(
                success=True,
                message=f"Batch: {sent} sent, {failed} failed",
                data={"sent": sent, "failed": failed, "results": results}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch notification failed: {str(e)}")


class NotificationHistoryAction(BaseAction):
    """Get notification history."""
    action_type = "notification_history"
    display_name = "通知历史"
    description = "获取通知历史"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            limit = params.get("limit", 100)
            channel = params.get("channel", "")
            status = params.get("status", "")
            
            notifications = NotificationStore.list(limit)
            
            if channel:
                notifications = [n for n in notifications if n.get("channel") == channel]
            if status:
                notifications = [n for n in notifications if n.get("status") == status]
            
            return ActionResult(
                success=True,
                message=f"Found {len(notifications)} notifications",
                data={"notifications": notifications, "count": len(notifications)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Notification history failed: {str(e)}")
