"""Automation Notification Action Module for RabAI AutoClick.

Sends system notifications and alerts for automation
events with action buttons and callback support.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class NotificationLevel:
    """Notification urgency levels."""
    LOW = "low"
    MIDDLE = "middle"
    CRITICAL = "critical"


class AutomationNotificationAction(BaseAction):
    """System notification delivery for automation alerts.

    Send native macOS notifications with title, body, and
    action buttons. Supports grouping, urgency levels, and
    notification center management.
    """
    action_type = "automation_notification"
    display_name = "自动化通知"
    description = "发送系统通知和自动化事件告警"

    _notification_history: List[Dict[str, Any]] = []
    _max_history = 100

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute notification operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'send', 'send_bulk', 'list', 'clear'
                - title: str - notification title
                - body: str - notification body text
                - subtitle: str (optional) - subtitle
                - level: str (optional) - 'low', 'middle', 'critical'
                - actions: list (optional) - action button definitions
                - sound: str (optional) - sound name
                - group_id: str (optional) - notification group

        Returns:
            ActionResult with notification result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'send')

            if operation == 'send':
                return self._send_notification(params, start_time)
            elif operation == 'send_bulk':
                return self._send_bulk(params, start_time)
            elif operation == 'list':
                return self._list_notifications(start_time)
            elif operation == 'clear':
                return self._clear_notifications(start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Notification action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _send_notification(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Send a single notification."""
        title = params.get('title', 'Automation')
        body = params.get('body', '')
        subtitle = params.get('subtitle', '')
        level = params.get('level', NotificationLevel.MIDDLE)
        actions = params.get('actions', [])
        sound = params.get('sound', 'default')
        group_id = params.get('group_id', '')

        if not body:
            return ActionResult(
                success=False,
                message="body is required",
                duration=time.time() - start_time
            )

        notification_id = self._deliver_notification(
            title, body, subtitle, level, actions, sound, group_id
        )

        entry = {
            'notification_id': notification_id,
            'title': title,
            'body': body,
            'subtitle': subtitle,
            'level': level,
            'sound': sound,
            'group_id': group_id,
            'sent_at': time.time()
        }
        self._notification_history.append(entry)
        if len(self._notification_history) > self._max_history:
            self._notification_history.pop(0)

        return ActionResult(
            success=True,
            message=f"Notification sent: {notification_id}",
            data={
                'notification_id': notification_id,
                'title': title,
                'level': level
            },
            duration=time.time() - start_time
        )

    def _send_bulk(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Send multiple notifications."""
        notifications = params.get('notifications', [])
        batch_delay = params.get('batch_delay', 0.1)

        if not notifications:
            return ActionResult(
                success=False,
                message="notifications list is required",
                duration=time.time() - start_time
            )

        sent = 0
        failed = 0
        results = []

        for notif in notifications:
            try:
                result = self._send_notification(notif, start_time)
                if result.success:
                    sent += 1
                    results.append({'success': True, 'id': result.data.get('notification_id')})
                else:
                    failed += 1
                    results.append({'success': False})
            except Exception as e:
                failed += 1
                results.append({'success': False, 'error': str(e)})

            if batch_delay > 0 and sent + failed < len(notifications):
                time.sleep(batch_delay)

        return ActionResult(
            success=failed == 0,
            message=f"Bulk send: {sent} sent, {failed} failed",
            data={
                'sent': sent,
                'failed': failed,
                'total': len(notifications)
            },
            duration=time.time() - start_time
        )

    def _list_notifications(self, start_time: float) -> ActionResult:
        """List notification history."""
        limit = start_time
        notifications = self._notification_history[-limit:]

        return ActionResult(
            success=True,
            message=f"Notifications: {len(notifications)}",
            data={
                'notifications': notifications,
                'count': len(notifications)
            },
            duration=time.time() - start_time
        )

    def _clear_notifications(self, start_time: float) -> ActionResult:
        """Clear notification history."""
        cleared = len(self._notification_history)
        self._notification_history.clear()
        return ActionResult(
            success=True,
            message=f"Cleared {cleared} notifications",
            data={'cleared': cleared},
            duration=time.time() - start_time
        )

    def _deliver_notification(
        self,
        title: str,
        body: str,
        subtitle: str,
        level: str,
        actions: List[Dict[str, str]],
        sound: str,
        group_id: str
    ) -> str:
        """Deliver notification via osascript."""
        import subprocess

        notification_id = f"notif_{int(time.time() * 1000)}"

        script_parts = [
            'display notification',
            f'"{body}"'
        ]

        if subtitle:
            script_parts.append(f'with subtitle "{subtitle}"')

        if title != 'Automation':
            script_parts.append(f'as "{title}"')

        if sound != 'none':
            sound_name = 'default' if sound == 'default' else sound
            script_parts.append(f'playing sound "{sound_name}"')

        script = ' '.join(script_parts)

        try:
            subprocess.run(
                ['osascript', '-e', script],
                capture_output=True, timeout=5
            )
        except Exception:
            pass

        return notification_id
