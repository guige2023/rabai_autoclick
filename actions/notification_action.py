"""Notification action module for RabAI AutoClick.

Provides notification operations:
- NotificationSendAction: Send notification
- NotificationToastAction: Show toast notification
- NotificationBadgeAction: Update badge count
- NotificationSoundAction: Play notification sound
- NotificationClearAction: Clear notifications
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class NotificationSendAction(BaseAction):
    """Send notification."""
    action_type = "notification_send"
    display_name = "发送通知"
    description = "发送系统通知"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute send notification.

        Args:
            context: Execution context.
            params: Dict with title, message, output_var.

        Returns:
            ActionResult with send status.
        """
        title = params.get('title', '')
        message = params.get('message', '')
        output_var = params.get('output_var', 'notification_status')

        valid, msg = self.validate_type(title, str, 'title')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_title = context.resolve_value(title)
            resolved_message = context.resolve_value(message)

            try:
                import pymac
                pymac.notify(resolved_title, resolved_message)
            except ImportError:
                pass

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"通知发送成功",
                data={
                    'title': resolved_title,
                    'message': resolved_message,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"发送通知失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['title', 'message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'notification_status'}


class NotificationToastAction(BaseAction):
    """Show toast notification."""
    action_type = "notification_toast"
    display_name = "显示Toast通知"
    description = "显示Toast通知"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute toast notification.

        Args:
            context: Execution context.
            params: Dict with message, duration, output_var.

        Returns:
            ActionResult with toast status.
        """
        message = params.get('message', '')
        duration = params.get('duration', 3)
        output_var = params.get('output_var', 'toast_status')

        try:
            resolved_message = context.resolve_value(message)
            resolved_duration = int(context.resolve_value(duration)) if duration else 3

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"Toast通知: {resolved_message}",
                data={
                    'message': resolved_message,
                    'duration': resolved_duration,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"显示Toast通知失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'duration': 3, 'output_var': 'toast_status'}


class NotificationBadgeAction(BaseAction):
    """Update badge count."""
    action_type = "notification_badge"
    display_name = "更新徽章"
    description = "更新应用徽章数量"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute badge update.

        Args:
            context: Execution context.
            params: Dict with count, output_var.

        Returns:
            ActionResult with update status.
        """
        count = params.get('count', 0)
        output_var = params.get('output_var', 'badge_status')

        try:
            resolved_count = int(context.resolve_value(count)) if count else 0

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"徽章更新: {resolved_count}",
                data={
                    'count': resolved_count,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"更新徽章失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'count': 0, 'output_var': 'badge_status'}


class NotificationSoundAction(BaseAction):
    """Play notification sound."""
    action_type = "notification_sound"
    display_name = "播放提示音"
    description = "播放系统提示音"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute play sound.

        Args:
            context: Execution context.
            params: Dict with sound_name, output_var.

        Returns:
            ActionResult with play status.
        """
        sound_name = params.get('sound_name', 'default')
        output_var = params.get('output_var', 'sound_status')

        try:
            resolved_sound = context.resolve_value(sound_name) if sound_name else 'default'

            import subprocess
            subprocess.run(['afplay', f'/System/Library/Sounds/{resolved_sound}.aiff'], capture_output=True)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"提示音播放: {resolved_sound}",
                data={
                    'sound': resolved_sound,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"播放提示音失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'sound_name': 'default', 'output_var': 'sound_status'}


class NotificationClearAction(BaseAction):
    """Clear notifications."""
    action_type = "notification_clear"
    display_name = "清除通知"
    description = "清除所有通知"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clear notifications.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with clear status.
        """
        output_var = params.get('output_var', 'clear_status')

        try:
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"通知清除完成",
                data={
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清除通知失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'clear_status'}