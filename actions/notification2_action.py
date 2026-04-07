"""Notification2 action module for RabAI AutoClick.

Provides advanced notification operations:
- NotificationAlertAction: Show alert notification
- NotificationSoundAction: Play sound notification
"""

import subprocess
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class NotificationAlertAction(BaseAction):
    """Show alert notification."""
    action_type = "notification_alert"
    display_name = "显示警告通知"
    description = "显示带警告的桌面通知"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute alert notification.

        Args:
            context: Execution context.
            params: Dict with title, message, sound.

        Returns:
            ActionResult indicating success.
        """
        title = params.get('title', 'Notification')
        message = params.get('message', '')
        sound = params.get('sound', True)

        valid, msg = self.validate_type(title, str, 'title')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(message, str, 'message')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_title = context.resolve_value(title)
            resolved_message = context.resolve_value(message)
            resolved_sound = context.resolve_value(sound)

            sound_arg = 'with sound' if resolved_sound else 'without sound'

            script = f'''osascript -e 'display notification "{resolved_message.replace('"', '\\"')}" with title "{resolved_title.replace('"', '\\"')}" {sound_arg}' '''
            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message=f"通知已显示: {resolved_title}",
                data={'title': resolved_title, 'message': resolved_message}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"显示通知失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['title', 'message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'sound': True}


class NotificationSoundAction(BaseAction):
    """Play sound notification."""
    action_type = "notification_sound"
    display_name = "播放提示音"
    description = "播放系统提示音"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sound notification.

        Args:
            context: Execution context.
            params: Dict with sound_name.

        Returns:
            ActionResult indicating success.
        """
        sound_name = params.get('sound_name', 'default')

        try:
            resolved_sound = context.resolve_value(sound_name)

            # Map common sound names to their sounds
            sound_map = {
                'default': 'Glass',
                'error': 'Basso',
                'success': 'Blow',
                'warning': 'Pop',
                'alert': 'Glass',
                'mail': 'Mail',
                'message': 'Message'
            }

            sound = sound_map.get(resolved_sound.lower(), resolved_sound)

            script = f'''osascript -e 'say "{sound}"' '''
            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message=f"提示音已播放: {resolved_sound}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"播放提示音失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'sound_name': 'default'}