"""Notification action module for RabAI AutoClick.

Provides notification actions:
- NotifyAction: Send system notification
- LogMessageAction: Log a message to the app logger
"""

import subprocess
from typing import Any, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


# Valid notification sounds
VALID_SOUNDS: List[str] = [
    'default', 'breeze', 'bubbles', 'calypso', 'chime',
    'etc', 'Funk', 'glass', 'heroic', 'input',
    'keys', 'latch', 'morse', 'pluck', 'pop',
    'purr', 'sax', 'solved', 'temp', 'tink',
    'trym', 'volleyball'
]


class NotifyAction(BaseAction):
    """Send a system notification."""
    action_type = "notify"
    display_name = "系统通知"
    description = "发送系统通知"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sending a notification.

        Args:
            context: Execution context.
            params: Dict with title, message, sound.

        Returns:
            ActionResult indicating success.
        """
        title = params.get('title', 'RabAI AutoClick')
        message = params.get('message', '')
        sound = params.get('sound', 'default')

        # Validate title
        valid, msg = self.validate_type(title, str, 'title')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Validate message
        valid, msg = self.validate_type(message, str, 'message')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Validate sound
        valid, msg = self.validate_in(sound, VALID_SOUNDS, 'sound')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            # Resolve {{variable}} references in message
            resolved_message = context.resolve_value(message)

            # Use osascript to show notification on macOS
            if sound == 'default':
                script = f'''
                display notification "{resolved_message}" with title "{title}"
                '''
            else:
                script = f'''
                display notification "{resolved_message}" with title "{title}" sound name "{sound}"
                '''

            subprocess.run(['osascript', '-e', script], capture_output=True, timeout=5)

            truncated = message[:50] + '...' if len(message) > 50 else message
            return ActionResult(
                success=True,
                message=f"通知已发送: {truncated}",
                data={'title': title, 'message': message, 'sound': sound}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"发送通知失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'title': 'RabAI AutoClick',
            'message': '',
            'sound': 'default'
        }


class LogMessageAction(BaseAction):
    """Log a message to the application logger."""
    action_type = "log_message"
    display_name = "记录日志"
    description = "向应用日志写入消息"

    VALID_LEVELS: List[str] = ['debug', 'info', 'warning', 'error', 'success']

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute logging a message.

        Args:
            context: Execution context.
            params: Dict with message, level.

        Returns:
            ActionResult indicating success.
        """
        message = params.get('message', '')
        level = params.get('level', 'info')

        # Validate message
        valid, msg = self.validate_type(message, str, 'message')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Validate level
        valid, msg = self.validate_in(level, self.VALID_LEVELS, 'level')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            # Resolve {{variable}} references in message
            resolved_message = context.resolve_value(message)

            # Get the app logger
            from utils.app_logger import app_logger

            # Log at the specified level
            if level == 'debug':
                app_logger.debug(resolved_message)
            elif level == 'info':
                app_logger.info(resolved_message)
            elif level == 'warning':
                app_logger.warning(resolved_message)
            elif level == 'error':
                app_logger.error(resolved_message)
            elif level == 'success':
                app_logger.success(resolved_message)

            return ActionResult(
                success=True,
                message=f"日志已记录 [{level}]: {message}",
                data={'message': resolved_message, 'level': level}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"记录日志失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'level': 'info'
        }