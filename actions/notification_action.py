"""Notification action module for RabAI AutoClick.

Provides system notification actions for user alerts,
including macOS notification center integration.
"""

import sys
import os
import subprocess
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class NotifyAction(BaseAction):
    """Send system notification.
    
    Supports title, body, sound, and icon options.
    Uses macOS osascript for native notifications.
    """
    action_type = "notify"
    display_name = "发送通知"
    description = "发送系统通知提醒"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Send notification.
        
        Args:
            context: Execution context.
            params: Dict with keys: title, message, sound,
                   subtitle, save_to_var.
        
        Returns:
            ActionResult with notification result.
        """
        title = params.get('title', 'RabAI AutoClick')
        message = params.get('message', '')
        sound = params.get('sound', True)
        subtitle = params.get('subtitle', '')
        save_to_var = params.get('save_to_var', None)

        if not message:
            return ActionResult(success=False, message="Message cannot be empty")

        try:
            # Build osascript command for notification
            script_parts = [
                'display notification',
                f'"{self._escape(message)}"'
            ]

            if subtitle:
                script_parts.append(f'with subtitle "{self._escape(subtitle)}"')

            script_parts.append(f'giving after 2 seconds')

            if sound:
                script_parts.append('with sound name "Glass"')

            script = ' '.join(script_parts)

            result = subprocess.run(
                ['osascript', '-e', script],
                capture_output=True,
                text=True
            )

            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"通知发送失败: {result.stderr}"
                )

            result_data = {
                'sent': True,
                'title': title,
                'message': message,
                'sound': sound
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"通知已发送: {message[:30]}",
                data=result_data
            )

        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="osascript not found (requires macOS)"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"通知发送失败: {str(e)}"
            )

    def _escape(self, text: str) -> str:
        """Escape double quotes in text."""
        return text.replace('"', '\\"').replace('\n', ' ')

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'title': 'RabAI AutoClick',
            'sound': True,
            'subtitle': '',
            'save_to_var': None
        }


class AlertAction(BaseAction):
    """Show alert dialog.
    
    Displays modal alert with message and buttons.
    """
    action_type = "alert"
    display_name = "显示警告框"
    description = "显示模态警告对话框"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Show alert dialog.
        
        Args:
            context: Execution context.
            params: Dict with keys: title, message, buttons,
                   default_button, save_to_var.
        
        Returns:
            ActionResult with button clicked.
        """
        title = params.get('title', 'Alert')
        message = params.get('message', '')
        buttons = params.get('buttons', ['OK'])
        default_button = params.get('default_button', 0)
        save_to_var = params.get('save_to_var', None)

        if not message:
            return ActionResult(success=False, message="Message cannot be empty")

        try:
            buttons_str = ', '.join(f'"{b}"' for b in buttons)
            script = f'''
            set response to button returned of (display dialog "{self._escape(message)}" buttons {{{buttons_str}}} default button {default_button + 1} with title "{self._escape(title)}")
            response
            '''

            result = subprocess.run(
                ['osascript', '-e', script],
                capture_output=True,
                text=True
            )

            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"警告框显示失败: {result.stderr}"
                )

            clicked = result.stdout.strip()
            button_index = buttons.index(clicked) if clicked in buttons else -1

            result_data = {
                'clicked': clicked,
                'button_index': button_index,
                'title': title
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"点击了: {clicked}",
                data=result_data
            )

        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="osascript not found (requires macOS)"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"警告框失败: {str(e)}"
            )

    def _escape(self, text: str) -> str:
        return text.replace('"', '\\"').replace('\n', ' ')

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'title': 'Alert',
            'buttons': ['OK'],
            'default_button': 0,
            'save_to_var': None
        }
