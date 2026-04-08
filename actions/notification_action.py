"""Notification action module for RabAI AutoClick.

Provides system notification actions with text, sound, and urgency levels.
"""

import os
import sys
import subprocess
from typing import Any, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class NotificationAction(BaseAction):
    """Send system notifications with customizable content and urgency.
    
    Supports macOS Notification Center with title, body, sound,
    and optional action buttons.
    """
    action_type = "notification"
    display_name = "系统通知"
    description = "发送macOS系统通知"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Send a system notification.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: title, body, sound, subtitle,
                   urgency, action_label, app_name.
        
        Returns:
            ActionResult with delivery status.
        """
        title = params.get('title', 'AutoClick')
        body = params.get('body', '')
        sound = params.get('sound', True)
        subtitle = params.get('subtitle', '')
        urgency = params.get('urgency', 'normal')
        action_label = params.get('action_label', '')
        app_name = params.get('app_name', 'RabAI AutoClick')
        
        if not body:
            return ActionResult(success=False, message="body is required")
        
        # Validate urgency
        valid_urgency = ['low', 'normal', 'critical']
        if urgency not in valid_urgency:
            return ActionResult(
                success=False,
                message=f"Invalid urgency: {urgency}. Must be one of {valid_urgency}"
            )
        
        # Build notification command using osascript
        # Using terminal-notifier or osascript fallback
        try:
            # Try using osascript for native macOS notifications
            script_parts = [
                'display notification',
                f'"{body}"'
            ]
            
            if title:
                script_parts.insert(0, f'set theScript to "{title}"')
                script_parts.insert(1, 'with title theScript')
            
            if subtitle:
                script_parts.append(f'with subtitle "{subtitle}"')
            
            if sound:
                script_parts.append('with sound name "default"')
            else:
                script_parts.append('silence')
            
            script = ' '.join(script_parts)
            
            # Execute osascript
            cmd = ['osascript', '-e', f'display notification "{body}"']
            if title:
                cmd[-1] = f'display notification "{body}" with title "{title}"'
            if subtitle:
                cmd[-1] += f' subtitle "{subtitle}"'
            if sound:
                cmd[-1] += ' sound name "default"'
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                return ActionResult(
                    success=True,
                    message=f"Notification sent: {title}",
                    data={'urgency': urgency}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"Notification failed: {result.stderr}",
                    data={'error': result.stderr}
                )
                
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="Notification timed out",
                data={'timeout': 10}
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="osascript not found",
                data={'error': 'osascript not available'}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Notification error: {e}",
                data={'error': str(e)}
            )


class NotificationWithSoundAction(BaseAction):
    """Send notification with custom sound file.
    
    Plays a specific sound file alongside the notification.
    """
    action_type = "notification_sound"
    display_name = "带声音的通知"
    description = "发送带自定义声音的系统通知"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Send a notification with custom sound.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: title, body, sound_file, volume.
        
        Returns:
            ActionResult with delivery status.
        """
        title = params.get('title', 'AutoClick')
        body = params.get('body', '')
        sound_file = params.get('sound_file', '/System/Library/Sounds/Tink.aiff')
        volume = params.get('volume', 0.7)
        
        if not body:
            return ActionResult(success=False, message="body is required")
        
        # Validate volume
        if not 0 <= volume <= 1:
            return ActionResult(
                success=False,
                message=f"Volume must be 0-1, got {volume}"
            )
        
        # Validate sound file exists
        if not os.path.exists(sound_file):
            return ActionResult(
                success=False,
                message=f"Sound file not found: {sound_file}"
            )
        
        try:
            # Send notification first
            notif_cmd = [
                'osascript', '-e',
                f'display notification "{body}" with title "{title}"'
            ]
            notif_result = subprocess.run(
                notif_cmd,
                capture_output=True,
                timeout=5
            )
            
            # Play sound using afplay
            sound_cmd = [
                'afplay',
                '-v', str(volume),
                sound_file
            ]
            sound_result = subprocess.run(
                sound_cmd,
                capture_output=True,
                timeout=10
            )
            
            if notif_result.returncode == 0:
                return ActionResult(
                    success=True,
                    message=f"Notification with sound sent",
                    data={'sound_file': sound_file, 'volume': volume}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"Notification failed: {notif_result.stderr.decode()}"
                )
                
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="Notification or sound timed out"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Error: {e}",
                data={'error': str(e)}
            )
