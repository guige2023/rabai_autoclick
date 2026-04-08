"""Notification center utilities for system notifications.

This module provides utilities for sending and managing system
notifications during automation tasks.
"""

from __future__ import annotations

import platform
import subprocess
from typing import Optional


IS_MACOS = platform.system() == "Darwin"
IS_LINUX = platform.system() == "Linux"
IS_WINDOWS = platform.system() == "Windows"


def send_notification(
    title: str,
    message: str,
    subtitle: Optional[str] = None,
    sound: bool = True,
    app_icon: Optional[str] = None,
) -> bool:
    """Send a system notification.
    
    Args:
        title: Notification title.
        message: Notification body text.
        subtitle: Optional subtitle (macOS only).
        sound: Whether to play the notification sound.
        app_icon: Optional app icon path.
    
    Returns:
        True if notification was sent successfully.
    """
    if IS_MACOS:
        return _send_macos_notification(title, message, subtitle, sound)
    elif IS_LINUX:
        return _send_linux_notification(title, message, sound)
    elif IS_WINDOWS:
        return _send_windows_notification(title, message, sound)
    return False


def _send_macos_notification(
    title: str,
    message: str,
    subtitle: Optional[str],
    sound: bool,
) -> bool:
    """Send notification on macOS using terminal-notifier or osascript."""
    # Try terminal-notifier first (more feature-rich)
    try:
        cmd = ["terminal-notifier", "-title", title, "-message", message]
        if subtitle:
            cmd.extend(["-subtitle", subtitle])
        if not sound:
            cmd.append("-sound")
        result = subprocess.run(cmd, capture_output=True, timeout=5)
        if result.returncode == 0:
            return True
    except FileNotFoundError:
        pass
    
    # Fallback to osascript
    try:
        script = f'display notification "{message}" '
        if subtitle:
            script += f'with title "{title}" subtitle "{subtitle}" '
        else:
            script += f'with title "{title}" '
        if sound:
            script += ' '
        else:
            script += ' '
        
        cmd = ["osascript", "-e", script]
        result = subprocess.run(cmd, capture_output=True, timeout=5)
        return result.returncode == 0
    except Exception:
        return False


def _send_linux_notification(title: str, message: str, sound: bool) -> bool:
    """Send notification on Linux using notify-send."""
    try:
        cmd = ["notify-send", title, message]
        if not sound:
            cmd.append("--urgency=low")
        result = subprocess.run(cmd, capture_output=True, timeout=5)
        return result.returncode == 0
    except FileNotFoundError:
        return False


def _send_windows_notification(title: str, message: str, sound: bool) -> bool:
    """Send notification on Windows using PowerShell toast."""
    try:
        import os
        # Use PowerShell for Windows toast notifications
        ps_script = f"""
        [Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] | Out-Null
        [Windows.Data.Xml.Dom.XmlDocument, Windows.Data.Xml.Dom.XmlDocument, ContentType = WindowsRuntime] | Out-Null
        $template = @"
        <toast>
            <visual>
                <binding template="ToastText02">
                    <text>{title}</text>
                    <text>{message}</text>
                </binding>
            </visual>
        </toast>
"@
        $xml = New-Object Windows.Data.Xml.Dom.XmlDocument
        $xml.LoadXml($template)
        $toast = [Windows.UI.Notifications.ToastNotification]::new($xml)
        $notifier = [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier("AutoClick")
        $notifier.Show($toast)
        """
        result = subprocess.run(
            ["powershell", "-Command", ps_script],
            capture_output=True,
            timeout=5
        )
        return result.returncode == 0
    except Exception:
        return False


def clear_notifications() -> bool:
    """Clear all pending notifications.
    
    Returns:
        True if notifications were cleared.
    """
    if IS_MACOS:
        try:
            subprocess.run(
                ["osascript", "-e", "clear notifications"],
                capture_output=True,
                timeout=3
            )
            return True
        except Exception:
            return False
    elif IS_LINUX:
        try:
            subprocess.run(
                ["notify-send", "--close"],
                capture_output=True,
                timeout=3
            )
            return True
        except Exception:
            return False
    return False
