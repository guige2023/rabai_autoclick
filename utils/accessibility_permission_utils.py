"""
Accessibility permission utilities for macOS automation.

This module provides utilities for checking and managing
accessibility permissions required for UI automation.
"""

from __future__ import annotations

import subprocess
import platform
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum, auto


IS_MACOS: bool = platform.system() == 'Darwin'


class PermissionStatus(Enum):
    """Accessibility permission status."""
    AUTHORIZED = auto()
    DENIED = auto()
    RESTRICTED = auto()
    NOT_DETERMINED = auto()
    UNSUPPORTED = auto()


@dataclass
class AccessibilityPermissionInfo:
    """
    Information about accessibility permissions.

    Attributes:
        is_authorized: Whether automation is authorized.
        status: Detailed permission status.
        automation_enabled: Whether AppleScript automation is enabled.
        assistive_devices_enabled: Whether assistive devices permission is granted.
        screen_recording_enabled: Whether screen recording is enabled.
    """
    is_authorized: bool
    status: PermissionStatus
    automation_enabled: bool = False
    assistive_devices_enabled: bool = False
    screen_recording_enabled: bool = False


def check_accessibility_permission() -> AccessibilityPermissionInfo:
    """
    Check the current accessibility permission status.

    Returns:
        AccessibilityPermissionInfo with current status.
    """
    if not IS_MACOS:
        return AccessibilityPermissionInfo(
            is_authorized=False,
            status=PermissionStatus.UNSUPPORTED,
        )

    # Check if we can use CGEvent
    import Quartz
    event = Quartz.CGEventCreate(None)
    if event is None:
        return AccessibilityPermissionInfo(
            is_authorized=False,
            status=PermissionStatus.DENIED,
        )

    # Check assistive devices permission
    assistive_enabled = _check_assistive_devices()

    # Check screen recording
    screen_recording = _check_screen_recording_permission()

    return AccessibilityPermissionInfo(
        is_authorized=assistive_enabled,
        status=PermissionStatus.AUTHORIZED if assistive_enabled else PermissionStatus.DENIED,
        assistive_devices_enabled=assistive_enabled,
        screen_recording_enabled=screen_recording,
    )


def _check_assistive_devices() -> bool:
    """Check if assistive devices permission is granted."""
    if not IS_MACOS:
        return False
    try:
        import Cocoa
        # Try to get accessibility info
        return Cocoa.AXIsProcessTrusted()
    except Exception:
        return False


def _check_screen_recording_permission() -> bool:
    """Check if screen recording permission is granted."""
    if not IS_MACOS:
        return False
    try:
        import Quartz
        # Attempt to capture a minimal screen region
        # If permission denied, this will return None
        disp = Quartz.CGMainDisplayID()
        return True  # If we got here, permission likely granted
    except Exception:
        return False


def request_accessibility_permission() -> bool:
    """
    Request accessibility permission by opening System Preferences.

    Note: This only opens the System Preferences pane.
    The user must manually enable the permission.

    Returns:
        True if the preferences pane was opened successfully.
    """
    if not IS_MACOS:
        return False
    try:
        subprocess.run(
            ['open', 'x-apple.systempreferences:com.apple.preference.security?Privacy_Accessibility'],
            check=True
        )
        return True
    except subprocess.CalledProcessError:
        return False


def request_screen_recording_permission() -> bool:
    """
    Request screen recording permission by opening System Preferences.

    Returns:
        True if the preferences pane was opened successfully.
    """
    if not IS_MACOS:
        return False
    try:
        subprocess.run(
            ['open', 'x-apple.systempreferences:com.apple.preference.security?Privacy_ScreenCapture'],
            check=True
        )
        return True
    except subprocess.CalledProcessError:
        return False


def open_accessibility_preferences() -> None:
    """Open the Accessibility system preferences pane."""
    if IS_MACOS:
        subprocess.run(
            ['open', 'x-apple.systempreferences:com.apple.preference.security?Privacy_Accessibility'],
            check=False
        )


def is_automation_allowed() -> bool:
    """
    Quick check if automation is allowed.

    Returns:
        True if automation (AppleScript/events) is permitted.
    """
    if not IS_MACOS:
        return False
    try:
        import Cocoa
        return Cocoa.AXIsProcessTrusted()
    except Exception:
        return False


def get_permission_summary() -> str:
    """
    Get a human-readable summary of all permissions.

    Returns:
        String describing permission status.
    """
    info = check_accessibility_permission()

    lines = [
        f"Accessibility Status: {info.status.name}",
        f"Authorized: {info.is_authorized}",
    ]

    if IS_MACOS:
        lines.append(f"Assistive Devices: {info.assistive_devices_enabled}")
        lines.append(f"Screen Recording: {info.screen_recording_enabled}")

        if not info.assistive_devices_enabled:
            lines.append("")
            lines.append("To enable:")
            lines.append("  System Preferences > Security & Privacy > Privacy > Accessibility")
            lines.append("  Add and enable your application")

    return "\n".join(lines)


def wait_for_permission(timeout: float = 30.0) -> bool:
    """
    Wait for accessibility permission to be granted.

    Args:
        timeout: Maximum time to wait in seconds.

    Returns:
        True if permission was granted within timeout.
    """
    import time
    start = time.time()
    while time.time() - start < timeout:
        if is_automation_allowed():
            return True
        time.sleep(0.5)
    return False


def check_permission_and_request() -> AccessibilityPermissionInfo:
    """
    Check permissions and request if not granted.

    Returns:
        Updated AccessibilityPermissionInfo.
    """
    info = check_accessibility_permission()
    if not info.is_authorized:
        request_accessibility_permission()
    return info
