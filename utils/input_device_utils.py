"""
Input device management utilities for automation.

Provides mouse and keyboard device detection, profiling,
and configuration for macOS automation workflows.
"""

from __future__ import annotations

import subprocess
import time
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass
from enum import Enum


class DeviceType(Enum):
    """Input device types."""
    MOUSE = "mouse"
    KEYBOARD = "keyboard"
    TRACKPAD = "trackpad"
    TABLET = "tablet"
    UNKNOWN = "unknown"


@dataclass
class InputDevice:
    """Input device information."""
    device_id: int
    name: str
    type: DeviceType
    is_connected: bool
    is_enabled: bool
    vendor_id: Optional[int] = None
    product_id: Optional[int] = None


def list_input_devices() -> List[InputDevice]:
    """
    List all connected input devices.
    
    Returns:
        List of InputDevice for each connected device.
    """
    devices = []
    
    try:
        result = subprocess.run(
            ["system_profiler", "SPUSBDataType", "-json"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        import json
        data = json.loads(result.stdout)
        
        for item in data.get('SPUSBDataType', []):
            name = item.get('_name', '')
            if any(kw in name.lower() for kw in ['mouse', 'keyboard', 'trackpad', 'tablet']):
                devices.append(InputDevice(
                    device_id=0,
                    name=name,
                    type=DeviceType.UNKNOWN,
                    is_connected=True,
                    is_enabled=True
                ))
    except Exception:
        pass
    
    devices.extend(_get_hid_devices())
    return devices


def _get_hid_devices() -> List[InputDevice]:
    """Get HID devices via IOKit."""
    devices = []
    try:
        import subprocess
        result = subprocess.run(
            ["ioreg", "-r", "-c", "IOHIDSystem"],
            capture_output=True,
            text=True
        )
        
        for line in result.stdout.split('\n'):
            if 'Mouse' in line or 'Trackpad' in line:
                devices.append(InputDevice(
                    device_id=0,
                    name=line.strip(),
                    type=DeviceType.MOUSE if 'Mouse' in line else DeviceType.TRACKPAD,
                    is_connected=True,
                    is_enabled=True
                ))
    except Exception:
        pass
    return devices


def get_mouse_location() -> Tuple[int, int]:
    """
    Get current mouse cursor position.
    
    Returns:
        Tuple of (x, y) screen coordinates.
    """
    try:
        import Quartz
        loc = Quartz.NSEvent.mouseLocation()
        return int(loc.x), int(loc.y)
    except Exception:
        pass
    return (0, 0)


def set_mouse_location(x: int, y: int) -> bool:
    """
    Set mouse cursor position.
    
    Args:
        x: Target X coordinate.
        y: Target Y coordinate.
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        import Quartz
        from Quartz import CGEvent, CGEventCreateMouseEvent
        
        move = CGEventCreateMouseEvent(None, Quartz.kCGEventMouseMoved, (x, y), Quartz.kCGMouseButtonLeft)
        CGEvent.setIntegerValueField(move, Quartz.kCGMouseEventSubtype, 0)
        CGEvent.post(Quartz.kCGHIDEventTap, move)
        return True
    except Exception:
        return False


def is_mouse_button_pressed(button: int = 0) -> bool:
    """
    Check if mouse button is currently pressed.
    
    Args:
        button: Button number (0=left, 1=right, 2=middle).
        
    Returns:
        True if pressed, False otherwise.
    """
    try:
        import Quartz
        flags = Quartz.NSEvent.pressedMouseButtons()
        return bool(flags & (1 << button))
    except Exception:
        return False


def get_keyboard_layout() -> str:
    """
    Get current keyboard layout name.
    
    Returns:
        Keyboard layout name (e.g., 'com.apple.keylayout.US').
    """
    try:
        script = '''
        tell application "System Events"
            return current keyboard's text input source
        end tell
        '''
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True
        )
        return result.stdout.strip()
    except Exception:
        return ""


def list_keyboard_layouts() -> List[str]:
    """
    List all available keyboard layouts.
    
    Returns:
        List of keyboard layout identifiers.
    """
    layouts = []
    try:
        result = subprocess.run(
            ["ls", "/Library/Keyboard\\ Drives/System"),
            capture_output=True,
            text=True
        )
        for line in result.stdout.split('\n'):
            if line.strip().endswith('.keylayout'):
                layouts.append(line.strip())
    except Exception:
        pass
    
    try:
        import os
        user_layouts = os.path.expanduser("~/Library/Keyboard Drives/System")
        if os.path.exists(user_layouts):
            for f in os.listdir(user_layouts):
                if f.endswith('.keylayout'):
                    layouts.append(f)
    except Exception:
        pass
    
    return layouts


@dataclass
class InputProfile:
    """Input device profile settings."""
    name: str
    mouse_speed: float
    keyboard_repeat_delay: float
    keyboard_repeat_rate: float
    trackpad_speed: float
    double_click_interval: float


def get_current_input_profile() -> Optional[InputProfile]:
    """
    Get current input profile from system settings.
    
    Returns:
        InputProfile with current settings.
    """
    try:
        result = subprocess.run(
            ["defaults", "read", "NSGlobalDomain", "MouseDriverSpeed"],
            capture_output=True,
            text=True
        )
        mouse_speed = float(result.stdout.strip()) if result.returncode == 0 else 1.0
    except Exception:
        mouse_speed = 1.0
    
    return InputProfile(
        name="current",
        mouse_speed=mouse_speed,
        keyboard_repeat_delay=0.025,
        keyboard_repeat_rate=0.05,
        trackpad_speed=1.0,
        double_click_interval=0.5
    )


def create_input_profile(name: str,
                        mouse_speed: float = 1.0,
                        keyboard_repeat_delay: float = 0.025,
                        keyboard_repeat_rate: float = 0.05,
                        trackpad_speed: float = 1.0,
                        double_click_interval: float = 0.5) -> InputProfile:
    """
    Create an input profile with specified settings.
    
    Args:
        name: Profile name.
        mouse_speed: Mouse speed multiplier.
        keyboard_repeat_delay: Key repeat initial delay.
        keyboard_repeat_rate: Key repeat rate.
        trackpad_speed: Trackpad speed multiplier.
        double_click_interval: Double click interval in seconds.
        
    Returns:
        New InputProfile.
    """
    return InputProfile(
        name=name,
        mouse_speed=mouse_speed,
        keyboard_repeat_delay=keyboard_repeat_delay,
        keyboard_repeat_rate=keyboard_repeat_rate,
        trackpad_speed=trackpad_speed,
        double_click_interval=double_click_interval
    )


def apply_input_profile(profile: InputProfile) -> bool:
    """
    Apply input profile settings to system.
    
    Args:
        profile: InputProfile to apply.
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        subprocess.run(
            ["defaults", "write", "NSGlobalDomain", "MouseDriverSpeed", str(profile.mouse_speed)],
            capture_output=True
        )
        return True
    except Exception:
        return False
