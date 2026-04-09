"""
Device Control Utilities for UI Automation.

This module provides utilities for controlling input devices and
managing device state in automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional, Callable


class DeviceType(Enum):
    """Types of input devices."""
    MOUSE = auto()
    KEYBOARD = auto()
    TOUCHSCREEN = auto()
    TRACKPAD = auto()
    STYLUS = auto()


class DeviceState(Enum):
    """Device connection state."""
    CONNECTED = auto()
    DISCONNECTED = auto()
    SUSPENDED = auto()
    ERROR = auto()


@dataclass
class DeviceInfo:
    """
    Information about an input device.
    
    Attributes:
        device_id: Unique device identifier
        device_type: Type of device
        name: Device name
        state: Connection state
        capabilities: Supported capabilities
        position: Current position (for pointer devices)
    """
    device_id: str
    device_type: DeviceType
    name: str
    state: DeviceState = DeviceState.CONNECTED
    capabilities: list[str] = field(default_factory=list)
    position: tuple[int, int] = (0, 0)
    metadata: dict = field(default_factory=dict)


@dataclass
class MouseButton(Enum):
    """Mouse button identifiers."""
    LEFT = 1
    RIGHT = 2
    MIDDLE = 3
    BACK = 4
    FORWARD = 5


class MouseController:
    """
    Controls mouse input.
    
    Example:
        mouse = MouseController()
        mouse.move_to(100, 200)
        mouse.click(MouseButton.LEFT)
    """
    
    def __init__(self, device_id: Optional[str] = None):
        self.device_id = device_id or "default_mouse"
        self._position = (0, 0)
        self._button_state = {b: False for b in MouseButton}
    
    def move_to(self, x: int, y: int, duration: float = 0.0) -> None:
        """
        Move mouse to absolute position.
        
        Args:
            x: Target X coordinate
            y: Target Y coordinate
            duration: Movement duration (0 for instant)
        """
        if duration > 0:
            self._smooth_move(x, y, duration)
        else:
            self._position = (x, y)
    
    def _smooth_move(self, target_x: int, target_y: int, duration: float) -> None:
        """Perform smooth mouse movement."""
        start_x, start_y = self._position
        steps = max(10, int(duration * 60))  # 60 fps
        
        for i in range(steps + 1):
            t = i / steps
            x = int(start_x + (target_x - start_x) * t)
            y = int(start_y + (target_y - start_y) * t)
            self._position = (x, y)
            time.sleep(duration / steps)
    
    def move_relative(self, dx: int, dy: int) -> None:
        """Move mouse relative to current position."""
        self._position = (self._position[0] + dx, self._position[1] + dy)
    
    def click(
        self, 
        button: MouseButton = MouseButton.LEFT,
        x: Optional[int] = None,
        y: Optional[int] = None
    ) -> None:
        """
        Click a mouse button.
        
        Args:
            button: Button to click
            x: Optional X coordinate (move first if specified)
            y: Optional Y coordinate (move first if specified)
        """
        if x is not None and y is not None:
            self.move_to(x, y)
        
        self._button_down(button)
        time.sleep(0.05)
        self._button_up(button)
    
    def double_click(
        self,
        button: MouseButton = MouseButton.LEFT,
        x: Optional[int] = None,
        y: Optional[int] = None
    ) -> None:
        """Perform a double click."""
        self.click(button, x, y)
        time.sleep(0.05)
        self.click(button)
    
    def right_click(
        self,
        x: Optional[int] = None,
        y: Optional[int] = None
    ) -> None:
        """Perform a right click."""
        self.click(MouseButton.RIGHT, x, y)
    
    def _button_down(self, button: MouseButton) -> None:
        """Press a mouse button down."""
        self._button_state[button] = True
    
    def _button_up(self, button: MouseButton) -> None:
        """Release a mouse button."""
        self._button_state[button] = False
    
    def scroll(self, amount: int, x: Optional[int] = None, y: Optional[int] = None) -> None:
        """
        Scroll the mouse wheel.
        
        Args:
            amount: Scroll amount (positive = up, negative = down)
            x: Optional X coordinate to scroll at
            y: Optional Y coordinate to scroll at
        """
        if x is not None and y is not None:
            self.move_to(x, y)
    
    def drag(
        self,
        start_x: int,
        start_y: int,
        end_x: int,
        end_y: int,
        duration: float = 0.5
    ) -> None:
        """
        Perform a drag operation.
        
        Args:
            start_x: Starting X coordinate
            start_y: Starting Y coordinate
            end_x: Ending X coordinate
            end_y: Ending Y coordinate
            duration: Drag duration
        """
        self.move_to(start_x, start_y)
        self._button_down(MouseButton.LEFT)
        time.sleep(0.05)
        self.move_to(end_x, end_y, duration)
        self._button_up(MouseButton.LEFT)
    
    def get_position(self) -> tuple[int, int]:
        """Get current mouse position."""
        return self._position


class KeyboardController:
    """
    Controls keyboard input.
    
    Example:
        keyboard = KeyboardController()
        keyboard.type("Hello, World!")
        keyboard.press_key("ctrl+c")
    """
    
    def __init__(self, device_id: Optional[str] = None):
        self.device_id = device_id or "default_keyboard"
        self._modifier_state = {
            "ctrl": False,
            "alt": False,
            "shift": False,
            "meta": False
        }
    
    def type(self, text: str, interval: float = 0.0) -> None:
        """
        Type a string of text.
        
        Args:
            text: Text to type
            interval: Delay between keystrokes (0 for instant)
        """
        for char in text:
            self._type_char(char)
            if interval > 0:
                time.sleep(interval)
    
    def _type_char(self, char: str) -> None:
        """Type a single character."""
        pass  # Placeholder
    
    def press_key(self, key: str) -> None:
        """
        Press and release a key.
        
        Args:
            key: Key name (e.g., "enter", "ctrl+c", "shift+a")
        """
        modifiers, base_key = self._parse_key_combo(key)
        
        for mod in modifiers:
            self._modifier_state[mod] = True
        
        time.sleep(0.02)
        
        for mod in modifiers:
            self._modifier_state[mod] = False
    
    def _parse_key_combo(self, key: str) -> tuple[list[str], str]:
        """Parse a key combination string."""
        parts = key.lower().split('+')
        if len(parts) > 1:
            modifiers = parts[:-1]
            base_key = parts[-1]
            return modifiers, base_key
        return [], key.lower()
    
    def hold_key(self, key: str) -> None:
        """Hold a key down until release_key is called."""
        modifiers, base_key = self._parse_key_combo(key)
        for mod in modifiers:
            self._modifier_state[mod] = True
    
    def release_key(self, key: str) -> None:
        """Release a held key."""
        modifiers, base_key = self._parse_key_combo(key)
        for mod in modifiers:
            self._modifier_state[mod] = False
    
    def is_modifier_active(self, modifier: str) -> bool:
        """Check if a modifier key is currently active."""
        return self._modifier_state.get(modifier, False)


class DeviceManager:
    """
    Manages input devices.
    
    Example:
        manager = DeviceManager()
        devices = manager.list_devices()
        mouse = manager.get_mouse()
    """
    
    def __init__(self):
        self._devices: dict[str, DeviceInfo] = {}
        self._mouse: Optional[MouseController] = None
        self._keyboard: Optional[KeyboardController] = None
    
    def list_devices(self, device_type: Optional[DeviceType] = None) -> list[DeviceInfo]:
        """
        List all connected devices.
        
        Args:
            device_type: Optional filter by device type
            
        Returns:
            List of DeviceInfo objects
        """
        devices = list(self._devices.values())
        if device_type:
            devices = [d for d in devices if d.device_type == device_type]
        return devices
    
    def get_mouse(self) -> MouseController:
        """Get the primary mouse controller."""
        if self._mouse is None:
            self._mouse = MouseController()
        return self._mouse
    
    def get_keyboard(self) -> KeyboardController:
        """Get the primary keyboard controller."""
        if self._keyboard is None:
            self._keyboard = KeyboardController()
        return self._keyboard
    
    def register_device(self, device_info: DeviceInfo) -> None:
        """Register a device."""
        self._devices[device_info.device_id] = device_info
    
    def unregister_device(self, device_id: str) -> bool:
        """Unregister a device."""
        if device_id in self._devices:
            del self._devices[device_id]
            return True
        return False
