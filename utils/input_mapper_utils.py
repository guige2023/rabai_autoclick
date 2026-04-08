"""
Input mapper utilities for key and mouse mapping.

Provides key code mapping, modifier handling, and
input event generation for automation.
"""

from __future__ import annotations

import Quartz
from Quartz import CGEvent, CGEventCreateKeyboardEvent, CGEventCreateMouseEvent
from typing import Optional, Dict, Tuple, List
from dataclasses import dataclass
from enum import Enum


class KeyCode(Enum):
    """Virtual key codes."""
    RETURN = 36
    TAB = 48
    SPACE = 49
    DELETE = 51
    ESCAPE = 53
    COMMAND = 55
    SHIFT = 56
    CAPS_LOCK = 57
    OPTION = 58
    CONTROL = 59
    RIGHT_COMMAND = 54
    RIGHT_SHIFT = 60
    RIGHT_OPTION = 61
    RIGHT_CONTROL = 62
    VOLUME_UP = 72
    VOLUME_DOWN = 73
    VOLUME_MUTE = 74
    F1 = 122
    F2 = 120
    F3 = 99
    F4 = 118
    F5 = 96
    F6 = 97
    F7 = 98
    F8 = 100
    F9 = 101
    F10 = 109
    F11 = 103
    F12 = 111


@dataclass
class KeyEvent:
    """Key event specification."""
    key_code: int
    key: Optional[str] = None
    modifiers: List[str] = None
    key_down: bool = True
    
    def __post_init__(self):
        if self.modifiers is None:
            self.modifiers = []


@dataclass
class MouseEvent:
    """Mouse event specification."""
    x: int
    y: int
    button: int = 0
    click_count: int = 1
    key_mask: int = 0


MODIFIER_MAP: Dict[str, int] = {
    'command': Quartz.kCGEventFlagMaskCommand,
    'cmd': Quartz.kCGEventFlagMaskCommand,
    'shift': Quartz.kCGEventFlagMaskShift,
    'control': Quartz.kCGEventFlagMaskControl,
    'ctrl': Quartz.kCGEventFlagMaskControl,
    'option': Quartz.kCGEventFlagMaskAlternate,
    'alt': Quartz.kCGEventFlagMaskAlternate,
}


KEY_NAME_TO_CODE: Dict[str, int] = {
    'return': 36, 'tab': 48, 'space': 49, 'delete': 51,
    'escape': 53, 'esc': 53, 'cmd': 55, 'command': 55,
    'shift': 56, 'caps': 57, 'capslock': 57, 'option': 58,
    'ctrl': 59, 'control': 59, 'volumeup': 72, 'volumedown': 73,
    'volumemute': 74, 'f1': 122, 'f2': 120, 'f3': 99, 'f4': 118,
    'f5': 96, 'f6': 97, 'f7': 98, 'f8': 100, 'f9': 101,
    'f10': 109, 'f11': 103, 'f12': 111,
    'rightcmd': 54, 'rightshift': 60, 'rightoption': 61, 'rightcontrol': 62,
}

CHAR_TO_KEY_CODE: Dict[str, int] = {
    'a': 0, 's': 1, 'd': 2, 'f': 3, 'h': 4, 'g': 5, 'z': 6,
    'x': 7, 'c': 8, 'v': 9, 'b': 11, 'q': 12, 'w': 13, 'e': 14,
    'r': 15, 'y': 16, 't': 17, '1': 18, '2': 19, '3': 20,
    '4': 21, '6': 22, '5': 23, '9': 25, '7': 26, '8': 28,
    '0': 29, 'o': 31, 'u': 32, 'i': 34, 'p': 35, 'l': 37,
    'j': 38, 'k': 40, 'n': 45, 'm': 46,
}


class InputMapper:
    """Maps and generates input events."""
    
    def __init__(self):
        self._key_state: Dict[int, bool] = {}
    
    def key_name_to_code(self, key_name: str) -> Optional[int]:
        """
        Convert key name to key code.
        
        Args:
            key_name: Key name (e.g., 'return', 'space', 'a').
            
        Returns:
            Key code or None.
        """
        key_lower = key_name.lower()
        
        if key_lower in KEY_NAME_TO_CODE:
            return KEY_NAME_TO_CODE[key_lower]
        
        if key_lower in CHAR_TO_KEY_CODE:
            return CHAR_TO_KEY_CODE[key_lower]
        
        if len(key_lower) == 1 and key_lower.isalpha():
            return CHAR_TO_KEY_CODE.get(key_lower)
        
        return None
    
    def parse_modifiers(self, modifier_str: str) -> int:
        """
        Parse modifier string to CGEvent flag mask.
        
        Args:
            modifier_str: Modifier string (e.g., 'cmd+shift').
            
        Returns:
            CGEvent flag mask.
        """
        mask = 0
        parts = modifier_str.replace('-', '+').split('+')
        
        for part in parts:
            part = part.strip().lower()
            if part in MODIFIER_MAP:
                mask |= MODIFIER_MAP[part]
        
        return mask
    
    def create_key_event(self, key_code: int, key_down: bool = True,
                        modifiers: int = 0) -> CGEvent:
        """
        Create keyboard event.
        
        Args:
            key_code: Virtual key code.
            key_down: True for key down, False for key up.
            modifiers: Modifier flags.
            
        Returns:
            CGEvent for keyboard.
        """
        event = CGEventCreateKeyboardEvent(None, key_code, key_down)
        
        if modifiers:
            flags = event.getIntegerValueField(Quartz.kCGKeyboardEventKeyboardFlagsSubtype)
            event.setIntegerValueField(Quartz.kCGKeyboardEventKeyboardFlagsSubtype,
                                       flags | modifiers)
        
        return event
    
    def create_mouse_event(self, x: int, y: int,
                          button: int = 0,
                          mouse_down: bool = True) -> CGEvent:
        """
        Create mouse event.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            button: Button number (0=left, 1=right, 2=middle).
            mouse_down: True for button down, False for button up.
            
        Returns:
            CGEvent for mouse.
        """
        button_type = {
            0: Quartz.kCGEventLeftMouseDown if mouse_down else Quartz.kCGEventLeftMouseUp,
            1: Quartz.kCGEventRightMouseDown if mouse_down else Quartz.kCGEventRightMouseUp,
            2: Quartz.kCGEventOtherMouseDown if mouse_down else Quartz.kCGEventOtherMouseUp,
        }.get(button, Quartz.kCGEventLeftMouseDown)
        
        event = CGEventCreateMouseEvent(
            None, button_type, (x, y),
            Quartz.kCGMouseButton(button) if button < 3 else Quartz.kCGMouseButtonLeft
        )
        
        return event
    
    def post_key(self, key_code: int, key_down: bool = True,
                modifiers: int = 0) -> None:
        """
        Post keyboard event to HID event tap.
        
        Args:
            key_code: Virtual key code.
            key_down: True for key down, False for key up.
            modifiers: Modifier flags.
        """
        event = self.create_key_event(key_code, key_down, modifiers)
        CGEvent.post(Quartz.kCGHIDEventTap, event)
    
    def post_mouse(self, x: int, y: int,
                   button: int = 0, mouse_down: bool = True) -> None:
        """
        Post mouse event to HID event tap.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            button: Button number.
            mouse_down: True for button down, False for button up.
        """
        event = self.create_mouse_event(x, y, button, mouse_down)
        CGEvent.post(Quartz.kCGHIDEventTap, event)
    
    def type_string(self, text: str, delay: float = 0.01) -> None:
        """
        Type a string of characters.
        
        Args:
            text: Text to type.
            delay: Delay between keystrokes.
        """
        import time
        
        for char in text:
            char_lower = char.lower()
            
            if char_lower in CHAR_TO_KEY_CODE:
                code = CHAR_TO_KEY_CODE[char_lower]
                is_shift = char.isupper() or char in '!@#$%^&*()_+{}|:"<>?'
                
                modifiers = Quartz.kCGEventFlagMaskShift if is_shift else 0
                
                self.post_key(code, True, modifiers)
                self.post_key(code, False, modifiers)
                
                time.sleep(delay)
    
    def press_hotkey(self, *keys: str) -> None:
        """
        Press a hotkey combination.
        
        Args:
            *keys: Keys in combination (e.g., 'cmd', 'shift', 's').
        """
        modifiers = 0
        key_code = None
        
        for key in keys[:-1]:
            key_lower = key.lower()
            if key_lower in MODIFIER_MAP:
                modifiers |= MODIFIER_MAP[key_lower]
        
        last_key = keys[-1].lower()
        key_code = self.key_name_to_code(last_key)
        
        if key_code is None and len(last_key) == 1:
            key_code = CHAR_TO_KEY_CODE.get(last_key)
        
        if key_code is not None:
            self.post_key(key_code, True, modifiers)
            self.post_key(key_code, False, modifiers)
    
    def click(self, x: int, y: int, button: int = 0) -> None:
        """
        Perform a mouse click.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            button: Button number.
        """
        self.post_mouse(x, y, button, True)
        self.post_mouse(x, y, button, False)
    
    def double_click(self, x: int, y: int, button: int = 0) -> None:
        """
        Perform a double click.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            button: Button number.
        """
        for _ in range(2):
            self.post_mouse(x, y, button, True)
            self.post_mouse(x, y, button, False)
    
    def right_click(self, x: int, y: int) -> None:
        """
        Perform a right click.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
        """
        self.click(x, y, button=1)
    
    def move_mouse(self, x: int, y: int) -> None:
        """
        Move mouse to position.
        
        Args:
            x: Target X.
            y: Target Y.
        """
        event = CGEventCreateMouseEvent(
            None, Quartz.kCGEventMouseMoved, (x, y), Quartz.kCGMouseButtonLeft
        )
        CGEvent.post(Quartz.kCGHIDEventTap, event)
    
    def drag(self, x1: int, y1: int, x2: int, y2: int,
            button: int = 0, steps: int = 10) -> None:
        """
        Drag from one position to another.
        
        Args:
            x1: Start X.
            y1: Start Y.
            x2: End X.
            y2: End Y.
            button: Mouse button.
            steps: Number of intermediate steps.
        """
        self.post_mouse(x1, y1, button, True)
        
        for i in range(steps + 1):
            t = i / steps
            x = int(x1 + (x2 - x1) * t)
            y = int(y1 + (y2 - y1) * t)
            self.move_mouse(x, y)
        
        self.post_mouse(x2, y2, button, False)
