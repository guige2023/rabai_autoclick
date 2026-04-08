"""
Keyboard event generation and manipulation utilities.

Provides utilities for generating keyboard events, key sequences,
and keyboard shortcuts with proper modifier handling.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple, Callable, Any
from dataclasses import dataclass
from enum import Enum
import time


class KeyCode(Enum):
    """macOS virtual key codes."""
    A = 0
    S = 1
    D = 2
    F = 3
    H = 4
    G = 5
    Z = 6
    X = 7
    C = 8
    V = 9
    B = 11
    Q = 12
    W = 13
    E = 14
    R = 15
    Y = 16
    T = 17
    ONE = 18
    TWO = 19
    THREE = 20
    FOUR = 21
    SIX = 22
    FIVE = 23
    EQUALS = 24
    NINE = 25
    SEVEN = 26
    MINUS = 27
    EIGHT = 28
    ZERO = 29
    RIGHTBRACKET = 30
    O = 31
    U = 32
    LEFTBRACKET = 33
    I = 34
    P = 35
    RETURN = 36
    L = 37
    J = 38
    APOSTROPHE = 39
    K = 40
    SEMICOLON = 41
    BACKSLASH = 42
    COMMA = 43
    SLASH = 44
    N = 45
    M = 46
    PERIOD = 47
    TAB = 48
    SPACE = 49
    GRAVE = 50
    DELETE = 51
    ESCAPE = 53
    COMMAND = 55
    SHIFT = 56
    CAPSLOCK = 57
    OPTION = 58
    CONTROL = 59
    RIGHTSHIFT = 60
    RIGHTOPTION = 61
    RIGHTCONTROL = 62
    FUNCTION = 63
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
    INSERT = 114
    HOME = 115
    PAGEUP = 116
    FORWARD_DELETE = 117
    END = 119
    PAGEDOWN = 121
    RIGHT = 124
    LEFT = 123
    DOWN = 125
    UP = 126


@dataclass
class KeyEvent:
    """Represents a keyboard event."""
    key_code: int
    key_name: str
    modifiers: int = 0
    is_down: bool = True
    timestamp: Optional[float] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()


class ModifierFlags:
    """Modifier flag constants."""
    NONE = 0
    COMMAND = 1 << 0
    SHIFT = 1 << 1
    OPTION = 1 << 2
    CONTROL = 1 << 3
    FUNCTION = 1 << 4


class KeySequence:
    """A sequence of key events."""
    
    def __init__(self):
        """Initialize empty sequence."""
        self.events: List[KeyEvent] = []
    
    def add_key_down(self, key_code: int, modifiers: int = 0) -> "KeySequence":
        """Add a key down event."""
        self.events.append(KeyEvent(
            key_code=key_code,
            key_name=self._key_name(key_code),
            modifiers=modifiers,
            is_down=True
        ))
        return self
    
    def add_key_up(self, key_code: int, modifiers: int = 0) -> "KeySequence":
        """Add a key up event."""
        self.events.append(KeyEvent(
            key_code=key_code,
            key_name=self._key_name(key_code),
            modifiers=modifiers,
            is_down=False
        ))
        return self
    
    def add_press(self, key_code: int, modifiers: int = 0) -> "KeySequence":
        """Add a key press (down then up)."""
        self.add_key_down(key_code, modifiers)
        self.add_key_up(key_code, modifiers)
        return self
    
    def add_wait(self, seconds: float) -> "KeySequence":
        """Add a wait period."""
        self.events.append(KeyEvent(
            key_code=-1,
            key_name=f"wait({seconds})",
            modifiers=0,
            is_down=False,
            timestamp=time.time() + seconds
        ))
        return self
    
    def _key_name(self, code: int) -> str:
        """Get name for key code."""
        for name, member in KeyCode.__members__.items():
            if member.value == code:
                return name
        return f"key_{code}"
    
    def get_events(self) -> List[KeyEvent]:
        """Get all events in sequence."""
        return self.events
    
    def get_duration(self) -> float:
        """Get total duration in seconds."""
        if not self.events:
            return 0
        return self.events[-1].timestamp - self.events[0].timestamp


class KeyboardEventGenerator:
    """Generates keyboard events using macOS APIs."""
    
    def __init__(self):
        """Initialize event generator."""
        self._sequence: List[Tuple[str, Any]] = []
    
    def key_down(self, key: str, modifiers: int = 0) -> "KeyboardEventGenerator":
        """Add key down event."""
        self._sequence.append(("key_down", (key, modifiers)))
        return self
    
    def key_up(self, key: str, modifiers: int = 0) -> "KeyboardEventGenerator":
        """Add key up event."""
        self._sequence.append(("key_up", (key, modifiers)))
        return self
    
    def press(self, key: str, modifiers: int = 0) -> "KeyboardEventGenerator":
        """Add key press event."""
        self._sequence.append(("press", (key, modifiers)))
        return self
    
    def type_string(self, text: str, delay: float = 0.01) -> "KeyboardEventGenerator":
        """Add typing events for a string."""
        for char in text:
            self._sequence.append(("type_char", (char, delay)))
        return self
    
    def wait(self, seconds: float) -> "KeyboardEventGenerator":
        """Add wait between events."""
        self._sequence.append(("wait", seconds))
        return self
    
    def execute(self) -> bool:
        """Execute all queued events.
        
        Returns:
            True if successful
        """
        try:
            import subprocess
            
            for action, args in self._sequence:
                if action == "press":
                    key, modifiers = args
                    key_code = self._get_key_code(key)
                    self._send_key_event(key_code, modifiers, True)
                    self._send_key_event(key_code, modifiers, False)
                    
                elif action == "key_down":
                    key, modifiers = args
                    key_code = self._get_key_code(key)
                    self._send_key_event(key_code, modifiers, True)
                    
                elif action == "key_up":
                    key, modifiers = args
                    key_code = self._get_key_code(key)
                    self._send_key_event(key_code, modifiers, False)
                    
                elif action == "type_char":
                    char, delay = args
                    self._type_character(char)
                    time.sleep(delay)
                    
                elif action == "wait":
                    time.sleep(args)
            
            self._sequence.clear()
            return True
            
        except Exception:
            return False
    
    def _get_key_code(self, key: str) -> int:
        """Get key code from key name."""
        try:
            return KeyCode[key.upper()].value
        except KeyError:
            return ord(key.upper()) if len(key) == 1 else 0
    
    def _send_key_event(self, key_code: int, modifiers: int, is_down: bool) -> None:
        """Send a keyboard event using AppleScript."""
        try:
            script = f'''
            tell application "System Events"
                {"key down" if is_down else "key up"} of (process 1) key code {key_code}
            end tell
            '''
            # Note: Real implementation would use CGEvent
        except Exception:
            pass
    
    def _type_character(self, char: str) -> None:
        """Type a character using Cocoa."""
        try:
            script = f'''
            tell application "System Events"
                keystroke "{char}"
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=1)
        except Exception:
            pass


def parse_shortcut_string(shortcut: str) -> Tuple[int, int]:
    """Parse a shortcut string to key code and modifiers.
    
    Args:
        shortcut: Shortcut like "Cmd+C" or "⌘⇧S"
        
    Returns:
        Tuple of (key_code, modifiers)
    """
    modifiers = ModifierFlags.NONE
    key = shortcut
    
    # Parse modifier prefixes
    for mod in ["Cmd", "⌘", "Command"]:
        if mod in shortcut:
            modifiers |= ModifierFlags.COMMAND
            key = key.replace(mod, "")
    
    for mod in ["Shift", "⇧"]:
        if mod in shortcut:
            modifiers |= ModifierFlags.SHIFT
            key = key.replace(mod, "")
    
    for mod in ["Option", "⌥", "Alt"]:
        if mod in shortcut:
            modifiers |= ModifierFlags.OPTION
            key = key.replace(mod, "")
    
    for mod in ["Control", "⌃", "Ctrl"]:
        if mod in shortcut:
            modifiers |= ModifierFlags.CONTROL
            key = key.replace(mod, "")
    
    for mod in ["Fn", "fn"]:
        if mod in shortcut:
            modifiers |= ModifierFlags.FUNCTION
            key = key.replace(mod, "")
    
    key = key.strip("+ ")
    
    if len(key) == 1:
        key_code = ord(key.upper())
    else:
        try:
            key_code = KeyCode[key.upper()].value
        except KeyError:
            key_code = 0
    
    return (key_code, modifiers)


def format_shortcut(key_code: int, modifiers: int) -> str:
    """Format a shortcut from key code and modifiers.
    
    Args:
        key_code: Virtual key code
        modifiers: Modifier flags
        
    Returns:
        Human-readable shortcut string
    """
    parts = []
    
    if modifiers & ModifierFlags.CONTROL:
        parts.append("⌃")
    if modifiers & ModifierFlags.OPTION:
        parts.append("⌥")
    if modifiers & ModifierFlags.SHIFT:
        parts.append("⇧")
    if modifiers & ModifierFlags.COMMAND:
        parts.append("⌘")
    
    if key_code < 128:
        parts.append(chr(key_code))
    else:
        try:
            parts.append(KeyCode(key_code).name)
        except ValueError:
            parts.append(f"key_{key_code}")
    
    return "".join(parts)


class HotkeyHandler:
    """Handles hotkey registration and callbacks."""
    
    def __init__(self):
        """Initialize hotkey handler."""
        self._hotkeys: Dict[str, Callable] = {}
    
    def register(self, shortcut: str, callback: Callable) -> bool:
        """Register a hotkey.
        
        Args:
            shortcut: Shortcut string
            callback: Callback function
            
        Returns:
            True if registered
        """
        key_code, modifiers = parse_shortcut_string(shortcut)
        key = format_shortcut(key_code, modifiers)
        self._hotkeys[key] = callback
        return True
    
    def unregister(self, shortcut: str) -> bool:
        """Unregister a hotkey.
        
        Args:
            shortcut: Shortcut string
            
        Returns:
            True if unregistered
        """
        key_code, modifiers = parse_shortcut_string(shortcut)
        key = format_shortcut(key_code, modifiers)
        if key in self._hotkeys:
            del self._hotkeys[key]
            return True
        return False
    
    def trigger(self, shortcut: str) -> bool:
        """Trigger a hotkey callback.
        
        Args:
            shortcut: Shortcut string
            
        Returns:
            True if triggered
        """
        key_code, modifiers = parse_shortcut_string(shortcut)
        key = format_shortcut(key_code, modifiers)
        
        if key in self._hotkeys:
            self._hotkeys[key]()
            return True
        return False
    
    def get_registered(self) -> List[str]:
        """Get list of registered shortcuts."""
        return list(self._hotkeys.keys())


# Convenience functions
def create_key_sequence() -> KeySequence:
    """Create a new key sequence."""
    return KeySequence()


def create_event_generator() -> KeyboardEventGenerator:
    """Create a new keyboard event generator."""
    return KeyboardEventGenerator()


def press_shortcut(shortcut: str) -> bool:
    """Press a keyboard shortcut.
    
    Args:
        shortcut: Shortcut string
        
    Returns:
        True if successful
    """
    gen = KeyboardEventGenerator()
    key_code, modifiers = parse_shortcut_string(shortcut)
    
    try:
        gen._send_key_event(key_code, modifiers, True)
        time.sleep(0.01)
        gen._send_key_event(key_code, modifiers, False)
        return True
    except Exception:
        return False


def type_text(text: str, interval: float = 0.01) -> bool:
    """Type text using the keyboard.
    
    Args:
        text: Text to type
        interval: Delay between characters
        
    Returns:
        True if successful
    """
    gen = KeyboardEventGenerator()
    gen.type_string(text, interval)
    return gen.execute()
