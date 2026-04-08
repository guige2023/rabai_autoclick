"""
Hotkey mapping utilities for keyboard shortcut management.

Provides hotkey parsing, registration, and execution
for automation workflows.
"""

from __future__ import annotations

from typing import Optional, Dict, List, Callable, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
import Quartz
from Quartz import CGEventCreateKeyboardEvent, CGEventPost


class Modifier(Enum):
    """Keyboard modifiers."""
    CMD = "cmd"
    COMMAND = "command"
    SHIFT = "shift"
    CTRL = "control"
    CONTROL = "control"
    ALT = "alt"
    OPTION = "option"
    FN = "fn"


@dataclass
class HotkeySpec:
    """Hotkey specification."""
    key: str
    modifiers: List[Modifier] = field(default_factory=list)
    description: str = ""
    
    @property
    def display_string(self) -> str:
        parts = []
        for mod in self.modifiers:
            name = mod.value.upper()
            if name == "CMD":
                name = "⌘"
            elif name == "SHIFT":
                name = "⇧"
            elif name == "CTRL":
                name = "^"
            elif name == "ALT":
                name = "⌥"
            parts.append(name)
        parts.append(self.key.upper())
        return "+".join(parts)


@dataclass
class HotkeyResult:
    """Result of hotkey execution."""
    success: bool
    message: str
    duration: float


MODIFIER_CODES: Dict[Modifier, int] = {
    Modifier.CMD: Quartz.kCGEventFlagMaskCommand,
    Modifier.COMMAND: Quartz.kCGEventFlagMaskCommand,
    Modifier.SHIFT: Quartz.kCGEventFlagMaskShift,
    Modifier.CTRL: Quartz.kCGEventFlagMaskControl,
    Modifier.CONTROL: Quartz.kCGEventFlagMaskControl,
    Modifier.ALT: Quartz.kCGEventFlagMaskAlternate,
    Modifier.OPTION: Quartz.kCGEventFlagMaskAlternate,
    Modifier.FN: Quartz.kCGEventFlagMaskSecondaryFn,
}


CHAR_TO_KEYCODE: Dict[str, int] = {
    'a': 0, 's': 1, 'd': 2, 'f': 3, 'h': 4, 'g': 5, 'z': 6,
    'x': 7, 'c': 8, 'v': 9, 'b': 11, 'q': 12, 'w': 13, 'e': 14,
    'r': 15, 'y': 16, 't': 17, '1': 18, '2': 19, '3': 20,
    '4': 21, '6': 22, '5': 23, '9': 25, '7': 26, '8': 28,
    '0': 29, 'o': 31, 'u': 32, 'i': 34, 'p': 35, 'l': 37,
    'j': 38, 'k': 40, 'n': 45, 'm': 46,
}


KEYCODE_TO_NAME: Dict[int, str] = {v: k for k, v in CHAR_TO_KEYCODE.items()}


def parse_hotkey_string(hotkey_str: str) -> Optional[HotkeySpec]:
    """
    Parse hotkey string like "cmd+shift+s" or "⌘⇧S".
    
    Args:
        hotkey_str: Hotkey string.
        
    Returns:
        HotkeySpec or None.
    """
    parts = hotkey_str.lower().replace(' ', '').split('+')
    
    if not parts:
        return None
    
    modifiers = []
    key = None
    
    modifier_map = {
        'cmd': Modifier.CMD, 'command': Modifier.COMMAND,
        'shift': Modifier.SHIFT, 'ctrl': Modifier.CONTROL, 'control': Modifier.CONTROL,
        'alt': Modifier.ALT, 'option': Modifier.OPTION, 'fn': Modifier.FN,
        '⌘': Modifier.CMD, '⇧': Modifier.SHIFT, '^': Modifier.CONTROL, '⌥': Modifier.ALT,
    }
    
    for part in parts:
        if part in modifier_map:
            modifiers.append(modifier_map[part])
        elif len(part) == 1:
            key = part.lower()
    
    if not key:
        return None
    
    return HotkeySpec(key=key, modifiers=modifiers)


def get_keycode(key: str) -> Optional[int]:
    """
    Get keycode for key.
    
    Args:
        key: Key character.
        
    Returns:
        Keycode or None.
    """
    return CHAR_TO_KEYCODE.get(key.lower())


def get_modifier_mask(modifiers: List[Modifier]) -> int:
    """
    Get CGEvent flag mask for modifiers.
    
    Args:
        modifiers: List of modifiers.
        
    Returns:
        Combined flag mask.
    """
    mask = 0
    for mod in modifiers:
        mask |= MODIFIER_CODES.get(mod, 0)
    return mask


class HotkeyExecutor:
    """Executes hotkey combinations."""
    
    def __init__(self):
        self._handlers: Dict[str, Callable] = {}
    
    def register(self, spec: HotkeySpec, handler: Callable[[], None]) -> None:
        """
        Register hotkey handler.
        
        Args:
            spec: HotkeySpec.
            handler: Callback function.
        """
        key = spec.display_string
        self._handlers[key] = handler
    
    def execute(self, spec: HotkeySpec) -> HotkeyResult:
        """
        Execute hotkey combination.
        
        Args:
            spec: HotkeySpec to execute.
            
        Returns:
            HotkeyResult.
        """
        import time
        start = time.time()
        
        try:
            keycode = get_keycode(spec.key)
            if keycode is None:
                return HotkeyResult(
                    success=False,
                    message=f"Unknown key: {spec.key}",
                    duration=time.time() - start
                )
            
            mask = get_modifier_mask(spec.modifiers)
            
            down = CGEventCreateKeyboardEvent(None, keycode, True)
            if mask:
                down.setIntegerValueField(
                    Quartz.kCGKeyboardEventKeyboardFlagsSubtype,
                    mask
                )
            CGEventPost(Quartz.kCGHIDEventTap, down)
            
            up = CGEventCreateKeyboardEvent(None, keycode, False)
            if mask:
                up.setIntegerValueField(
                    Quartz.kCGKeyboardEventKeyboardFlagsSubtype,
                    mask
                )
            CGEventPost(Quartz.kCGHIDEventTap, up)
            
            return HotkeyResult(
                success=True,
                message=f"Executed: {spec.display_string}",
                duration=time.time() - start
            )
        except Exception as e:
            return HotkeyResult(
                success=False,
                message=f"Execution failed: {e}",
                duration=time.time() - start
            )
    
    def execute_string(self, hotkey_str: str) -> HotkeyResult:
        """
        Execute hotkey from string.
        
        Args:
            hotkey_str: Hotkey string.
            
        Returns:
            HotkeyResult.
        """
        spec = parse_hotkey_string(hotkey_str)
        if not spec:
            return HotkeyResult(
                success=False,
                message=f"Failed to parse: {hotkey_str}",
                duration=0
            )
        return self.execute(spec)
    
    def unregister(self, spec: HotkeySpec) -> bool:
        """
        Unregister hotkey handler.
        
        Args:
            spec: HotkeySpec to unregister.
            
        Returns:
            True if unregistered, False if not found.
        """
        key = spec.display_string
        if key in self._handlers:
            del self._handlers[key]
            return True
        return False


def press_hotkey(key: str, *modifiers: Modifier) -> HotkeyResult:
    """
    Press hotkey combination.
    
    Args:
        key: Key character.
        *modifiers: Modifier keys.
        
    Returns:
        HotkeyResult.
    """
    spec = HotkeySpec(key=key.lower(), modifiers=list(modifiers))
    executor = HotkeyExecutor()
    return executor.execute(spec)


def press_standard_hotkey(action: str) -> HotkeyResult:
    """
    Press standard macOS hotkey.
    
    Args:
        action: Action name (copy, paste, undo, etc.).
        
    Returns:
        HotkeyResult.
    """
    standard_hotkeys = {
        'copy': HotkeySpec(key='c', modifiers=[Modifier.CMD]),
        'paste': HotkeySpec(key='v', modifiers=[Modifier.CMD]),
        'undo': HotkeySpec(key='z', modifiers=[Modifier.CMD]),
        'redo': HotkeySpec(key='z', modifiers=[Modifier.CMD, Modifier.SHIFT]),
        'select_all': HotkeySpec(key='a', modifiers=[Modifier.CMD]),
        'save': HotkeySpec(key='s', modifiers=[Modifier.CMD]),
        'quit': HotkeySpec(key='q', modifiers=[Modifier.CMD]),
        'new': HotkeySpec(key='n', modifiers=[Modifier.CMD]),
        'open': HotkeySpec(key='o', modifiers=[Modifier.CMD]),
        'close': HotkeySpec(key='w', modifiers=[Modifier.CMD]),
        'find': HotkeySpec(key='f', modifiers=[Modifier.CMD]),
        'tab_next': HotkeySpec(key='tab', modifiers=[Modifier.CMD]),
        'tab_prev': HotkeySpec(key='tab', modifiers=[Modifier.CMD, Modifier.SHIFT]),
    }
    
    spec = standard_hotkeys.get(action.lower())
    if not spec:
        return HotkeyResult(
            success=False,
            message=f"Unknown action: {action}",
            duration=0
        )
    
    executor = HotkeyExecutor()
    return executor.execute(spec)
