"""Keyboard input utilities for simulating keyboard events.

This module provides utilities for simulating keyboard input including
key presses, releases, typing text, and handling modifier keys,
useful for UI automation and testing workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, List, Set, Callable
import time


class KeyModifier(Enum):
    """Keyboard modifier keys."""
    NONE = 0
    SHIFT = 1
    CTRL = 2
    ALT = 4
    META = 8
    CMD = 8  # Alias for META (Mac)
    SUPER = 8


@dataclass
class KeyEvent:
    """A keyboard event."""
    key: str
    code: Optional[str] = None
    modifiers: Set[KeyModifier] = None
    timestamp: float = 0.0
    
    def __post_init__(self):
        if self.modifiers is None:
            self.modifiers = set()


@dataclass
class KeyboardConfig:
    """Configuration for keyboard simulation."""
    delay_ms: float = 10.0
    hold_duration_ms: float = 50.0
    use_key_code: bool = False


@dataclass
class KeyboardResult:
    """Result of keyboard operation."""
    success: bool
    keys_pressed: int
    duration_ms: float
    error_message: Optional[str] = None


def press_key(
    key: str,
    modifiers: Optional[Set[KeyModifier]] = None,
    on_press: Optional[Callable[[str, Set[KeyModifier]], None]] = None,
    on_release: Optional[Callable[[str, Set[KeyModifier]], None]] = None,
) -> KeyboardResult:
    """Press and release a single key.
    
    Args:
        key: Key name or character.
        modifiers: Set of modifier keys to hold.
        on_press: Callback when key is pressed.
        on_release: Callback when key is released.
    
    Returns:
        KeyboardResult with operation details.
    """
    start_time = time.time()
    modifiers = modifiers or set()
    
    try:
        if on_press:
            on_press(key, modifiers)
        
        time.sleep(0.05)
        
        if on_release:
            on_release(key, modifiers)
        
        duration = (time.time() - start_time) * 1000
        
        return KeyboardResult(
            success=True,
            keys_pressed=1,
            duration_ms=duration,
        )
    except Exception as e:
        return KeyboardResult(
            success=False,
            keys_pressed=0,
            duration_ms=0,
            error_message=str(e),
        )


def type_text(
    text: str,
    config: Optional[KeyboardConfig] = None,
    on_key: Optional[Callable[[str, Set[KeyModifier]], None]] = None,
) -> KeyboardResult:
    """Type a string of text.
    
    Args:
        text: Text to type.
        config: Keyboard configuration.
        on_key: Callback for each key event.
    
    Returns:
        KeyboardResult with operation details.
    """
    config = config or KeyboardConfig()
    start_time = time.time()
    
    try:
        keys_pressed = 0
        
        for char in text:
            modifiers = set()
            
            if char.isupper():
                modifiers.add(KeyModifier.SHIFT)
                char = char.lower()
            elif char in "!@#$%^&*()_+{}|:\"<>?":
                modifiers.add(KeyModifier.SHIFT)
                char = _get_shift_char(char)
            
            if on_key:
                on_key(char, modifiers)
            
            keys_pressed += 1
            
            time.sleep(config.delay_ms / 1000.0)
        
        duration = (time.time() - start_time) * 1000
        
        return KeyboardResult(
            success=True,
            keys_pressed=keys_pressed,
            duration_ms=duration,
        )
    except Exception as e:
        return KeyboardResult(
            success=False,
            keys_pressed=0,
            duration_ms=0,
            error_message=str(e),
        )


def _get_shift_char(char: str) -> str:
    """Get shifted character for symbols."""
    shift_map = {
        '!': '1', '@': '2', '#': '3', '$': '4', '%': '5',
        '^': '6', '&': '7', '*': '8', '(': '9', ')': '0',
        '_': '-', '+': '=', '{': '[', '}': ']', '|': '\\',
        ':': ';', '"': "'", '<': ',', '>': '.', '?': '/',
    }
    return shift_map.get(char, char)


def press_hotkey(
    *keys: str,
    on_execute: Optional[Callable[[List[str]], None]] = None,
) -> KeyboardResult:
    """Press a hotkey combination (e.g., Ctrl+C).
    
    Args:
        *keys: Keys in the combination.
        on_execute: Callback when combination is executed.
    
    Returns:
        KeyboardResult with operation details.
    """
    start_time = time.time()
    
    try:
        if on_execute:
            on_execute(list(keys))
        
        duration = (time.time() - start_time) * 1000
        
        return KeyboardResult(
            success=True,
            keys_pressed=len(keys),
            duration_ms=duration,
        )
    except Exception as e:
        return KeyboardResult(
            success=False,
            keys_pressed=0,
            duration_ms=0,
            error_message=str(e),
        )


def hold_keys(
    keys: List[str],
    duration_ms: float,
    on_press: Optional[Callable[[str], None]] = None,
    on_release: Optional[Callable[[str], None]] = None,
) -> KeyboardResult:
    """Hold multiple keys for a duration.
    
    Args:
        keys: List of keys to hold.
        duration_ms: Duration to hold in milliseconds.
        on_press: Callback when keys are pressed.
        on_release: Callback when keys are released.
    
    Returns:
        KeyboardResult with operation details.
    """
    start_time = time.time()
    
    try:
        for key in keys:
            if on_press:
                on_press(key)
        
        time.sleep(duration_ms / 1000.0)
        
        for key in keys:
            if on_release:
                on_release(key)
        
        duration = (time.time() - start_time) * 1000
        
        return KeyboardResult(
            success=True,
            keys_pressed=len(keys),
            duration_ms=duration,
        )
    except Exception as e:
        return KeyboardResult(
            success=False,
            keys_pressed=0,
            duration_ms=0,
            error_message=str(e),
        )


def repeat_key(
    key: str,
    times: int,
    interval_ms: float = 100.0,
    on_key: Optional[Callable[[str], None]] = None,
) -> KeyboardResult:
    """Repeat a key press multiple times.
    
    Args:
        key: Key to repeat.
        times: Number of times to press.
        interval_ms: Interval between presses.
        on_key: Callback for each key event.
    
    Returns:
        KeyboardResult with operation details.
    """
    start_time = time.time()
    
    try:
        for _ in range(times):
            if on_key:
                on_key(key)
            time.sleep(interval_ms / 1000.0)
        
        duration = (time.time() - start_time) * 1000
        
        return KeyboardResult(
            success=True,
            keys_pressed=times,
            duration_ms=duration,
        )
    except Exception as e:
        return KeyboardResult(
            success=False,
            keys_pressed=0,
            duration_ms=0,
            error_message=str(e),
        )
