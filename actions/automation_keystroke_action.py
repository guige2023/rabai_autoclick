"""
Keystroke automation module.

Provides keyboard input simulation and hotkey automation
with support for modifier keys, key sequences, and text input.

Author: Aito Auto Agent
"""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional, Callable, Union


class KeyModifier(Enum):
    """Keyboard modifier keys."""
    COMMAND = auto()
    SHIFT = auto()
    OPTION = auto()
    CONTROL = auto()
    FUNCTION = auto()
    CAPS_LOCK = auto()
    RETURN = auto()
    TAB = auto()
    DELETE = auto()
    ESCAPE = auto()
    SPACE = auto()


@dataclass
class KeyStroke:
    """Represents a single key or key combination."""
    key: str
    modifiers: list[KeyModifier] = field(default_factory=list)
    hold_duration_ms: float = 0.0
    delay_after_ms: float = 0.0


@dataclass
class KeySequence:
    """A sequence of keystrokes to execute."""
    name: str
    strokes: list[KeyStroke]
    repeat_count: int = 1
    description: Optional[str] = None


class KeyCodeMapper:
    """Maps key names to platform-specific key codes."""

    _MACOS_KEY_MAP = {
        "a": "a", "b": "b", "c": "c", "d": "d", "e": "e",
        "f": "f", "g": "g", "h": "h", "i": "i", "j": "j",
        "k": "k", "l": "l", "m": "m", "n": "n", "o": "o",
        "p": "p", "q": "q", "r": "r", "s": "s", "t": "t",
        "u": "u", "v": "v", "w": "w", "x": "x", "y": "y",
        "z": "z",
        "0": "0", "1": "1", "2": "2", "3": "3", "4": "4",
        "5": "5", "6": "6", "7": "7", "8": "8", "9": "9",
        "return": "\\r", "tab": "\\t", "space": " ",
        "delete": "\\d", "escape": "\\e",
        "up": "\\eu", "down": "\\ed", "left": "\\el", "right": "\\er",
        "f1": "\\<f1>", "f2": "\\<f2>", "f3": "\\<f3>", "f4": "\\<f4>",
        "f5": "\\<f5>", "f6": "\\<f6>", "f7": "\\<f7>", "f8": "\\<f8>",
        "f9": "\\<f9>", "f10": "\\<f10>", "f11": "\\<f11>", "f12": "\\<f12>",
    }

    _MODIFIER_MACOS = {
        KeyModifier.COMMAND: "command",
        KeyModifier.SHIFT: "shift",
        KeyModifier.OPTION: "option",
        KeyModifier.CONTROL: "control",
    }

    @classmethod
    def to_mac_codes(cls, keystroke: KeyStroke) -> str:
        """Convert KeyStroke to macOS osascript format."""
        key = cls._MACOS_KEY_MAP.get(keystroke.key.lower(), keystroke.key)

        modifiers = []
        for mod in keystroke.modifiers:
            if mod in cls._MODIFIER_MACOS:
                modifiers.append(cls._MODIFIER_MACOS[mod])

        if modifiers:
            return f'{{{", ".join(modifiers)} of {key}}}'
        return key


class KeystrokeAutomator:
    """
    Keystroke automation for keyboard input simulation.

    Supports single keys, key combinations, and sequences with
    configurable timing and repetition.

    Example:
        automator = KeystrokeAutomator(platform="macos")

        # Type text
        automator.type_text("Hello, World!")

        # Press single key
        automator.press_key("return")

        # Key combination
        automator.press_combo([KeyModifier.COMMAND, "s"])

        # Execute sequence
        automator.execute_sequence(my_sequence)
    """

    def __init__(self, platform: str = "macos"):
        self._platform = platform
        self._mapper = KeyCodeMapper()

    def press_key(self, key: str, hold_duration_ms: float = 0.0) -> bool:
        """
        Press and release a single key.

        Args:
            key: Key name
            hold_duration_ms: How long to hold the key

        Returns:
            True if successful
        """
        if hold_duration_ms > 0:
            time.sleep(hold_duration_ms / 1000)

        if self._platform == "macos":
            return self._press_key_macos(key)
        elif self._platform == "windows":
            return self._press_key_windows(key)
        else:
            return self._press_key_x11(key)

    def _press_key_macos(self, key: str) -> bool:
        """Press key on macOS."""
        try:
            script = f'tell application "System Events" to keystroke "{key}"'
            subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=5
            )
            return True
        except Exception:
            return False

    def _press_key_windows(self, key: str) -> bool:
        """Press key on Windows using PowerShell."""
        try:
            script = f'''
            Add-Type -AssemblyName System.Windows.Forms
            [System.Windows.Forms.SendKeys]::SendWait("{{{key}}}")
            '''
            subprocess.run(
                ["powershell", "-Command", script],
                capture_output=True,
                timeout=5
            )
            return True
        except Exception:
            return False

    def _press_key_x11(self, key: str) -> bool:
        """Press key on X11/Linux using xdotool."""
        try:
            subprocess.run(
                ["xdotool", "key", key],
                capture_output=True,
                timeout=5
            )
            return True
        except Exception:
            return False

    def press_combo(
        self,
        modifiers_and_key: list[Union[KeyModifier, str]],
        hold_duration_ms: float = 0.0
    ) -> bool:
        """
        Press a key combination with modifiers.

        Args:
            modifiers_and_key: List of modifiers followed by the key
            hold_duration_ms: How long to hold the key

        Returns:
            True if successful
        """
        modifiers = [m for m in modifiers_and_key if isinstance(m, KeyModifier)]
        key = next((k for k in modifiers_and_key if isinstance(k, str)), None)

        if not key:
            return False

        if hold_duration_ms > 0:
            time.sleep(hold_duration_ms / 1000)

        if self._platform == "macos":
            return self._press_combo_macos(modifiers, key)
        elif self._platform == "windows":
            return self._press_combo_windows(modifiers, key)
        else:
            return self._press_combo_x11(modifiers, key)

    def _press_combo_macos(self, modifiers: list[KeyModifier], key: str) -> bool:
        """Press key combination on macOS."""
        try:
            mod_parts = []
            for mod in modifiers:
                if mod == KeyModifier.COMMAND:
                    mod_parts.append("command down")
                elif mod == KeyModifier.SHIFT:
                    mod_parts.append("shift down")
                elif mod == KeyModifier.OPTION:
                    mod_parts.append("option down")
                elif mod == KeyModifier.CONTROL:
                    mod_parts.append("control down")

            script_parts = []
            for mod in mod_parts:
                script_parts.append(f'keystroke "{mod}" using down keys')

            key_code = self._mapper._MACOS_KEY_MAP.get(key.lower(), key)
            script_parts.append(f'keystroke "{key_code}"')

            script = 'tell application "System Events" to ' + ' & '.join(script_parts)
            subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=5
            )
            return True
        except Exception:
            return False

    def _press_combo_windows(self, modifiers: list[KeyModifier], key: str) -> bool:
        """Press key combination on Windows."""
        try:
            mod_prefix = ""
            for mod in modifiers:
                if mod == KeyModifier.COMMAND:
                    mod_prefix += "^"
                elif mod == KeyModifier.SHIFT:
                    mod_prefix += "+"
                elif mod == KeyModifier.OPTION:
                    mod_prefix += "%"
                elif mod == KeyModifier.CONTROL:
                    mod_prefix += "^"

            script = f'''
            Add-Type -AssemblyName System.Windows.Forms
            [System.Windows.Forms.SendKeys]::SendWait("{mod_prefix}{key}")
            '''
            subprocess.run(
                ["powershell", "-Command", script],
                capture_output=True,
                timeout=5
            )
            return True
        except Exception:
            return False

    def _press_combo_x11(self, modifiers: list[KeyModifier], key: str) -> bool:
        """Press key combination on X11."""
        try:
            mod_args = []
            for mod in modifiers:
                if mod == KeyModifier.COMMAND:
                    mod_args.append("super")
                elif mod == KeyModifier.SHIFT:
                    mod_args.append("shift")
                elif mod == KeyModifier.OPTION:
                    mod_args.append("alt")
                elif mod == KeyModifier.CONTROL:
                    mod_args.append("ctrl")

            args = mod_args + [key]
            subprocess.run(
                ["xdotool", "key", "--".join(args)],
                capture_output=True,
                timeout=5
            )
            return True
        except Exception:
            return False

    def type_text(self, text: str, delay_per_char_ms: float = 0.0) -> bool:
        """
        Type a string of text.

        Args:
            text: Text to type
            delay_per_char_ms: Delay between each character

        Returns:
            True if successful
        """
        if self._platform == "macos":
            return self._type_text_macos(text, delay_per_char_ms)
        elif self._platform == "windows":
            return self._type_text_windows(text, delay_per_char_ms)
        else:
            return self._type_text_x11(text, delay_per_char_ms)

    def _type_text_macos(self, text: str, delay_per_char_ms: float) -> bool:
        """Type text on macOS."""
        try:
            escaped_text = text.replace('"', '\\"').replace('\n', '\\n')
            script = f'tell application "System Events" to keystroke "{escaped_text}"'
            subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=len(text) * 0.1 + 5
            )

            if delay_per_char_ms > 0:
                time.sleep(len(text) * delay_per_char_ms / 1000)

            return True
        except Exception:
            return False

    def _type_text_windows(self, text: str, delay_per_char_ms: float) -> bool:
        """Type text on Windows."""
        try:
            escaped = text.replace('{', '{{').replace('}', '}}').replace('^', '{^}')

            script = f'''
            Add-Type -AssemblyName System.Windows.Forms
            [System.Windows.Forms.SendKeys]::SendWait("{escaped}")
            '''
            subprocess.run(
                ["powershell", "-Command", script],
                capture_output=True,
                timeout=len(text) * 0.1 + 5
            )

            if delay_per_char_ms > 0:
                time.sleep(len(text) * delay_per_char_ms / 1000)

            return True
        except Exception:
            return False

    def _type_text_x11(self, text: str, delay_per_char_ms: float) -> bool:
        """Type text on X11."""
        try:
            for char in text:
                subprocess.run(
                    ["xdotool", "type", char],
                    capture_output=True,
                    timeout=2
                )
                if delay_per_char_ms > 0:
                    time.sleep(delay_per_char_ms / 1000)
            return True
        except Exception:
            return False

    def execute_keystroke(self, stroke: KeyStroke) -> bool:
        """
        Execute a single KeyStroke.

        Args:
            stroke: The keystroke to execute

        Returns:
            True if successful
        """
        if stroke.modifiers:
            return self.press_combo(
                stroke.modifiers + [stroke.key],
                hold_duration_ms=stroke.hold_duration_ms
            )
        else:
            return self.press_key(stroke.key, hold_duration_ms=stroke.hold_duration_ms)

    def execute_sequence(
        self,
        sequence: KeySequence,
        delay_between_strokes_ms: float = 50.0
    ) -> bool:
        """
        Execute a KeySequence.

        Args:
            sequence: The sequence to execute
            delay_between_strokes_ms: Delay between each stroke

        Returns:
            True if all strokes executed successfully
        """
        for _ in range(sequence.repeat_count):
            for stroke in sequence.strokes:
                if not self.execute_keystroke(stroke):
                    return False

                if stroke.delay_after_ms > 0:
                    time.sleep(stroke.delay_after_ms / 1000)
                elif delay_between_strokes_ms > 0:
                    time.sleep(delay_between_strokes_ms / 1000)

        return True


def create_keystroke_automator(platform: str = "macos") -> KeystrokeAutomator:
    """Factory function to create a KeystrokeAutomator."""
    return KeystrokeAutomator(platform=platform)


def create_keysequence(
    name: str,
    strokes: list[tuple[list[KeyModifier], str]],
    repeat_count: int = 1,
    description: Optional[str] = None
) -> KeySequence:
    """
    Helper to create a KeySequence from tuples.

    Args:
        name: Sequence name
        strokes: List of (modifiers, key) tuples
        repeat_count: Number of times to repeat
        description: Optional description

    Returns:
        A new KeySequence
    """
    return KeySequence(
        name=name,
        strokes=[
            KeyStroke(key=key, modifiers=mods)
            for mods, key in strokes
        ],
        repeat_count=repeat_count,
        description=description
    )
