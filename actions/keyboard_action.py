"""Keyboard automation action for key typing and hotkeys.

This module provides keyboard automation including
key typing, hotkey combinations, and text input.

Example:
    >>> action = KeyboardAction()
    >>> result = action.execute(command="type", text="Hello World")
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class HotkeyCombo:
    """Represents a hotkey combination."""
    keys: list[str]
    description: Optional[str] = None


class KeyboardAction:
    """Keyboard automation action.

    Provides key typing, hotkey combinations, and
    keyboard control for automation.

    Example:
        >>> action = KeyboardAction()
        >>> result = action.execute(
        ...     command="hotkey",
        ...     keys=["cmd", "s"]
        ... )
    """

    def __init__(self) -> None:
        """Initialize keyboard action."""
        self._paused = False

    def execute(
        self,
        command: str,
        text: Optional[str] = None,
        key: Optional[str] = None,
        keys: Optional[list[str]] = None,
        interval: float = 0.0,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute keyboard command.

        Args:
            command: Command (type, press, hotkey, key_down, key_up).
            text: Text to type.
            key: Single key to press.
            keys: List of keys for hotkey.
            interval: Interval between key presses.
            **kwargs: Additional parameters.

        Returns:
            Command result dictionary.

        Raises:
            ValueError: If command is invalid.
        """
        try:
            import pyautogui
            pyautogui.FAILSAFE = True
        except ImportError:
            return {
                "success": False,
                "error": "pyautogui not installed. Run: pip install pyautogui",
            }

        cmd = command.lower()
        result: dict[str, Any] = {"command": cmd, "success": True}

        if cmd in ("type", "write", "typewrite"):
            if text is None:
                raise ValueError("text required for 'type' command")
            pyautogui.write(text, interval=interval)
            result["text"] = text

        elif cmd == "press":
            if key is None:
                raise ValueError("key required for 'press' command")
            pyautogui.press(key)
            result["key"] = key

        elif cmd == "hotkey":
            if not keys:
                raise ValueError("keys required for 'hotkey' command")
            pyautogui.hotkey(*keys)
            result["keys"] = keys

        elif cmd == "key_down":
            if key is None:
                raise ValueError("key required for 'key_down' command")
            pyautogui.keyDown(key)
            result["key"] = key

        elif cmd == "key_up":
            if key is None:
                raise ValueError("key required for 'key_up' command")
            pyautogui.keyUp(key)
            result["key"] = key

        elif cmd == "hold":
            if not keys:
                raise ValueError("keys required for 'hold' command")
            for k in keys:
                pyautogui.keyDown(k)
            result["held"] = keys

        elif cmd == "release":
            if not keys:
                raise ValueError("keys required for 'release' command")
            for k in reversed(keys):
                pyautogui.keyUp(k)
            result["released"] = keys

        elif cmd == "type_sequence":
            sequence = kwargs.get("sequence", [])
            for k in sequence:
                if isinstance(k, str):
                    pyautogui.press(k)
                else:
                    pyautogui.hotkey(*k)
                time.sleep(interval)
            result["sequence"] = sequence

        elif cmd == "select_all":
            pyautogui.hotkey("cmd", "a")
            result["selected"] = "all"

        elif cmd == "copy":
            pyautogui.hotkey("cmd", "c")
            result["copied"] = True

        elif cmd == "paste":
            pyautogui.hotkey("cmd", "v")
            result["pasted"] = True

        elif cmd == "cut":
            pyautogui.hotkey("cmd", "x")
            result["cut"] = True

        elif cmd == "undo":
            pyautogui.hotkey("cmd", "z")
            result["undone"] = True

        elif cmd == "redo":
            pyautogui.hotkey("cmd", "shift", "z")
            result["redone"] = True

        elif cmd == "find":
            pyautogui.hotkey("cmd", "f")
            result["find_opened"] = True

        elif cmd == "replace":
            pyautogui.hotkey("cmd", "h")
            result["replace_opened"] = True

        else:
            raise ValueError(f"Unknown command: {command}")

        return result

    def type_with_delay(self, text: str, delay: float = 0.05) -> dict[str, Any]:
        """Type text with delay between keystrokes.

        Args:
            text: Text to type.
            delay: Delay between keystrokes.

        Returns:
            Result dictionary.
        """
        return self.execute(command="type", text=text, interval=delay)

    def enter_text(self, text: str) -> dict[str, Any]:
        """Type text and press Enter.

        Args:
            text: Text to type.

        Returns:
            Result dictionary.
        """
        self.execute(command="type", text=text)
        return self.execute(command="press", key="enter")

    def clear_field(self) -> dict[str, Any]:
        """Clear current field (select all and delete).

        Returns:
            Result dictionary.
        """
        self.execute(command="select_all")
        time.sleep(0.1)
        self.execute(command="press", key="delete")
        return {"cleared": True}

    def repeat_key(self, key: str, count: int, interval: float = 0.1) -> dict[str, Any]:
        """Repeat a key press multiple times.

        Args:
            key: Key to press.
            count: Number of times to press.
            interval: Interval between presses.

        Returns:
            Result dictionary.
        """
        for _ in range(count):
            self.execute(command="press", key=key)
            time.sleep(interval)
        return {"repeated": key, "count": count}
