"""Click automation action for UI interaction simulation.

This module provides mouse and keyboard automation including
clicking, typing, scrolling, and hotkey combinations.

Example:
    >>> action = ClickAction()
    >>> action.execute(command="click", x=100, y=200)
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class Point:
    """Represents a screen coordinate point."""
    x: int
    y: int


class ClickAction:
    """Click automation action for UI interaction.

    Provides mouse and keyboard automation using pyautogui
    and platform-specific implementations.

    Example:
        >>> action = ClickAction()
        >>> action.execute(command="click", x=100, y=200)
    """

    def __init__(self) -> None:
        """Initialize click automation."""
        self._paused = False

    def execute(
        self,
        command: str,
        x: Optional[int] = None,
        y: Optional[int] = None,
        clicks: int = 1,
        interval: float = 0.0,
        button: str = "left",
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute click/keyboard command.

        Args:
            command: Command name (click, move, type, scroll, etc.).
            x: X coordinate (required for mouse commands).
            y: Y coordinate (required for mouse commands).
            clicks: Number of clicks.
            interval: Interval between clicks.
            button: Mouse button ('left', 'right', 'middle').
            **kwargs: Additional command parameters.

        Returns:
            Command result dictionary.

        Raises:
            ValueError: If coordinates are missing for mouse commands.
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

        if cmd in ("click", "left_click"):
            if x is None or y is None:
                raise ValueError("x and y coordinates required for 'click'")
            pyautogui.click(x, y, clicks=clicks, interval=interval, button=button)
            result["position"] = (x, y)

        elif cmd == "right_click":
            if x is None or y is None:
                raise ValueError("x and y coordinates required for 'right_click'")
            pyautogui.rightClick(x, y)
            result["position"] = (x, y)

        elif cmd == "middle_click":
            if x is None or y is None:
                raise ValueError("x and y coordinates required for 'middle_click'")
            pyautogui.middleClick(x, y)
            result["position"] = (x, y)

        elif cmd == "double_click":
            if x is None or y is None:
                raise ValueError("x and y coordinates required for 'double_click'")
            pyautogui.doubleClick(x, y)
            result["position"] = (x, y)

        elif cmd == "move":
            if x is None or y is None:
                raise ValueError("x and y coordinates required for 'move'")
            duration = kwargs.get("duration", 0.0)
            pyautogui.moveTo(x, y, duration=duration)
            result["position"] = (x, y)

        elif cmd == "drag":
            if x is None or y is None:
                raise ValueError("x and y coordinates required for 'drag'")
            start_x = kwargs.get("start_x")
            start_y = kwargs.get("start_y")
            duration = kwargs.get("duration", 0.5)
            if start_x is not None and start_y is not None:
                pyautogui.moveTo(start_x, start_y)
            pyautogui.dragTo(x, y, duration=duration, button=button)
            result["position"] = (x, y)

        elif cmd == "scroll":
            clicks = kwargs.get("clicks", 3)
            pyautogui.scroll(clicks, x=x or 0, y=y or 0)
            result["scroll_amount"] = clicks

        elif cmd in ("type", "write", "typewrite"):
            text = kwargs.get("text", "")
            interval = kwargs.get("interval", 0.0)
            pyautogui.write(text, interval=interval)
            result["text"] = text

        elif cmd == "press":
            key = kwargs.get("key")
            if not key:
                raise ValueError("key required for 'press'")
            pyautogui.press(key)
            result["key"] = key

        elif cmd == "hotkey":
            keys = kwargs.get("keys", [])
            if not keys:
                raise ValueError("keys required for 'hotkey'")
            pyautogui.hotkey(*keys)
            result["keys"] = keys

        elif cmd == "key_down":
            key = kwargs.get("key")
            if not key:
                raise ValueError("key required for 'key_down'")
            pyautogui.keyDown(key)
            result["key"] = key

        elif cmd == "key_up":
            key = kwargs.get("key")
            if not key:
                raise ValueError("key required for 'key_up'")
            pyautogui.keyUp(key)
            result["key"] = key

        elif cmd == "screenshot":
            path = kwargs.get("path", "/tmp/screenshot.png")
            pyautogui.screenshot(path)
            result["path"] = path

        elif cmd == "position":
            pos = pyautogui.position()
            result["position"] = (pos.x, pos.y)

        else:
            raise ValueError(f"Unknown command: {command}")

        return result

    def screenshot(self, region: Optional[tuple[int, int, int, int]] = None) -> Any:
        """Take screenshot of screen or region.

        Args:
            region: Optional (x, y, width, height) region.

        Returns:
            PIL Image object.
        """
        import pyautogui
        if region:
            return pyautogui.screenshot(region=region)
        return pyautogui.screenshot()

    def locate_on_screen(
        self,
        image_path: str,
        confidence: float = 0.9,
    ) -> Optional[tuple[int, int, int, int]]:
        """Locate image on screen.

        Args:
            image_path: Path to image to find.
            confidence: Match confidence level.

        Returns:
            Bounding box (x, y, width, height) or None.
        """
        try:
            import pyautogui
        except ImportError:
            return None

        try:
            return pyautogui.locateOnScreen(image_path, confidence=confidence)
        except Exception:
            return None

    def pause(self, value: bool = True) -> None:
        """Set pause state for all operations.

        Args:
            value: True to pause, False to resume.
        """
        self._paused = value

    def get_screen_size(self) -> tuple[int, int]:
        """Get screen dimensions.

        Returns:
            (width, height) tuple.
        """
        import pyautogui
        return pyautogui.size()
