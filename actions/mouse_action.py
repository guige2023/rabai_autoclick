"""Mouse automation action for cursor control.

This module provides mouse control including
movement, clicking, dragging, and position detection.

Example:
    >>> action = MouseAction()
    >>> result = action.execute(command="move", x=100, y=200)
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass
class MousePosition:
    """Mouse cursor position."""
    x: int
    y: int


class MouseAction:
    """Mouse automation action.

    Provides cursor control, clicking, dragging, and
    position detection for automation.

    Example:
        >>> action = MouseAction()
        >>> result = action.execute(
        ...     command="click",
        ...     x=100,
        ...     y=200,
        ...     button="right"
        ... )
    """

    def __init__(self) -> None:
        """Initialize mouse action."""
        self._dragging = False

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
        """Execute mouse command.

        Args:
            command: Command (move, click, drag, scroll, etc.).
            x: X coordinate.
            y: Y coordinate.
            clicks: Number of clicks.
            interval: Interval between clicks.
            button: Mouse button ('left', 'right', 'middle').
            **kwargs: Additional parameters.

        Returns:
            Command result dictionary.

        Raises:
            ValueError: If coordinates are missing for position commands.
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

        if cmd == "move":
            if x is None or y is None:
                raise ValueError("x and y required for 'move'")
            duration = kwargs.get("duration", 0.0)
            pyautogui.moveTo(x, y, duration=duration)
            result["position"] = (x, y)

        elif cmd == "click":
            if x is not None and y is not None:
                pyautogui.click(x, y, clicks=clicks, interval=interval, button=button)
            else:
                pyautogui.click(clicks=clicks, interval=interval, button=button)
            result["clicked"] = (x, y) if x is not None else "current"

        elif cmd == "double_click":
            if x is not None and y is not None:
                pyautogui.doubleClick(x, y)
            else:
                pyautogui.doubleClick()
            result["double_clicked"] = True

        elif cmd == "right_click":
            if x is not None and y is not None:
                pyautogui.rightClick(x, y)
            else:
                pyautogui.rightClick()
            result["right_clicked"] = True

        elif cmd == "middle_click":
            if x is not None and y is not None:
                pyautogui.middleClick(x, y)
            else:
                pyautogui.middleClick()
            result["middle_clicked"] = True

        elif cmd == "down":
            if x is not None and y is not None:
                pyautogui.mouseDown(x, y, button=button)
            else:
                pyautogui.mouseDown(button=button)
            result["button_down"] = True
            self._dragging = True

        elif cmd == "up":
            pyautogui.mouseUp(button=button)
            result["button_up"] = True
            self._dragging = False

        elif cmd == "drag":
            start_x = kwargs.get("start_x", x)
            start_y = kwargs.get("start_y", y)
            end_x = x
            end_y = y
            duration = kwargs.get("duration", 0.5)

            if start_x is not None and start_y is not None:
                pyautogui.moveTo(start_x, start_y)
            pyautogui.drag(end_x - (start_x or 0), end_y - (start_y or 0), duration=duration, button=button)
            result["dragged"] = True

        elif cmd == "scroll":
            amount = kwargs.get("amount", 3)
            if x is not None and y is not None:
                pyautogui.scroll(amount, x=x, y=y)
            else:
                pyautogui.scroll(amount)
            result["scrolled"] = amount

        elif cmd == "position":
            pos = pyautogui.position()
            result["position"] = (pos.x, pos.y)

        elif cmd == "hover":
            if x is None or y is None:
                raise ValueError("x and y required for 'hover'")
            duration = kwargs.get("duration", 1.0)
            pyautogui.moveTo(x, y, duration=duration)
            result["hovered"] = True

        elif cmd == "move_relative":
            if x is None or y is None:
                raise ValueError("x and y required for 'move_relative'")
            pyautogui.move(x, y)
            result["moved"] = (x, y)

        elif cmd == "smooth_move":
            if x is None or y is None:
                raise ValueError("x and y required for 'smooth_move'")
            steps = kwargs.get("steps", 20)
            start_pos = pyautogui.position()
            start_x, start_y = start_pos.x, start_pos.y

            for i in range(steps + 1):
                ratio = i / steps
                current_x = int(start_x + (x - start_x) * ratio)
                current_y = int(start_y + (y - start_y) * ratio)
                pyautogui.moveTo(current_x, current_y)
                time.sleep(0.01)

            result["moved"] = (x, y)

        else:
            raise ValueError(f"Unknown command: {command}")

        return result

    def is_dragging(self) -> bool:
        """Check if mouse button is held down.

        Returns:
            True if dragging.
        """
        return self._dragging

    def get_position(self) -> MousePosition:
        """Get current mouse position.

        Returns:
            MousePosition object.
        """
        import pyautogui
        pos = pyautogui.position()
        return MousePosition(x=pos.x, y=pos.y)

    def wait_for_position(
        self,
        x: int,
        y: int,
        tolerance: int = 5,
        timeout: float = 10.0,
    ) -> dict[str, Any]:
        """Wait for mouse to reach position.

        Args:
            x: Target X coordinate.
            y: Target Y coordinate.
            tolerance: Position tolerance.
            timeout: Maximum wait time.

        Returns:
            Wait result dictionary.
        """
        import pyautogui
        start_time = time.time()

        while time.time() - start_time < timeout:
            pos = pyautogui.position()
            if abs(pos.x - x) <= tolerance and abs(pos.y - y) <= tolerance:
                return {"reached": True, "time": time.time() - start_time}
            time.sleep(0.1)

        return {"reached": False, "timeout": True}
