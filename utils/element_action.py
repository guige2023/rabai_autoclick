"""
Element Action Utility

Performs common UI actions on elements (click, type, scroll, etc.).
Provides abstraction over different input methods.

Example:
    >>> actioner = ElementActionExecutor(accessibility)
    >>> actioner.click(element)
    >>> actioner.type_text(element, "Hello World")
"""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class ActionResult:
    """Result of an element action."""
    success: bool
    error: Optional[str] = None
    duration: float = 0.0


class ElementActionExecutor:
    """
    Executes UI actions on accessibility elements.

    Supports macOS accessibility API for performing actions.
    """

    def __init__(self) -> None:
        self._last_duration = 0.0

    def click(
        self,
        element: dict,
        button: int = 0,
        x: Optional[int] = None,
        y: Optional[int] = None,
    ) -> ActionResult:
        """
        Click an element.

        Args:
            element: Element dict with bounds and role.
            button: Mouse button (0=left, 1=right, 2=middle).
            x: Click offset from element left edge.
            y: Click offset from element top edge.

        Returns:
            ActionResult indicating success or failure.
        """
        start = time.time()
        try:
            bounds = element.get("bounds", [0, 0, 100, 100])
            if len(bounds) < 4:
                return ActionResult(success=False, error="Invalid bounds", duration=0.0)

            cx = x if x is not None else bounds[2] // 2
            cy = y if y is not None else bounds[3] // 2
            click_x = bounds[0] + cx
            click_y = bounds[1] + cy

            self._mouse_click(click_x, click_y, button)
            self._last_duration = time.time() - start
            return ActionResult(success=True, duration=self._last_duration)

        except Exception as e:
            return ActionResult(success=False, error=str(e), duration=time.time() - start)

    def double_click(
        self,
        element: dict,
        x: Optional[int] = None,
        y: Optional[int] = None,
    ) -> ActionResult:
        """Double-click an element."""
        start = time.time()
        try:
            bounds = element.get("bounds", [0, 0, 100, 100])
            cx = x if x is not None else bounds[2] // 2
            cy = y if y is not None else bounds[3] // 2
            click_x = bounds[0] + cx
            click_y = bounds[1] + cy

            self._mouse_click(click_x, click_y, 0)
            time.sleep(0.05)
            self._mouse_click(click_x, click_y, 0)
            self._last_duration = time.time() - start
            return ActionResult(success=True, duration=self._last_duration)

        except Exception as e:
            return ActionResult(success=False, error=str(e), duration=time.time() - start)

    def right_click(
        self,
        element: dict,
        x: Optional[int] = None,
        y: Optional[int] = None,
    ) -> ActionResult:
        """Right-click an element."""
        return self.click(element, button=2, x=x, y=y)

    def type_text(
        self,
        element: dict,
        text: str,
        blur_after: bool = False,
    ) -> ActionResult:
        """
        Type text into an element.

        Args:
            element: Element dict.
            text: Text to type.
            blur_after: Whether to unfocus after typing.

        Returns:
            ActionResult indicating success or failure.
        """
        start = time.time()
        try:
            # Click element to focus
            self.click(element)

            # Type each character
            for char in text:
                self._type_key(char)
                time.sleep(0.01)

            if blur_after:
                self._press_key("Tab")

            self._last_duration = time.time() - start
            return ActionResult(success=True, duration=self._last_duration)

        except Exception as e:
            return ActionResult(success=False, error=str(e), duration=time.time() - start)

    def scroll(
        self,
        element: dict,
        amount: int,
        direction: str = "down",
    ) -> ActionResult:
        """
        Scroll within an element.

        Args:
            element: Element dict with bounds.
            amount: Number of scroll units.
            direction: 'up', 'down', 'left', 'right'.

        Returns:
            ActionResult indicating success or failure.
        """
        start = time.time()
        try:
            bounds = element.get("bounds", [0, 0, 100, 100])
            cx = bounds[0] + bounds[2] // 2
            cy = bounds[1] + bounds[3] // 2

            self._mouse_click(cx, cy, 0)
            time.sleep(0.05)

            # Use scroll wheel
            dx = 0
            dy = 0
            if direction == "up":
                dy = -amount
            elif direction == "down":
                dy = amount
            elif direction == "left":
                dx = -amount
            elif direction == "right":
                dx = amount

            self._scroll_wheel(dx, dy)
            self._last_duration = time.time() - start
            return ActionResult(success=True, duration=self._last_duration)

        except Exception as e:
            return ActionResult(success=False, error=str(e), duration=time.time() - start)

    def hover(
        self,
        element: dict,
        x: Optional[int] = None,
        y: Optional[int] = None,
    ) -> ActionResult:
        """
        Move mouse to element (hover).

        Args:
            element: Element dict with bounds.
            x: X offset from element left.
            y: Y offset from element top.

        Returns:
            ActionResult indicating success or failure.
        """
        start = time.time()
        try:
            bounds = element.get("bounds", [0, 0, 100, 100])
            cx = x if x is not None else bounds[2] // 2
            cy = y if y is not None else bounds[3] // 2
            target_x = bounds[0] + cx
            target_y = bounds[1] + cy

            self._mouse_move(target_x, target_y)
            self._last_duration = time.time() - start
            return ActionResult(success=True, duration=self._last_duration)

        except Exception as e:
            return ActionResult(success=False, error=str(e), duration=time.time() - start)

    def _mouse_click(self, x: int, y: int, button: int) -> None:
        """Simulate mouse click at coordinates."""
        script = f"""
        osascript -e '
        tell application "System Events"
            set mousePos to {{x}, {y}}}
            set mouseDist to 0
            repeat while mouseDist < 1
                set currPos to mouse position
                set mouseDist to ((item 1 of currPos - (item 1 of mousePos))^2 + (item 2 of currPos - (item 2 of mousePos))^2)
            end repeat
            click at mousePos
        end tell
        '
        """
        subprocess.run(script, shell=True, capture_output=True, timeout=2.0)

    def _mouse_move(self, x: int, y: int) -> None:
        """Move mouse to coordinates."""
        script = f"""
        osascript -e '
        tell application "System Events"
            set mousePos to {{{x}, {y}}}
            set mouseDist to 0
            repeat while mouseDist < 1
                set currPos to mouse position
                set mouseDist to ((item 1 of currPos - (item 1 of mousePos))^2 + (item 2 of currPos - (item 2 of mousePos))^2)
            end repeat
        end tell
        '
        """
        subprocess.run(script, shell=True, capture_output=True, timeout=2.0)

    def _scroll_wheel(self, dx: int, dy: int) -> None:
        """Simulate scroll wheel."""
        script = f"""
        osascript -e '
        tell application "System Events"
            set mousePos to mouse position
            click at mousePos
            set抓起 event "MMWHEEL" to make new events at end of events
        end tell
        '
        """
        subprocess.run(script, shell=True, capture_output=True, timeout=1.0)

    def _type_key(self, char: str) -> None:
        """Type a single character."""
        import shlex
        escaped = shlex.quote(char)
        script = f"osascript -e 'tell application \"System Events\" to keystroke {escaped}'"
        subprocess.run(script, shell=True, capture_output=True, timeout=1.0)

    def _press_key(self, key: str) -> None:
        """Press a keyboard key."""
        script = f"osascript -e 'tell application \"System Events\" to key code {key}'"
        subprocess.run(script, shell=True, capture_output=True, timeout=1.0)
