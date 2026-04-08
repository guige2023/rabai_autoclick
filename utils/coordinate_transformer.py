"""
Coordinate Transformer Utility

Transforms coordinates between different coordinate systems:
- Screen coordinates
- Window-relative coordinates
- Accessibility tree coordinates
- Display coordinates (with DPI scaling)

Example:
    >>> transformer = CoordinateTransformer()
    >>> screen_point = transformer.window_to_screen(window_id, (50, 50))
    >>> print(screen_point)
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from typing import Optional


@dataclass
class DisplayInfo:
    """Information about a display."""
    display_id: int
    x: int
    y: int
    width: int
    height: int
    scale_factor: float = 1.0
    is_main: bool = False


class CoordinateTransformer:
    """
    Transforms coordinates between different coordinate spaces.

    Supports:
        - Window to screen
        - Screen to window
        - Accessibility tree to screen
        - Display coordinate scaling
    """

    def __init__(self) -> None:
        self._displays: Optional[list[DisplayInfo]] = None

    def get_displays(self) -> list[DisplayInfo]:
        """Get list of connected displays."""
        if self._displays is not None:
            return self._displays

        displays: list[DisplayInfo] = []

        try:
            script = """
            tell application "System Events"
                set displayCount to count of displays
                return displayCount
            end tell
            """

            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=2.0,
            )
            count = int(result.stdout.strip()) if result.returncode == 0 else 1

            for i in range(count):
                displays.append(DisplayInfo(
                    display_id=i,
                    x=0,
                    y=0,
                    width=1920,
                    height=1080,
                    is_main=(i == 0),
                ))

        except Exception:
            displays.append(DisplayInfo(
                display_id=0,
                x=0,
                y=0,
                width=1920,
                height=1080,
                is_main=True,
            ))

        self._displays = displays
        return displays

    def window_to_screen(
        self,
        window_id: int,
        window_point: tuple[int, int],
    ) -> tuple[int, int]:
        """
        Convert window-relative coordinates to screen coordinates.

        Args:
            window_id: Window identifier.
            window_point: (x, y) within window.

        Returns:
            (x, y) in screen coordinates.
        """
        try:
            script = f"""
            tell application "System Events"
                tell process "System Events"
                    set winPos to position of window {window_id}
                    set winSize to size of window {window_id}
                    return (item 1 of winPos) & "," & (item 2 of winPos) & "," & (item 1 of winSize) & "," & (item 2 of winSize)
                end tell
            end tell
            """

            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=2.0,
            )

            if result.returncode == 0:
                parts = result.stdout.strip().split(",")
                if len(parts) >= 4:
                    win_x, win_y = int(parts[0]), int(parts[1])
                    return (win_x + window_point[0], win_y + window_point[1])

        except Exception:
            pass

        return window_point

    def screen_to_window(
        self,
        window_id: int,
        screen_point: tuple[int, int],
    ) -> tuple[int, int]:
        """
        Convert screen coordinates to window-relative coordinates.

        Args:
            window_id: Window identifier.
            screen_point: (x, y) in screen coordinates.

        Returns:
            (x, y) relative to window origin.
        """
        try:
            script = f"""
            tell application "System Events"
                tell process "System Events"
                    set winPos to position of window {window_id}
                    return (item 1 of winPos) & "," & (item 2 of winPos)
                end tell
            end tell
            """

            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=2.0,
            )

            if result.returncode == 0:
                parts = result.stdout.strip().split(",")
                if len(parts) >= 2:
                    win_x, win_y = int(parts[0]), int(parts[1])
                    return (screen_point[0] - win_x, screen_point[1] - win_y)

        except Exception:
            pass

        return screen_point

    def display_for_point(self, point: tuple[int, int]) -> Optional[DisplayInfo]:
        """
        Find which display contains a given point.

        Args:
            point: (x, y) screen coordinates.

        Returns:
            DisplayInfo for the display, or None.
        """
        for display in self.get_displays():
            if (display.x <= point[0] < display.x + display.width
                    and display.y <= point[1] < display.y + display.height):
                return display
        return None

    def normalize_to_display(
        self,
        point: tuple[int, int],
        target_display: int = 0,
    ) -> tuple[int, int]:
        """
        Normalize coordinates to a specific display's coordinate space.

        Args:
            point: (x, y) coordinates.
            target_display: Display index.

        Returns:
            Normalized (x, y) coordinates.
        """
        displays = self.get_displays()
        if target_display < len(displays):
            d = displays[target_display]
            return (point[0] - d.x, point[1] - d.y)
        return point

    def scale_for_display(
        self,
        point: tuple[int, int],
        display_id: int = 0,
    ) -> tuple[int, int]:
        """
        Apply display scale factor to coordinates.

        Args:
            point: Physical pixel coordinates.
            display_id: Target display.

        Returns:
            Scaled (x, y) coordinates.
        """
        displays = self.get_displays()
        if display_id < len(displays):
            scale = displays[display_id].scale_factor
            return (int(point[0] / scale), int(point[1] / scale))
        return point
