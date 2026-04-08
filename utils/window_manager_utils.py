"""Window manager utilities for RabAI AutoClick.

Provides:
- Window arrangement helpers
- Window snapping
- Multi-monitor support
"""

from __future__ import annotations

from typing import (
    Dict,
    List,
    NamedTuple,
    Optional,
    Tuple,
)


class Rect(NamedTuple):
    """A rectangle."""
    x: int
    y: int
    width: int
    height: int


class WindowManager:
    """Manage window layouts and arrangements."""

    def __init__(self) -> None:
        self._layouts: Dict[str, List[Rect]] = {}

    def register_layout(self, name: str, rects: List[Rect]) -> None:
        """Register a named layout.

        Args:
            name: Layout name.
            rects: List of window rectangles.
        """
        self._layouts[name] = rects

    def get_layout(self, name: str) -> List[Rect]:
        """Get a registered layout.

        Args:
            name: Layout name.

        Returns:
            List of rectangles or empty list.
        """
        return self._layouts.get(name, [])

    def snap_left(self, window_id: int, screen_rect: Rect) -> Rect:
        """Snap window to left half of screen.

        Args:
            window_id: Window ID.
            screen_rect: Screen bounds.

        Returns:
            New window rectangle.
        """
        return Rect(
            x=screen_rect.x,
            y=screen_rect.y,
            width=screen_rect.width // 2,
            height=screen_rect.height,
        )

    def snap_right(self, window_id: int, screen_rect: Rect) -> Rect:
        """Snap window to right half of screen.

        Args:
            window_id: Window ID.
            screen_rect: Screen bounds.

        Returns:
            New window rectangle.
        """
        half = screen_rect.width // 2
        return Rect(
            x=screen_rect.x + half,
            y=screen_rect.y,
            width=half,
            height=screen_rect.height,
        )

    def snap_maximize(self, window_id: int, screen_rect: Rect) -> Rect:
        """Maximize window to fill screen.

        Args:
            window_id: Window ID.
            screen_rect: Screen bounds.

        Returns:
            New window rectangle.
        """
        return screen_rect

    def snap_center(
        self,
        window_id: int,
        screen_rect: Rect,
        width: int,
        height: int,
    ) -> Rect:
        """Center window on screen.

        Args:
            window_id: Window ID.
            screen_rect: Screen bounds.
            width: Window width.
            height: Window height.

        Returns:
            New window rectangle.
        """
        return Rect(
            x=screen_rect.x + (screen_rect.width - width) // 2,
            y=screen_rect.y + (screen_rect.height - height) // 2,
            width=width,
            height=height,
        )


__all__ = [
    "Rect",
    "WindowManager",
]
