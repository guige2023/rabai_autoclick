"""
Viewport Manager Action Module.

Manages viewport state including scroll position, zoom level,
viewport dimensions, and virtual viewport for large page handling.
"""

from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class ViewportState:
    """State of a viewport."""
    x: int
    y: int
    width: int
    height: int
    scroll_x: int
    scroll_y: int
    zoom: float = 1.0
    content_width: int = 0
    content_height: int = 0


class ViewportManager:
    """Manages viewport state and scrolling."""

    def __init__(
        self,
        viewport_width: int,
        viewport_height: int,
    ):
        """
        Initialize viewport manager.

        Args:
            viewport_width: Viewport width in pixels.
            viewport_height: Viewport height in pixels.
        """
        self._viewport_width = viewport_width
        self._viewport_height = viewport_height
        self._scroll_x = 0
        self._scroll_y = 0
        self._content_width = 0
        self._content_height = 0
        self._zoom = 1.0

    def get_state(self) -> ViewportState:
        """Get current viewport state."""
        return ViewportState(
            x=0,
            y=0,
            width=self._viewport_width,
            height=self._viewport_height,
            scroll_x=self._scroll_x,
            scroll_y=self._scroll_y,
            zoom=self._zoom,
            content_width=self._content_width,
            content_height=self._content_height,
        )

    def set_scroll(self, x: int, y: int) -> None:
        """
        Set scroll position.

        Args:
            x: Horizontal scroll offset.
            y: Vertical scroll offset.
        """
        self._scroll_x = max(0, x)
        self._scroll_y = max(0, y)

    def scroll_by(self, dx: int, dy: int) -> None:
        """
        Scroll by delta amount.

        Args:
            dx: Horizontal delta.
            dy: Vertical delta.
        """
        self._scroll_x = max(0, self._scroll_x + dx)
        self._scroll_y = max(0, self._scroll_y + dy)

    def scroll_to_element(
        self,
        element_bounds: Tuple[int, int, int, int],
        offset: int = 0,
    ) -> None:
        """
        Scroll to make an element visible.

        Args:
            element_bounds: Element bounding box (x1, y1, x2, y2).
            offset: Optional offset from viewport edge.
        """
        x1, y1, x2, y2 = element_bounds

        target_y = y1 - offset
        if y2 > self._scroll_y + self._viewport_height:
            target_y = y1 - offset
        if y1 < self._scroll_y:
            target_y = y1 - offset

        target_x = x1 - offset
        if x2 > self._scroll_x + self._viewport_width:
            target_x = x1 - offset
        if x1 < self._scroll_x:
            target_x = x1 - offset

        self.set_scroll(target_x, target_y)

    def is_element_visible(
        self,
        element_bounds: Tuple[int, int, int, int],
    ) -> bool:
        """
        Check if an element is visible in viewport.

        Args:
            element_bounds: Element bounding box.

        Returns:
            True if element is at least partially visible.
        """
        x1, y1, x2, y2 = element_bounds

        view_x2 = self._scroll_x + self._viewport_width
        view_y2 = self._scroll_y + self._viewport_height

        visible_x = x2 > self._scroll_x and x1 < view_x2
        visible_y = y2 > self._scroll_y and y1 < view_y2

        return visible_x and visible_y

    def get_visible_region(self) -> Tuple[int, int, int, int]:
        """
        Get the visible region in page coordinates.

        Returns:
            Tuple of (x1, y1, x2, y2).
        """
        return (
            self._scroll_x,
            self._scroll_y,
            self._scroll_x + self._viewport_width,
            self._scroll_y + self._viewport_height,
        )

    def set_content_size(self, width: int, height: int) -> None:
        """
        Set the total content size.

        Args:
            width: Total content width.
            height: Total content height.
        """
        self._content_width = width
        self._content_height = height

    def set_zoom(self, zoom: float) -> None:
        """
        Set zoom level.

        Args:
            zoom: Zoom factor (1.0 = 100%).
        """
        self._zoom = max(0.1, min(zoom, 10.0))

    def can_scroll_horizontal(self) -> bool:
        """Check if horizontal scrolling is possible."""
        return self._content_width > self._viewport_width

    def can_scroll_vertical(self) -> bool:
        """Check if vertical scrolling is possible."""
        return self._content_height > self._viewport_height

    def get_max_scroll_x(self) -> int:
        """Get maximum horizontal scroll position."""
        return max(0, self._content_width - self._viewport_width)

    def get_max_scroll_y(self) -> int:
        """Get maximum vertical scroll position."""
        return max(0, self._content_height - self._viewport_height)
