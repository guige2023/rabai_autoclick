"""
Coordinate Transformer Action Module.

Transforms coordinates between different coordinate systems:
screen, window, viewport, and element-relative coordinates.
"""

from typing import NamedTuple, Optional, Tuple


class Point(NamedTuple):
    """2D point with x and y coordinates."""
    x: float
    y: float


class Rect(NamedTuple):
    """Rectangle defined by top-left and bottom-right corners."""
    x1: float
    y1: float
    x2: float
    y2: float


class CoordinateTransformer:
    """Transforms coordinates between coordinate systems."""

    def __init__(
        self,
        screen_width: int,
        screen_height: int,
        window_x: int = 0,
        window_y: int = 0,
        viewport_x: int = 0,
        viewport_y: int = 0,
    ):
        """
        Initialize transformer with screen and window geometry.

        Args:
            screen_width: Total screen width in pixels.
            screen_height: Total screen height in pixels.
            window_x: Window origin x relative to screen.
            window_y: Window origin y relative to screen.
            viewport_x: Viewport scroll x offset.
            viewport_y: Viewport scroll y offset.
        """
        self.screen_width = screen_width
        self.screen_height = screen_height
        self.window_x = window_x
        self.window_y = window_y
        self.viewport_x = viewport_x
        self.viewport_y = viewport_y

    def screen_to_window(self, point: Point) -> Point:
        """
        Convert screen coordinates to window coordinates.

        Args:
            point: Point in screen coordinates.

        Returns:
            Point in window coordinates.
        """
        return Point(point.x - self.window_x, point.y - self.window_y)

    def window_to_screen(self, point: Point) -> Point:
        """
        Convert window coordinates to screen coordinates.

        Args:
            point: Point in window coordinates.

        Returns:
            Point in screen coordinates.
        """
        return Point(point.x + self.window_x, point.y + self.window_y)

    def window_to_viewport(self, point: Point) -> Point:
        """
        Convert window coordinates to viewport-relative coordinates.

        Args:
            point: Point in window coordinates.

        Returns:
            Point in viewport coordinates.
        """
        return Point(
            point.x + self.viewport_x, point.y + self.viewport_y
        )

    def viewport_to_window(self, point: Point) -> Point:
        """
        Convert viewport coordinates to window coordinates.

        Args:
            point: Point in viewport coordinates.

        Returns:
            Point in window coordinates.
        """
        return Point(
            point.x - self.viewport_x, point.y - self.viewport_y
        )

    def screen_to_viewport(self, point: Point) -> Point:
        """
        Convert screen coordinates to viewport coordinates.

        Args:
            point: Point in screen coordinates.

        Returns:
            Point in viewport coordinates.
        """
        wp = self.screen_to_window(point)
        return self.window_to_viewport(wp)

    def viewport_to_screen(self, point: Point) -> Point:
        """
        Convert viewport coordinates to screen coordinates.

        Args:
            point: Point in viewport coordinates.

        Returns:
            Point in screen coordinates.
        """
        wp = self.viewport_to_window(point)
        return self.window_to_screen(wp)

    def element_to_screen(
        self,
        element_bounds: Rect,
        element_offset: Point,
    ) -> Point:
        """
        Convert element-relative coordinates to screen coordinates.

        Args:
            element_bounds: Bounding box of the element.
            element_offset: Offset within the element.

        Returns:
            Point in screen coordinates.
        """
        elem_x = element_bounds.x1 + element_offset.x
        elem_y = element_bounds.y1 + element_offset.y
        return self.window_to_screen(Point(elem_x, elem_y))

    def screen_to_element(
        self,
        point: Point,
        element_bounds: Rect,
    ) -> Point:
        """
        Convert screen coordinates to element-relative coordinates.

        Args:
            point: Point in screen coordinates.
            element_bounds: Bounding box of the element.

        Returns:
            Point relative to element top-left corner.
        """
        screen_p = self.screen_to_window(point)
        return Point(
            screen_p.x - element_bounds.x1,
            screen_p.y - element_bounds.y1,
        )

    def normalize_point(
        self,
        point: Point,
        source_system: str,
    ) -> Point:
        """
        Normalize a point to 0-1 range.

        Args:
            point: The point to normalize.
            source_system: 'screen', 'window', or 'viewport'.

        Returns:
            Normalized point (0-1 on each axis).
        """
        if source_system == "screen":
            return Point(
                point.x / self.screen_width,
                point.y / self.screen_height,
            )
        elif source_system == "window":
            return Point(
                point.x / self.screen_width,
                point.y / self.screen_height,
            )
        else:
            return Point(
                point.x / self.screen_width,
                point.y / self.screen_height,
            )

    def denormalize_point(
        self,
        normalized: Point,
        target_system: str,
    ) -> Point:
        """
        Convert normalized (0-1) point to target system coordinates.

        Args:
            normalized: Normalized point.
            target_system: 'screen', 'window', or 'viewport'.

        Returns:
            Denormalized point.
        """
        x = normalized.x * self.screen_width
        y = normalized.y * self.screen_height

        if target_system == "screen":
            return Point(x, y)
        elif target_system == "window":
            return Point(x - self.window_x, y - self.window_y)
        else:
            wp = self.screen_to_window(Point(x, y))
            return self.window_to_viewport(wp)
