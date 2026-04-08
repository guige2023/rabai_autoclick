"""
Coordinate Transform Utilities.

Transform coordinates between different reference frames including
screen coordinates, window coordinates, and accessibility tree coordinates.

Usage:
    from utils.coordinate_transform_utils import CoordinateTransformer

    transformer = CoordinateTransformer(display_manager)
    screen_coords = transformer.tree_to_screen(tree_coords, element)
"""

from __future__ import annotations

from typing import Optional, Tuple, Dict, Any, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    pass


@dataclass
class Point:
    """A 2D point."""
    x: float
    y: float

    def __add__(self, other: "Point") -> "Point":
        return Point(self.x + other.x, self.y + other.y)

    def __sub__(self, other: "Point") -> "Point":
        return Point(self.x - other.x, self.y - other.y)

    def __mul__(self, scale: float) -> "Point":
        return Point(self.x * scale, self.y * scale)


@dataclass
class Rect:
    """A rectangle defined by origin and size."""
    x: float
    y: float
    width: float
    height: float

    @property
    def origin(self) -> Point:
        return Point(self.x, self.y)

    @property
    def center(self) -> Point:
        return Point(self.x + self.width / 2, self.y + self.height / 2)

    @property
    def top_left(self) -> Point:
        return Point(self.x, self.y)

    @property
    def bottom_right(self) -> Point:
        return Point(self.x + self.width, self.y + self.height)


class CoordinateTransformer:
    """
    Transform coordinates between different reference frames.

    Supports transformations between:
    - Screen coordinates (absolute)
    - Window coordinates (relative to window origin)
    - Element coordinates (relative to element origin)
    - Tree coordinates (accessibility tree position)

    Example:
        transformer = CoordinateTransformer()
        screen_point = transformer.tree_to_screen(tree_point, element)
    """

    def __init__(self, display_manager: Optional[Any] = None) -> None:
        """
        Initialize the coordinate transformer.

        Args:
            display_manager: Optional DisplayManager instance.
        """
        self._display_manager = display_manager

    def screen_to_window(
        self,
        x: float,
        y: float,
        window_bounds: Tuple[int, int, int, int],
    ) -> Tuple[float, float]:
        """
        Transform screen coordinates to window-relative coordinates.

        Args:
            x: Screen X coordinate.
            y: Screen Y coordinate.
            window_bounds: (x, y, width, height) of the window.

        Returns:
            (x, y) in window-relative coordinates.
        """
        wx, wy, ww, wh = window_bounds
        return (x - wx, y - wy)

    def window_to_screen(
        self,
        x: float,
        y: float,
        window_bounds: Tuple[int, int, int, int],
    ) -> Tuple[float, float]:
        """
        Transform window coordinates to screen coordinates.

        Args:
            x: Window X coordinate.
            y: Window Y coordinate.
            window_bounds: (x, y, width, height) of the window.

        Returns:
            (x, y) in screen coordinates.
        """
        wx, wy, ww, wh = window_bounds
        return (x + wx, y + wy)

    def element_to_window(
        self,
        x: float,
        y: float,
        element_bounds: Tuple[int, int, int, int],
    ) -> Tuple[float, float]:
        """
        Transform element coordinates to window coordinates.

        Args:
            x: Element X coordinate.
            y: Element Y coordinate.
            element_bounds: (x, y, width, height) of the element.

        Returns:
            (x, y) in window coordinates.
        """
        ex, ey, ew, eh = element_bounds
        return (x - ex, y - ey)

    def window_to_element(
        self,
        x: float,
        y: float,
        element_bounds: Tuple[int, int, int, int],
    ) -> Tuple[float, float]:
        """
        Transform window coordinates to element coordinates.

        Args:
            x: Window X coordinate.
            y: Window Y coordinate.
            element_bounds: (x, y, width, height) of the element.

        Returns:
            (x, y) in element coordinates.
        """
        ex, ey, ew, eh = element_bounds
        return (x + ex, y + ey)

    def tree_to_screen(
        self,
        x: float,
        y: float,
        element: Dict[str, Any],
    ) -> Tuple[float, float]:
        """
        Transform tree coordinates (from accessibility tree) to screen coordinates.

        Args:
            x: Tree X coordinate.
            y: Tree Y coordinate.
            element: Element dictionary with bounds.

        Returns:
            (x, y) in screen coordinates.
        """
        bounds = element.get("rect", {})
        ex = bounds.get("x", 0)
        ey = bounds.get("y", 0)
        return (ex + x, ey + y)

    def screen_to_tree(
        self,
        x: float,
        y: float,
        element: Dict[str, Any],
    ) -> Tuple[float, float]:
        """
        Transform screen coordinates to tree coordinates.

        Args:
            x: Screen X coordinate.
            y: Screen Y coordinate.
            element: Element dictionary with bounds.

        Returns:
            (x, y) in tree coordinates.
        """
        bounds = element.get("rect", {})
        ex = bounds.get("x", 0)
        ey = bounds.get("y", 0)
        return (x - ex, y - ey)

    def transform_point(
        self,
        point: Point,
        from_frame: str,
        to_frame: str,
        **context,
    ) -> Point:
        """
        Transform a point between reference frames.

        Args:
            point: Point to transform.
            from_frame: Source frame ("screen", "window", "element").
            to_frame: Target frame.
            **context: Additional context (window_bounds, element, etc.).

        Returns:
            Transformed Point.
        """
        x, y = point.x, point.y

        if from_frame == "screen" and to_frame == "window":
            wb = context.get("window_bounds", (0, 0, 0, 0))
            x, y = self.screen_to_window(x, y, wb)
        elif from_frame == "window" and to_frame == "screen":
            wb = context.get("window_bounds", (0, 0, 0, 0))
            x, y = self.window_to_screen(x, y, wb)
        elif from_frame == "element" and to_frame == "window":
            eb = context.get("element_bounds", (0, 0, 0, 0))
            x, y = self.element_to_window(x, y, eb)
        elif from_frame == "window" and to_frame == "element":
            eb = context.get("element_bounds", (0, 0, 0, 0))
            x, y = self.window_to_element(x, y, eb)

        return Point(x, y)

    def translate_across_displays(
        self,
        x: float,
        y: float,
        from_display: int,
        to_display: int,
    ) -> Tuple[float, float]:
        """
        Translate coordinates from one display to another.

        Args:
            x: Source X coordinate.
            y: Source Y coordinate.
            from_display: Source display ID.
            to_display: Target display ID.

        Returns:
            Translated (x, y) coordinates.
        """
        if self._display_manager is None:
            return (x, y)

        from_d = self._display_manager.get_display_by_id(from_display)
        to_d = self._display_manager.get_display_by_id(to_display)

        if from_d is None or to_d is None:
            return (x, y)

        rel_x = x - from_d.x
        rel_y = y - from_d.y

        return (to_d.x + rel_x, to_d.y + rel_y)
