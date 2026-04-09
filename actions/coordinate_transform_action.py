"""Coordinate transformation action for UI automation.

Handles coordinate transformations between different coordinate spaces:
- Screen coordinates
- Window coordinates
- Element-relative coordinates
- Device-independent pixels (DIP) to physical pixels
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence


@dataclass
class Point:
    """2D point in coordinate space."""
    x: float
    y: float

    def __add__(self, other: Point) -> Point:
        return Point(self.x + other.x, self.y + other.y)

    def __sub__(self, other: Point) -> Point:
        return Point(self.x - other.x, self.y - other.y)

    def __mul__(self, scalar: float) -> Point:
        return Point(self.x * scalar, self.y * scalar)

    def distance_to(self, other: Point) -> float:
        """Euclidean distance to another point."""
        dx = self.x - other.x
        dy = self.y - other.y
        return math.sqrt(dx * dx + dy * dy)

    def angle_to(self, other: Point) -> float:
        """Angle in radians to another point."""
        return math.atan2(other.y - self.y, other.x - self.x)

    def rotate(self, angle: float, center: Point | None = None) -> Point:
        """Rotate point around center by angle (radians)."""
        if center is None:
            center = Point(0, 0)
        cos_a = math.cos(angle)
        sin_a = math.sin(angle)
        dx = self.x - center.x
        dy = self.y - center.y
        return Point(
            center.x + dx * cos_a - dy * sin_a,
            center.y + dx * sin_a + dy * cos_a,
        )


@dataclass
class Rect:
    """2D rectangle defined by origin and size."""
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
    def top_right(self) -> Point:
        return Point(self.x + self.width, self.y)

    @property
    def bottom_left(self) -> Point:
        return Point(self.x, self.y + self.height)

    @property
    def bottom_right(self) -> Point:
        return Point(self.x + self.width, self.y + self.height)

    @property
    def corners(self) -> list[Point]:
        return [self.top_left, self.top_right, self.bottom_right, self.bottom_left]

    def contains(self, point: Point) -> bool:
        """Check if point is inside rectangle."""
        return (self.x <= point.x <= self.x + self.width and
                self.y <= point.y <= self.y + self.height)

    def intersects(self, other: Rect) -> bool:
        """Check if rectangles intersect."""
        return not (self.x + self.width < other.x or
                    other.x + other.width < self.x or
                    self.y + self.height < other.y or
                    other.y + other.height < self.y)

    def intersection(self, other: Rect) -> Rect | None:
        """Get intersection rectangle."""
        if not self.intersects(other):
            return None
        x = max(self.x, other.x)
        y = max(self.y, other.y)
        width = min(self.x + self.width, other.x + other.width) - x
        height = min(self.y + self.height, other.y + other.height) - y
        return Rect(x, y, width, height)

    def union(self, other: Rect) -> Rect:
        """Get bounding rectangle of both."""
        x = min(self.x, other.x)
        y = min(self.y, other.y)
        width = max(self.x + self.width, other.x + other.width) - x
        height = max(self.y + self.height, other.y + other.height) - y
        return Rect(x, y, width, height)

    def expand(self, margin: float) -> Rect:
        """Expand rectangle by margin."""
        return Rect(
            self.x - margin,
            self.y - margin,
            self.width + 2 * margin,
            self.height + 2 * margin,
        )


@dataclass
class DisplayInfo:
    """Display configuration."""
    index: int
    bounds: Rect
    scale_factor: float = 1.0
    is_main: bool = False
    rotation: int = 0  # 0, 90, 180, 270


class CoordinateSpace(Enum):
    """Coordinate space types."""
    SCREEN = "screen"  # Absolute screen coordinates
    WINDOW = "window"  # Relative to window origin
    ELEMENT = "element"  # Relative to element origin
    DIP = "dip"  # Device-independent pixels
    PHYSICAL = "physical"  # Physical pixels


class CoordinateTransformer:
    """Transforms coordinates between different spaces.

    Handles:
    - DIP <-> Physical pixel conversion
    - Screen <-> Window coordinates
    - Multi-display coordinate transformation
    - Coordinate system rotation (for rotated displays)
    """

    def __init__(self, displays: Sequence[DisplayInfo] | None = None):
        self.displays: list[DisplayInfo] = list(displays) if displays else []
        self._current_display: int = 0

    def add_display(self, display: DisplayInfo) -> None:
        """Add a display to the coordinate system."""
        self.displays.append(display)

    def set_current_display(self, index: int) -> None:
        """Set which display is current for transformations."""
        if index < 0 or index >= len(self.displays):
            raise ValueError(f"Invalid display index: {index}")
        self._current_display = index

    @property
    def current_display(self) -> DisplayInfo:
        """Get current display info."""
        if not self.displays:
            raise CoordinateError("No displays configured")
        return self.displays[self._current_display]

    def dip_to_physical(self, point: Point, display_index: int | None = None) -> Point:
        """Convert DIP to physical pixels.

        Args:
            point: Point in DIP coordinates
            display_index: Target display (uses current if None)

        Returns:
            Point in physical pixels
        """
        if display_index is None:
            display_index = self._current_display
        display = self.displays[display_index] if self.displays else DisplayInfo(
            index=0, bounds=Rect(0, 0, 0, 0), scale_factor=1.0
        )
        return Point(point.x * display.scale_factor, point.y * display.scale_factor)

    def physical_to_dip(self, point: Point, display_index: int | None = None) -> Point:
        """Convert physical pixels to DIP.

        Args:
            point: Point in physical pixels
            display_index: Source display (uses current if None)

        Returns:
            Point in DIP
        """
        if display_index is None:
            display_index = self._current_display
        display = self.displays[display_index] if self.displays else DisplayInfo(
            index=0, bounds=Rect(0, 0, 0, 0), scale_factor=1.0
        )
        return Point(point.x / display.scale_factor, point.y / display.scale_factor)

    def screen_to_window(
        self,
        point: Point,
        window_origin: Point,
        window_display: int | None = None,
    ) -> Point:
        """Convert screen coordinates to window-relative.

        Args:
            point: Point in screen coordinates
            window_origin: Window's origin in screen coordinates
            window_display: Display index for coordinate space

        Returns:
            Point relative to window origin
        """
        if window_display is None:
            window_display = self._current_display
        display = self.displays[window_display] if self.displays else DisplayInfo(
            index=0, bounds=Rect(0, 0, 0, 0), scale_factor=1.0
        )

        # Adjust for display origin if multi-monitor
        adj_point = Point(
            point.x - display.bounds.x,
            point.y - display.bounds.y,
        )
        return adj_point - window_origin

    def window_to_screen(
        self,
        point: Point,
        window_origin: Point,
        window_display: int | None = None,
    ) -> Point:
        """Convert window-relative to screen coordinates.

        Args:
            point: Point relative to window origin
            window_origin: Window's origin in screen coordinates
            window_display: Display index for coordinate space

        Returns:
            Point in screen coordinates
        """
        if window_display is None:
            window_display = self._current_display
        display = self.displays[window_display] if self.displays else DisplayInfo(
            index=0, bounds=Rect(0, 0, 0, 0), scale_factor=1.0
        )

        abs_point = point + window_origin
        return Point(abs_point.x + display.bounds.x, abs_point.y + display.bounds.y)

    def element_to_screen(
        self,
        point: Point,
        element_rect: Rect,
    ) -> Point:
        """Convert element-relative to screen coordinates.

        Args:
            point: Point relative to element origin
            element_rect: Element's rectangle in screen coordinates

        Returns:
            Point in screen coordinates
        """
        return Point(point.x + element_rect.x, point.y + element_rect.y)

    def screen_to_element(
        self,
        point: Point,
        element_rect: Rect,
    ) -> Point:
        """Convert screen to element-relative coordinates.

        Args:
            point: Point in screen coordinates
            element_rect: Element's rectangle in screen coordinates

        Returns:
            Point relative to element origin
        """
        return Point(point.x - element_rect.x, point.y - element_rect.y)

    def transform_point(
        self,
        point: Point,
        from_space: CoordinateSpace,
        to_space: CoordinateSpace,
        context: dict | None = None,
    ) -> Point:
        """Transform point between coordinate spaces.

        Args:
            point: Source point
            from_space: Source coordinate space
            to_space: Target coordinate space
            context: Additional context (window_origin, element_rect, display_index)

        Returns:
            Transformed point
        """
        if from_space == to_space:
            return point

        context = context or {}
        display_index = context.get("display_index", self._current_display)

        # Handle DIP <-> Physical
        if from_space == CoordinateSpace.DIP and to_space == CoordinateSpace.PHYSICAL:
            return self.dip_to_physical(point, display_index)
        if from_space == CoordinateSpace.PHYSICAL and to_space == CoordinateSpace.DIP:
            return self.physical_to_dip(point, display_index)

        # Handle screen <-> window
        if from_space == CoordinateSpace.SCREEN and to_space == CoordinateSpace.WINDOW:
            window_origin = context.get("window_origin", Point(0, 0))
            return self.screen_to_window(point, window_origin, display_index)
        if from_space == CoordinateSpace.WINDOW and to_space == CoordinateSpace.SCREEN:
            window_origin = context.get("window_origin", Point(0, 0))
            return self.window_to_screen(point, window_origin, display_index)

        # Handle element <-> screen
        if from_space == CoordinateSpace.ELEMENT and to_space == CoordinateSpace.SCREEN:
            element_rect = context.get("element_rect", Rect(0, 0, 0, 0))
            return self.element_to_screen(point, element_rect)
        if from_space == CoordinateSpace.SCREEN and to_space == CoordinateSpace.ELEMENT:
            element_rect = context.get("element_rect", Rect(0, 0, 0, 0))
            return self.screen_to_element(point, element_rect)

        raise CoordinateError(f"Unsupported transformation: {from_space} -> {to_space}")

    def get_display_for_point(self, point: Point) -> DisplayInfo | None:
        """Find which display contains the point.

        Args:
            point: Point to locate

        Returns:
            Display containing point, or None
        """
        for display in self.displays:
            if display.bounds.contains(point):
                return display
        return None


class CoordinateError(Exception):
    """Coordinate transformation error."""
    pass


def create_coordinate_transformer(displays: Sequence[DisplayInfo] | None = None) -> CoordinateTransformer:
    """Create coordinate transformer with displays."""
    return CoordinateTransformer(displays)
