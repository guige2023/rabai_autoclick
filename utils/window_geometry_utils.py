"""Window Geometry and Bounds Utilities.

Geometry calculations for window positioning and arrangement.
Supports multi-monitor setups, alignment, and spatial queries.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional


class Anchor(Enum):
    """Anchor points for positioning."""

    TOP_LEFT = auto()
    TOP_CENTER = auto()
    TOP_RIGHT = auto()
    MIDDLE_LEFT = auto()
    CENTER = auto()
    MIDDLE_RIGHT = auto()
    BOTTOM_LEFT = auto()
    BOTTOM_CENTER = auto()
    BOTTOM_RIGHT = auto()


class Alignment(Enum):
    """Alignment options."""

    LEFT = auto()
    CENTER_H = auto()
    RIGHT = auto()
    TOP = auto()
    CENTER_V = auto()
    BOTTOM = auto()


@dataclass
class Rectangle:
    """2D rectangle for bounds representation.

    Attributes:
        x: Left edge X coordinate.
        y: Top edge Y coordinate.
        width: Rectangle width.
        height: Rectangle height.
    """

    x: float
    y: float
    width: float
    height: float

    @property
    def left(self) -> float:
        """Left edge X coordinate."""
        return self.x

    @property
    def right(self) -> float:
        """Right edge X coordinate."""
        return self.x + self.width

    @property
    def top(self) -> float:
        """Top edge Y coordinate."""
        return self.y

    @property
    def bottom(self) -> float:
        """Bottom edge Y coordinate."""
        return self.y + self.height

    @property
    def center_x(self) -> float:
        """Center X coordinate."""
        return self.x + self.width / 2

    @property
    def center_y(self) -> float:
        """Center Y coordinate."""
        return self.y + self.height / 2

    @property
    def center(self) -> tuple[float, float]:
        """Center coordinates."""
        return (self.center_x, self.center_y)

    @property
    def area(self) -> float:
        """Rectangle area."""
        return self.width * self.height

    @property
    def perimeter(self) -> float:
        """Rectangle perimeter."""
        return 2 * (self.width + self.height)

    def contains_point(self, px: float, py: float) -> bool:
        """Check if point is inside rectangle."""
        return self.left <= px <= self.right and self.top <= py <= self.bottom

    def contains_rect(self, other: "Rectangle") -> bool:
        """Check if another rectangle is inside this one."""
        return (
            self.left <= other.left
            and self.right >= other.right
            and self.top <= other.top
            and self.bottom >= other.bottom
        )

    def intersects(self, other: "Rectangle") -> bool:
        """Check if rectangles intersect."""
        return not (
            self.right < other.left
            or self.left > other.right
            or self.bottom < other.top
            or self.top > other.bottom
        )

    def intersection(self, other: "Rectangle") -> Optional["Rectangle"]:
        """Get intersection rectangle."""
        if not self.intersects(other):
            return None

        left = max(self.left, other.left)
        top = max(self.top, other.top)
        right = min(self.right, other.right)
        bottom = min(self.bottom, other.bottom)

        return Rectangle(
            x=left,
            y=top,
            width=right - left,
            height=bottom - top,
        )

    def distance_to(self, other: "Rectangle") -> float:
        """Calculate minimum distance to another rectangle."""
        if self.intersects(other):
            return 0.0

        dx = max(0, max(other.left - self.right, self.left - other.right))
        dy = max(0, max(other.top - self.bottom, self.top - other.bottom))
        return math.sqrt(dx * dx + dy * dy)


@dataclass
class MonitorInfo:
    """Information about a display monitor.

    Attributes:
        index: Monitor index.
        bounds: Monitor bounds.
        work_area: Usable area (excluding taskbar, etc.).
        is_primary: Whether this is the primary monitor.
        scale_factor: DPI scale factor.
    """

    index: int
    bounds: Rectangle
    work_area: Rectangle
    is_primary: bool = False
    scale_factor: float = 1.0


class GeometryCalculator:
    """Calculates geometry for window operations.

    Example:
        calc = GeometryCalculator()
        centered = calc.center_in_region(window_bounds, screen_bounds)
    """

    @staticmethod
    def center_in_region(
        inner: Rectangle,
        outer: Rectangle,
    ) -> Rectangle:
        """Center a rectangle within another.

        Args:
            inner: Rectangle to center.
            outer: Container rectangle.

        Returns:
            Centered rectangle.
        """
        x = outer.center_x - inner.width / 2
        y = outer.center_y - inner.height / 2
        return Rectangle(x=x, y=y, width=inner.width, height=inner.height)

    @staticmethod
    def align_to_anchor(
        inner: Rectangle,
        outer: Rectangle,
        anchor: Anchor,
    ) -> Rectangle:
        """Align a rectangle to an anchor point.

        Args:
            inner: Rectangle to align.
            outer: Container rectangle.
            anchor: Anchor point to align to.

        Returns:
            Aligned rectangle.
        """
        if anchor == Anchor.TOP_LEFT:
            return Rectangle(x=outer.left, y=outer.top, width=inner.width, height=inner.height)
        elif anchor == Anchor.TOP_CENTER:
            return Rectangle(x=outer.center_x - inner.width / 2, y=outer.top, width=inner.width, height=inner.height)
        elif anchor == Anchor.TOP_RIGHT:
            return Rectangle(x=outer.right - inner.width, y=outer.top, width=inner.width, height=inner.height)
        elif anchor == Anchor.MIDDLE_LEFT:
            return Rectangle(x=outer.left, y=outer.center_y - inner.height / 2, width=inner.width, height=inner.height)
        elif anchor == Anchor.CENTER:
            return Rectangle(x=outer.center_x - inner.width / 2, y=outer.center_y - inner.height / 2, width=inner.width, height=inner.height)
        elif anchor == Anchor.MIDDLE_RIGHT:
            return Rectangle(x=outer.right - inner.width, y=outer.center_y - inner.height / 2, width=inner.width, height=inner.height)
        elif anchor == Anchor.BOTTOM_LEFT:
            return Rectangle(x=outer.left, y=outer.bottom - inner.height, width=inner.width, height=inner.height)
        elif anchor == Anchor.BOTTOM_CENTER:
            return Rectangle(x=outer.center_x - inner.width / 2, y=outer.bottom - inner.height, width=inner.width, height=inner.height)
        elif anchor == Anchor.BOTTOM_RIGHT:
            return Rectangle(x=outer.right - inner.width, y=outer.bottom - inner.height, width=inner.width, height=inner.height)
        return inner

    @staticmethod
    def align_windows(
        windows: list[Rectangle],
        alignment: Alignment,
        gap: float = 0,
    ) -> list[Rectangle]:
        """Align multiple windows.

        Args:
            windows: List of window rectangles.
            alignment: Alignment direction.
            gap: Gap between windows.

        Returns:
            List of aligned rectangles.
        """
        if not windows:
            return []

        if alignment == Alignment.LEFT:
            min_x = min(w.x for w in windows)
            return [Rectangle(x=min_x, y=w.y, width=w.width, height=w.height) for w in windows]
        elif alignment == Alignment.RIGHT:
            max_x = max(w.right for w in windows) - windows[0].width
            return [Rectangle(x=max_x, y=w.y, width=w.width, height=w.height) for w in windows]
        elif alignment == Alignment.CENTER_H:
            avg_center_y = sum(w.center_y for w in windows) / len(windows)
            return [Rectangle(x=w.x, y=avg_center_y - w.height / 2, width=w.width, height=w.height) for w in windows]
        elif alignment == Alignment.TOP:
            min_y = min(w.y for w in windows)
            return [Rectangle(x=w.x, y=min_y, width=w.width, height=w.height) for w in windows]
        elif alignment == Alignment.BOTTOM:
            max_y = max(w.bottom for w in windows) - windows[0].height
            return [Rectangle(x=w.x, y=max_y, width=w.width, height=w.height) for w in windows]
        elif alignment == Alignment.CENTER_V:
            avg_center_x = sum(w.center_x for w in windows) / len(windows)
            return [Rectangle(x=avg_center_x - w.width / 2, y=w.y, width=w.width, height=w.height) for w in windows]

        return windows

    @staticmethod
    def tile_horizontal(
        windows: list[Rectangle],
        container: Rectangle,
        gap: float = 0,
    ) -> list[Rectangle]:
        """Tile windows horizontally.

        Args:
            windows: List of window rectangles.
            container: Container rectangle.
            gap: Gap between windows.

        Returns:
            List of tiled rectangles.
        """
        if not windows:
            return []

        total_width = container.width - gap * (len(windows) - 1)
        window_width = total_width / len(windows)

        results = []
        x = container.left
        for w in windows:
            results.append(Rectangle(
                x=x,
                y=container.top,
                width=window_width,
                height=container.height,
            ))
            x += window_width + gap

        return results

    @staticmethod
    def tile_vertical(
        windows: list[Rectangle],
        container: Rectangle,
        gap: float = 0,
    ) -> list[Rectangle]:
        """Tile windows vertically.

        Args:
            windows: List of window rectangles.
            container: Container rectangle.
            gap: Gap between windows.

        Returns:
            List of tiled rectangles.
        """
        if not windows:
            return []

        total_height = container.height - gap * (len(windows) - 1)
        window_height = total_height / len(windows)

        results = []
        y = container.top
        for w in windows:
            results.append(Rectangle(
                x=container.left,
                y=y,
                width=container.width,
                height=window_height,
            ))
            y += window_height + gap

        return results

    @staticmethod
    def snap_to_grid(
        rect: Rectangle,
        grid_width: float,
        grid_height: float,
    ) -> Rectangle:
        """Snap rectangle to a grid.

        Args:
            rect: Rectangle to snap.
            grid_width: Grid cell width.
            grid_height: Grid cell height.

        Returns:
            Snapped rectangle.
        """
        return Rectangle(
            x=round(rect.x / grid_width) * grid_width,
            y=round(rect.y / grid_height) * grid_height,
            width=rect.width,
            height=rect.height,
        )

    @staticmethod
    def resize_to_aspect(
        rect: Rectangle,
        aspect_ratio: float,
        anchor: Anchor = Anchor.CENTER,
    ) -> Rectangle:
        """Resize rectangle to a specific aspect ratio.

        Args:
            rect: Rectangle to resize.
            aspect_ratio: Target width/height ratio.
            anchor: Anchor point for resizing.

        Returns:
            Resized rectangle.
        """
        new_width = rect.height * aspect_ratio
        new_height = rect.width / aspect_ratio

        if abs(new_width - rect.width) < abs(new_height - rect.height):
            new_height = rect.height
        else:
            new_width = rect.width

        x, y = rect.x, rect.y

        if anchor in (Anchor.MIDDLE_LEFT, Anchor.CENTER, Anchor.MIDDLE_RIGHT):
            y = rect.center_y - new_height / 2
        elif anchor in (Anchor.BOTTOM_LEFT, Anchor.BOTTOM_CENTER, Anchor.BOTTOM_RIGHT):
            y = rect.bottom - new_height

        if anchor in (Anchor.TOP_CENTER, Anchor.CENTER, Anchor.BOTTOM_CENTER):
            x = rect.center_x - new_width / 2
        elif anchor in (Anchor.TOP_RIGHT, Anchor.MIDDLE_RIGHT, Anchor.BOTTOM_RIGHT):
            x = rect.right - new_width

        return Rectangle(x=x, y=y, width=new_width, height=new_height)


class MultiMonitorManager:
    """Manages multi-monitor configurations.

    Example:
        mgr = MultiMonitorManager()
        monitors = mgr.get_all_monitors()
        primary = mgr.get_primary_monitor()
    """

    def __init__(self):
        """Initialize the monitor manager."""
        self._monitors: list[MonitorInfo] = []

    def add_monitor(self, monitor: MonitorInfo) -> None:
        """Add a monitor.

        Args:
            monitor: MonitorInfo to add.
        """
        self._monitors.append(monitor)

    def get_monitor_count(self) -> int:
        """Get number of monitors."""
        return len(self._monitors)

    def get_monitor(self, index: int) -> Optional[MonitorInfo]:
        """Get monitor by index.

        Args:
            index: Monitor index.

        Returns:
            MonitorInfo or None.
        """
        return self._monitors[index] if index < len(self._monitors) else None

    def get_primary_monitor(self) -> Optional[MonitorInfo]:
        """Get the primary monitor.

        Returns:
            Primary MonitorInfo or None.
        """
        for m in self._monitors:
            if m.is_primary:
                return m
        return self._monitors[0] if self._monitors else None

    def get_monitor_at_point(self, x: float, y: float) -> Optional[MonitorInfo]:
        """Get monitor containing a point.

        Args:
            x: X coordinate.
            y: Y coordinate.

        Returns:
            MonitorInfo or None.
        """
        for m in self._monitors:
            if m.bounds.contains_point(x, y):
                return m
        return None

    def get_all_monitors(self) -> list[MonitorInfo]:
        """Get all monitors.

        Returns:
            List of MonitorInfo.
        """
        return list(self._monitors)

    def get_workspace_bounds(self) -> Rectangle:
        """Get bounds covering all monitors.

        Returns:
            Rectangle covering all monitors.
        """
        if not self._monitors:
            return Rectangle(x=0, y=0, width=1920, height=1080)

        min_x = min(m.bounds.left for m in self._monitors)
        min_y = min(m.bounds.top for m in self._monitors)
        max_x = max(m.bounds.right for m in self._monitors)
        max_y = max(m.bounds.bottom for m in self._monitors)

        return Rectangle(
            x=min_x,
            y=min_y,
            width=max_x - min_x,
            height=max_y - min_y,
        )
