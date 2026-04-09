"""
Coordinate System Action Module

Provides coordinate transformation, screen space mapping, and
multi-monitor coordinate management for UI automation.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class CoordinateSpace(Enum):
    """Supported coordinate spaces."""

    SCREEN = "screen"
    WINDOW = "window"
    RELATIVE = "relative"
    NORMALIZED = "normalized"


@dataclass
class Point:
    """Represents a 2D point."""

    x: float
    y: float

    def distance_to(self, other: Point) -> float:
        """Calculate Euclidean distance to another point."""
        return math.sqrt((self.x - other.x) ** 2 + (self.y - other.y) ** 2)

    def midpoint_to(self, other: Point) -> Point:
        """Get midpoint between two points."""
        return Point(x=(self.x + other.x) / 2, y=(self.y + other.y) / 2)

    def translate(self, dx: float, dy: float) -> Point:
        """Translate point by offset."""
        return Point(x=self.x + dx, y=self.y + dy)

    def scale(self, sx: float, sy: Optional[float] = None) -> Point:
        """Scale point coordinates."""
        sy = sy or sx
        return Point(x=self.x * sx, y=self.y * sy)

    def rotate(self, angle: float, center: Optional[Point] = None) -> Point:
        """Rotate point around center by angle in radians."""
        center = center or Point(0, 0)
        cos_a = math.cos(angle)
        sin_a = math.sin(angle)
        dx = self.x - center.x
        dy = self.y - center.y
        return Point(
            x=center.x + dx * cos_a - dy * sin_a,
            y=center.y + dx * sin_a + dy * cos_a,
        )


@dataclass
class Rect:
    """Represents a rectangle."""

    x: float
    y: float
    width: float
    height: float

    @property
    def left(self) -> float:
        return self.x

    @property
    def top(self) -> float:
        return self.y

    @property
    def right(self) -> float:
        return self.x + self.width

    @property
    def bottom(self) -> float:
        return self.y + self.height

    @property
    def center(self) -> Point:
        return Point(x=self.x + self.width / 2, y=self.y + self.height / 2)

    @property
    def area(self) -> float:
        return self.width * self.height

    def contains(self, point: Point) -> bool:
        """Check if point is inside rectangle."""
        return self.left <= point.x <= self.right and self.top <= point.y <= self.bottom

    def intersects(self, other: Rect) -> bool:
        """Check if this rectangle intersects another."""
        return (
            self.left < other.right
            and self.right > other.left
            and self.top < other.bottom
            and self.bottom > other.top
        )

    def intersection(self, other: Rect) -> Optional[Rect]:
        """Get intersection rectangle with another."""
        if not self.intersects(other):
            return None
        return Rect(
            x=max(self.left, other.left),
            y=max(self.top, other.top),
            width=min(self.right, other.right) - max(self.left, other.left),
            height=min(self.bottom, other.bottom) - max(self.top, other.top),
        )

    def union(self, other: Rect) -> Rect:
        """Get bounding rectangle that contains both."""
        return Rect(
            x=min(self.left, other.left),
            y=min(self.top, other.top),
            width=max(self.right, other.right) - min(self.left, other.left),
            height=max(self.bottom, other.bottom) - min(self.top, other.top),
        )


@dataclass
class Monitor:
    """Represents a display monitor."""

    id: str
    name: str
    bounds: Rect
    is_primary: bool = False
    scale_factor: float = 1.0
    dpi: int = 96


@dataclass
class CoordinateSystemConfig:
    """Configuration for coordinate system."""

    primary_monitor_id: str = "primary"
    default_dpi: int = 96
    enable_highdpi: bool = True


class CoordinateTransformer:
    """
    Handles coordinate transformations between different spaces.

    Supports transformations between screen, window, relative,
    and normalized coordinate spaces with DPI awareness.
    """

    def __init__(
        self,
        monitors: Optional[List[Monitor]] = None,
        config: Optional[CoordinateSystemConfig] = None,
    ):
        self.config = config or CoordinateSystemConfig()
        self.monitors: Dict[str, Monitor] = {}

        if monitors:
            for monitor in monitors:
                self.monitors[monitor.id] = monitor
        else:
            self._register_default_monitors()

    def _register_default_monitors(self) -> None:
        """Register a default primary monitor."""
        default = Monitor(
            id=self.config.primary_monitor_id,
            name="Primary Display",
            bounds=Rect(x=0, y=0, width=1920, height=1080),
            is_primary=True,
        )
        self.monitors[self.config.primary_monitor_id] = default

    def add_monitor(self, monitor: Monitor) -> None:
        """Add a monitor to the coordinate system."""
        self.monitors[monitor.id] = monitor
        logger.info(f"Added monitor: {monitor.name} ({monitor.id})")

    def remove_monitor(self, monitor_id: str) -> bool:
        """Remove a monitor from the coordinate system."""
        if monitor_id in self.monitors:
            del self.monitors[monitor_id]
            return True
        return False

    def get_monitor_at(self, x: float, y: float) -> Optional[Monitor]:
        """Find the monitor containing the given coordinates."""
        for monitor in self.monitors.values():
            if monitor.bounds.contains(Point(x, y)):
                return monitor
        return None

    def screen_to_window(
        self,
        point: Point,
        window_bounds: Rect,
        source_monitor: Optional[Monitor] = None,
    ) -> Point:
        """
        Transform screen coordinates to window-relative coordinates.

        Args:
            point: Point in screen coordinates
            window_bounds: Window bounding rectangle
            source_monitor: Source monitor (auto-detected if None)

        Returns:
            Point in window-relative coordinates
        """
        return Point(
            x=point.x - window_bounds.x,
            y=point.y - window_bounds.y,
        )

    def window_to_screen(
        self,
        point: Point,
        window_bounds: Rect,
    ) -> Point:
        """
        Transform window-relative coordinates to screen coordinates.

        Args:
            point: Point in window-relative coordinates
            window_bounds: Window bounding rectangle

        Returns:
            Point in screen coordinates
        """
        return Point(
            x=point.x + window_bounds.x,
            y=point.y + window_bounds.y,
        )

    def normalize(
        self,
        point: Point,
        bounds: Rect,
    ) -> Point:
        """
        Normalize coordinates to [0, 1] range.

        Args:
            point: Point in absolute coordinates
            bounds: Reference bounding rectangle

        Returns:
            Normalized point
        """
        return Point(
            x=(point.x - bounds.x) / bounds.width if bounds.width > 0 else 0,
            y=(point.y - bounds.y) / bounds.height if bounds.height > 0 else 0,
        )

    def denormalize(
        self,
        point: Point,
        bounds: Rect,
    ) -> Point:
        """
        Transform normalized [0, 1] coordinates to absolute coordinates.

        Args:
            point: Normalized point
            bounds: Target bounding rectangle

        Returns:
            Absolute point
        """
        return Point(
            x=bounds.x + point.x * bounds.width,
            y=bounds.y + point.y * bounds.height,
        )

    def transform_point(
        self,
        point: Point,
        from_space: CoordinateSpace,
        to_space: CoordinateSpace,
        context: Optional[Dict[str, Any]] = None,
    ) -> Point:
        """
        General coordinate transformation between spaces.

        Args:
            point: Source point
            from_space: Source coordinate space
            to_space: Target coordinate space
            context: Additional context (window_bounds, monitor_id, etc.)

        Returns:
            Transformed point
        """
        context = context or {}

        if from_space == to_space:
            return point

        if from_space == CoordinateSpace.SCREEN and to_space == CoordinateSpace.WINDOW:
            window_bounds = context.get("window_bounds", Rect(0, 0, 0, 0))
            return self.screen_to_window(point, window_bounds)

        if from_space == CoordinateSpace.WINDOW and to_space == CoordinateSpace.SCREEN:
            window_bounds = context.get("window_bounds", Rect(0, 0, 0, 0))
            return self.window_to_screen(point, window_bounds)

        if from_space == CoordinateSpace.NORMALIZED:
            bounds = context.get("bounds", Rect(0, 0, 1920, 1080))
            return self.denormalize(point, bounds)

        if to_space == CoordinateSpace.NORMALIZED:
            bounds = context.get("bounds", Rect(0, 0, 1920, 1080))
            return self.normalize(point, bounds)

        if from_space == CoordinateSpace.RELATIVE:
            monitor = context.get("monitor") or self.get_monitor_at(point.x, point.y)
            if monitor:
                return Point(
                    x=point.x / monitor.bounds.width,
                    y=point.y / monitor.bounds.height,
                )

        logger.warning(f"Unsupported transformation: {from_space} -> {to_space}")
        return point

    def get_visible_area(
        self,
        window_bounds: Rect,
        monitor_id: Optional[str] = None,
    ) -> Rect:
        """
        Get the visible portion of a window on its monitor.

        Args:
            window_bounds: Window bounding rectangle
            monitor_id: Optional specific monitor ID

        Returns:
            Visible rectangle
        """
        if monitor_id and monitor_id in self.monitors:
            monitor = self.monitors[monitor_id]
            return window_bounds.intersection(monitor.bounds) or window_bounds

        for monitor in self.monitors.values():
            visible = window_bounds.intersection(monitor.bounds)
            if visible:
                return visible

        return window_bounds


def create_coordinate_transformer(
    monitors: Optional[List[Monitor]] = None,
) -> CoordinateTransformer:
    """Factory function to create a CoordinateTransformer."""
    return CoordinateTransformer(monitors=monitors)
