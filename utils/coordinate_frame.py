"""Coordinate frame utilities for UI automation.

Handles conversion between different coordinate frames:
screen, window, element, and relative coordinates.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional


class CoordinateFrameType(Enum):
    """Types of coordinate frames."""
    SCREEN = auto()      # Absolute screen coordinates
    WINDOW = auto()     # Window-relative coordinates
    ELEMENT = auto()     # Element-relative coordinates
    RELATIVE = auto()    # Normalized 0.0-1.0 coordinates
    PERCENTAGE = auto()  # Percentage of parent dimensions


@dataclass
class CoordinateFrame:
    """A coordinate frame for converting between coordinate spaces.

    Attributes:
        frame_type: The type of this coordinate frame.
        origin_x: X origin offset in parent coordinates.
        origin_y: Y origin offset in parent coordinates.
        scale_x: X scale relative to parent (1.0 = same size).
        scale_y: Y scale relative to parent (1.0 = same size).
        parent: Parent coordinate frame (None for screen frame).
        rotation: Rotation in degrees (positive = counter-clockwise).
        name: Optional name for debugging.
    """
    frame_type: CoordinateFrameType
    origin_x: float = 0.0
    origin_y: float = 0.0
    scale_x: float = 1.0
    scale_y: float = 1.0
    parent: Optional[CoordinateFrame] = None
    rotation: float = 0.0
    name: str = ""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_screen(self, x: float, y: float) -> tuple[float, float]:
        """Convert local coordinates to absolute screen coordinates."""
        if self.parent:
            x, y = self.parent.to_screen(x, y)
        return (
            self.origin_x + x * self.scale_x,
            self.origin_y + y * self.scale_y,
        )

    def from_screen(self, sx: float, sy: float) -> tuple[float, float]:
        """Convert screen coordinates to local coordinates."""
        x = (sx - self.origin_x) / self.scale_x
        y = (sy - self.origin_y) / self.scale_y
        if self.parent:
            x, y = self.parent.from_screen(x, y)
        return (x, y)

    def to_frame(
        self,
        x: float,
        y: float,
        target: CoordinateFrame,
    ) -> tuple[float, float]:
        """Convert coordinates from this frame to another frame."""
        sx, sy = self.to_screen(x, y)
        return target.from_screen(sx, sy)

    def to_relative(self, x: float, y: float) -> tuple[float, float]:
        """Convert local coordinates to normalized 0.0-1.0."""
        return (x, y)

    def from_relative(self, rx: float, ry: float) -> tuple[float, float]:
        """Convert normalized coordinates to local coordinates."""
        return (rx, ry)

    def is_root(self) -> bool:
        """Return True if this is the root (screen) frame."""
        return self.parent is None and self.frame_type == CoordinateFrameType.SCREEN

    def get_root(self) -> CoordinateFrame:
        """Return the root frame of this chain."""
        current = self
        while current.parent:
            current = current.parent
        return current

    def chain_depth(self) -> int:
        """Return the depth of this frame in the chain."""
        depth = 0
        current = self.parent
        while current:
            depth += 1
            current = current.parent
        return depth


@dataclass
class ScreenFrame:
    """Factory and manager for coordinate frames.

    Provides easy creation of window and element frames,
    and utilities for coordinate conversion.
    """
    screen_width: float
    screen_height: float

    def create_screen_frame(self) -> CoordinateFrame:
        """Create the root screen coordinate frame."""
        return CoordinateFrame(
            frame_type=CoordinateFrameType.SCREEN,
            name="screen",
        )

    def create_window_frame(
        self,
        window_x: float,
        window_y: float,
        window_width: float,
        window_height: float,
        parent: Optional[CoordinateFrame] = None,
        name: str = "",
    ) -> CoordinateFrame:
        """Create a window-relative coordinate frame."""
        return CoordinateFrame(
            frame_type=CoordinateFrameType.WINDOW,
            origin_x=window_x,
            origin_y=window_y,
            scale_x=window_width,
            scale_y=window_height,
            parent=parent,
            name=name or "window",
        )

    def create_element_frame(
        self,
        element_x: float,
        element_y: float,
        element_width: float,
        element_height: float,
        parent: CoordinateFrame,
        name: str = "",
    ) -> CoordinateFrame:
        """Create an element-relative coordinate frame."""
        parent_sx, parent_sy = (0.0, 0.0)
        if not parent.is_root():
            parent_sx, parent_sy = parent.origin_x, parent.origin_y

        return CoordinateFrame(
            frame_type=CoordinateFrameType.ELEMENT,
            origin_x=parent_sx + element_x * parent.scale_x,
            origin_y=parent_sy + element_y * parent.scale_y,
            scale_x=element_width * parent.scale_x,
            scale_y=element_height * parent.scale_y,
            parent=parent,
            name=name or "element",
        )

    def create_relative_frame(
        self,
        parent: CoordinateFrame,
        name: str = "",
    ) -> CoordinateFrame:
        """Create a normalized (0.0-1.0) coordinate frame."""
        return CoordinateFrame(
            frame_type=CoordinateFrameType.RELATIVE,
            origin_x=0.0,
            origin_y=0.0,
            scale_x=1.0,
            scale_y=1.0,
            parent=parent,
            name=name or "relative",
        )

    def screen_to_window(
        self,
        sx: float,
        sy: float,
        window_x: float,
        window_y: float,
    ) -> tuple[float, float]:
        """Convert screen coordinates to window coordinates."""
        return (sx - window_x, sy - window_y)

    def window_to_screen(
        self,
        wx: float,
        wy: float,
        window_x: float,
        window_y: float,
    ) -> tuple[float, float]:
        """Convert window coordinates to screen coordinates."""
        return (wx + window_x, wy + window_y)

    def normalize(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
    ) -> tuple[float, float]:
        """Normalize coordinates to 0.0-1.0 range within given bounds."""
        return (x / width, y / height)

    def denormalize(
        self,
        rx: float,
        ry: float,
        width: float,
        height: float,
    ) -> tuple[float, float]:
        """Convert normalized coordinates back to absolute."""
        return (rx * width, ry * height)


class MultiMonitorCoordinateMapper:
    """Handles coordinate mapping across multiple monitors.

    Each monitor is treated as a separate frame with its own
    origin relative to the virtual screen.
    """

    @dataclass
    class Monitor:
        """Represents a physical monitor."""
        name: str
        x: float
        y: float
        width: float
        height: float
        is_primary: bool = False
        scale_factor: float = 1.0

    def __init__(self) -> None:
        """Initialize with no monitors."""
        self._monitors: dict[str, MultiMonitorCoordinateMapper.Monitor] = {}
        self._primary: Optional[str] = None

    def add_monitor(
        self,
        monitor_id: str,
        name: str,
        x: float,
        y: float,
        width: float,
        height: float,
        is_primary: bool = False,
        scale_factor: float = 1.0,
    ) -> None:
        """Register a monitor."""
        self._monitors[monitor_id] = MultiMonitorCoordinateMapper.Monitor(
            name=name, x=x, y=y, width=width, height=height,
            is_primary=is_primary, scale_factor=scale_factor,
        )
        if is_primary:
            self._primary = monitor_id

    def get_monitor_at(self, x: float, y: float) -> Optional[Monitor]:
        """Find which monitor contains the given screen coordinate."""
        for monitor in self._monitors.values():
            if (monitor.x <= x < monitor.x + monitor.width
                    and monitor.y <= y < monitor.y + monitor.height):
                return monitor
        return None

    def to_monitor_local(
        self, x: float, y: float, monitor_id: str
    ) -> Optional[tuple[float, float]]:
        """Convert screen coordinates to monitor-local coordinates."""
        monitor = self._monitors.get(monitor_id)
        if not monitor:
            return None
        return ((x - monitor.x) / monitor.scale_factor,
                (y - monitor.y) / monitor.scale_factor)

    def to_screen(
        self, mx: float, my: float, monitor_id: str
    ) -> Optional[tuple[float, float]]:
        """Convert monitor-local to screen coordinates."""
        monitor = self._monitors.get(monitor_id)
        if not monitor:
            return None
        return (monitor.x + mx * monitor.scale_factor,
                monitor.y + my * monitor.scale_factor)

    def get_primary(self) -> Optional[Monitor]:
        """Return the primary monitor."""
        if self._primary:
            return self._monitors.get(self._primary)
        return next((m for m in self._monitors.values() if m.is_primary), None)
