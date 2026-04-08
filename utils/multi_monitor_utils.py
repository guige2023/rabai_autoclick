"""Multi-monitor utilities for UI automation.

Handles coordinate mapping, monitor detection, and window placement
across multiple monitors.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional


class MonitorOrientation(Enum):
    """Monitor orientation."""
    LANDSCAPE = auto()
    PORTRAIT = auto()
    LANDSCAPE_FLIPPED = auto()
    PORTRAIT_FLIPPED = auto()


class MonitorConnectionType(Enum):
    """Monitor connection type."""
    INTERNAL = auto()
    HDMI = auto()
    DISPLAYPORT = auto()
    USB_C = auto()
    UNKNOWN = auto()


@dataclass
class MonitorInfo:
    """Information about a physical monitor.

    Attributes:
        monitor_id: Unique identifier.
        name: Display name.
        x: Left edge X coordinate (in virtual screen space).
        y: Top edge Y coordinate (in virtual screen space).
        width: Width in pixels.
        height: Height in pixels.
        scale_factor: Display scale factor (e.g., 2.0 for Retina).
        is_primary: Whether this is the primary monitor.
        orientation: Monitor orientation.
        connection_type: How the monitor is connected.
        work_x: Left edge of work area.
        work_y: Top edge of work area.
        work_width: Width of work area (excludes taskbar, etc.).
        work_height: Height of work area.
    """
    monitor_id: str
    name: str
    x: float = 0.0
    y: float = 0.0
    width: float = 1920.0
    height: float = 1080.0
    scale_factor: float = 1.0
    is_primary: bool = False
    orientation: MonitorOrientation = MonitorOrientation.LANDSCAPE
    connection_type: MonitorConnectionType = MonitorConnectionType.UNKNOWN
    work_x: float = 0.0
    work_y: float = 0.0
    work_width: float = 1920.0
    work_height: float = 1080.0
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    @property
    def x2(self) -> float:
        """Right edge X coordinate."""
        return self.x + self.width

    @property
    def y2(self) -> float:
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
    def is_portrait(self) -> bool:
        """Return True if monitor is in portrait orientation."""
        return self.orientation in (
            MonitorOrientation.PORTRAIT,
            MonitorOrientation.PORTRAIT_FLIPPED,
        )

    def contains_point(self, x: float, y: float) -> bool:
        """Check if point is within this monitor's bounds."""
        return self.x <= x < self.x2 and self.y <= y < self.y2

    def to_local(self, x: float, y: float) -> tuple[float, float]:
        """Convert virtual screen coordinates to local coordinates."""
        return (x - self.x, y - self.y)

    def to_virtual(self, x: float, y: float) -> tuple[float, float]:
        """Convert local coordinates to virtual screen coordinates."""
        return (x + self.x, y + self.y)


@dataclass
class VirtualScreen:
    """The virtual screen encompassing all monitors."""
    x: float = 0.0
    y: float = 0.0
    width: float = 0.0
    height: float = 0.0

    @property
    def x2(self) -> float:
        return self.x + self.width

    @property
    def y2(self) -> float:
        return self.y + self.height


class MultiMonitorManager:
    """Manages multi-monitor setup and coordinate mapping."""

    def __init__(self) -> None:
        """Initialize with empty monitor list."""
        self._monitors: dict[str, MonitorInfo] = {}
        self._virtual_screen = VirtualScreen()
        self._primary_id: Optional[str] = None

    def add_monitor(self, monitor: MonitorInfo) -> str:
        """Register a monitor."""
        self._monitors[monitor.id] = monitor
        if monitor.is_primary:
            self._primary_id = monitor.id
        self._update_virtual_screen()
        return monitor.id

    def remove_monitor(self, monitor_id: str) -> bool:
        """Remove a monitor. Returns True if found."""
        if monitor_id in self._monitors:
            del self._monitors[monitor_id]
            if self._primary_id == monitor_id:
                self._primary_id = None
            self._update_virtual_screen()
            return True
        return False

    def get_monitor(self, monitor_id: str) -> Optional[MonitorInfo]:
        """Get a monitor by ID."""
        return self._monitors.get(monitor_id)

    def get_monitor_at(self, x: float, y: float) -> Optional[MonitorInfo]:
        """Find which monitor contains the given point."""
        for monitor in self._monitors.values():
            if monitor.contains_point(x, y):
                return monitor
        return None

    def get_primary(self) -> Optional[MonitorInfo]:
        """Get the primary monitor."""
        if self._primary_id:
            return self._monitors.get(self._primary_id)
        return next((m for m in self._monitors.values() if m.is_primary), None)

    def get_all_monitors(self) -> list[MonitorInfo]:
        """Return all registered monitors."""
        return list(self._monitors.values())

    def get_virtual_screen(self) -> VirtualScreen:
        """Return the virtual screen bounds."""
        return self._virtual_screen

    def _update_virtual_screen(self) -> None:
        """Recompute virtual screen bounds from all monitors."""
        if not self._monitors:
            self._virtual_screen = VirtualScreen()
            return

        min_x = min(m.x for m in self._monitors.values())
        min_y = min(m.y for m in self._monitors.values())
        max_x = max(m.x2 for m in self._monitors.values())
        max_y = max(m.y2 for m in self._monitors.values())

        self._virtual_screen = VirtualScreen(
            x=min_x, y=min_y,
            width=max_x - min_x, height=max_y - min_y,
        )

    def move_to_monitor(
        self,
        x: float,
        y: float,
        target_monitor_id: str,
    ) -> tuple[float, float]:
        """Move a point to a target monitor, preserving relative position."""
        target = self._monitors.get(target_monitor_id)
        if not target:
            return (x, y)
        source = self.get_monitor_at(x, y)
        if not source:
            return (x, y)

        local_x, local_y = source.to_local(x, y)
        scale_x = target.width / max(source.width, 1.0)
        scale_y = target.height / max(source.height, 1.0)
        new_local_x = local_x * scale_x
        new_local_y = local_y * scale_y
        return target.to_virtual(new_local_x, new_local_y)

    @property
    def monitor_count(self) -> int:
        """Return number of monitors."""
        return len(self._monitors)

    @property
    def is_multi_monitor(self) -> bool:
        """Return True if multiple monitors are connected."""
        return len(self._monitors) > 1
