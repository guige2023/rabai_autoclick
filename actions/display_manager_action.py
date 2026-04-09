"""
Display Manager Action Module

Manages multiple displays, monitors, screen regions,
and display configuration for UI automation.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class DisplayOrientation(Enum):
    """Display orientation types."""

    LANDSCAPE = "landscape"
    PORTRAIT = "portrait"
    LANDSCAPE_FLIPPED = "landscape_flipped"
    PORTRAIT_FLIPPED = "portrait_flipped"


class DisplayMode(Enum):
    """Display mode types."""

    PRIMARY = "primary"
    SECONDARY = "secondary"
    MIRROR = "mirror"
    EXTENDED = "extended"


@dataclass
class DisplayInfo:
    """Display information."""

    id: str
    name: str
    is_primary: bool
    bounds: Tuple[int, int, int, int]  # x, y, width, height
    work_area: Tuple[int, int, int, int]
    dpi: int = 96
    scale_factor: float = 1.0
    orientation: DisplayOrientation = DisplayOrientation.LANDSCAPE
    mode: DisplayMode = DisplayMode.EXTENDED
    is_active: bool = True


@dataclass
class ScreenRegion:
    """Represents a screen region for capture or action."""

    x: int
    y: int
    width: int
    height: int
    monitor_id: Optional[str] = None
    name: Optional[str] = None

    @property
    def bounds(self) -> Tuple[int, int, int, int]:
        return (self.x, self.y, self.width, self.height)

    @property
    def center(self) -> Tuple[int, int]:
        return (self.x + self.width // 2, self.y + self.height // 2)

    @property
    def area(self) -> int:
        return self.width * self.height

    def contains_point(self, x: int, y: int) -> bool:
        """Check if point is inside region."""
        return self.x <= x < self.x + self.width and self.y <= y < self.y + self.height

    def intersection(self, other: "ScreenRegion") -> Optional["ScreenRegion"]:
        """Get intersection with another region."""
        x1 = max(self.x, other.x)
        y1 = max(self.y, other.y)
        x2 = min(self.x + self.width, other.x + other.width)
        y2 = min(self.y + self.height, other.y + other.height)

        if x1 < x2 and y1 < y2:
            return ScreenRegion(
                x=x1, y=y1, width=x2 - x1, height=y2 - y1
            )
        return None


@dataclass
class DisplayManagerConfig:
    """Configuration for display manager."""

    detect_dpi: bool = True
    track_displays: bool = True
    cache_info: bool = True
    refresh_interval: float = 5.0


class DisplayManager:
    """
    Manages displays and screens for automation.

    Supports multi-monitor setups, screen regions,
    display enumeration, and configuration changes.
    """

    def __init__(
        self,
        config: Optional[DisplayManagerConfig] = None,
        platform_handler: Optional[Any] = None,
    ):
        self.config = config or DisplayManagerConfig()
        self.platform_handler = platform_handler
        self._displays: Dict[str, DisplayInfo] = {}
        self._regions: Dict[str, ScreenRegion] = {}
        self._last_refresh: float = 0

    def refresh_displays(self) -> List[DisplayInfo]:
        """
        Refresh display information.

        Returns:
            List of detected displays
        """
        if self.platform_handler and hasattr(self.platform_handler, "get_displays"):
            raw_displays = self.platform_handler.get_displays()
            self._displays = {
                d.id: self._parse_display(d)
                for d in raw_displays
            }
        else:
            self._displays = {
                "primary": DisplayInfo(
                    id="primary",
                    name="Primary Display",
                    is_primary=True,
                    bounds=(0, 0, 1920, 1080),
                    work_area=(0, 0, 1920, 1040),
                )
            }

        self._last_refresh = time.time()
        return list(self._displays.values())

    def _parse_display(self, raw: Any) -> DisplayInfo:
        """Parse raw display data."""
        return DisplayInfo(
            id=raw.get("id", "unknown"),
            name=raw.get("name", "Display"),
            is_primary=raw.get("is_primary", False),
            bounds=raw.get("bounds", (0, 0, 1920, 1080)),
            work_area=raw.get("work_area", (0, 0, 1920, 1040)),
            dpi=raw.get("dpi", 96),
            scale_factor=raw.get("scale_factor", 1.0),
        )

    def get_display(self, display_id: str) -> Optional[DisplayInfo]:
        """Get display by ID."""
        if not self._displays:
            self.refresh_displays()
        return self._displays.get(display_id)

    def get_primary_display(self) -> Optional[DisplayInfo]:
        """Get the primary display."""
        for display in self._displays.values():
            if display.is_primary:
                return display
        return list(self._displays.values())[0] if self._displays else None

    def get_display_at(self, x: int, y: int) -> Optional[DisplayInfo]:
        """Get display containing the given coordinates."""
        for display in self._displays.values():
            bx, by, bw, bh = display.bounds
            if bx <= x < bx + bw and by <= y < by + bh:
                return display
        return None

    def define_region(
        self,
        name: str,
        x: int,
        y: int,
        width: int,
        height: int,
        monitor_id: Optional[str] = None,
    ) -> ScreenRegion:
        """
        Define a named screen region.

        Args:
            name: Region identifier
            x: X coordinate
            y: Y coordinate
            width: Region width
            height: Region height
            monitor_id: Optional associated monitor

        Returns:
            Created ScreenRegion
        """
        region = ScreenRegion(
            x=x, y=y, width=width, height=height, monitor_id=monitor_id, name=name
        )
        self._regions[name] = region
        return region

    def get_region(self, name: str) -> Optional[ScreenRegion]:
        """Get a named region."""
        return self._regions.get(name)

    def list_regions(self) -> List[str]:
        """List all defined region names."""
        return list(self._regions.keys())

    def delete_region(self, name: str) -> bool:
        """Delete a named region."""
        if name in self._regions:
            del self._regions[name]
            return True
        return False

    def capture_region(
        self,
        region: Union[ScreenRegion, str],
        format: str = "png",
    ) -> Optional[bytes]:
        """
        Capture a screen region.

        Args:
            region: Region or region name
            format: Output format (png, jpg, bmp)

        Returns:
            Raw image bytes or None
        """
        if isinstance(region, str):
            region = self._regions.get(region)
            if not region:
                return None

        if self.platform_handler and hasattr(self.platform_handler, "capture"):
            return self.platform_handler.capture(
                region.x, region.y, region.width, region.height, format
            )

        logger.debug(f"Capture region: {region.bounds}")
        return None

    def get_all_regions(self) -> List[ScreenRegion]:
        """Get all defined regions."""
        return list(self._regions.values())

    def get_displays(self) -> List[DisplayInfo]:
        """Get all displays."""
        if not self._displays:
            self.refresh_displays()
        return list(self._displays.values())

    def set_primary_display(self, display_id: str) -> bool:
        """
        Set a display as primary.

        Args:
            display_id: Display to make primary

        Returns:
            True if successful
        """
        if display_id not in self._displays:
            return False

        for display in self._displays.values():
            display.is_primary = display.id == display_id

        if self.platform_handler and hasattr(self.platform_handler, "set_primary"):
            return self.platform_handler.set_primary(display_id)

        return True


import time


def create_display_manager(
    config: Optional[DisplayManagerConfig] = None,
) -> DisplayManager:
    """Factory function to create a DisplayManager."""
    return DisplayManager(config=config)
