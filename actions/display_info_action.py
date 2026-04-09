"""Display information action for UI automation.

Provides display enumeration, configuration, and monitoring:
- Multi-monitor detection
- Display properties (resolution, scale, rotation)
- Display state monitoring
- Screen capture coordination
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Protocol


class DisplayConnectionType(Enum):
    """Display connection types."""
    BUILT_IN = auto()
    USB_C = auto()
    THUNDERBOLT = auto()
    HDMI = auto()
    DISPLAYPORT = auto()
    VGA = auto()
    UNKNOWN = auto()


class DisplayBacklightType(Enum):
    """Display backlight types."""
    LED = auto()
    OLED = auto()
    CCFL = auto()
    UNKNOWN = auto()


@dataclass
class DisplayMode:
    """Display mode configuration."""
    width: int
    height: int
    refresh_rate: float
    scale_factor: float = 1.0
    is_native: bool = False


@dataclass
class DisplayCapabilities:
    """Display capabilities."""
    max_resolution: tuple[int, int]
    supported_refresh_rates: list[float]
    supported_scale_factors: list[float]
    supports_hdr: bool = False
    supports_wide_color: bool = False
    supports_variable_refresh: bool = False


@dataclass
class Display:
    """Display information."""
    display_id: str
    name: str
    is_main: bool = False
    is_builtin: bool = False
    is_primary: bool = False
    bounds_x: int = 0
    bounds_y: int = 0
    bounds_width: int = 1920
    bounds_height: int = 1080
    work_area_x: int = 0
    work_area_y: int = 0
    work_area_width: int = 1920
    work_area_height: int = 1080
    scale_factor: float = 1.0
    rotation: int = 0  # 0, 90, 180, 270
    connection_type: DisplayConnectionType = DisplayConnectionType.UNKNOWN
    backlight_type: DisplayBacklightType = DisplayBacklightType.UNKNOWN
    current_mode: DisplayMode | None = None
    capabilities: DisplayCapabilities | None = None
    has_notch: bool = False
    has_camera_band: bool = False
    is_on: bool = True
    brightness: int = 100  # 0-100
    metadata: dict = field(default_factory=dict)

    @property
    def resolution(self) -> tuple[int, int]:
        return (self.bounds_width, self.bounds_height)

    @property
    def work_area(self) -> tuple[int, int, int, int]:
        return (self.work_area_x, self.work_area_y,
                self.work_area_width, self.work_area_height)

    @property
    def bounds(self) -> tuple[int, int, int, int]:
        return (self.bounds_x, self.bounds_y,
                self.bounds_width, self.bounds_height)

    @property
    def center(self) -> tuple[int, int]:
        return (
            self.bounds_x + self.bounds_width // 2,
            self.bounds_y + self.bounds_height // 2,
        )

    @property
    def aspect_ratio(self) -> float:
        return self.bounds_width / self.bounds_height if self.bounds_height > 0 else 16/9

    def physical_to_logical(self, x: int, y: int) -> tuple[int, int]:
        """Convert physical pixels to logical coordinates."""
        return (
            int((x - self.bounds_x) / self.scale_factor),
            int((y - self.bounds_y) / self.scale_factor),
        )

    def logical_to_physical(self, x: int, y: int) -> tuple[int, int]:
        """Convert logical coordinates to physical pixels."""
        return (
            int(x * self.scale_factor + self.bounds_x),
            int(y * self.scale_factor + self.bounds_y),
        )


class DisplayChangeCallback(Protocol):
    """Protocol for display change callbacks."""
    def on_display_added(self, display: Display) -> None: ...
    def on_display_removed(self, display_id: str) -> None: ...
    def on_display_changed(self, display: Display) -> None: ...
    def on_mode_changed(self, display: Display, old_mode: DisplayMode) -> None: ...


class DisplayInfoService:
    """Service for display information and management.

    Features:
    - Display enumeration
    - Display properties query
    - Display change monitoring
    - Multi-display coordination
    - Brightness control
    """

    def __init__(self):
        self._displays: dict[str, Display] = {}
        self._change_callbacks: list[DisplayChangeCallback] = []
        self._enumerate_func: Callable | None = None
        self._brightness_func: Callable | None = None
        self._capture_func: Callable | None = None

    def set_enumerate_func(self, func: Callable) -> None:
        """Set display enumeration function.

        Args:
            func: Function that returns list of display info dicts
        """
        self._enumerate_func = func

    def set_brightness_func(self, func: Callable) -> None:
        """Set brightness control function.

        Args:
            func: Function(display_id, brightness) -> bool
        """
        self._brightness_func = func

    def set_capture_func(self, func: Callable) -> None:
        """Set screen capture function.

        Args:
            func: Function(display_id, region) -> image_bytes
        """
        self._capture_func = func

    def refresh(self) -> list[Display]:
        """Refresh display list from system.

        Returns:
            List of current displays
        """
        if self._enumerate_func:
            display_list = self._enumerate_func()
            self._update_displays(display_list)
        return self.list_displays()

    def _update_displays(self, display_list: list[dict]) -> None:
        """Update internal display state."""
        old_ids = set(self._displays.keys())
        new_ids = set()

        for info in display_list:
            display_id = info.get("display_id", info.get("id", ""))
            new_ids.add(display_id)

            display = self._displays.get(display_id)
            if display:
                # Update existing
                self._update_display_from_dict(display, info)
            else:
                # Create new
                display = self._create_display_from_dict(info)
                self._displays[display_id] = display
                self._notify_display_added(display)

        # Check for removed displays
        for removed_id in old_ids - new_ids:
            display = self._displays.pop(removed_id, None)
            if display:
                self._notify_display_removed(removed_id)

    def _create_display_from_dict(self, info: dict) -> Display:
        """Create Display from info dict."""
        return Display(
            display_id=info.get("display_id", info.get("id", "")),
            name=info.get("name", info.get("displayName", "Unknown")),
            is_main=info.get("isMain", False),
            is_builtin=info.get("isBuiltIn", False),
            is_primary=info.get("isPrimary", False),
            bounds_x=info.get("boundsX", 0),
            bounds_y=info.get("boundsY", 0),
            bounds_width=info.get("boundsWidth", 1920),
            bounds_height=info.get("boundsHeight", 1080),
            work_area_x=info.get("workAreaX", 0),
            work_area_y=info.get("workAreaY", 0),
            work_area_width=info.get("workAreaWidth", 1920),
            work_area_height=info.get("workAreaHeight", 1080),
            scale_factor=info.get("scaleFactor", 1.0),
            rotation=info.get("rotation", 0),
        )

    def _update_display_from_dict(self, display: Display, info: dict) -> None:
        """Update existing Display from info dict."""
        old_bounds = display.bounds
        display.name = info.get("name", display.name)
        display.is_main = info.get("isMain", display.is_main)
        display.is_builtin = info.get("isBuiltIn", display.is_builtin)
        display.is_primary = info.get("isPrimary", display.is_primary)
        display.bounds_x = info.get("boundsX", display.bounds_x)
        display.bounds_y = info.get("boundsY", display.bounds_y)
        display.bounds_width = info.get("boundsWidth", display.bounds_width)
        display.bounds_height = info.get("boundsHeight", display.bounds_height)
        display.work_area_x = info.get("workAreaX", display.work_area_x)
        display.work_area_y = info.get("workAreaY", display.work_area_y)
        display.work_area_width = info.get("workAreaWidth", display.work_area_width)
        display.work_area_height = info.get("workAreaHeight", display.work_area_height)
        display.scale_factor = info.get("scaleFactor", display.scale_factor)
        display.rotation = info.get("rotation", display.rotation)

        if display.bounds != old_bounds:
            self._notify_display_changed(display)

    def list_displays(self) -> list[Display]:
        """List all displays."""
        return list(self._displays.values())

    def get_display(self, display_id: str) -> Display | None:
        """Get display by ID."""
        return self._displays.get(display_id)

    def get_main_display(self) -> Display | None:
        """Get main/primary display."""
        for display in self._displays.values():
            if display.is_main or display.is_primary:
                return display
        return next(iter(self._displays.values())) if self._displays else None

    def get_display_at_point(self, x: int, y: int) -> Display | None:
        """Get display containing point."""
        for display in self._displays.values():
            if (display.bounds_x <= x < display.bounds_x + display.bounds_width and
                display.bounds_y <= y < display.bounds_y + display.bounds_height):
                return display
        return None

    def get_displays_by_type(self, connection_type: DisplayConnectionType) -> list[Display]:
        """Get displays by connection type."""
        return [
            d for d in self._displays.values()
            if d.connection_type == connection_type
        ]

    def get_builtin_display(self) -> Display | None:
        """Get built-in display (e.g., laptop screen)."""
        for display in self._displays.values():
            if display.is_builtin:
                return display
        return None

    def set_brightness(self, display_id: str, brightness: int) -> bool:
        """Set display brightness.

        Args:
            display_id: Target display
            brightness: Brightness level (0-100)

        Returns:
            True if successful
        """
        brightness = max(0, min(100, brightness))

        display = self._displays.get(display_id)
        if not display:
            return False

        if self._brightness_func:
            success = self._brightness_func(display_id, brightness)
            if success:
                display.brightness = brightness
                return True
            return False

        display.brightness = brightness
        return True

    def capture_display(
        self,
        display_id: str,
        region: tuple[int, int, int, int] | None = None,
    ) -> bytes | None:
        """Capture screenshot of display.

        Args:
            display_id: Target display
            region: Optional region (x, y, w, h)

        Returns:
            Image bytes or None
        """
        if self._capture_func:
            return self._capture_func(display_id, region)
        return None

    def get_combined_bounds(self) -> tuple[int, int, int, int]:
        """Get combined bounds of all displays.

        Returns:
            (min_x, min_y, max_x, max_y)
        """
        if not self._displays:
            return (0, 0, 0, 0)

        min_x = min(d.bounds_x for d in self._displays.values())
        min_y = min(d.bounds_y for d in self._displays.values())
        max_x = max(d.bounds_x + d.bounds_width for d in self._displays.values())
        max_y = max(d.bounds_y + d.bounds_height for d in self._displays.values())

        return (min_x, min_y, max_x - min_x, max_y - min_y)

    def register_change_callback(self, callback: DisplayChangeCallback) -> None:
        """Register for display change events."""
        self._change_callbacks.append(callback)

    def unregister_change_callback(self, callback: DisplayChangeCallback) -> None:
        """Unregister display change callback."""
        if callback in self._change_callbacks:
            self._change_callbacks.remove(callback)

    def _notify_display_added(self, display: Display) -> None:
        for cb in self._change_callbacks:
            try:
                cb.on_display_added(display)
            except Exception:
                pass

    def _notify_display_removed(self, display_id: str) -> None:
        for cb in self._change_callbacks:
            try:
                cb.on_display_removed(display_id)
            except Exception:
                pass

    def _notify_display_changed(self, display: Display) -> None:
        for cb in self._change_callbacks:
            try:
                cb.on_display_changed(display)
            except Exception:
                pass


def create_display_info_service() -> DisplayInfoService:
    """Create display info service."""
    return DisplayInfoService()
