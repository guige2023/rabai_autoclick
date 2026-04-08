"""
Display Arrangement Utilities.

Utilities for querying and managing multi-monitor display configurations,
including screen bounds, DPI scaling, rotation, and arrangement.

Usage:
    from utils.display_arrangement_utils import (
        DisplayManager, get_display_info, get_displays_in_order
    )

    manager = DisplayManager()
    for display in manager.displays:
        print(f"Display: {display.name}, bounds: {display.bounds}")
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Tuple, TYPE_CHECKING
from dataclasses import dataclass, field

if TYPE_CHECKING:
    pass


@dataclass
class DisplayMode:
    """Represents a display mode configuration."""
    width: int
    height: int
    scale: float
    refresh_rate: float
    mode_id: int = 0


@dataclass
class DisplayInfo:
    """Information about a single display."""
    display_id: int
    name: str
    is_main: bool
    bounds: Tuple[int, int, int, int]  # x, y, width, height
    work_area: Tuple[int, int, int, int]
    scale_factor: float
    orientation: int  # 0=landscape, 90, 180, 270
    mode: Optional[DisplayMode] = None
    uuid: Optional[str] = None

    @property
    def resolution(self) -> Tuple[int, int]:
        """Return (width, height) of the display."""
        return self.bounds[2], self.bounds[3]

    @property
    def position(self) -> Tuple[int, int]:
        """Return (x, y) origin of the display."""
        return self.bounds[0], self.bounds[1]

    @property
    def is_vertical(self) -> bool:
        """Return True if display is in portrait orientation."""
        return self.orientation in (90, 270)

    def __repr__(self) -> str:
        return (
            f"DisplayInfo(id={self.display_id}, "
            f"res={self.bounds[2]}x{self.bounds[3]}, "
            f"pos=({self.bounds[0]},{self.bounds[1]})"
        )


class DisplayManager:
    """
    Manage display configurations and information.

    Provides a high-level interface for querying display properties,
    arranging displays spatially, and detecting configuration changes.

    Example:
        manager = DisplayManager()
        print(f"Found {len(manager.displays)} displays")
        main = manager.get_main_display()
        print(f"Main: {main.name}")
    """

    _instance: Optional["DisplayManager"] = None

    def __new__(cls) -> "DisplayManager":
        """Singleton pattern for display manager."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init()
        return cls._instance

    def _init(self) -> None:
        """Initialize the display manager."""
        self._displays: List[DisplayInfo] = []
        self._displays_changed_callbacks: List[callable] = []
        self._refresh_displays()

    def _refresh_displays(self) -> None:
        """Refresh the list of connected displays."""
        try:
            from Quartz import CGGetActiveDisplayList, CGDisplayID
            max_displays = 16
            display_ids = (CGDisplayID * max_displays)(*([0] * max_displays))
            count = [max_displays]
            result = CGGetActiveDisplayList(max_displays, display_ids, count)
            if result != 0:
                self._displays = self._get_fallback_displays()
                return
            count_val = count[0]
            self._displays = []
            for i in range(count_val):
                did = display_ids[i]
                info = self._build_display_info(did)
                if info:
                    self._displays.append(info)
        except Exception:
            self._displays = self._get_fallback_displays()

    def _build_display_info(self, display_id: int) -> Optional[DisplayInfo]:
        """Build DisplayInfo for a given display ID."""
        try:
            from Quartz import (
                CGDisplayBounds, CGDisplayIsMain,
                CGDisplayCopyDisplayMode, CGDisplayScreenSize,
            )
            bounds = CGDisplayBounds(display_id)
            is_main = CGDisplayIsMain(display_id)
            mode = CGDisplayCopyDisplayMode(display_id)
            scale = CGDisplayScreenSize(display_id)

            width_mm = scale.width
            height_mm = scale.height
            scale_factor = (bounds.width * 25.4) / width_mm if width_mm > 0 else 1.0

            return DisplayInfo(
                display_id=display_id,
                name=f"Display {display_id}",
                is_main=bool(is_main),
                bounds=(
                    int(bounds.x),
                    int(bounds.y),
                    int(bounds.width),
                    int(bounds.height),
                ),
                work_area=(
                    int(bounds.x),
                    int(bounds.y),
                    int(bounds.width),
                    int(bounds.height),
                ),
                scale_factor=scale_factor / 72.0 if scale_factor else 1.0,
                orientation=0,
                mode=DisplayMode(
                    width=int(mode.width) if mode else int(bounds.width),
                    height=int(mode.height) if mode else int(bounds.height),
                    scale=scale_factor / 72.0 if scale_factor else 1.0,
                    refresh_rate=float(mode.refreshRate) if mode else 60.0,
                    mode_id=int(mode.ID) if mode else 0,
                ) if mode else None,
            )
        except Exception:
            return None

    def _get_fallback_displays(self) -> List[DisplayInfo]:
        """Return fallback display info when Quartz is unavailable."""
        return [
            DisplayInfo(
                display_id=1,
                name="Main Display",
                is_main=True,
                bounds=(0, 0, 1920, 1080),
                work_area=(0, 0, 1920, 1040),
                scale_factor=1.0,
                orientation=0,
            )
        ]

    @property
    def displays(self) -> List[DisplayInfo]:
        """Return the list of active displays."""
        return list(self._displays)

    @property
    def main_display(self) -> Optional[DisplayInfo]:
        """Return the main/primary display."""
        for d in self._displays:
            if d.is_main:
                return d
        return self._displays[0] if self._displays else None

    def get_display_at(
        self,
        x: int,
        y: int,
    ) -> Optional[DisplayInfo]:
        """
        Get the display containing the given point.

        Args:
            x: X coordinate.
            y: Y coordinate.

        Returns:
            DisplayInfo containing the point, or None.
        """
        for d in self._displays:
            bx, by, bw, bh = d.bounds
            if bx <= x < bx + bw and by <= y < by + bh:
                return d
        return None

    def get_displays_in_order(
        self,
        axis: str = "x",
    ) -> List[DisplayInfo]:
        """
        Return displays sorted along an axis.

        Args:
            axis: "x" for left-to-right, "y" for top-to-bottom.

        Returns:
            List of displays sorted by position.
        """
        idx = 0 if axis == "x" else 1
        return sorted(self._displays, key=lambda d: d.bounds[idx])

    def on_displays_changed(
        self,
        callback: callable,
    ) -> None:
        """
        Register a callback for display configuration changes.

        Args:
            callback: Function called when displays change.
        """
        self._displays_changed_callbacks.append(callback)

    def detect_arrangement(self) -> Dict[str, Any]:
        """
        Detect the relative arrangement of displays.

        Returns:
            Dictionary describing how displays are positioned relative
            to each other (left_of, right_of, above, below, primary).
        """
        result: Dict[str, Any] = {"displays": {}, "relationships": []}
        for d in self._displays:
            result["displays"][str(d.display_id)] = {
                "bounds": d.bounds,
                "is_primary": d.is_main,
            }

        displays = self._displays
        for i, di in enumerate(displays):
            for j, dj in enumerate(displays):
                if i >= j:
                    continue
                xi, yi = di.position
                xj, yj = dj.position

                if abs(yi - yj) < 100:
                    if xi < xj:
                        result["relationships"].append(
                            {"left": di.display_id, "right": dj.display_id}
                        )
                    else:
                        result["relationships"].append(
                            {"left": dj.display_id, "right": di.display_id}
                        )

        return result


def get_display_info(display_id: Optional[int] = None) -> Optional[DisplayInfo]:
    """
    Get information about a specific display or the main display.

    Args:
        display_id: Display ID (defaults to main display).

    Returns:
        DisplayInfo for the display, or None.
    """
    manager = DisplayManager()
    if display_id is None:
        return manager.main_display
    for d in manager.displays:
        if d.display_id == display_id:
            return d
    return None


def get_displays_in_order(axis: str = "x") -> List[DisplayInfo]:
    """
    Get all displays sorted along an axis.

    Args:
        axis: "x" (left-to-right) or "y" (top-to-bottom).

    Returns:
        Sorted list of DisplayInfo objects.
    """
    return DisplayManager().get_displays_in_order(axis)
