"""
Multi-Display Utilities.

Utilities for working with multiple displays including
coordinate translation, display enumeration, and
cross-display window operations.

Usage:
    from utils.multi_display_utils import DisplayHelper, translate_coords

    helper = DisplayHelper()
    translated = helper.translate_to_display(x=100, y=200, from_display=0, to_display=1)
"""

from __future__ import annotations

from typing import Optional, List, Tuple, Dict, Any, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    pass


@dataclass
class Display:
    """Represents a display/monitor."""
    display_id: int
    bounds: Tuple[int, int, int, int]
    is_main: bool
    scale_factor: float = 1.0
    name: str = ""

    @property
    def x(self) -> int:
        return self.bounds[0]

    @property
    def y(self) -> int:
        return self.bounds[1]

    @property
    def width(self) -> int:
        return self.bounds[2]

    @property
    def height(self) -> int:
        return self.bounds[3]

    @property
    def center(self) -> Tuple[int, int]:
        return (self.x + self.width // 2, self.y + self.height // 2)


class DisplayHelper:
    """
    Helper for multi-display operations.

    Provides utilities for coordinate translation between displays,
    display enumeration, and cross-display operations.

    Example:
        helper = DisplayHelper()
        displays = helper.get_displays()
        print(f"Found {len(displays)} displays")
    """

    def __init__(self) -> None:
        """Initialize the display helper."""
        self._displays: List[Display] = []
        self._refresh()

    def _refresh(self) -> None:
        """Refresh the list of displays."""
        self._displays = self._get_displays()

    def _get_displays(self) -> List[Display]:
        """Get all displays."""
        try:
            import Quartz
            max_displays = 16
            display_ids = (Quartz.CGDisplayID * max_displays)(*([0] * max_displays))
            count = [max_displays]
            result = Quartz.CGGetActiveDisplayList(max_displays, display_ids, count)

            if result != 0:
                return self._fallback_displays()

            displays = []
            main_id = Quartz.CGMainDisplayID()

            for i in range(count[0]):
                did = display_ids[i]
                bounds = Quartz.CGDisplayBounds(did)
                scale = Quartz.CGDisplayScaleFactor(did)

                displays.append(Display(
                    display_id=did,
                    bounds=(
                        int(bounds.origin.x),
                        int(bounds.origin.y),
                        int(bounds.size.width),
                        int(bounds.size.height),
                    ),
                    is_main=(did == main_id),
                    scale_factor=scale,
                    name=f"Display {did}",
                ))

            return displays

        except Exception:
            return self._fallback_displays()

    def _fallback_displays(self) -> List[Display]:
        """Return fallback display."""
        return [
            Display(
                display_id=1,
                bounds=(0, 0, 1920, 1080),
                is_main=True,
                scale_factor=1.0,
                name="Main Display",
            )
        ]

    def get_displays(self) -> List[Display]:
        """Get all displays."""
        self._refresh()
        return list(self._displays)

    def get_display_at(
        self,
        x: int,
        y: int,
    ) -> Optional[Display]:
        """
        Get the display containing the given point.

        Args:
            x: X coordinate.
            y: Y coordinate.

        Returns:
            Display containing the point or None.
        """
        for d in self._displays:
            bx, by, bw, bh = d.bounds
            if bx <= x < bx + bw and by <= y < by + bh:
                return d
        return None

    def translate_to_display(
        self,
        x: int,
        y: int,
        from_display: int,
        to_display: int,
    ) -> Tuple[int, int]:
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
        from_d = self._get_display_by_id(from_display)
        to_d = self._get_display_by_id(to_display)

        if from_d is None or to_d is None:
            return (x, y)

        rel_x = x - from_d.x
        rel_y = y - from_d.y

        return (to_d.x + rel_x, to_d.y + rel_y)

    def _get_display_by_id(
        self,
        display_id: int,
    ) -> Optional[Display]:
        """Get a display by ID."""
        for d in self._displays:
            if d.display_id == display_id:
                return d
        return None

    def get_primary_display(self) -> Optional[Display]:
        """Get the primary/main display."""
        for d in self._displays:
            if d.is_main:
                return d
        return self._displays[0] if self._displays else None

    def get_display_count(self) -> int:
        """Get the number of connected displays."""
        return len(self._displays)

    def are_on_same_display(
        self,
        x1: int,
        y1: int,
        x2: int,
        y2: int,
    ) -> bool:
        """
        Check if two coordinate pairs are on the same display.

        Args:
            x1, y1: First coordinate pair.
            x2, y2: Second coordinate pair.

        Returns:
            True if both points are on the same display.
        """
        d1 = self.get_display_at(x1, y1)
        d2 = self.get_display_at(x2, y2)
        if d1 is None or d2 is None:
            return False
        return d1.display_id == d2.display_id


def translate_coords(
    x: int,
    y: int,
    from_display: int,
    to_display: int,
) -> Tuple[int, int]:
    """
    Translate coordinates between displays.

    Args:
        x: Source X.
        y: Source Y.
        from_display: Source display ID.
        to_display: Target display ID.

    Returns:
        Translated (x, y).
    """
    helper = DisplayHelper()
    return helper.translate_to_display(x, y, from_display, to_display)
