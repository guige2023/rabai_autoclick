"""Screen Region Operations Utilities.

Performs operations on screen regions: capture, highlight, annotate.

Example:
    >>> from screen_region_operations_utils import ScreenRegionOps
    >>> ops = ScreenRegionOps()
    >>> ops.highlight_region((100, 100, 200, 200), duration=1.0)
    >>> ops.capture_region((100, 100, 200, 200))
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple


@dataclass
class ScreenRegion:
    """A rectangular region on screen."""
    x: int
    y: int
    width: int
    height: int

    @property
    def bounds(self) -> Tuple[int, int, int, int]:
        """Get bounds as (x, y, width, height)."""
        return (self.x, self.y, self.width, self.height)

    @property
    def center(self) -> Tuple[int, int]:
        """Get center point."""
        return (self.x + self.width // 2, self.y + self.height // 2)

    def contains(self, x: int, y: int) -> bool:
        """Check if point is inside region."""
        return self.x <= x < self.x + self.width and self.y <= y < self.y + self.height

    def intersects(self, other: ScreenRegion) -> bool:
        """Check if this region intersects another."""
        return not (
            self.x + self.width < other.x or
            other.x + other.width < self.x or
            self.y + self.height < other.y or
            other.y + other.height < self.y
        )


class ScreenRegionOps:
    """Operations on screen regions."""

    def __init__(self):
        """Initialize operations handler."""
        self._overlays: List[Any] = []

    def highlight_region(
        self,
        bounds: Tuple[int, int, int, int],
        color: str = "red",
        duration: float = 1.0,
    ) -> None:
        """Highlight a screen region with an overlay.

        Args:
            bounds: (x, y, width, height).
            color: Highlight color name.
            duration: Highlight duration in seconds.
        """
        pass

    def capture_region(
        self, bounds: Tuple[int, int, int, int]
    ) -> Optional[bytes]:
        """Capture pixels from a region.

        Args:
            bounds: (x, y, width, height).

        Returns:
            Image bytes or None.
        """
        return None

    def annotate_region(
        self,
        bounds: Tuple[int, int, int, int],
        text: str,
        callback: Optional[Callable[..., None]] = None,
    ) -> None:
        """Annotate a region with text.

        Args:
            bounds: Region bounds.
            text: Annotation text.
            callback: Optional callback for click events.
        """
        pass

    def clear_annotations(self) -> None:
        """Clear all annotations."""
        self._overlays.clear()

    def region_from_points(
        self, p1: Tuple[int, int], p2: Tuple[int, int]
    ) -> ScreenRegion:
        """Create region from two corner points.

        Args:
            p1: First corner (x, y).
            p2: Opposite corner (x, y).

        Returns:
            ScreenRegion.
        """
        x = min(p1[0], p2[0])
        y = min(p1[1], p2[1])
        w = abs(p2[0] - p1[0])
        h = abs(p2[1] - p1[1])
        return ScreenRegion(x, y, w, h)
