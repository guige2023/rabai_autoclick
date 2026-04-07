"""Screen region utilities for RabAI AutoClick.

Provides:
- Screen region definition
- Region comparison
- Region cache
"""

from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass
class Region:
    """Screen region definition."""
    x: int
    y: int
    width: int
    height: int

    @property
    def left(self) -> int:
        """Get left edge."""
        return self.x

    @property
    def top(self) -> int:
        """Get top edge."""
        return self.y

    @property
    def right(self) -> int:
        """Get right edge."""
        return self.x + self.width

    @property
    def bottom(self) -> int:
        """Get bottom edge."""
        return self.y + self.height

    @property
    def center(self) -> Tuple[int, int]:
        """Get center point."""
        return (self.x + self.width // 2, self.y + self.height // 2)

    def contains(self, x: int, y: int) -> bool:
        """Check if point is inside region.

        Args:
            x: X coordinate.
            y: Y coordinate.

        Returns:
            True if inside.
        """
        return self.left <= x <= self.right and self.top <= y <= self.bottom

    def overlaps(self, other: "Region") -> bool:
        """Check if regions overlap.

        Args:
            other: Other region.

        Returns:
            True if overlap.
        """
        return not (
            self.right < other.left
            or self.left > other.right
            or self.bottom < other.top
            or self.top > other.bottom
        )

    def intersection(self, other: "Region") -> Optional["Region"]:
        """Get intersection with another region.

        Args:
            other: Other region.

        Returns:
            Intersection region or None.
        """
        if not self.overlaps(other):
            return None

        left = max(self.left, other.left)
        top = max(self.top, other.top)
        right = min(self.right, other.right)
        bottom = min(self.bottom, other.bottom)

        return Region(
            x=left,
            y=top,
            width=right - left,
            height=bottom - top,
        )

    def union(self, other: "Region") -> "Region":
        """Get union with another region.

        Args:
            other: Other region.

        Returns:
            Union region.
        """
        left = min(self.left, other.left)
        top = min(self.top, other.top)
        right = max(self.right, other.right)
        bottom = max(self.bottom, other.bottom)

        return Region(
            x=left,
            y=top,
            width=right - left,
            height=bottom - top,
        )

    def scale(self, factor: float) -> "Region":
        """Scale region from center.

        Args:
            factor: Scale factor.

        Returns:
            Scaled region.
        """
        cx, cy = self.center
        new_width = int(self.width * factor)
        new_height = int(self.height * factor)

        return Region(
            x=cx - new_width // 2,
            y=cy - new_height // 2,
            width=new_width,
            height=new_height,
        )

    def translate(self, dx: int, dy: int) -> "Region":
        """Translate region.

        Args:
            dx: X offset.
            dy: Y offset.

        Returns:
            Translated region.
        """
        return Region(
            x=self.x + dx,
            y=self.y + dy,
            width=self.width,
            height=self.height,
        )

    def to_tuple(self) -> Tuple[int, int, int, int]:
        """Convert to (x, y, width, height) tuple.

        Returns:
            Region tuple.
        """
        return (self.x, self.y, self.width, self.height)


class ScreenRegions:
    """Predefined screen regions."""

    @staticmethod
    def full_screen() -> Region:
        """Get full screen region."""
        try:
            import win32api
            width = win32api.GetSystemMetrics(0)
            height = win32api.GetSystemMetrics(1)
            return Region(0, 0, width, height)
        except Exception:
            return Region(0, 0, 1920, 1080)

    @staticmethod
    def primary_monitor() -> Region:
        """Get primary monitor region."""
        return ScreenRegions.full_screen()

    @staticmethod
    def work_area() -> Region:
        """Get work area (excluding taskbar)."""
        try:
            import win32api
            width = win32api.GetSystemMetrics(0)
            height = win32api.GetSystemMetrics(1)
            # Simplified - actual work area would use SystemParametersInfo
            return Region(0, 0, width, height)
        except Exception:
            return Region(0, 0, 1920, 1080)


class RegionMatcher:
    """Match regions on screen."""

    def __init__(self, threshold: float = 0.8) -> None:
        """Initialize matcher.

        Args:
            threshold: Match confidence threshold.
        """
        self.threshold = threshold

    def find_region(
        self,
        template_path: str,
        region: Optional[Region] = None,
    ) -> Optional[Region]:
        """Find region on screen.

        Args:
            template_path: Path to template image.
            region: Optional search region.

        Returns:
            Found region or None.
        """
        try:
            from PIL import ImageGrab
            import numpy as np
            import cv2

            # Capture screen
            if region:
                screenshot = ImageGrab.grab(bbox=(
                    region.x, region.y,
                    region.x + region.width,
                    region.y + region.height,
                ))
            else:
                screenshot = ImageGrab.grab()

            # Load template
            template = cv2.imread(template_path, 0)
            if template is None:
                return None

            # Convert screenshot to grayscale
            screen_np = np.array(screenshot)
            screen_gray = cv2.cvtColor(screen_np, cv2.COLOR_BGR2GRAY)

            # Template matching
            result = cv2.matchTemplate(
                screen_gray, template, cv2.TM_CCOEFF_NORMED
            )
            _, max_val, _, max_loc = cv2.minMaxLoc(result)

            if max_val >= self.threshold:
                return Region(
                    x=max_loc[0],
                    y=max_loc[1],
                    width=template.shape[1],
                    height=template.shape[0],
                )
            return None
        except Exception:
            return None


class RegionCache:
    """Cache region captures for performance."""

    def __init__(self, max_size: int = 10) -> None:
        """Initialize cache.

        Args:
            max_size: Maximum cached regions.
        """
        self._max_size = max_size
        self._cache: List[Tuple[Region, object]] = []

    def get(self, region: Region) -> Optional[object]:
        """Get cached region.

        Args:
            region: Region key.

        Returns:
            Cached image data or None.
        """
        for cached_region, data in self._cache:
            if self._regions_equal(cached_region, region):
                return data
        return None

    def put(self, region: Region, data: object) -> None:
        """Cache region.

        Args:
            region: Region key.
            data: Image data.
        """
        # Remove if already exists
        self._cache = [
            (r, d) for r, d in self._cache
            if not self._regions_equal(r, region)
        ]

        # Add to front
        self._cache.insert(0, (region, data))

        # Trim to max size
        if len(self._cache) > self._max_size:
            self._cache.pop()

    def clear(self) -> None:
        """Clear cache."""
        self._cache.clear()

    def _regions_equal(self, r1: Region, r2: Region) -> bool:
        """Check if regions are equal.

        Args:
            r1: First region.
            r2: Second region.

        Returns:
            True if equal.
        """
        return r1.x == r2.x and r1.y == r2.y and r1.width == r2.width and r1.height == r2.height


class RegionSelector:
    """Interactive region selection."""

    def __init__(self) -> None:
        """Initialize selector."""
        self._start_x: Optional[int] = None
        self._start_y: Optional[int] = None
        self._end_x: Optional[int] = None
        self._end_y: Optional[int] = None

    def on_mouse_down(self, x: int, y: int) -> None:
        """Handle mouse down event.

        Args:
            x: X coordinate.
            y: Y coordinate.
        """
        self._start_x = x
        self._start_y = y

    def on_mouse_up(self, x: int, y: int) -> Optional[Region]:
        """Handle mouse up event.

        Args:
            x: X coordinate.
            y: Y coordinate.

        Returns:
            Selected region or None.
        """
        if self._start_x is None or self._start_y is None:
            return None

        self._end_x = x
        self._end_y = y

        left = min(self._start_x, self._end_x)
        top = min(self._start_y, self._end_y)
        width = abs(self._end_x - self._start_x)
        height = abs(self._end_y - self._start_y)

        return Region(x=left, y=top, width=width, height=height)

    def reset(self) -> None:
        """Reset selection."""
        self._start_x = None
        self._start_y = None
        self._end_x = None
        self._end_y = None
