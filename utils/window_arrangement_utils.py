"""
Window arrangement utilities for multi-window management.

This module provides utilities for arranging, positioning,
and managing multiple windows on screen.
"""

from __future__ import annotations

import platform
from typing import List, Tuple, Optional, Dict, Any, Callable
from dataclasses import dataclass, field
from enum import Enum, auto


IS_MACOS: bool = platform.system() == 'Darwin'


class ArrangementType(Enum):
    """Window arrangement types."""
    CASCADE = auto()
    TILE_HORIZONTAL = auto()
    TILE_VERTICAL = auto()
    HALF_LEFT = auto()
    HALF_RIGHT = auto()
    HALF_TOP = auto()
    HALF_BOTTOM = auto()
    QUADRANT = auto()
    CUSTOM = auto()


@dataclass
class WindowPosition:
    """
    Represents a window's position and size.

    Attributes:
        x: X coordinate of top-left corner.
        y: Y coordinate of top-left corner.
        width: Window width.
        height: Window height.
        title: Window title (if available).
        app_name: Application name.
        window_id: Unique window identifier.
    """
    x: int
    y: int
    width: int
    height: int
    title: Optional[str] = None
    app_name: Optional[str] = None
    window_id: Optional[int] = None

    @property
    def x1(self) -> int:
        """Left edge."""
        return self.x

    @property
    def y1(self) -> int:
        """Top edge."""
        return self.y

    @property
    def x2(self) -> int:
        """Right edge."""
        return self.x + self.width

    @property
    def y2(self) -> int:
        """Bottom edge."""
        return self.y + self.height

    @property
    def center(self) -> Tuple[int, int]:
        """Center point."""
        return (self.x + self.width // 2, self.y + self.height // 2)

    @property
    def frame(self) -> Tuple[int, int, int, int]:
        """Frame as (x, y, width, height)."""
        return (self.x, self.y, self.width, self.height)

    @property
    def bounds(self) -> Tuple[int, int, int, int]:
        """Bounds as (x1, y1, x2, y2)."""
        return (self.x1, self.y1, self.x2, self.y2)

    def fits_in(self, container_x: int, container_y: int,
                container_width: int, container_height: int) -> bool:
        """Check if window fits within a container."""
        return (
            self.x >= container_x
            and self.y >= container_y
            and self.x2 <= container_x + container_width
            and self.y2 <= container_y + container_height
        )

    def scale_to_fit(
        self, target_x: int, target_y: int,
        target_width: int, target_height: int,
        maintain_aspect: bool = True
    ) -> Tuple[int, int, int, int]:
        """
        Calculate scaled position to fit in target region.

        Args:
            target_x: Target region x.
            target_y: Target region y.
            target_width: Target region width.
            target_height: Target region height.
            maintain_aspect: Whether to maintain aspect ratio.

        Returns:
            Tuple of (x, y, width, height) for the scaled position.
        """
        if not maintain_aspect:
            return (target_x, target_y, target_width, target_height)

        scale_w = target_width / self.width
        scale_h = target_height / self.height
        scale = min(scale_w, scale_h)

        new_w = int(self.width * scale)
        new_h = int(self.height * scale)
        new_x = target_x + (target_width - new_w) // 2
        new_y = target_y + (target_height - new_h) // 2

        return (new_x, new_y, new_w, new_h)


@dataclass
class ArrangementPlan:
    """
    A plan for arranging multiple windows.

    Attributes:
        arrangement_type: Type of arrangement.
        positions: List of window positions.
        gaps: Gap between windows in pixels.
        padding: Padding from screen edges.
    """
    arrangement_type: ArrangementType
    positions: List[WindowPosition] = field(default_factory=list)
    gaps: int = 10
    padding: int = 0

    def add_window(self, pos: WindowPosition) -> None:
        """Add a window position to the plan."""
        self.positions.append(pos)


def get_all_windows() -> List[WindowPosition]:
    """
    Get all visible windows.

    Returns:
        List of WindowPosition objects.
    """
    if IS_MACOS:
        return _get_macos_windows()
    return []


def _get_macos_windows() -> List[WindowPosition]:
    """Get all windows on macOS."""
    import Quartz
    windows = []

    for app in Quartz.CGWindowListCopyWindowInfo(
        Quartz.kCGWindowListOptionOnScreenOnly | Quartz.kCGWindowListExcludeDesktopElements,
        Quartz.kCGNullWindowID
    ):
        try:
            bounds = app.get('kCGWindowBounds', {})
            window_id = app.get('kCGWindowNumber')
            layer = app.get('kCGWindowLayer')

            # Skip menu bar and other system windows (layer != 0)
            if layer != 0:
                continue

            pos = WindowPosition(
                x=int(bounds.get('X', 0)),
                y=int(bounds.get('Y', 0)),
                width=int(bounds.get('Width', 0)),
                height=int(bounds.get('Height', 0)),
                title=app.get('kCGWindowName'),
                window_id=window_id,
            )
            windows.append(pos)
        except (KeyError, TypeError):
            continue

    return windows


def calculate_cascade_arrangement(
    num_windows: int,
    screen_width: int, screen_height: int,
    base_width: int = 800, base_height: int = 600,
    offset_x: int = 30, offset_y: int = 30
) -> List[Tuple[int, int, int, int]]:
    """
    Calculate positions for cascaded windows.

    Args:
        num_windows: Number of windows to arrange.
        screen_width: Screen width.
        screen_height: Screen height.
        base_width: Base window width.
        base_height: Base window height.
        offset_x: Horizontal offset between windows.
        offset_y: Vertical offset between windows.

    Returns:
        List of (x, y, width, height) tuples.
    """
    positions = []
    for i in range(num_windows):
        x = i * offset_x
        y = i * offset_y
        # Cascade from top-left, don't exceed screen
        w = min(base_width, screen_width - x)
        h = min(base_height, screen_height - y)
        positions.append((x, y, w, h))
    return positions


def calculate_tile_arrangement(
    num_windows: int,
    screen_width: int, screen_height: int,
    arrangement: ArrangementType = ArrangementType.TILE_HORIZONTAL,
    gaps: int = 5
) -> List[Tuple[int, int, int, int]]:
    """
    Calculate positions for tiled windows.

    Args:
        num_windows: Number of windows.
        screen_width: Screen width.
        screen_height: Screen height.
        arrangement: Tile arrangement type.
        gaps: Gap between windows.

    Returns:
        List of (x, y, width, height) tuples.
    """
    if num_windows == 0:
        return []

    if arrangement == ArrangementType.TILE_HORIZONTAL:
        w = (screen_width - gaps * (num_windows + 1)) // num_windows
        return [
            (gaps + i * (w + gaps), gaps, w, screen_height - 2 * gaps)
            for i in range(num_windows)
        ]
    elif arrangement == ArrangementType.TILE_VERTICAL:
        h = (screen_height - gaps * (num_windows + 1)) // num_windows
        return [
            (gaps, gaps + i * (h + gaps), screen_width - 2 * gaps, h)
            for i in range(num_windows)
        ]
    elif arrangement == ArrangementType.HALF_LEFT:
        return [(0, 0, screen_width // 2 - gaps // 2, screen_height)]
    elif arrangement == ArrangementType.HALF_RIGHT:
        return [(screen_width // 2 + gaps // 2, 0, screen_width // 2 - gaps // 2, screen_height)]
    elif arrangement == ArrangementType.HALF_TOP:
        return [(0, 0, screen_width, screen_height // 2 - gaps // 2)]
    elif arrangement == ArrangementType.HALF_BOTTOM:
        return [(0, screen_height // 2 + gaps // 2, screen_width, screen_height // 2 - gaps // 2)]
    elif arrangement == ArrangementType.QUADRANT:
        half_w = screen_width // 2 - gaps // 2
        half_h = screen_height // 2 - gaps // 2
        return [
            (0, 0, half_w, half_h),
            (screen_width // 2 + gaps // 2, 0, half_w, half_h),
            (0, screen_height // 2 + gaps // 2, half_w, half_h),
            (screen_width // 2 + gaps // 2, screen_height // 2 + gaps // 2, half_w, half_h),
        ][:num_windows]

    return []


def arrange_windows(
    windows: List[WindowPosition],
    arrangement: ArrangementType,
    screen_width: int, screen_height: int,
    gaps: int = 5
) -> Dict[int, Tuple[int, int, int, int]]:
    """
    Calculate new positions for windows based on arrangement.

    Args:
        windows: List of windows to arrange.
        arrangement: Arrangement type.
        screen_width: Screen width.
        screen_height: Screen height.
        gaps: Gap between windows.

    Returns:
        Dictionary mapping window_id to new (x, y, width, height).
    """
    positions = calculate_tile_arrangement(
        len(windows), screen_width, screen_height,
        arrangement, gaps
    )

    result = {}
    for i, window in enumerate(windows):
        if i < len(positions):
            result[window.window_id or i] = positions[i]

    return result


def center_window_on_screen(
    window_width: int, window_height: int,
    screen_width: int, screen_height: int
) -> Tuple[int, int]:
    """
    Calculate centered position for a window.

    Args:
        window_width: Window width.
        window_height: Window height.
        screen_width: Screen width.
        screen_height: Screen height.

    Returns:
        Tuple of (x, y) for centered position.
    """
    x = (screen_width - window_width) // 2
    y = (screen_height - window_height) // 2
    return (x, y)


def fit_windows_in_region(
    windows: List[Tuple[int, int]],
    region_x: int, region_y: int,
    region_width: int, region_height: int,
    gaps: int = 5, padding: int = 0
) -> List[Tuple[int, int, int, int]]:
    """
    Fit multiple windows into a region with equal sizing.

    Args:
        windows: List of (width, height) for each window.
        region_x: Region x.
        region_y: Region y.
        region_width: Region width.
        region_height: Region height.
        gaps: Gap between windows.
        padding: Padding from region edges.

    Returns:
        List of (x, y, width, height) positions.
    """
    if not windows:
        return []

    n = len(windows)
    available_w = region_width - 2 * padding - gaps * (n - 1)
    available_h = region_height - 2 * padding - gaps * (n - 1)

    # Use the largest common size
    common_w = available_w // n
    common_h = available_h

    positions = []
    for i, (w, h) in enumerate(windows):
        # Scale to common size maintaining aspect
        scale = min(common_w / max(w, 1), common_h / max(h, 1))
        scaled_w = int(w * scale)
        scaled_h = int(h * scale)
        x = region_x + padding + i * (common_w + gaps)
        y = region_y + padding + (common_h - scaled_h) // 2
        positions.append((x, y, scaled_w, scaled_h))

    return positions
