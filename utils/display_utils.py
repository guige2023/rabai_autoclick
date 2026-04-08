"""Display enumeration and management utilities for multi-monitor setups.

Provides a unified interface for querying display information on macOS,
including screen bounds, DPI scaling factors, display arrangement,
and primary/secondary screen detection.

Example:
    >>> from utils.display_utils import get_all_displays, get_primary_display
    >>> displays = get_all_displays()
    >>> primary = get_primary_display()
    >>> print(f"Primary: {primary.bounds}")
"""

from __future__ import annotations

from typing import NamedTuple, Optional

__all__ = [
    "DisplayInfo",
    "get_all_displays",
    "get_primary_display",
    "get_display_for_point",
    "get_display_count",
    "get_arrangement",
]


class DisplayInfo(NamedTuple):
    """Information about a single display."""

    display_id: int
    bounds: tuple[float, float, float, float]
    scale_factor: float
    is_primary: bool
    is_builtin: bool
    name: str

    @property
    def width(self) -> float:
        return self.bounds[2]

    @property
    def height(self) -> float:
        return self.bounds[3]

    @property
    def resolution(self) -> tuple[int, int]:
        return (int(self.width), int(self.height))


# Platform-specific implementation
_display_info_cache: list[DisplayInfo] | None = None


def _get_display_info_cached() -> list[DisplayInfo]:
    """Load and cache display information."""
    global _display_info_cache
    if _display_info_cache is not None:
        return _display_info_cache

    import sys

    if sys.platform == "darwin":
        _display_info_cache = _get_displays_darwin()
    else:
        _display_info_cache = []

    return _display_info_cache


def _get_displays_darwin() -> list[DisplayInfo]:
    """Query displays using the CGDisplay API on macOS."""
    import ctypes
    import ctypes.util

    core_graphics = ctypes.CDLL(ctypes.util.find_library("CoreGraphics"))

    class CGRect(ctypes.Structure):
        _fields_ = [
            ("x", ctypes.c_double),
            ("y", ctypes.c_double),
            ("width", ctypes.c_double),
            ("height", ctypes.c_double),
        ]

    # CGGetActiveDisplayList
    core_graphics.CGGetActiveDisplayList.argtypes = [
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.POINTER(ctypes.c_uint32),
    ]
    core_graphics.CGGetActiveDisplayList.restype = ctypes.c_int32

    max_displays = 16
    display_ids = (ctypes.c_uint32 * max_displays)()
    display_count = ctypes.c_uint32(max_displays)

    result = core_graphics.CGGetActiveDisplayList(
        max_displays, display_ids, ctypes.byref(display_count)
    )

    if result != 0:
        return []

    displays = []
    for i in range(display_count.value):
        disp_id = display_ids[i]
        rect = core_graphics.CGDisplayBounds(disp_id)
        bounds = (rect.origin.x, rect.origin.y, rect.size.width, rect.size.height)

        # CGDisplayScreenSize gives physical size in millimeters
        phys_size = core_graphics.CGDisplayScreenSize(disp_id)
        if phys_size.width > 0 and phys_size.height > 0:
            scale_x = rect.size.width / (phys_size.width / 25.4)
            scale_y = rect.size.height / (phys_size.height / 25.4)
            scale = (scale_x + scale_y) / 2
        else:
            scale = 1.0

        is_primary = bool(core_graphics.CGDisplayIsMain(disp_id))
        is_builtin = bool(core_graphics.CGDisplayIsBuiltin(disp_id))

        # Try to get display name
        name = f"Display {disp_id}"

        displays.append(
            DisplayInfo(
                display_id=int(disp_id),
                bounds=bounds,
                scale_factor=scale,
                is_primary=is_primary,
                is_builtin=is_builtin,
                name=name,
            )
        )

    return displays


def get_all_displays() -> list[DisplayInfo]:
    """Return information for all active displays.

    Returns:
        List of DisplayInfo objects sorted by display ID.
    """
    displays = _get_display_info_cached()
    return sorted(displays, key=lambda d: d.display_id)


def get_primary_display() -> Optional[DisplayInfo]:
    """Return the primary display information.

    Returns:
        DisplayInfo for the primary display, or None if none found.
    """
    displays = get_all_displays()
    for d in displays:
        if d.is_primary:
            return d
    return displays[0] if displays else None


def get_display_for_point(x: float, y: float) -> Optional[DisplayInfo]:
    """Find the display containing the given screen point.

    Args:
        x: Screen X coordinate.
        y: Screen Y coordinate.

    Returns:
        DisplayInfo for the display at that point, or None.
    """
    for d in get_all_displays():
        bx, by, bw, bh = d.bounds
        if bx <= x < bx + bw and by <= y < by + bh:
            return d
    return None


def get_display_count() -> int:
    """Return the number of active displays.

    Returns:
        Display count as an integer.
    """
    return len(_get_display_info_cached())


def get_arrangement() -> dict[int, tuple[float, float]]:
    """Get the relative arrangement of displays (origin offsets).

    Returns:
        Dictionary mapping display_id to (x_offset, y_offset).
    """
    arrangement = {}
    for d in get_all_displays():
        arrangement[d.display_id] = (d.bounds[0], d.bounds[1])
    return arrangement
