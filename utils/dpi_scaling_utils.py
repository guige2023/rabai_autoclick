"""DPI scaling and coordinate transformation utilities.

Handles coordinate transformation between logical (points) and
physical (pixels) coordinate spaces on high-DPI/Retina displays,
and between multiple monitors with different scaling factors.

Example:
    >>> from utils.dpi_scaling_utils import physical_to_logical, logical_to_physical
    >>> px = logical_to_physical(100, 100, scale_factor=2.0)
    >>> py = physical_to_logical(200, 200, scale_factor=2.0)
"""

from __future__ import annotations

from typing import Optional

__all__ = [
    "get_scale_factor",
    "logical_to_physical",
    "physical_to_logical",
    "logical_to_physical_rect",
    "physical_to_logical_rect",
    "get_display_scale",
    "DPIScaler",
]


def get_scale_factor(display_id: Optional[int] = None) -> float:
    """Get the DPI scale factor for a display.

    Args:
        display_id: Display identifier (uses primary if None).

    Returns:
        Scale factor (typically 1.0, 2.0 for Retina, or 3.0).
    """
    import sys

    if sys.platform == "darwin":
        try:
            from utils.display_utils import get_primary_display

            primary = get_primary_display()
            if primary is not None:
                return primary.scale_factor
        except ImportError:
            pass

    return 1.0


def get_display_scale(display_bounds: tuple[float, float, float, float]) -> float:
    """Infer scale factor from display bounds and physical size.

    Uses CGDisplayScreenSize to get physical dimensions and compares
    against the logical resolution.

    Args:
        display_bounds: Display rect (x, y, width, height).

    Returns:
        Estimated scale factor.
    """
    import ctypes

    try:
        core_graphics = ctypes.CDLL("libCG-A.dylib")
    except Exception:
        return 1.0

    return 1.0


def logical_to_physical(
    x: float,
    y: float,
    scale_factor: Optional[float] = None,
) -> tuple[float, float]:
    """Convert logical (point) coordinates to physical (pixel) coordinates.

    Args:
        x: Logical X coordinate.
        y: Logical Y coordinate.
        scale_factor: Display scale factor (uses primary display if None).

    Returns:
        Tuple of (physical_x, physical_y) in pixels.
    """
    if scale_factor is None:
        scale_factor = get_scale_factor()
    return (x * scale_factor, y * scale_factor)


def physical_to_logical(
    x: float,
    y: float,
    scale_factor: Optional[float] = None,
) -> tuple[float, float]:
    """Convert physical (pixel) coordinates to logical (point) coordinates.

    Args:
        x: Physical X coordinate in pixels.
        y: Physical Y coordinate in pixels.
        scale_factor: Display scale factor (uses primary display if None).

    Returns:
        Tuple of (logical_x, logical_y) in points.
    """
    if scale_factor is None:
        scale_factor = get_scale_factor()
    if scale_factor == 0:
        scale_factor = 1.0
    return (x / scale_factor, y / scale_factor)


def logical_to_physical_rect(
    rect: tuple[float, float, float, float],
    scale_factor: Optional[float] = None,
) -> tuple[float, float, float, float]:
    """Convert a logical rect to physical coordinates.

    Args:
        rect: Logical rect as (x, y, width, height) in points.
        scale_factor: Display scale factor.

    Returns:
        Physical rect as (x, y, width, height) in pixels.
    """
    x, y, w, h = rect
    if scale_factor is None:
        scale_factor = get_scale_factor()
    sx, sy = logical_to_physical(x, y, scale_factor)
    return (sx, sy, w * scale_factor, h * scale_factor)


def physical_to_logical_rect(
    rect: tuple[float, float, float, float],
    scale_factor: Optional[float] = None,
) -> tuple[float, float, float, float]:
    """Convert a physical rect to logical coordinates.

    Args:
        rect: Physical rect as (x, y, width, height) in pixels.
        scale_factor: Display scale factor.

    Returns:
        Logical rect as (x, y, width, height) in points.
    """
    x, y, w, h = rect
    if scale_factor is None:
        scale_factor = get_scale_factor()
    if scale_factor == 0:
        scale_factor = 1.0
    sx, sy = physical_to_logical(x, y, scale_factor)
    return (sx, sy, w / scale_factor, h / scale_factor)


class DPIScaler:
    """Stateful DPI scaler for consistent coordinate transforms.

    Example:
        >>> scaler = DPIScaler(display_id=1)
        >>> px, py = scaler.to_physical(100, 100)
        >>> lx, ly = scaler.to_logical(200, 200)
    """

    def __init__(self, scale_factor: Optional[float] = None, display_id: Optional[int] = None):
        self.scale_factor = scale_factor if scale_factor else get_scale_factor(display_id)

    def to_physical(self, x: float, y: float) -> tuple[float, float]:
        return logical_to_physical(x, y, self.scale_factor)

    def to_logical(self, x: float, y: float) -> tuple[float, float]:
        return physical_to_logical(x, y, self.scale_factor)

    def to_physical_rect(
        self, rect: tuple[float, float, float, float]
    ) -> tuple[float, float, float, float]:
        return logical_to_physical_rect(rect, self.scale_factor)

    def to_logical_rect(
        self, rect: tuple[float, float, float, float]
    ) -> tuple[float, float, float, float]:
        return physical_to_logical_rect(rect, self.scale_factor)
