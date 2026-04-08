"""UI element detection and management utilities.

This module provides helpers for discovering and interacting with
UI elements in the autoclick context, including element matching
by image template, color sampling, and region-based element
grouping.

Example:
    >>> from utils.ui_element_utils import find_all_in_region, get_region_elements
    >>> buttons = find_all_in_region('button_template.png', region=(0, 0, 1920, 1080))
"""

from __future__ import annotations

from typing import Optional, Callable

__all__ = [
    "find_by_color",
    "find_by_image",
    "find_all_in_region",
    "get_region_elements",
    "element_to_rect",
    "group_nearby_elements",
    "UIElement",
]


import sys

if sys.platform == "darwin":
    try:
        from utils.accessibility_utils import (
            get_element_at_point,
            get_element_role,
            get_element_title,
            get_element_rect,
            get_element_children,
            get_all_windows,
            get_application_element,
            get_element_value,
            is_element_enabled,
        )

        _AX_AVAILABLE = True
    except ImportError:
        _AX_AVAILABLE = False
else:
    _AX_AVAILABLE = False


class UIElement:
    """Represents a UI element with position, size, and attributes.

    Attributes:
        x: Left edge X coordinate.
        y: Top edge Y coordinate.
        width: Element width.
        height: Element height.
        role: Accessibility role string.
        title: Element title/label.
        value: Current value.
        enabled: Whether the element is enabled.
        image: Optional screenshot of the element.
    """

    def __init__(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
        role: Optional[str] = None,
        title: Optional[str] = None,
        value: Optional[str] = None,
        enabled: bool = True,
        image: Optional[bytes] = None,
    ):
        self.x = x
        self.y = y
        self.y = y
        self.width = width
        self.height = height
        self.role = role
        self.title = title
        self.value = value
        self.enabled = enabled
        self.image = image

    @property
    def center(self) -> tuple[float, float]:
        """Center point of the element."""
        return (self.x + self.width / 2, self.y + self.height / 2)

    @property
    def rect(self) -> tuple[float, float, float, float]:
        """Rectangle (x, y, width, height)."""
        return (self.x, self.y, self.width, self.height)

    def contains_point(self, x: float, y: float) -> bool:
        """Check if a point lies within this element."""
        return self.x <= x < self.x + self.width and self.y <= y < self.y + self.height

    def distance_to(self, x: float, y: float) -> float:
        """Euclidean distance from element center to point."""
        cx, cy = self.center
        return ((cx - x) ** 2 + (cy - y) ** 2) ** 0.5

    def __repr__(self) -> str:
        return (
            f"UIElement(role={self.role!r}, title={self.title!r}, "
            f"rect={self.rect}, enabled={self.enabled})"
        )


def element_to_rect(element: Any) -> Optional[tuple[float, float, float, float]]:
    """Convert an accessibility element to a rect tuple.

    Args:
        element: AXUIElement from the accessibility API.

    Returns:
        (x, y, width, height) or None if unavailable.
    """
    if not _AX_AVAILABLE:
        return None
    return get_element_rect(element)


def get_region_elements(
    region: tuple[float, float, float, float],
    roles: Optional[list[str]] = None,
    pid: Optional[int] = None,
) -> list[UIElement]:
    """Get all UI elements within a screen region.

    Args:
        region: Screen region as (x, y, width, height).
        roles: Optional filter by accessibility roles (e.g., ['AXButton']).
        pid: Optional process ID to scope the search.

    Returns:
        List of UIElement objects within the region.
    """
    if not _AX_AVAILABLE:
        return []

    rx, ry, rw, rh = region
    elements: list[UIElement] = []

    if pid is not None:
        windows = get_all_windows(pid)
    else:
        # System-wide search would require enumerating all apps
        return []

    def _collect_recursive(element: Any, depth: int = 0) -> None:
        if depth > 10:  # prevent infinite recursion
            return

        rect = element_to_rect(element)
        if rect is not None:
            ex, ey, ew, eh = rect
            # Check if element intersects region
            if not (
                rx < ex + ew and rx + rw > ex and ry < ey + eh and ry + rh > ey
            ):
                for child in get_element_children(element):
                    _collect_recursive(child, depth + 1)
                return

            role = get_element_role(element)
            if roles is None or role in roles:
                title = get_element_title(element)
                value = get_element_value(element)
                enabled = is_element_enabled(element)

                elements.append(
                    UIElement(
                        x=ex,
                        y=ey,
                        width=ew,
                        height=eh,
                        role=role,
                        title=title,
                        value=value,
                        enabled=enabled,
                    )
                )

        for child in get_element_children(element):
            _collect_recursive(child, depth + 1)

    for window in windows:
        _collect_recursive(window)

    return elements


def find_by_color(
    region: tuple[float, float, float, float],
    target_rgb: tuple[int, int, int],
    tolerance: int = 10,
    screenshot: Optional[bytes] = None,
) -> list[tuple[int, int]]:
    """Find all pixels matching a target color within a screen region.

    This performs a simple per-pixel scan of the provided screenshot
    (or captures one if not provided) for pixels within color tolerance.

    Args:
        region: Screen region as (x, y, width, height).
        target_rgb: Target color as (R, G, B) with values 0-255.
        tolerance: Color distance tolerance (0-255).
        screenshot: Optional raw RGBA screenshot bytes.

    Returns:
        List of (x, y) coordinates of matching pixels (absolute screen coords).
    """
    import struct
    import zlib

    if screenshot is None:
        screenshot = _capture_region(region)

    rx, ry, rw, rh = region
    matches: list[tuple[int, int]] = []
    r_t, g_t, b_t = target_rgb

    # screenshot is RGBA at 4 bytes per pixel
    idx = 0
    for y in range(int(rh)):
        for x in range(int(rw)):
            if idx + 3 < len(screenshot):
                r, g, b = screenshot[idx], screenshot[idx + 1], screenshot[idx + 2]
                dist = abs(r - r_t) + abs(g - g_t) + abs(b - b_t)
                if dist <= tolerance * 3:
                    matches.append((int(rx + x), int(ry + y)))
            idx += 4

    return matches


def find_by_image(
    template_path: str,
    region: Optional[tuple[float, float, float, float]] = None,
    threshold: float = 0.8,
) -> list[tuple[int, int]]:
    """Find template image matches within a screen region.

    This uses simple template matching via OpenCV if available.

    Args:
        template_path: Path to the template PNG image.
        region: Optional search region (x, y, width, height).
        threshold: Match confidence threshold (0.0-1.0).

    Returns:
        List of (x, y) top-left coordinates of matches.
    """
    try:
        import cv2
        import numpy as np
    except ImportError:
        return []

    if region is not None:
        rx, ry, rw, rh = region
    else:
        rx, ry, rw, rh = 0, 0, 1920, 1080

    screen = _capture_region((rx, ry, rw, rh))
    if screen is None:
        return []

    screen_img = _bytes_to_cv2(screen, int(rw), int(rh))
    template = cv2.imread(template_path, cv2.IMREAD_COLOR)
    if template is None:
        return []

    result = cv2.matchTemplate(screen_img, template, cv2.TM_CCOEFF_NORMED)
    locations = np.where(result >= threshold)

    matches = []
    for pt in zip(*locations[::-1]):
        matches.append((int(pt[0] + rx), int(pt[1] + ry)))

    return matches


def find_all_in_region(
    what: str,
    region: tuple[float, float, float, float],
    threshold: float = 0.8,
) -> list[tuple[int, int]]:
    """Find all occurrences of a template image within a region.

    Args:
        what: Template image path.
        region: Screen region as (x, y, width, height).
        threshold: Match confidence threshold.

    Returns:
        List of (x, y) top-left corners of each match.
    """
    return find_by_image(what, region=region, threshold=threshold)


def group_nearby_elements(
    elements: list[UIElement],
    max_distance: float = 20.0,
) -> list[list[UIElement]]:
    """Group elements that are close to each other.

    Uses single-linkage clustering with the specified distance threshold.

    Args:
        elements: List of UIElement objects.
        max_distance: Maximum center-to-center distance to group.

    Returns:
        List of element groups (each group is a list of UIElements).
    """
    if not elements:
        return []

    n = len(elements)
    # Simple union-find clustering
    parent = list(range(n))

    def find(i: int) -> int:
        if parent[i] != i:
            parent[i] = find(parent[i])
        return parent[i]

    def union(i: int, j: int) -> None:
        pi, pj = find(i), find(j)
        if pi != pj:
            parent[pi] = pj

    for i in range(n):
        for j in range(i + 1, n):
            dist = elements[i].distance_to(*elements[j].center)
            if dist <= max_distance:
                union(i, j)

    groups: dict[int, list[UIElement]] = {}
    for i, elem in enumerate(elements):
        root = find(i)
        groups.setdefault(root, []).append(elem)

    return list(groups.values())


# -------------------------------------------------------------------
# Internal helpers
# -------------------------------------------------------------------

def _capture_region(region: tuple[float, float, float, float]) -> Optional[bytes]:
    """Capture a screen region as RGBA bytes using screencapture."""
    import subprocess

    rx, ry, rw, rh = region
    x, y, w, h = int(rx), int(ry), int(rw), int(rh)

    try:
        result = subprocess.run(
            [
                "screencapture",
                "-x",
                "-R",
                f"{x},{y},{w},{h}",
                "-",
            ],
            capture_output=True,
            timeout=5,
        )
        if result.returncode == 0 and result.stdout:
            import struct

            # Parse PNG to RGBA
            import io

            try:
                import png

                reader = png.Reader(bytes=result.stdout)
                width, height, rows, info = reader.read()
                rgba_rows = []
                for row in rows:
                    if info["planes"] == 3:
                        # RGB -> RGBA
                        new_row = []
                        for i in range(0, len(row), 3):
                            new_row.extend([row[i], row[i + 1], row[i + 2], 255])
                        row = new_row
                    rgba_rows.append(bytes(row))
                total = sum(len(r) for r in rgba_rows)
                return b"".join(rgba_rows)
            except Exception:
                return result.stdout  # return as-is
    except Exception:
        pass
    return None


def _bytes_to_cv2(data: bytes, width: int, height: int):
    """Convert RGBA bytes to a cv2 image."""
    import numpy as np
    import cv2

    arr = np.frombuffer(data, dtype=np.uint8)
    if len(arr) >= width * height * 4:
        rgba = arr[: width * height * 4].reshape((height, width, 4))
        return cv2.cvtColor(rgba, cv2.COLOR_RGBA2BGR)
    return None
