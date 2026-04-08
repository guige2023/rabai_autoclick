"""Point and Click Target Utilities.

Utilities for computing optimal click targets on UI elements.
Handles hit testing, target centering, and click position optimization.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional


class ClickTarget(Enum):
    """Types of click target positions."""

    CENTER = auto()
    TOP_LEFT = auto()
    TOP_CENTER = auto()
    TOP_RIGHT = auto()
    MIDDLE_LEFT = auto()
    MIDDLE_RIGHT = auto()
    BOTTOM_LEFT = auto()
    BOTTOM_CENTER = auto()
    BOTTOM_RIGHT = auto()
    NEAREST_EDGE = auto()
    HOTSPOT = auto()


@dataclass
class ClickTargetInfo:
    """Information about a computed click target.

    Attributes:
        x: X coordinate of click target.
        y: Y coordinate of click target.
        target_type: Type of target computed.
        element_id: Element this target belongs to.
        confidence: Confidence score (0.0 to 1.0).
        offset: Offset from element origin.
    """

    x: float
    y: float
    target_type: ClickTarget
    element_id: str
    confidence: float = 1.0
    offset: tuple[float, float] = (0.0, 0.0)


@dataclass
class HitTestResult:
    """Result of a hit test.

    Attributes:
        hit: Whether a point is within a target.
        element_id: ID of hit element, if any.
        distance_to_edge: Distance from point to nearest edge.
        coverage_ratio: Portion of click point covered (for edge hits).
    """

    hit: bool
    element_id: Optional[str] = None
    distance_to_edge: float = 0.0
    coverage_ratio: float = 0.0


class HitTester:
    """Tests whether points hit UI elements.

    Supports rectangular, circular, and polygonal hit regions.

    Example:
        tester = HitTester()
        if tester.point_in_rect((100, 100), (50, 50, 150, 150)):
            print("Hit!")
    """

    @staticmethod
    def point_in_rect(
        point: tuple[float, float],
        bounds: tuple[float, float, float, float],
    ) -> bool:
        """Test if a point is inside a rectangle.

        Args:
            point: (x, y) coordinates.
            bounds: (x, y, width, height) of rectangle.

        Returns:
            True if point is inside the rectangle.
        """
        px, py = point
        x, y, w, h = bounds
        return x <= px <= x + w and y <= py <= y + h

    @staticmethod
    def point_in_circle(
        point: tuple[float, float],
        center: tuple[float, float],
        radius: float,
    ) -> bool:
        """Test if a point is inside a circle.

        Args:
            point: (x, y) coordinates.
            center: (cx, cy) center of circle.
            radius: Circle radius.

        Returns:
            True if point is inside the circle.
        """
        px, py = point
        cx, cy = center
        dx = px - cx
        dy = py - cy
        return dx * dx + dy * dy <= radius * radius

    @staticmethod
    def distance_to_rect_edge(
        point: tuple[float, float],
        bounds: tuple[float, float, float, float],
    ) -> float:
        """Calculate distance from point to nearest rectangle edge.

        Args:
            point: (x, y) coordinates.
            bounds: (x, y, width, height) of rectangle.

        Returns:
            Minimum distance to edge.
        """
        px, py = point
        x, y, w, h = bounds

        # Clamp point to rectangle bounds
        nearest_x = max(x, min(px, x + w))
        nearest_y = max(y, min(py, y + h))

        dx = px - nearest_x
        dy = py - nearest_y
        return math.sqrt(dx * dx + dy * dy)

    @staticmethod
    def nearest_point_on_rect(
        point: tuple[float, float],
        bounds: tuple[float, float, float, float],
    ) -> tuple[float, float]:
        """Find nearest point on rectangle boundary to a point.

        Args:
            point: (x, y) coordinates.
            bounds: (x, y, width, height) of rectangle.

        Returns:
            (x, y) of nearest point on rectangle edge.
        """
        px, py = point
        x, y, w, h = bounds

        nearest_x = max(x, min(px, x + w))
        nearest_y = max(y, min(py, y + h))

        return (nearest_x, nearest_y)


class ClickTargetCalculator:
    """Computes optimal click targets for UI elements.

    Example:
        calculator = ClickTargetCalculator()
        target = calculator.compute_target(
            bounds=(100, 100, 200, 50),
            target_type=ClickTarget.CENTER,
            element_id="button1",
        )
    """

    def __init__(self, padding: float = 2.0):
        """Initialize calculator.

        Args:
            padding: Padding inside element bounds for clickable area.
        """
        self.padding = padding
        self.hit_tester = HitTester()

    def compute_target(
        self,
        bounds: tuple[float, float, float, float],
        target_type: ClickTarget,
        element_id: str,
        hotspots: Optional[list[tuple[float, float]]] = None,
    ) -> ClickTargetInfo:
        """Compute click target position.

        Args:
            bounds: Element bounds (x, y, width, height).
            target_type: Desired target type.
            element_id: Element identifier.
            hotspots: Optional list of preferred click points.

        Returns:
            ClickTargetInfo with computed position.
        """
        x, y, w, h = bounds
        padded = self._apply_padding(bounds)

        if target_type == ClickTarget.CENTER:
            cx = x + w / 2
            cy = y + h / 2
            return ClickTargetInfo(
                x=cx, y=cy, target_type=target_type, element_id=element_id,
                offset=(cx - x, cy - y)
            )
        elif target_type == ClickTarget.TOP_LEFT:
            return ClickTargetInfo(
                x=padded[0], y=padded[1], target_type=target_type, element_id=element_id,
                offset=(0, 0)
            )
        elif target_type == ClickTarget.TOP_RIGHT:
            return ClickTargetInfo(
                x=padded[0] + padded[2] - 1, y=padded[1], target_type=target_type,
                element_id=element_id, offset=(padded[2] - 1, 0)
            )
        elif target_type == ClickTarget.BOTTOM_CENTER:
            return ClickTargetInfo(
                x=x + w / 2, y=y + h - 1, target_type=target_type, element_id=element_id,
                offset=(w / 2, h - 1)
            )
        elif target_type == ClickTarget.NEAREST_EDGE:
            return self._compute_nearest_edge_target(bounds, element_id)
        elif target_type == ClickTarget.HOTSPOT and hotspots:
            return self._compute_hotspot_target(bounds, hotspots, element_id)
        else:
            # Default to center
            return self.compute_target(bounds, ClickTarget.CENTER, element_id)

    def _apply_padding(self, bounds: tuple[float, float, float, float]) -> tuple[float, float, float, float]:
        """Apply padding to bounds.

        Args:
            bounds: Original bounds.

        Returns:
            Padded bounds.
        """
        x, y, w, h = bounds
        p = self.padding
        return (x + p, y + p, max(1, w - 2 * p), max(1, h - 2 * p))

    def _compute_nearest_edge_target(
        self,
        bounds: tuple[float, float, float, float],
        element_id: str,
    ) -> ClickTargetInfo:
        """Compute target at edge nearest to center of element.

        Args:
            bounds: Element bounds.
            element_id: Element identifier.

        Returns:
            ClickTargetInfo for nearest edge.
        """
        x, y, w, h = bounds
        cx, cy = x + w / 2, y + h / 2

        edges = {
            "top": (cx, y),
            "bottom": (cx, y + h - 1),
            "left": (x, cy),
            "right": (x + w - 1, cy),
        }

        min_dist = float("inf")
        best_edge = "center"
        best_point = (cx, cy)

        for edge_name, (ex, ey) in edges.items():
            dist = math.sqrt((ex - cx) ** 2 + (ey - cy) ** 2)
            if dist < min_dist:
                min_dist = dist
                best_edge = edge_name
                best_point = (ex, ey)

        return ClickTargetInfo(
            x=best_point[0], y=best_point[1],
            target_type=ClickTarget.NEAREST_EDGE,
            element_id=element_id,
        )

    def _compute_hotspot_target(
        self,
        bounds: tuple[float, float, float, float],
        hotspots: list[tuple[float, float]],
        element_id: str,
    ) -> ClickTargetInfo:
        """Compute click target at best hotspot location.

        Args:
            bounds: Element bounds.
            hotspots: List of hotspot coordinates.
            element_id: Element identifier.

        Returns:
            ClickTargetInfo for best hotspot.
        """
        bx, by, bw, bh = bounds

        valid_hotspots = []
        for hx, hy in hotspots:
            if bx <= hx <= bx + bw and by <= hy <= by + bh:
                valid_hotspots.append((hx, hy))

        if not valid_hotspots:
            return self.compute_target(bounds, ClickTarget.CENTER, element_id)

        # Pick hotspot closest to center
        cx, cy = bx + bw / 2, by + bh / 2
        best = min(
            valid_hotspots,
            key=lambda p: (p[0] - cx) ** 2 + (p[1] - cy) ** 2
        )

        return ClickTargetInfo(
            x=best[0], y=best[1],
            target_type=ClickTarget.HOTSPOT,
            element_id=element_id,
            confidence=0.9,
        )


class MultiElementClickResolver:
    """Resolves click targets when multiple elements overlap.

    Determines which element should receive the click based on
    z-order, visibility, and interaction priority.

    Example:
        resolver = MultiElementClickResolver()
        target = resolver.resolve_click(
            click_point=(100, 100),
            elements=[elem1, elem2, elem3],
        )
    """

    def __init__(self, calculator: Optional[ClickTargetCalculator] = None):
        """Initialize resolver.

        Args:
            calculator: ClickTargetCalculator to use.
        """
        self.calculator = calculator or ClickTargetCalculator()
        self.hit_tester = HitTester()

    def resolve_click(
        self,
        click_point: tuple[float, float],
        elements: list[tuple[str, tuple[float, float, float, float]]],
        z_order: Optional[list[str]] = None,
    ) -> Optional[ClickTargetInfo]:
        """Resolve which element should receive a click.

        Args:
            click_point: (x, y) click coordinates.
            elements: List of (element_id, bounds) tuples.
            z_order: Optional list of element IDs in z-order (top to bottom).

        Returns:
            ClickTargetInfo for the element to click, or None.
        """
        # Find all elements that contain the click point
        hit_elements = []
        for element_id, bounds in elements:
            if self.hit_tester.point_in_rect(click_point, bounds):
                dist = self.hit_tester.distance_to_rect_edge(click_point, bounds)
                hit_elements.append((element_id, bounds, dist))

        if not hit_elements:
            return None

        # Sort by z-order if provided, then by distance to edge (prefer center)
        if z_order:
            z_index = {eid: i for i, eid in enumerate(z_order)}
            hit_elements.sort(key=lambda e: (z_index.get(e[0], 999), e[2]))
        else:
            hit_elements.sort(key=lambda e: e[2])

        top_element_id, bounds, _ = hit_elements[0]
        return self.calculator.compute_target(
            bounds=bounds,
            target_type=ClickTarget.CENTER,
            element_id=top_element_id,
        )
