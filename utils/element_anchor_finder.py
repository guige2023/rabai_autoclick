"""
Element anchor finder for stable element identification.

Finds stable anchor points for elements that may change
position but have stable surrounding context.

Author: AutoClick Team
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class Anchor:
    """
    A stable anchor point for element location.

    Attributes:
        element: The anchored element
        anchor_text: Stable text near the element
        anchor_offset: Offset from anchor point
        confidence: Anchor confidence score 0.0-1.0
    """

    element: dict[str, Any]
    anchor_text: str
    anchor_offset: tuple[float, float]
    confidence: float


class ElementAnchorFinder:
    """
    Finds stable anchor points for UI elements.

    Uses surrounding text and sibling elements as anchors
    when direct element identification is unreliable.

    Example:
        finder = ElementAnchorFinder(hierarchy=accessibility_tree)
        anchor = finder.find_anchor(
            target_role="button",
            anchor_text="Submit",
            max_distance=50,
        )
        if anchor:
            click(anchor.x + anchor.anchor_offset[0], anchor.y + anchor.anchor_offset[1])
    """

    def __init__(
        self,
        confidence_threshold: float = 0.7,
        search_radius: float = 100.0,
    ) -> None:
        """
        Initialize anchor finder.

        Args:
            confidence_threshold: Minimum confidence to return anchor
            search_radius: Max pixels to search from anchor
        """
        self._confidence_threshold = confidence_threshold
        self._search_radius = search_radius

    def find_anchor(
        self,
        elements: list[dict[str, Any]],
        target_role: str,
        anchor_text: str,
        max_distance: float = 50.0,
    ) -> Anchor | None:
        """
        Find an anchor point near text.

        Args:
            elements: List of UI elements
            target_role: Role of element to find (button, static_text, etc)
            anchor_text: Text that should be near the target
            max_distance: Maximum distance from anchor to target

        Returns:
            Anchor or None if not found
        """
        anchor_element = self._find_element_with_text(elements, anchor_text)
        if not anchor_element:
            return None

        target = self._find_nearest_target(
            elements, anchor_element, target_role, max_distance
        )
        if not target:
            return None

        offset = self._calculate_offset(anchor_element, target)
        confidence = self._calculate_confidence(anchor_element, target, max_distance)

        if confidence < self._confidence_threshold:
            return None

        return Anchor(
            element=target,
            anchor_text=anchor_text,
            anchor_offset=offset,
            confidence=confidence,
        )

    def _find_element_with_text(
        self,
        elements: list[dict[str, Any]],
        text: str,
    ) -> dict[str, Any] | None:
        """Find element containing specific text."""
        for element in elements:
            if element.get("value") == text or element.get("title") == text:
                return element
            if element.get("description") and text in element.get("description", ""):
                return element
        return None

    def _find_nearest_target(
        self,
        elements: list[dict[str, Any]],
        anchor: dict[str, Any],
        target_role: str,
        max_distance: float,
    ) -> dict[str, Any] | None:
        """Find nearest element matching target role to anchor."""
        anchor_x = anchor.get("x", 0)
        anchor_y = anchor.get("y", 0)
        anchor_rect = anchor.get("rect", {})

        if not anchor_rect:
            return None

        candidates = []

        for element in elements:
            if element.get("role") != target_role:
                continue

            rect = element.get("rect", {})
            if not rect:
                continue

            distance = self._rect_distance(anchor_rect, rect)
            if distance <= max_distance:
                candidates.append((element, distance))

        if not candidates:
            return None

        candidates.sort(key=lambda x: x[1])
        return candidates[0][0]

    def _rect_distance(
        self,
        rect_a: dict[str, float],
        rect_b: dict[str, float],
    ) -> float:
        """Calculate minimum distance between two rectangles."""
        ax = rect_a.get("x", 0)
        ay = rect_a.get("y", 0)
        aw = rect_a.get("width", 0)
        ah = rect_a.get("height", 0)

        bx = rect_b.get("x", 0)
        by = rect_b.get("y", 0)
        bw = rect_b.get("width", 0)
        bh = rect_b.get("height", 0)

        dx = max(ax - (bx + bw), bx - (ax + aw), 0)
        dy = max(ay - (by + bh), by - (ay + ah), 0)

        return (dx * dx + dy * dy) ** 0.5

    def _calculate_offset(
        self,
        anchor: dict[str, Any],
        target: dict[str, Any],
    ) -> tuple[float, float]:
        """Calculate offset from anchor to target."""
        anchor_rect = anchor.get("rect", {})
        target_rect = target.get("rect", {})

        anchor_center_x = anchor_rect.get("x", 0) + anchor_rect.get("width", 0) / 2
        anchor_center_y = anchor_rect.get("y", 0) + anchor_rect.get("height", 0) / 2

        target_center_x = target_rect.get("x", 0) + target_rect.get("width", 0) / 2
        target_center_y = target_rect.get("y", 0) + target_rect.get("height", 0) / 2

        return (target_center_x - anchor_center_x, target_center_y - anchor_center_y)

    def _calculate_confidence(
        self,
        anchor: dict[str, Any],
        target: dict[str, Any],
        max_distance: float,
    ) -> float:
        """Calculate confidence score for anchor."""
        anchor_rect = anchor.get("rect", {})
        target_rect = target.get("rect", {})

        distance = self._rect_distance(anchor_rect, target_rect)

        distance_score = max(0.0, 1.0 - (distance / max_distance))

        role_match = 0.3 if target.get("role") else 0.0

        return distance_score * 0.7 + role_match


def find_anchors_by_example(
    elements: list[dict[str, Any]],
    example_anchor: str,
    target_role: str,
) -> list[Anchor]:
    """
    Find multiple anchors matching an example pattern.

    Args:
        elements: All UI elements
        example_anchor: Text pattern for anchor
        target_role: Role of targets

    Returns:
        List of found anchors
    """
    finder = ElementAnchorFinder()
    anchors: list[Anchor] = []

    anchor_elements = [e for e in elements if example_anchor in str(e.get("value", ""))]

    for anchor_el in anchor_elements:
        rect = anchor_el.get("rect", {})
        if not rect:
            continue

        import time

        anchor = Anchor(
            element=anchor_el,
            anchor_text=str(anchor_el.get("value", "")),
            anchor_offset=(0.0, 0.0),
            confidence=1.0,
        )
        anchors.append(anchor)

    return anchors
