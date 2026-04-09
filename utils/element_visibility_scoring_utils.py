"""
Element Visibility Scoring Utilities

Score the visibility of UI elements based on factors including
area coverage, occlusion, transparency, and viewport position.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class VisibilityScore:
    """Visibility score for a UI element."""
    overall: float  # 0.0 to 1.0
    area_score: float
    occlusion_score: float
    position_score: float
    is_visible: bool


class ElementVisibilityScorer:
    """
    Compute a composite visibility score for UI elements.

    Visibility is affected by: how much of the element is on-screen,
    how much is covered by other elements, and where on the screen it is.
    """

    def __init__(
        self,
        viewport_width: float = 1920,
        viewport_height: float = 1080,
        edge_margin: float = 10.0,
    ):
        self.viewport_width = viewport_width
        self.viewport_height = viewport_height
        self.edge_margin = edge_margin

    def score(
        self,
        element_bounds: Tuple[float, float, float, float],  # x, y, width, height
        occluded_area_percent: float = 0.0,
        transparency: float = 0.0,
    ) -> VisibilityScore:
        """
        Compute visibility score for an element.

        Args:
            element_bounds: Element's bounding box (x, y, width, height).
            occluded_area_percent: Percentage of element area that is occluded (0.0 to 1.0).
            transparency: Element transparency (0.0 = opaque, 1.0 = fully transparent).

        Returns:
            VisibilityScore with per-component and overall scores.
        """
        x, y, w, h = element_bounds

        area_score = self._compute_area_score(x, y, w, h)
        occlusion_score = max(0.0, 1.0 - occluded_area_percent)
        position_score = self._compute_position_score(x, y, w, h)

        # Transparency significantly reduces visibility
        transparency_factor = 1.0 - (transparency * 0.8)

        overall = (
            area_score * 0.4
            + occlusion_score * 0.3
            + position_score * 0.3
        ) * transparency_factor

        return VisibilityScore(
            overall=max(0.0, min(1.0, overall)),
            area_score=area_score,
            occlusion_score=occlusion_score,
            position_score=position_score,
            is_visible=overall > 0.3 and area_score > 0.1,
        )

    def _compute_area_score(
        self,
        x: float, y: float, w: float, h: float,
    ) -> float:
        """Compute score based on how much of the element is on screen."""
        screen_w, screen_h = self.viewport_width, self.viewport_height

        # Compute intersection with viewport
        x1 = max(x, 0)
        y1 = max(y, 0)
        x2 = min(x + w, screen_w)
        y2 = min(y + h, screen_h)

        if x2 <= x1 or y2 <= y1:
            return 0.0

        intersection_area = (x2 - x1) * (y2 - y1)
        element_area = w * h
        if element_area <= 0:
            return 0.0

        return min(1.0, intersection_area / element_area)

    def _compute_position_score(
        self,
        x: float, y: float, w: float, h: float,
    ) -> float:
        """Compute score based on element position (central elements score higher)."""
        cx = x + w / 2
        cy = y + h / 2

        # Center of screen
        center_x = self.viewport_width / 2
        center_y = self.viewport_height / 2

        # Distance from center normalized
        max_dist = math.sqrt(center_x ** 2 + center_y ** 2)
        dist = math.sqrt((cx - center_x) ** 2 + (cy - center_y) ** 2)

        # Elements near edges get lower scores
        edge_x = min(cx / self.edge_margin, (self.viewport_width - cx) / self.edge_margin, 1.0)
        edge_y = min(cy / self.edge_margin, (self.viewport_height - cy) / self.edge_margin, 1.0)
        edge_score = min(1.0, edge_x * edge_y)

        position_score = max(0.0, 1.0 - (dist / max_dist)) * 0.5 + edge_score * 0.5
        return min(1.0, position_score)
