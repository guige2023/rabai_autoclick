"""
Hit testing utilities for UI element interaction.

Provides hit testing logic to determine which element is at a given
coordinate, including support for transparent pixels and overlapping elements.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional, Callable


@dataclass
class HitResult:
    """Result of a hit test."""
    x: float
    y: float
    element: Optional[str] = None
    confidence: float = 0.0
    path: list[str] = None

    def __post_init__(self):
        if self.path is None:
            self.path = []


@dataclass
class HitTestConfig:
    """Configuration for hit testing behavior."""
    tolerance: int = 0
    check_alpha: bool = False
    alpha_threshold: int = 128
    prefer_topmost: bool = True
    hit_radius: float = 0.0
    bounds_check: bool = True


class HitTester:
    """Main hit testing engine."""

    def __init__(self, config: Optional[HitTestConfig] = None):
        self.config = config or HitTestConfig()
        self._elements: dict[str, ElementInfo] = {}

    def register_element(
        self,
        element_id: str,
        bounds: tuple[float, float, float, float],
        alpha: Optional[bytes] = None,
        z_order: int = 0,
    ) -> None:
        """Register an element for hit testing.

        Args:
            element_id: Unique element identifier
            bounds: (x, y, width, height)
            alpha: Optional RGBA alpha channel data for per-pixel hit testing
            z_order: Z-order index (higher = on top)
        """
        self._elements[element_id] = ElementInfo(
            element_id=element_id,
            bounds=bounds,
            alpha=alpha,
            z_order=z_order,
        )

    def unregister_element(self, element_id: str) -> None:
        self._elements.pop(element_id, None)

    def clear(self) -> None:
        self._elements.clear()

    def hit_test(self, x: float, y: float) -> HitResult:
        """Perform hit test at coordinates.

        Args:
            x: Screen X coordinate
            y: Screen Y coordinate

        Returns:
            HitResult with the topmost element at that position
        """
        candidates: list[tuple[float, ElementInfo]] = []

        for elem_info in self._elements.values():
            bx, by, bw, bh = elem_info.bounds
            if self.config.bounds_check:
                if not (bx <= x <= bx + bw and by <= y <= by + bh):
                    continue

            confidence = self._compute_confidence(x, y, elem_info)
            if confidence > 0:
                candidates.append((confidence, elem_info))

        if not candidates:
            return HitResult(x=x, y=y)

        candidates.sort(key=lambda c: c[0], reverse=True)

        if self.config.prefer_topmost:
            top = max(candidates, key=lambda c: c[1].z_order)
            return HitResult(
                x=x, y=y,
                element=top[1].element_id,
                confidence=top[0],
                path=[top[1].element_id],
            )

        best = candidates[0]
        return HitResult(
            x=x, y=y,
            element=best[1].element_id,
            confidence=best[0],
            path=[best[1].element_id],
        )

    def _compute_confidence(self, x: float, y: float, elem_info: ElementInfo) -> float:
        """Compute hit confidence based on element type and position."""
        bx, by, bw, bh = elem_info.bounds

        if self.config.tolerance > 0:
            tx = self.config.tolerance
            ty = self.config.tolerance
            if not (bx - tx <= x <= bx + bw + tx and by - ty <= y <= by + bh + ty):
                return 0.0

        if self.config.check_alpha and elem_info.alpha is not None:
            local_x = int(x - bx)
            local_y = int(y - by)
            if 0 <= local_y < bh and 0 <= local_x < bw:
                alpha_idx = (local_y * bw + local_x)
                if alpha_idx < len(elem_info.alpha):
                    if elem_info.alpha[alpha_idx] < self.config.alpha_threshold:
                        return 0.0

        if self.config.hit_radius > 0:
            cx = bx + bw / 2
            cy = by + bh / 2
            dist = math.hypot(x - cx, y - cy)
            max_dist = math.hypot(bw / 2, bh / 2) + self.config.hit_radius
            return max(0.0, 1.0 - dist / max_dist)

        if bx <= x <= bx + bw and by <= y <= by + bh:
            return 1.0

        return 0.0

    def hit_test_all(self, x: float, y: float) -> list[HitResult]:
        """Get all elements at a given position, sorted by z-order."""
        results = []
        for elem_info in self._elements.values():
            bx, by, bw, bh = elem_info.bounds
            if bx <= x <= bx + bw and by <= y <= by + bh:
                confidence = self._compute_confidence(x, y, elem_info)
                if confidence > 0:
                    results.append(HitResult(
                        x=x, y=y,
                        element=elem_info.element_id,
                        confidence=confidence,
                        path=[elem_info.element_id],
                    ))

        results.sort(key=lambda r: self._elements[r.element].z_order if r.element else 0)
        return results


@dataclass
class ElementInfo:
    """Information about a registered element."""
    element_id: str
    bounds: tuple[float, float, float, float]
    alpha: Optional[bytes] = None
    z_order: int = 0


__all__ = ["HitTester", "HitResult", "HitTestConfig", "ElementInfo"]
