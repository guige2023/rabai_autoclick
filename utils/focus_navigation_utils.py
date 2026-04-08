"""
Focus navigation utilities for keyboard-based element traversal.

Provides focus order management and keyboard navigation
through focusable elements.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class FocusableElement:
    """A focusable UI element."""
    element_id: str
    bounds: tuple[float, float, float, float] = (0, 0, 0, 0)
    tab_index: int = 0
    focus_order: int = -1
    can_receive_focus: bool = True
    metadata: dict = field(default_factory=dict)

    @property
    def x(self) -> float:
        return self.bounds[0]

    @property
    def y(self) -> float:
        return self.bounds[1]

    @property
    def center_x(self) -> float:
        return self.bounds[0] + self.bounds[2] / 2

    @property
    def center_y(self) -> float:
        return self.bounds[1] + self.bounds[3] / 2


@dataclass
class NavigationResult:
    """Result of focus navigation operation."""
    target_element: Optional[str]
    navigation_type: str  # "next", "prev", "up", "down", "left", "right"
    wrapped: bool = False


class FocusNavigationEngine:
    """Manages focus navigation order and keyboard traversal."""

    def __init__(self):
        self._elements: dict[str, FocusableElement] = {}
        self._focus_order: list[str] = []
        self._current_focus: Optional[str] = None

    def register(self, element: FocusableElement) -> None:
        """Register a focusable element."""
        self._elements[element.element_id] = element
        self._rebuild_focus_order()

    def unregister(self, element_id: str) -> None:
        """Unregister an element."""
        self._elements.pop(element_id, None)
        if self._current_focus == element_id:
            self._current_focus = None
        self._rebuild_focus_order()

    def set_focus(self, element_id: str) -> bool:
        """Set focus to an element."""
        if element_id in self._elements:
            self._current_focus = element_id
            return True
        return False

    def get_focused(self) -> Optional[str]:
        """Get currently focused element ID."""
        return self._current_focus

    def navigate_next(self) -> NavigationResult:
        """Navigate to the next focusable element."""
        if not self._focus_order:
            return NavigationResult(None, "next")

        if self._current_focus is None:
            target = self._focus_order[0]
            return NavigationResult(target, "next")

        try:
            idx = self._focus_order.index(self._current_focus)
            next_idx = (idx + 1) % len(self._focus_order)
            wrapped = next_idx == 0
            return NavigationResult(
                self._focus_order[next_idx],
                "next",
                wrapped=wrapped,
            )
        except ValueError:
            return NavigationResult(self._focus_order[0], "next")

    def navigate_prev(self) -> NavigationResult:
        """Navigate to the previous focusable element."""
        if not self._focus_order:
            return NavigationResult(None, "prev")

        if self._current_focus is None:
            target = self._focus_order[-1]
            return NavigationResult(target, "prev")

        try:
            idx = self._focus_order.index(self._current_focus)
            prev_idx = (idx - 1 + len(self._focus_order)) % len(self._focus_order)
            wrapped = prev_idx == len(self._focus_order) - 1
            return NavigationResult(
                self._focus_order[prev_idx],
                "prev",
                wrapped=wrapped,
            )
        except ValueError:
            return NavigationResult(self._focus_order[-1], "prev")

    def navigate_to_first(self) -> NavigationResult:
        """Navigate to the first focusable element."""
        if self._focus_order:
            return NavigationResult(self._focus_order[0], "next")
        return NavigationResult(None, "next")

    def navigate_to_last(self) -> NavigationResult:
        """Navigate to the last focusable element."""
        if self._focus_order:
            return NavigationResult(self._focus_order[-1], "prev")
        return NavigationResult(None, "prev")

    def navigate_by_direction(self, direction: str) -> NavigationResult:
        """Navigate in a spatial direction (up/down/left/right)."""
        if not self._current_focus or self._current_focus not in self._elements:
            return NavigationResult(None, direction)

        current = self._elements[self._current_focus]
        candidates = [
            (eid, e) for eid, e in self._elements.items()
            if eid != self._current_focus
        ]

        dx = dy = 0
        if direction == "up":
            dy = -1
        elif direction == "down":
            dy = 1
        elif direction == "left":
            dx = -1
        elif direction == "right":
            dx = 1

        best = None
        best_score = float("inf")

        for eid, elem in candidates:
            ecx, ecy = elem.center_x, elem.center_y
            ccx, ccy = current.center_x, current.center_y

            # Only consider elements in the correct direction
            if dx != 0 and (ecx - ccx) * dx < 0:
                continue
            if dy != 0 and (ecy - ccy) * dy < 0:
                continue

            # Score by distance
            dist = abs(ecx - ccx) + abs(ecy - ccy)
            if dist < best_score:
                best_score = dist
                best = eid

        return NavigationResult(best, direction)

    def _rebuild_focus_order(self) -> None:
        """Rebuild the tab order list."""
        elements = sorted(
            self._elements.values(),
            key=lambda e: (e.tab_index, e.focus_order),
        )
        self._focus_order = [e.element_id for e in elements if e.can_receive_focus]


__all__ = ["FocusNavigationEngine", "FocusableElement", "NavigationResult"]
