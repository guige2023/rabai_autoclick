"""
Scroll Behavior Utilities

Provides utilities for managing scroll behavior
in UI automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto


class ScrollDirection(Enum):
    """Scroll direction."""
    UP = auto()
    DOWN = auto()
    LEFT = auto()
    RIGHT = auto()


class ScrollBehavior:
    """
    Manages scroll behavior for UI elements.
    
    Supports smooth scrolling, scroll-to-element,
    and scroll momentum.
    """

    def __init__(self) -> None:
        self._scroll_amount = 100
        self._smooth_scroll = True

    def set_scroll_amount(self, amount: int) -> None:
        """Set default scroll amount in pixels."""
        self._scroll_amount = amount

    def set_smooth_scroll(self, enabled: bool) -> None:
        """Enable or disable smooth scrolling."""
        self._smooth_scroll = enabled

    def get_scroll_delta(
        self,
        direction: ScrollDirection,
        multiplier: float = 1.0,
    ) -> tuple[int, int]:
        """
        Get scroll delta for direction.
        
        Args:
            direction: Scroll direction.
            multiplier: Amount multiplier.
            
        Returns:
            (dx, dy) scroll delta.
        """
        amount = int(self._scroll_amount * multiplier)
        if direction == ScrollDirection.UP:
            return (0, -amount)
        elif direction == ScrollDirection.DOWN:
            return (0, amount)
        elif direction == ScrollDirection.LEFT:
            return (-amount, 0)
        else:
            return (amount, 0)

    def calculate_scroll_to_element(
        self,
        element_bounds: tuple[int, int, int, int],
        viewport: tuple[int, int, int, int],
    ) -> tuple[int, int]:
        """Calculate scroll delta to bring element into view."""
        ex, ey, ew, eh = element_bounds
        vx, vy, vw, vh = viewport
        dx, dy = 0, 0
        if ex < vx:
            dx = ex - vx
        elif ex + ew > vx + vw:
            dx = (ex + ew) - (vx + vw)
        if ey < vy:
            dy = ey - vy
        elif ey + eh > vy + vh:
            dy = (ey + eh) - (vy + vh)
        return (dx, dy)
