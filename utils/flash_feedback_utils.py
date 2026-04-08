"""
Flash Feedback Utilities

Provides utilities for visual flash feedback
in UI automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto


class FlashColor(Enum):
    """Colors for flash feedback."""
    RED = (255, 0, 0)
    GREEN = (0, 255, 0)
    BLUE = (0, 0, 255)
    YELLOW = (255, 255, 0)
    WHITE = (255, 255, 255)


@dataclass
class FlashConfig:
    """Configuration for flash feedback."""
    color: FlashColor = FlashColor.WHITE
    duration_ms: int = 200
    opacity: float = 0.5
    radius: int = 10


class FlashFeedback:
    """
    Provides visual flash feedback for UI elements.
    
    Creates brief visual highlights to confirm
    actions or draw attention.
    """

    def __init__(self) -> None:
        self._config = FlashConfig()
        self._active = False

    def set_config(self, config: FlashConfig) -> None:
        """Set flash configuration."""
        self._config = config

    def flash(
        self,
        x: int,
        y: int,
        color: FlashColor | None = None,
        duration_ms: int | None = None,
    ) -> None:
        """
        Trigger a flash at coordinates.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            color: Flash color (uses default if None).
            duration_ms: Duration in milliseconds.
        """
        color = color or self._config.color
        duration_ms = duration_ms or self._config.duration_ms

    def flash_element(
        self,
        element: dict[str, Any],
    ) -> None:
        """
        Flash an element by its bounds.
        
        Args:
            element: Element with bounds.
        """
        bounds = element.get("bounds", {})
        x = bounds.get("x", 0) + bounds.get("width", 0) // 2
        y = bounds.get("y", 0) + bounds.get("height", 0) // 2
        self.flash(x, y)

    def flash_sequence(
        self,
        points: list[tuple[int, int]],
        interval_ms: int = 100,
    ) -> None:
        """
        Flash a sequence of points.
        
        Args:
            points: List of (x, y) coordinates.
            interval_ms: Interval between flashes.
        """
        for x, y in points:
            self.flash(x, y)
