"""Visual feedback utilities.

This module provides utilities for displaying visual feedback
during automation (highlighting elements, showing overlays, etc).
"""

from __future__ import annotations

from typing import Optional, Tuple
from dataclasses import dataclass
from enum import Enum, auto
import time


class FeedbackType(Enum):
    """Types of visual feedback."""
    HIGHLIGHT = auto()
    CROSSHAIR = auto()
    RING = auto()
    ARROW = auto()
    LABEL = auto()
    ANIMATION = auto()


@dataclass
class FeedbackConfig:
    """Configuration for visual feedback."""
    color: Tuple[int, int, int, int] = (255, 0, 0, 180)
    border_width: int = 3
    duration_ms: int = 500
    fade_out_ms: int = 200


@dataclass
class FeedbackElement:
    """A visual feedback element."""
    feedback_type: FeedbackType
    x: int
    y: int
    width: int = 0
    height: int = 0
    label: str = ""
    config: Optional[FeedbackConfig] = None
    created_at: float = 0.0

    def __post_init__(self) -> None:
        if self.created_at == 0.0:
            self.created_at = time.perf_counter()
        if self.config is None:
            self.config = FeedbackConfig()


class VisualFeedbackManager:
    """Manages visual feedback overlays."""

    def __init__(self) -> None:
        self._elements: list[FeedbackElement] = []
        self._enabled = True

    def enable(self) -> None:
        self._enabled = True

    def disable(self) -> None:
        self._enabled = False

    @property
    def is_enabled(self) -> bool:
        return self._enabled

    def highlight_region(
        self,
        x: int,
        y: int,
        width: int,
        height: int,
        config: Optional[FeedbackConfig] = None,
    ) -> FeedbackElement:
        """Add a region highlight.

        Args:
            x: X coordinate.
            y: Y coordinate.
            width: Region width.
            height: Region height.
            config: Feedback configuration.

        Returns:
            Created FeedbackElement.
        """
        elem = FeedbackElement(
            feedback_type=FeedbackType.HIGHLIGHT,
            x=x,
            y=y,
            width=width,
            height=height,
            config=config,
        )
        self._elements.append(elem)
        return elem

    def show_crosshair(
        self,
        x: int,
        y: int,
        config: Optional[FeedbackConfig] = None,
    ) -> FeedbackElement:
        """Show a crosshair at coordinates.

        Args:
            x: X coordinate.
            y: Y coordinate.
            config: Feedback configuration.

        Returns:
            Created FeedbackElement.
        """
        elem = FeedbackElement(
            feedback_type=FeedbackType.CROSSHAIR,
            x=x,
            y=y,
            config=config,
        )
        self._elements.append(elem)
        return elem

    def show_ring(
        self,
        x: int,
        y: int,
        radius: int = 20,
        config: Optional[FeedbackConfig] = None,
    ) -> FeedbackElement:
        """Show a ring at coordinates.

        Args:
            x: Center X.
            y: Center Y.
            radius: Ring radius.
            config: Feedback configuration.

        Returns:
            Created FeedbackElement.
        """
        elem = FeedbackElement(
            feedback_type=FeedbackType.RING,
            x=x,
            y=y,
            width=radius,
            height=radius,
            config=config,
        )
        self._elements.append(elem)
        return elem

    def show_label(
        self,
        x: int,
        y: int,
        text: str,
        config: Optional[FeedbackConfig] = None,
    ) -> FeedbackElement:
        """Show a label at coordinates.

        Args:
            x: X coordinate.
            y: Y coordinate.
            text: Label text.
            config: Feedback configuration.

        Returns:
            Created FeedbackElement.
        """
        elem = FeedbackElement(
            feedback_type=FeedbackType.LABEL,
            x=x,
            y=y,
            label=text,
            config=config,
        )
        self._elements.append(elem)
        return elem

    def clear(self) -> None:
        """Clear all feedback elements."""
        self._elements.clear()

    def clear_expired(self, max_age_ms: float = 1000.0) -> None:
        """Clear expired feedback elements.

        Args:
            max_age_ms: Maximum age in milliseconds.
        """
        now = time.perf_counter()
        max_age_s = max_age_ms / 1000.0
        self._elements = [
            e for e in self._elements
            if (now - e.created_at) * 1000 < max_age_ms
        ]

    @property
    def elements(self) -> list[FeedbackElement]:
        """Get all feedback elements."""
        return self._elements.copy()


__all__ = [
    "FeedbackType",
    "FeedbackConfig",
    "FeedbackElement",
    "VisualFeedbackManager",
]
