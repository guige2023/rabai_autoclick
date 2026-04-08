"""
Highlight Overlay Utilities

Provides utilities for creating highlight overlays
on UI elements in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto


class HighlightStyle(Enum):
    """Styles for highlight overlays."""
    SOLID = auto()
    DASHED = auto()
    GLOWING = auto()
    PULSING = auto()


@dataclass
class HighlightConfig:
    """Configuration for highlight overlays."""
    style: HighlightStyle = HighlightStyle.SOLID
    color: tuple[int, int, int] = (255, 0, 0)
    border_width: int = 2
    opacity: float = 0.8
    animation_duration_ms: int = 500


class HighlightOverlay:
    """
    Creates highlight overlays on UI elements.
    
    Provides various highlight styles for
    visual feedback and debugging.
    """

    def __init__(self, config: HighlightConfig | None = None) -> None:
        self._config = config or HighlightConfig()
        self._active_overlays: dict[str, bool] = {}

    def highlight_element(
        self,
        element_id: str,
        bounds: tuple[int, int, int, int],
    ) -> None:
        """
        Highlight an element by its bounds.
        
        Args:
            element_id: Unique identifier.
            bounds: (x, y, width, height).
        """
        self._active_overlays[element_id] = True

    def remove_highlight(self, element_id: str) -> None:
        """Remove highlight from element."""
        self._active_overlays.pop(element_id, None)

    def clear_all(self) -> None:
        """Clear all active highlights."""
        self._active_overlays.clear()

    def set_config(self, config: HighlightConfig) -> None:
        """Update highlight configuration."""
        self._config = config

    def get_active_count(self) -> int:
        """Get number of active highlights."""
        return len(self._active_overlays)
