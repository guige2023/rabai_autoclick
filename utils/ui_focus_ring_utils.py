"""
UI focus ring utilities for rendering and managing focus indicators.

Provides focus ring rendering, animation, and positioning
for accessibility and teaching mode features.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class FocusRingStyle:
    """Style configuration for a focus ring."""
    color: tuple[int, int, int, int] = (0, 120, 215, 255)  # RGBA blue
    width: float = 2.0
    corner_radius: float = 4.0
    blur_radius: float = 0.0
    dash_pattern: Optional[tuple[float, ...]] = None
    animation_duration_ms: float = 200.0


@dataclass
class FocusRing:
    """A focus ring around an element."""
    element_id: str
    x: float
    y: float
    width: float
    height: float
    style: FocusRingStyle = field(default_factory=FocusRingStyle)
    is_visible: bool = True
    z_order: int = 1000


class FocusRingManager:
    """Manages focus rings for UI elements."""

    def __init__(self):
        self._rings: dict[str, FocusRing] = {}
        self._active_ring: Optional[str] = None
        self._style = FocusRingStyle()

    def set_style(self, style: FocusRingStyle) -> None:
        """Set the default focus ring style."""
        self._style = style

    def show_ring(self, element_id: str, x: float, y: float, width: float, height: float) -> FocusRing:
        """Show a focus ring around an element."""
        ring = FocusRing(
            element_id=element_id,
            x=x, y=y, width=width, height=height,
            style=self._style,
            is_visible=True,
        )
        self._rings[element_id] = ring
        self._active_ring = element_id
        return ring

    def hide_ring(self, element_id: str) -> None:
        """Hide a focus ring."""
        if element_id in self._rings:
            self._rings[element_id].is_visible = False
        if self._active_ring == element_id:
            self._active_ring = None

    def remove_ring(self, element_id: str) -> None:
        """Remove a focus ring completely."""
        self._rings.pop(element_id, None)
        if self._active_ring == element_id:
            self._active_ring = None

    def get_ring(self, element_id: str) -> Optional[FocusRing]:
        """Get a focus ring by element ID."""
        return self._rings.get(element_id)

    def get_active_ring(self) -> Optional[FocusRing]:
        """Get the currently active focus ring."""
        if self._active_ring:
            return self._rings.get(self._active_ring)
        return None

    def get_all_visible_rings(self) -> list[FocusRing]:
        """Get all visible focus rings."""
        return [r for r in self._rings.values() if r.is_visible]

    def move_ring(self, element_id: str, x: float, y: float, width: float, height: float) -> None:
        """Update ring position (when element moves)."""
        if element_id in self._rings:
            ring = self._rings[element_id]
            ring.x = x
            ring.y = y
            ring.width = width
            ring.height = height

    def animate_ring(self, element_id: str, to_style: FocusRingStyle) -> None:
        """Animate ring to a new style."""
        if element_id in self._rings:
            # In a real implementation, this would trigger an animation
            self._rings[element_id].style = to_style

    def clear_all(self) -> None:
        """Hide all focus rings."""
        for ring in self._rings.values():
            ring.is_visible = False
        self._active_ring = None

    def bring_to_front(self, element_id: str) -> None:
        """Bring a ring to the front of the z-order."""
        if element_id in self._rings:
            max_z = max(r.z_order for r in self._rings.values()) if self._rings else 0
            self._rings[element_id].z_order = max_z + 1


__all__ = ["FocusRingManager", "FocusRing", "FocusRingStyle"]
