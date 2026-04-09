"""
Visual feedback and overlay utilities for UI automation.

Provides visual feedback mechanisms like highlights,
overlays, crosshairs, and animated indicators.

Author: Auto-generated
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable


class FeedbackType(Enum):
    """Types of visual feedback."""
    HIGHLIGHT = auto()
    CROSSHAIR = auto()
    PULSE = auto()
    RIPPLE = auto()
    SPOTLIGHT = auto()
    DRAW_RECT = auto()


@dataclass
class FeedbackConfig:
    """Configuration for visual feedback."""
    color: str = "#FF0000"
    width: float = 3.0
    duration_ms: float = 1000
    fade_in_ms: float = 100
    fade_out_ms: float = 200
    fill_color: str | None = None
    fill_opacity: float = 0.2


@dataclass
class OverlayElement:
    """An element to render in an overlay."""
    element_type: FeedbackType
    x: float
    y: float
    width: float = 0
    height: float = 0
    config: FeedbackConfig = field(default_factory=FeedbackConfig)
    metadata: dict = field(default_factory=dict)
    
    @property
    def bounds(self) -> tuple[float, float, float, float]:
        """Get bounds as (x, y, width, height)."""
        return (self.x, self.y, self.width, self.height)


class VisualFeedbackOverlay:
    """
    Renders visual feedback overlays.
    
    Example:
        overlay = VisualFeedbackOverlay()
        overlay.show_highlight(100, 100, 200, 200)
        time.sleep(1)
        overlay.hide()
    """
    
    def __init__(self, render_callback: Callable[[list[OverlayElement]], None] | None = None):
        self._render_callback = render_callback
        self._elements: list[OverlayElement] = []
        self._active = False
        self._animation_frame: Callable | None = None
    
    def show_highlight(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
        config: FeedbackConfig | None = None,
    ) -> OverlayElement:
        """
        Show a highlight around an area.
        
        Args:
            x, y: Top-left position
            width, height: Size of highlight area
            config: Optional feedback configuration
            
        Returns:
            Created OverlayElement
        """
        element = OverlayElement(
            element_type=FeedbackType.HIGHLIGHT,
            x=x,
            y=y,
            width=width,
            height=height,
            config=config or FeedbackConfig(),
        )
        self._elements.append(element)
        self._render()
        return element
    
    def show_crosshair(
        self,
        x: float,
        y: float,
        size: float = 20,
        config: FeedbackConfig | None = None,
    ) -> OverlayElement:
        """
        Show a crosshair at a point.
        
        Args:
            x, y: Center position
            size: Size of crosshair lines
            config: Optional feedback configuration
            
        Returns:
            Created OverlayElement
        """
        element = OverlayElement(
            element_type=FeedbackType.CROSSHAIR,
            x=x,
            y=y,
            width=size,
            height=size,
            config=config or FeedbackConfig(),
        )
        self._elements.append(element)
        self._render()
        return element
    
    def show_pulse(
        self,
        x: float,
        y: float,
        radius: float = 30,
        config: FeedbackConfig | None = None,
    ) -> OverlayElement:
        """
        Show a pulsing circle at a point.
        
        Args:
            x, y: Center position
            radius: Initial radius of pulse
            config: Optional feedback configuration
            
        Returns:
            Created OverlayElement
        """
        element = OverlayElement(
            element_type=FeedbackType.PULSE,
            x=x,
            y=y,
            width=radius,
            height=radius,
            config=config or FeedbackConfig(),
            metadata={"max_radius": radius * 2},
        )
        self._elements.append(element)
        self._start_animation(element)
        return element
    
    def show_ripple(
        self,
        x: float,
        y: float,
        config: FeedbackConfig | None = None,
    ) -> OverlayElement:
        """
        Show a ripple effect at a point.
        
        Args:
            x, y: Center of ripple
            config: Optional feedback configuration
            
        Returns:
            Created OverlayElement
        """
        element = OverlayElement(
            element_type=FeedbackType.RIPPLE,
            x=x,
            y=y,
            width=10,
            height=10,
            config=config or FeedbackConfig(duration_ms=600),
            metadata={"max_radius": 100},
        )
        self._elements.append(element)
        self._start_animation(element)
        return element
    
    def show_rectangle(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
        config: FeedbackConfig | None = None,
    ) -> OverlayElement:
        """
        Show a rectangle outline.
        
        Args:
            x, y: Top-left position
            width, height: Rectangle dimensions
            config: Optional feedback configuration
            
        Returns:
            Created OverlayElement
        """
        element = OverlayElement(
            element_type=FeedbackType.DRAW_RECT,
            x=x,
            y=y,
            width=width,
            height=height,
            config=config or FeedbackConfig(),
        )
        self._elements.append(element)
        self._render()
        return element
    
    def _render(self) -> None:
        """Render all elements via callback."""
        if self._render_callback:
            self._render_callback(self._elements)
    
    def _start_animation(self, element: OverlayElement) -> None:
        """Start animation for an animated element."""
        pass  # Animation handled by external loop
    
    def hide(self, element: OverlayElement | None = None) -> None:
        """
        Hide feedback overlay.
        
        Args:
            element: Specific element to hide, or None to hide all
        """
        if element is None:
            self._elements.clear()
        elif element in self._elements:
            self._elements.remove(element)
        self._render()
    
    def clear(self) -> None:
        """Clear all feedback elements."""
        self._elements.clear()
        self._render()
    
    def get_elements(self) -> list[OverlayElement]:
        """Get all current overlay elements."""
        return list(self._elements)
    
    def is_active(self) -> bool:
        """Check if overlay is active."""
        return self._active


class SpotlightEffect:
    """
    Creates a spotlight effect that dims everything except
    a highlighted area.
    """
    
    def __init__(self, render_callback: Callable[[list], None] | None = None):
        self._render_callback = render_callback
        self._spotlight_x: float = 0
        self._spotlight_y: float = 0
        self._spotlight_radius: float = 50
        self._dim_color: str = "rgba(0, 0, 0, 0.5)"
        self._active = False
    
    def show(
        self,
        x: float,
        y: float,
        radius: float = 50,
        dim_color: str = "rgba(0, 0, 0, 0.5)",
    ) -> None:
        """
        Show spotlight effect.
        
        Args:
            x, y: Center of spotlight
            radius: Radius of clear area
            dim_color: Color for dimmed area
        """
        self._spotlight_x = x
        self._spotlight_y = y
        self._spotlight_radius = radius
        self._dim_color = dim_color
        self._active = True
        self._render()
    
    def move(self, x: float, y: float) -> None:
        """Move spotlight to new position."""
        self._spotlight_x = x
        self._spotlight_y = y
        if self._active:
            self._render()
    
    def hide(self) -> None:
        """Hide spotlight effect."""
        self._active = False
        self._render()
    
    def _render(self) -> None:
        """Render via callback."""
        if self._render_callback:
            self._render_callback([self])
    
    @property
    def spotlight_data(self) -> dict:
        """Get spotlight data for rendering."""
        return {
            "x": self._spotlight_x,
            "y": self._spotlight_y,
            "radius": self._spotlight_radius,
            "dim_color": self._dim_color,
            "active": self._active,
        }


def create_click_feedback(
    x: float,
    y: float,
    feedback_type: str = "ripple",
) -> OverlayElement:
    """
    Create a click feedback element at position.
    
    Args:
        x, y: Click position
        feedback_type: Type of feedback ('ripple', 'pulse', 'highlight')
        
    Returns:
        OverlayElement configured for click feedback
    """
    config = FeedbackConfig(
        color="#FFFFFF",
        width=2.0,
        duration_ms=400,
        fill_color="#007AFF",
        fill_opacity=0.3,
    )
    
    if feedback_type == "ripple":
        element_type = FeedbackType.RIPPLE
        metadata = {"max_radius": 80}
    elif feedback_type == "pulse":
        element_type = FeedbackType.PULSE
        metadata = {"max_radius": 60}
    else:
        element_type = FeedbackType.HIGHLIGHT
        metadata = {}
    
    return OverlayElement(
        element_type=element_type,
        x=x,
        y=y,
        width=10,
        height=10,
        config=config,
        metadata=metadata,
    )
