"""
Touch Rendering Utilities for UI Automation.

This module provides utilities for rendering visual feedback
for touch interactions in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple
from enum import Enum


class RenderShape(Enum):
    """Shape of touch point visualization."""
    CIRCLE = "circle"
    DOT = "dot"
    CROSSHAIR = "crosshair"
    RING = "ring"
    RIPPLE = "ripple"


class RenderStyle(Enum):
    """Visual style of touch visualization."""
    DEFAULT = "default"
    MINIMAL = "minimal"
    HIGH_CONTRAST = "high_contrast"
    CUSTOM = "custom"


@dataclass
class RenderConfig:
    """Configuration for touch rendering."""
    shape: RenderShape = RenderShape.CIRCLE
    style: RenderStyle = RenderStyle.DEFAULT
    size: float = 30.0
    color: str = "#007AFF"
    opacity: float = 0.8
    ring_width: float = 2.0
    show_ripple: bool = True
    ripple_duration_ms: float = 400.0
    fade_out_duration_ms: float = 200.0


@dataclass
class TouchRender:
    """Represents a rendered touch visualization."""
    x: float
    y: float
    size: float
    color: str
    opacity: float
    shape: RenderShape
    age_ms: float
    is_ripple: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


class TouchRenderer:
    """Renders visual feedback for touch interactions."""

    def __init__(self, config: Optional[RenderConfig] = None) -> None:
        self._config = config or RenderConfig()
        self._active_renders: List[TouchRender] = []
        self._ripples: List[TouchRender] = []
        self._last_update: float = time.time()

    def set_config(self, config: RenderConfig) -> None:
        """Update the render configuration."""
        self._config = config

    def get_config(self) -> RenderConfig:
        """Get the current render configuration."""
        return self._config

    def render_touch_down(
        self,
        x: float,
        y: float,
        size: Optional[float] = None,
        color: Optional[str] = None,
    ) -> TouchRender:
        """Render a touch-down event."""
        touch = TouchRender(
            x=x,
            y=y,
            size=size or self._config.size,
            color=color or self._config.color,
            opacity=self._config.opacity,
            shape=self._config.shape,
            age_ms=0.0,
        )
        self._active_renders.append(touch)

        if self._config.show_ripple:
            ripple = TouchRender(
                x=x,
                y=y,
                size=0.0,
                color=color or self._config.color,
                opacity=self._config.opacity,
                shape=RenderShape.RIPPLE,
                age_ms=0.0,
                is_ripple=True,
            )
            self._ripples.append(ripple)

        return touch

    def render_touch_move(
        self,
        x: float,
        y: float,
        size: Optional[float] = None,
    ) -> None:
        """Update render position for a touch move."""
        if not self._active_renders:
            return

        active = self._active_renders[-1]
        active.x = x
        active.y = y
        if size is not None:
            active.size = size

    def render_touch_up(self) -> Optional[TouchRender]:
        """Render a touch-up event."""
        if not self._active_renders:
            return None

        touch = self._active_renders.pop()
        return touch

    def update(self, current_time: Optional[float] = None) -> None:
        """Update all render states for animation."""
        if current_time is None:
            current_time = time.time()

        dt_ms = (current_time - self._last_update) * 1000.0
        self._last_update = current_time

        for render in self._active_renders:
            render.age_ms += dt_ms

        for ripple in self._ripples:
            ripple.age_ms += dt_ms

        self._ripples = [
            r for r in self._ripples
            if r.age_ms < self._config.ripple_duration_ms
        ]

        max_fade_age = self._config.ripple_duration_ms + self._config.fade_out_duration_ms
        self._active_renders = [
            r for r in self._active_renders
            if r.age_ms < max_fade_age
        ]

    def get_active_renders(self) -> List[TouchRender]:
        """Get all currently active renders."""
        return list(self._active_renders)

    def get_ripples(self) -> List[TouchRender]:
        """Get all active ripple effects."""
        return list(self._ripples)

    def get_ripple_progress(self, ripple: TouchRender) -> float:
        """Get the progress of a ripple animation (0-1)."""
        return min(ripple.age_ms / self._config.ripple_duration_ms, 1.0)

    def get_ripple_size(self, ripple: TouchRender) -> float:
        """Calculate the current size of a ripple effect."""
        progress = self.get_ripple_progress(ripple)
        return self._config.size * (1.0 + progress * 3.0)

    def get_ripple_opacity(self, ripple: TouchRender) -> float:
        """Calculate the current opacity of a ripple effect."""
        progress = self.get_ripple_progress(ripple)
        if progress < 0.3:
            return self._config.opacity
        return self._config.opacity * (1.0 - (progress - 0.3) / 0.7)

    def clear(self) -> None:
        """Clear all renders."""
        self._active_renders.clear()
        self._ripples.clear()

    def render_all(
        self,
        framebuffer: Any,
    ) -> None:
        """Render all touches to a framebuffer (stub for integration)."""
        self.update()

        for render in self._active_renders:
            if render.is_ripple:
                continue
            self._draw_touch(framebuffer, render)

        for ripple in self._ripples:
            self._draw_ripple(framebuffer, ripple)

    def _draw_touch(
        self,
        framebuffer: Any,
        render: TouchRender,
    ) -> None:
        """Draw a single touch visualization."""
        pass

    def _draw_ripple(
        self,
        framebuffer: Any,
        ripple: TouchRender,
    ) -> None:
        """Draw a single ripple effect."""
        pass
