"""Cursor trail animation utilities.

This module provides utilities for creating cursor trail effects,
which can be used for visual feedback during automation.
"""

from __future__ import annotations

import platform
import time
from typing import Callable, Optional


IS_MACOS = platform.system() == "Darwin"
IS_LINUX = platform.system() == "Linux"
IS_WINDOWS = platform.system() == "Windows"


class CursorTrailEffect:
    """Animated cursor trail effect."""
    
    def __init__(
        self,
        color: tuple[int, int, int, int] = (255, 255, 255, 180),
        length: int = 20,
        fade_duration: float = 0.3,
    ):
        self.color = color
        self.length = length
        self.fade_duration = fade_duration
        self._positions: list[tuple[int, int, float]] = []  # x, y, timestamp
        self._running = False
    
    def record_position(self, x: int, y: int) -> None:
        """Record a cursor position for the trail."""
        self._positions.append((x, y, time.monotonic()))
        # Remove old positions
        cutoff = time.monotonic() - self.fade_duration
        self._positions = [(px, py, t) for px, py, t in self._positions if t > cutoff]
        # Limit length
        if len(self._positions) > self.length:
            self._positions = self._positions[-self.length:]
    
    def get_trail_points(self) -> list[tuple[int, int, float]]:
        """Get trail points with opacity based on age."""
        now = time.monotonic()
        return [
            (x, y, max(0.0, 1.0 - (now - t) / self.fade_duration))
            for x, y, t in self._positions
        ]
    
    def clear(self) -> None:
        """Clear all trail positions."""
        self._positions = []


def draw_cursor_trail(
    positions: list[tuple[int, int, float]],
    color: tuple[int, int, int, int],
) -> None:
    """Draw a cursor trail overlay.
    
    Args:
        positions: List of (x, y, opacity) tuples.
        color: RGBA color tuple.
    """
    if IS_MACOS:
        try:
            import numpy as np
            from PIL import Image, ImageDraw
            
            # Create overlay image
            width = 1920
            height = 1080
            overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
            draw = ImageDraw.Draw(overlay)
            
            for x, y, opacity in positions:
                alpha = int(255 * opacity)
                r, g, b, _ = color
                draw.ellipse(
                    [x - 3, y - 3, x + 3, y + 3],
                    fill=(r, g, b, alpha)
                )
            
            # Draw lines between points
            if len(positions) > 1:
                for i in range(len(positions) - 1):
                    x1, y1, o1 = positions[i]
                    x2, y2, o2 = positions[i + 1]
                    alpha = int(255 * ((o1 + o2) / 2))
                    draw.line(
                        [x1, y1, x2, y2],
                        fill=(color[0], color[1], color[2], alpha),
                        width=2,
                    )
        except ImportError:
            pass


def animate_cursor_path(
    path: list[tuple[int, int]],
    duration: float = 1.0,
    easing: Callable[[float], float] = lambda t: t,
) -> list[tuple[int, int, float]]:
    """Generate animated cursor positions along a path.
    
    Args:
        path: List of (x, y) waypoints.
        duration: Total animation duration.
        easing: Easing function.
    
    Returns:
        List of (x, y, progress) tuples.
    """
    if len(path) < 2:
        return [(path[0][0], path[0][1], 0.0), (path[0][0], path[0][1], 1.0)] if path else []
    
    result = []
    total_points = len(path) - 1
    
    for i, (x1, y1) in enumerate(path[:-1]):
        x2, y2 = path[i + 1]
        steps = 10
        for j in range(steps):
            t = j / steps
            eased_t = easing(t)
            x = int(x1 + (x2 - x1) * eased_t)
            y = int(y1 + (y2 - y1) * eased_t)
            progress = (i + t) / total_points
            result.append((x, y, progress))
    
    # Add final point
    result.append((path[-1][0], path[-1][1], 1.0))
    return result


def glow_effect(
    x: int,
    y: int,
    radius: int = 20,
    intensity: float = 0.5,
) -> list[tuple[int, int, float]]:
    """Generate glow effect points around a center.
    
    Args:
        x: Center X coordinate.
        y: Center Y coordinate.
        radius: Glow radius in pixels.
        intensity: Initial intensity (0.0 to 1.0).
    
    Returns:
        List of (x, y, opacity) points.
    """
    points = []
    steps = 16
    for i in range(steps):
        angle = (i / steps) * 2 * 3.14159
        px = int(x + radius * 0.8 * (1 + (i % 3) * 0.2) * __import__("math").cos(angle))
        py = int(y + radius * 0.8 * (1 + (i % 3) * 0.2) * __import__("math").sin(angle))
        opacity = intensity * (0.5 + 0.5 * (i % 2))
        points.append((px, py, opacity))
    return points
