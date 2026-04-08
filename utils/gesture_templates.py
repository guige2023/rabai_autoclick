"""
Gesture Templates Utility

Pre-defined gesture templates for common UI interactions.
Supports parameterized gestures for automation replay.

Example:
    >>> templates = GestureTemplates()
    >>> swipe = templates.get("swipe_left_fast")
    >>> steps = swipe.generate(start=(400, 300), duration=0.3)
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Optional, Callable


@dataclass
class GestureStep:
    """A single step in a gesture."""
    t: float  # 0.0 to 1.0 normalized progress
    x: float
    y: float
    pressure: float = 1.0
    duration_ms: int = 0  # Time to hold at this point


@dataclass
class GestureTemplate:
    """
    A parameterized gesture template.

    Attributes:
        name: Unique identifier.
        description: Human-readable description.
        steps: List of key points defining the gesture.
        fingers: Number of fingers required.
        duration: Default duration in seconds.
    """
    name: str
    description: str
    steps: list[tuple[float, float, float]]  # (t, x, y) normalized
    fingers: int = 1
    duration: float = 0.5

    def generate(
        self,
        start: tuple[float, float],
        end: tuple[float, float],
        duration: Optional[float] = None,
    ) -> list[GestureStep]:
        """
        Generate gesture steps between start and end points.

        Args:
            start: (x, y) starting coordinates.
            end: (x, y) ending coordinates.
            duration: Override default duration.

        Returns:
            List of GestureStep objects with interpolated positions.
        """
        dur = duration if duration is not None else self.duration
        sx, sy = start
        ex, ey = end

        steps: list[GestureStep] = []
        for t, nx, ny in self.steps:
            x = sx + (ex - sx) * nx
            y = sy + (ey - sy) * ny
            steps.append(GestureStep(t=t, x=x, y=y, pressure=1.0, duration_ms=0))
        return steps


class GestureTemplates:
    """
    Library of pre-defined gesture templates.
    """

    def __init__(self) -> None:
        self._templates: dict[str, GestureTemplate] = {}
        self._register_defaults()

    def _register_defaults(self) -> None:
        """Register the default gesture templates."""
        templates = [
            GestureTemplate(
                name="tap",
                description="Single tap",
                steps=[(0.0, 0.0, 0.0), (1.0, 0.0, 0.0)],
                fingers=1,
                duration=0.1,
            ),
            GestureTemplate(
                name="double_tap",
                description="Double tap",
                steps=[(0.0, 0.0, 0.0), (0.25, 0.0, 0.0), (0.5, 0.0, 0.0), (1.0, 0.0, 0.0)],
                fingers=1,
                duration=0.3,
            ),
            GestureTemplate(
                name="swipe_left",
                description="Swipe left",
                steps=[(0.0, 0.0, 0.0), (1.0, -1.0, 0.0)],
                fingers=1,
                duration=0.3,
            ),
            GestureTemplate(
                name="swipe_right",
                description="Swipe right",
                steps=[(0.0, 0.0, 0.0), (1.0, 1.0, 0.0)],
                fingers=1,
                duration=0.3,
            ),
            GestureTemplate(
                name="swipe_up",
                description="Swipe up",
                steps=[(0.0, 0.0, 0.0), (1.0, 0.0, -1.0)],
                fingers=1,
                duration=0.3,
            ),
            GestureTemplate(
                name="swipe_down",
                description="Swipe down",
                steps=[(0.0, 0.0, 0.0), (1.0, 0.0, 1.0)],
                fingers=1,
                duration=0.3,
            ),
            GestureTemplate(
                name="swipe_left_fast",
                description="Fast swipe left",
                steps=[(0.0, 0.0, 0.0), (1.0, -1.0, 0.0)],
                fingers=1,
                duration=0.15,
            ),
            GestureTemplate(
                name="swipe_right_fast",
                description="Fast swipe right",
                steps=[(0.0, 0.0, 0.0), (1.0, 1.0, 0.0)],
                fingers=1,
                duration=0.15,
            ),
            GestureTemplate(
                name="long_press",
                description="Long press",
                steps=[(0.0, 0.0, 0.0), (1.0, 0.0, 0.0)],
                fingers=1,
                duration=1.0,
            ),
            GestureTemplate(
                name="drag",
                description="Slow drag",
                steps=[(0.0, 0.0, 0.0), (1.0, 0.0, 0.0)],
                fingers=1,
                duration=1.0,
            ),
            GestureTemplate(
                name="pinch",
                description="Pinch gesture",
                steps=[(0.0, 0.0, 0.0), (1.0, -0.5, 0.0)],
                fingers=2,
                duration=0.5,
            ),
            GestureTemplate(
                name="spread",
                description="Spread gesture",
                steps=[(0.0, 0.0, 0.0), (1.0, 0.5, 0.0)],
                fingers=2,
                duration=0.5,
            ),
            GestureTemplate(
                name="two_finger_swipe_left",
                description="Two-finger swipe left",
                steps=[(0.0, 0.0, 0.0), (1.0, -1.0, 0.0)],
                fingers=2,
                duration=0.3,
            ),
            GestureTemplate(
                name="two_finger_swipe_right",
                description="Two-finger swipe right",
                steps=[(0.0, 0.0, 0.0), (1.0, 1.0, 0.0)],
                fingers=2,
                duration=0.3,
            ),
            GestureTemplate(
                name="two_finger_swipe_up",
                description="Two-finger swipe up",
                steps=[(0.0, 0.0, 0.0), (1.0, 0.0, -1.0)],
                fingers=2,
                duration=0.3,
            ),
            GestureTemplate(
                name="two_finger_swipe_down",
                description="Two-finger swipe down",
                steps=[(0.0, 0.0, 0.0), (1.0, 0.0, 1.0)],
                fingers=2,
                duration=0.3,
            ),
            GestureTemplate(
                name="edge_swipe_left",
                description="Edge swipe from left",
                steps=[(0.0, 0.0, 0.0), (1.0, -1.0, 0.0)],
                fingers=1,
                duration=0.4,
            ),
            GestureTemplate(
                name="edge_swipe_right",
                description="Edge swipe from right",
                steps=[(0.0, 0.0, 0.0), (1.0, 1.0, 0.0)],
                fingers=1,
                duration=0.4,
            ),
            GestureTemplate(
                name="rotate_cw",
                description="Clockwise rotation",
                steps=[
                    (0.0, 0.0, 0.0),
                    (0.25, 0.5, -0.5),
                    (0.5, 1.0, 0.0),
                    (0.75, 0.5, 0.5),
                    (1.0, 0.0, 0.0),
                ],
                fingers=2,
                duration=0.6,
            ),
            GestureTemplate(
                name="rotate_ccw",
                description="Counter-clockwise rotation",
                steps=[
                    (0.0, 0.0, 0.0),
                    (0.25, -0.5, -0.5),
                    (0.5, -1.0, 0.0),
                    (0.75, -0.5, 0.5),
                    (1.0, 0.0, 0.0),
                ],
                fingers=2,
                duration=0.6,
            ),
        ]

        for t in templates:
            self._templates[t.name] = t

    def register(self, template: GestureTemplate) -> None:
        """Register a custom gesture template."""
        self._templates[template.name] = template

    def get(self, name: str) -> Optional[GestureTemplate]:
        """Get a template by name."""
        return self._templates.get(name)

    def list_templates(self) -> list[str]:
        """List all available template names."""
        return list(self._templates.keys())

    def unregister(self, name: str) -> bool:
        """Remove a custom template. Returns False if not found."""
        if name in self._templates:
            del self._templates[name]
            return True
        return False
