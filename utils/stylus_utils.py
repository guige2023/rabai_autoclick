"""
Stylus Input Utilities for UI Automation.

This module provides utilities for handling stylus/pen input
in UI automation workflows on macOS.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple
from enum import Enum


class StylusButton(Enum):
    """Stylus button states."""
    NONE = "none"
    PRIMARY = "primary"
    SECONDARY = "secondary"
    ERASER = "eraser"


class StylusTip(Enum):
    """Stylus tip types."""
    FINE = "fine"
    MEDIUM = "medium"
    BROAD = "broad"


@dataclass
class StylusPoint:
    """Represents a point in stylus input."""
    x: float
    y: float
    pressure: float = 0.5
    tilt_x: float = 0.0
    tilt_y: float = 0.0
    azimuth: float = 0.0
    altitude: float = 90.0
    timestamp: float = 0.0
    button: StylusButton = StylusButton.NONE
    tip: StylusTip = StylusTip.MEDIUM


@dataclass
class StylusStroke:
    """A collection of stylus points forming a stroke."""
    points: List[StylusPoint] = field(default_factory=list)
    tool: str = "stylus"
    color: str = "#000000"
    line_width: float = 2.0


class StylusInputHandler:
    """Handles stylus input for UI automation."""

    def __init__(self) -> None:
        self._pressure_curve: List[float] = [0.0, 0.1, 0.3, 0.6, 1.0]
        self._active_stroke: Optional[StylusStroke] = None
        self._completed_strokes: List[StylusStroke] = []

    def begin_stroke(
        self,
        x: float,
        y: float,
        pressure: float = 0.5,
        button: StylusButton = StylusButton.NONE,
    ) -> StylusStroke:
        """Begin a new stylus stroke."""
        self._active_stroke = StylusStroke()
        point = StylusPoint(
            x=x,
            y=y,
            pressure=pressure,
            timestamp=time.time(),
            button=button,
        )
        self._active_stroke.points.append(point)
        return self._active_stroke

    def add_point(
        self,
        x: float,
        y: float,
        pressure: float = 0.5,
        tilt_x: float = 0.0,
        tilt_y: float = 0.0,
    ) -> None:
        """Add a point to the active stroke."""
        if self._active_stroke is None:
            return

        point = StylusPoint(
            x=x,
            y=y,
            pressure=pressure,
            tilt_x=tilt_x,
            tilt_y=tilt_y,
            timestamp=time.time(),
        )
        self._active_stroke.points.append(point)

    def end_stroke(self) -> Optional[StylusStroke]:
        """End the current stroke and return it."""
        if self._active_stroke is None:
            return None

        stroke = self._active_stroke
        self._completed_strokes.append(stroke)
        self._active_stroke = None
        return stroke

    def get_active_stroke(self) -> Optional[StylusStroke]:
        """Get the currently active stroke."""
        return self._active_stroke

    def get_completed_strokes(self) -> List[StylusStroke]:
        """Get all completed strokes."""
        return list(self._completed_strokes)

    def clear_strokes(self) -> None:
        """Clear all completed strokes."""
        self._completed_strokes.clear()

    def apply_pressure_curve(
        self,
        raw_pressure: float,
    ) -> float:
        """Apply a pressure curve to raw pressure input."""
        index = int(raw_pressure * (len(self._pressure_curve) - 1))
        index = max(0, min(index, len(self._pressure_curve) - 1))
        return self._pressure_curve[index]

    def set_pressure_curve(self, curve: List[float]) -> None:
        """Set a custom pressure curve."""
        if len(curve) < 2:
            raise ValueError("Pressure curve must have at least 2 points")
        self._pressure_curve = list(curve)

    def get_stroke_bounds(
        self,
        stroke: StylusStroke,
    ) -> Tuple[float, float, float, float]:
        """Get bounding box of a stroke as (min_x, min_y, max_x, max_y)."""
        if not stroke.points:
            return (0.0, 0.0, 0.0, 0.0)

        xs = [p.x for p in stroke.points]
        ys = [p.y for p in stroke.points]
        return (min(xs), min(ys), max(xs), max(ys))

    def get_average_pressure(self, stroke: StylusStroke) -> float:
        """Calculate average pressure for a stroke."""
        if not stroke.points:
            return 0.0
        return sum(p.pressure for p in stroke.points) / len(stroke.points)


def create_stylus_point(
    x: float,
    y: float,
    pressure: float = 0.5,
    **kwargs: Any,
) -> StylusPoint:
    """Create a stylus point with the specified parameters."""
    return StylusPoint(x=x, y=y, pressure=pressure, **kwargs)
