"""Touch simulation utilities.

This module provides utilities for simulating touch events
on touch-enabled devices.
"""

from __future__ import annotations

import time
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum, auto


class TouchPhase(Enum):
    """Touch event phases."""
    BEGAN = auto()
    MOVED = auto()
    STATIONARY = auto()
    ENDED = auto()
    CANCELLED = auto()


@dataclass
class TouchPoint:
    """A single touch point."""
    touch_id: int
    x: float
    y: float
    phase: TouchPhase
    timestamp: float
    pressure: float = 1.0
    radius: float = 1.0


@dataclass
class TouchGesture:
    """A complete touch gesture."""
    points: List[TouchPoint] = field(default_factory=list)
    gesture_type: str = ""

    def add_point(self, point: TouchPoint) -> None:
        self.points.append(point)

    def duration(self) -> float:
        if len(self.points) < 2:
            return 0.0
        return self.points[-1].timestamp - self.points[0].timestamp


class TouchSimulator:
    """Simulates touch events."""

    def __init__(self) -> None:
        self._active_touches: Dict[int, TouchPoint] = {}
        self._gestures: List[TouchGesture] = []
        self._handlers: Dict[TouchPhase, List[callable]] = {
            phase: [] for phase in TouchPhase
        }

    def register_handler(self, phase: TouchPhase, handler: callable) -> None:
        self._handlers[phase].append(handler)

    def touch_began(
        self,
        touch_id: int,
        x: float,
        y: float,
        pressure: float = 1.0,
    ) -> TouchPoint:
        point = TouchPoint(
            touch_id=touch_id,
            x=x,
            y=y,
            phase=TouchPhase.BEGAN,
            timestamp=time.time(),
            pressure=pressure,
        )
        self._active_touches[touch_id] = point
        self._dispatch(TouchPhase.BEGAN, point)
        return point

    def touch_moved(
        self,
        touch_id: int,
        x: float,
        y: float,
        pressure: float = 1.0,
    ) -> Optional[TouchPoint]:
        if touch_id not in self._active_touches:
            return None
        point = TouchPoint(
            touch_id=touch_id,
            x=x,
            y=y,
            phase=TouchPhase.MOVED,
            timestamp=time.time(),
            pressure=pressure,
        )
        self._active_touches[touch_id] = point
        self._dispatch(TouchPhase.MOVED, point)
        return point

    def touch_ended(self, touch_id: int) -> Optional[TouchPoint]:
        if touch_id not in self._active_touches:
            return None
        point = TouchPoint(
            touch_id=touch_id,
            x=self._active_touches[touch_id].x,
            y=self._active_touches[touch_id].y,
            phase=TouchPhase.ENDED,
            timestamp=time.time(),
        )
        self._dispatch(TouchPhase.ENDED, point)
        del self._active_touches[touch_id]
        return point

    def touch_cancelled(self, touch_id: int) -> Optional[TouchPoint]:
        if touch_id not in self._active_touches:
            return None
        point = TouchPoint(
            touch_id=touch_id,
            x=self._active_touches[touch_id].x,
            y=self._active_touches[touch_id].y,
            phase=TouchPhase.CANCELLED,
            timestamp=time.time(),
        )
        self._dispatch(TouchPhase.CANCELLED, point)
        del self._active_touches[touch_id]
        return point

    def _dispatch(self, phase: TouchPhase, point: TouchPoint) -> None:
        for handler in self._handlers[phase]:
            handler(point)

    @property
    def active_touches(self) -> Dict[int, TouchPoint]:
        return self._active_touches.copy()

    def clear(self) -> None:
        self._active_touches.clear()
        self._gestures.clear()


__all__ = [
    "TouchPhase",
    "TouchPoint",
    "TouchGesture",
    "TouchSimulator",
]
