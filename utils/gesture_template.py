"""Gesture template management for UI automation.

Stores, matches, and recognizes gesture templates (e.g., swipe, pinch, rotate)
used in touch-based UI automation.
"""

from __future__ import annotations

import uuid
import math
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Optional


class GesturePhase(Enum):
    """Phases of a gesture."""
    BEGAN = auto()
    CHANGED = auto()
    ENDED = auto()
    CANCELLED = auto()
    POSSIBLE = auto()


class GestureType(Enum):
    """Types of recognized gestures."""
    TAP = auto()
    DOUBLE_TAP = auto()
    LONG_PRESS = auto()
    SWIPE_LEFT = auto()
    SWIPE_RIGHT = auto()
    SWIPE_UP = auto()
    SWIPE_DOWN = auto()
    PINCH = auto()
    PINCH_OPEN = auto()
    PINCH_CLOSE = auto()
    ROTATE = auto()
    DRAG = auto()
    PAN = auto()
    CUSTOM = auto()


@dataclass
class GesturePoint:
    """A single point in a gesture path.

    Attributes:
        x: X coordinate.
        y: Y coordinate.
        timestamp: Time in seconds from gesture start.
        pressure: Touch pressure (0.0-1.0), if available.
        radius: Touch radius, if available.
    """
    x: float
    y: float
    timestamp: float = 0.0
    pressure: float = 1.0
    radius: float = 1.0

    def distance_to(self, other: GesturePoint) -> float:
        """Euclidean distance to another point."""
        return math.sqrt((self.x - other.x) ** 2 + (self.y - other.y) ** 2)

    def angle_to(self, other: GesturePoint) -> float:
        """Angle in radians from this point to another."""
        return math.atan2(other.y - self.y, other.x - self.x)


@dataclass
class GestureStroke:
    """A continuous stroke within a gesture."""
    points: list[GesturePoint] = field(default_factory=list)

    def add_point(self, point: GesturePoint) -> None:
        """Add a point to this stroke."""
        self.points.append(point)

    @property
    def start(self) -> Optional[GesturePoint]:
        """Return the first point."""
        return self.points[0] if self.points else None

    @property
    def end(self) -> Optional[GesturePoint]:
        """Return the last point."""
        return self.points[-1] if self.points else None

    @property
    def duration(self) -> float:
        """Total duration of the stroke."""
        if not self.points:
            return 0.0
        return self.points[-1].timestamp - self.points[0].timestamp

    @property
    def path_length(self) -> float:
        """Total path length of the stroke."""
        if len(self.points) < 2:
            return 0.0
        total = 0.0
        for i in range(1, len(self.points)):
            total += self.points[i - 1].distance_to(self.points[i])
        return total

    def direction(self) -> Optional[float]:
        """Return overall direction angle in radians, or None if no points."""
        if not self.start or not self.end:
            return None
        return self.start.angle_to(self.end)

    def average_velocity(self) -> float:
        """Average velocity (path_length / duration)."""
        dur = self.duration
        if dur <= 0:
            return 0.0
        return self.path_length / dur

    def bounding_box(self) -> tuple[float, float, float, float]:
        """Return (min_x, min_y, max_x, max_y) bounding box."""
        if not self.points:
            return (0, 0, 0, 0)
        xs = [p.x for p in self.points]
        ys = [p.y for p in self.points]
        return (min(xs), min(ys), max(xs), max(ys))


@dataclass
class GestureTemplate:
    """A template for gesture matching.

    Attributes:
        name: Human-readable name for this gesture.
        gesture_type: The type/kind of gesture.
        strokes: List of strokes that define the gesture.
        min_duration: Minimum gesture duration in seconds.
        max_duration: Maximum gesture duration in seconds.
        tolerance: Distance tolerance for matching (in pixels).
        direction_hint: Primary direction for swipe gestures.
        metadata: Additional template metadata.
    """
    name: str
    gesture_type: GestureType
    strokes: list[GestureStroke] = field(default_factory=list)
    min_duration: float = 0.0
    max_duration: float = 5.0
    tolerance: float = 20.0
    direction_hint: Optional[str] = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    metadata: dict = field(default_factory=dict)

    def add_stroke(self, stroke: GestureStroke) -> None:
        """Add a stroke to this template."""
        self.strokes.append(stroke)

    def get_main_direction(self) -> Optional[str]:
        """Return 'left', 'right', 'up', 'down', or None based on strokes."""
        if not self.strokes:
            return None
        direction = self.strokes[0].direction()
        if direction is None:
            return None
        angle_deg = math.degrees(direction) % 360
        if angle_deg < 45 or angle_deg >= 315:
            return "right"
        if 45 <= angle_deg < 135:
            return "down"
        if 135 <= angle_deg < 225:
            return "left"
        return "up"


class GestureRecognizer:
    """Recognizes gestures against registered templates."""

    def __init__(self) -> None:
        """Initialize with no templates."""
        self._templates: dict[str, GestureTemplate] = {}

    def register(self, template: GestureTemplate) -> str:
        """Register a gesture template. Returns template ID."""
        self._templates[template.id] = template
        return template.id

    def unregister(self, template_id: str) -> bool:
        """Remove a template. Returns True if found."""
        if template_id in self._templates:
            del self._templates[template_id]
            return True
        return False

    def recognize(self, stroke: GestureStroke) -> Optional[tuple[GestureTemplate, float]]:
        """Attempt to recognize a stroke against registered templates.

        Returns (matched_template, confidence) or None if no match.
        Confidence is 0.0-1.0.
        """
        if not stroke.points:
            return None

        best_template: Optional[GestureTemplate] = None
        best_confidence = 0.0

        for template in self._templates.values():
            confidence = self._match_stroke(stroke, template)
            if confidence > best_confidence:
                best_confidence = confidence
                best_template = template

        if best_template and best_confidence >= 0.6:
            return (best_template, best_confidence)
        return None

    def _match_stroke(
        self,
        stroke: GestureStroke,
        template: GestureTemplate,
    ) -> float:
        """Compute match confidence between stroke and template."""
        if not stroke.points or not template.strokes:
            return 0.0

        t_stroke = template.strokes[0]
        t_dir = t_stroke.direction()
        s_dir = stroke.direction()

        if t_dir is None or s_dir is None:
            return 0.0

        angle_diff = abs(t_dir - s_dir)
        if angle_diff > math.pi:
            angle_diff = 2 * math.pi - angle_diff
        angle_score = 1.0 - (angle_diff / math.pi)

        len_ratio = stroke.path_length / max(t_stroke.path_length, 1.0)
        len_score = min(len_ratio, 1.0 / max(len_ratio, 0.01))

        dur_ratio = stroke.duration / max(t_stroke.duration, 0.001)
        dur_score = min(dur_ratio, 1.0 / max(dur_ratio, 0.01))

        confidence = (angle_score * 0.5) + (len_score * 0.3) + (dur_score * 0.2)
        return max(0.0, min(1.0, confidence))

    def classify_swipe_direction(self, stroke: GestureStroke) -> Optional[str]:
        """Classify a stroke as a swipe direction.

        Returns 'left', 'right', 'up', 'down', or None.
        """
        if not stroke.points or stroke.path_length < 30:
            return None

        direction = stroke.direction()
        if direction is None:
            return None

        angle_deg = math.degrees(direction) % 360
        if angle_deg < 45 or angle_deg >= 315:
            return "right"
        if 45 <= angle_deg < 135:
            return "down"
        if 135 <= angle_deg < 225:
            return "left"
        return "up"


# Built-in template factory
def create_swipe_template(
    name: str,
    direction: str,
    start: tuple[float, float],
    end: tuple[float, float],
    duration: float = 0.3,
) -> GestureTemplate:
    """Create a swipe gesture template."""
    direction_map = {
        "left": GestureType.SWIPE_LEFT,
        "right": GestureType.SWIPE_RIGHT,
        "up": GestureType.SWIPE_UP,
        "down": GestureType.SWIPE_DOWN,
    }
    gesture_type = direction_map.get(direction.lower(), GestureType.SWIPE_RIGHT)

    stroke = GestureStroke()
    steps = 10
    for i in range(steps + 1):
        t = i / steps
        x = start[0] + (end[0] - start[0]) * t
        y = start[1] + (end[1] - start[1]) * t
        ts = t * duration
        stroke.add_point(GesturePoint(x=x, y=y, timestamp=ts))

    template = GestureTemplate(
        name=name,
        gesture_type=gesture_type,
        strokes=[stroke],
        min_duration=duration * 0.5,
        max_duration=duration * 2.0,
        direction_hint=direction,
    )
    return template


def create_tap_template(
    name: str,
    x: float,
    y: float,
    count: int = 1,
) -> GestureTemplate:
    """Create a tap gesture template."""
    gesture_type = GestureType.TAP
    if count == 2:
        gesture_type = GestureType.DOUBLE_TAP

    stroke = GestureStroke()
    stroke.add_point(GesturePoint(x=x, y=y, timestamp=0.0))
    stroke.add_point(GesturePoint(x=x, y=y, timestamp=0.05))

    template = GestureTemplate(
        name=name,
        gesture_type=gesture_type,
        strokes=[stroke],
        min_duration=0.05,
        max_duration=0.5,
    )
    return template
