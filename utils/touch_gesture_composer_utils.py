"""
Touch gesture composer utilities for composing complex multi-touch gestures.

Provides composition of touch gestures from primitive actions
with support for parallel and sequential execution.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class GesturePrimitive:
    """A primitive gesture action."""
    action_type: str  # "tap", "swipe", "hold", "pinch"
    points: list[tuple[float, float]]  # List of (x, y) points
    duration_ms: float = 100.0
    finger_count: int = 1
    metadata: dict = field(default_factory=dict)


@dataclass
class ComposedGesture:
    """A composed gesture with multiple primitives."""
    name: str
    primitives: list[GesturePrimitive] = field(default_factory=list)
    execution_mode: str = "sequential"  # "sequential" or "parallel"
    finger_count: int = 1


class TouchGestureComposer:
    """Composes complex touch gestures from primitives."""

    def __init__(self):
        self._gestures: dict[str, ComposedGesture] = {}

    def create_tap(self, x: float, y: float, finger_id: int = 0) -> GesturePrimitive:
        """Create a tap gesture."""
        return GesturePrimitive(
            action_type="tap",
            points=[(x, y)],
            duration_ms=50,
            finger_count=1,
            metadata={"finger_id": finger_id},
        )

    def create_swipe(
        self,
        x1: float, y1: float,
        x2: float, y2: float,
        duration_ms: float = 200.0,
    ) -> GesturePrimitive:
        """Create a swipe gesture."""
        return GesturePrimitive(
            action_type="swipe",
            points=[(x1, y1), (x2, y2)],
            duration_ms=duration_ms,
            finger_count=1,
        )

    def create_hold(
        self,
        x: float, y: float,
        duration_ms: float = 500.0,
        finger_id: int = 0,
    ) -> GesturePrimitive:
        """Create a hold gesture."""
        return GesturePrimitive(
            action_type="hold",
            points=[(x, y)],
            duration_ms=duration_ms,
            finger_count=1,
            metadata={"finger_id": finger_id},
        )

    def create_pinch(
        self,
        cx: float, cy: float,
        start_distance: float,
        end_distance: float,
        duration_ms: float = 300.0,
    ) -> GesturePrimitive:
        """Create a pinch gesture (zoom in/out)."""
        # Two fingers start apart, move closer (pinch) or closer, move apart (zoom)
        half = start_distance / 2
        points = [
            (cx - half, cy), (cx + half, cy),  # Start
            (cx - end_distance / 2, cy), (cx + end_distance / 2, cy),  # End
        ]
        return GesturePrimitive(
            action_type="pinch",
            points=points,
            duration_ms=duration_ms,
            finger_count=2,
        )

    def create_drag(
        self,
        x1: float, y1: float,
        x2: float, y2: float,
        duration_ms: float = 400.0,
    ) -> GesturePrimitive:
        """Create a slow drag gesture."""
        return GesturePrimitive(
            action_type="drag",
            points=[(x1, y1), (x2, y2)],
            duration_ms=duration_ms,
            finger_count=1,
        )

    def compose(
        self,
        name: str,
        primitives: list[GesturePrimitive],
        execution_mode: str = "sequential",
    ) -> ComposedGesture:
        """Compose multiple primitives into a gesture."""
        max_fingers = max(p.finger_count for p in primitives)
        gesture = ComposedGesture(
            name=name,
            primitives=primitives,
            execution_mode=execution_mode,
            finger_count=max_fingers,
        )
        self._gestures[name] = gesture
        return gesture

    def get(self, name: str) -> Optional[ComposedGesture]:
        """Get a composed gesture by name."""
        return self._gestures.get(name)

    def total_duration(self, gesture: ComposedGesture) -> float:
        """Calculate total duration of a gesture."""
        if gesture.execution_mode == "sequential":
            return sum(p.duration_ms for p in gesture.primitives)
        else:  # parallel
            return max(p.duration_ms for p in gesture.primitives) if gesture.primitives else 0.0


# Pre-built gesture compositions
def compose_circle_gesture(cx: float, cy: float, radius: float, duration_ms: float = 400.0) -> ComposedGesture:
    """Compose a circular gesture."""
    import math
    composer = TouchGestureComposer()

    # Sample points along a circle
    num_points = 20
    points = []
    for i in range(num_points + 1):
        angle = 2 * math.pi * i / num_points
        x = cx + radius * math.cos(angle)
        y = cy + radius * math.sin(angle)
        points.append((x, y))

    primitive = GesturePrimitive(
        action_type="circle",
        points=points,
        duration_ms=duration_ms,
        finger_count=1,
    )

    return composer.compose("circle", [primitive])


__all__ = ["TouchGestureComposer", "ComposedGesture", "GesturePrimitive", "compose_circle_gesture"]
