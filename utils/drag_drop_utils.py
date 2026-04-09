"""
Drag and Drop Utilities for UI Automation

Provides drag-and-drop gesture simulation with configurable
paths, timing, and drop zone detection.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Optional


class DropZonePolicy(Enum):
    """Policy for determining valid drop zones."""
    CENTER = auto()
    OVERLAP = auto()
    THRESHOLD = auto()


@dataclass
class DropZone:
    """Represents a valid drop target area."""
    x: float
    y: float
    width: float
    height: float
    name: str
    accept_types: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)


@dataclass
class DragDropConfig:
    """Configuration for drag and drop behavior."""
    duration: float = 0.5
    num_steps: int = 20
    ease_profile: str = "ease_out"
    hold_duration: float = 0.1
    drop_detection: DropZonePolicy = DropZonePolicy.CENTER
    overlap_threshold: float = 0.5


@dataclass
class DragDropEvent:
    """Event emitted during drag and drop."""
    event_type: str
    x: float
    y: float
    timestamp: float
    metadata: dict = field(default_factory=dict)


class DragDropSimulator:
    """
    Simulates drag and drop gestures with various strategies.

    Supports straight-line drags, curved paths, and
    drop zone detection.
    """

    def __init__(self, config: DragDropConfig | None = None) -> None:
        self.config = config or DragDropConfig()
        self._event_callback: Optional[Callable[[DragDropEvent], None]] = None
        self._drop_zones: dict[str, DropZone] = {}

    def set_event_callback(
        self,
        callback: Callable[[DragDropEvent], None],
    ) -> None:
        """Set callback for drag drop events."""
        self._event_callback = callback

    def add_drop_zone(self, zone: DropZone) -> None:
        """Register a drop zone."""
        self._drop_zones[zone.name] = zone

    def remove_drop_zone(self, name: str) -> bool:
        """Remove a drop zone by name."""
        if name in self._drop_zones:
            del self._drop_zones[name]
            return True
        return False

    def execute_drag_drop(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        path_type: str = "straight",
    ) -> tuple[float, float, Optional[DropZone]]:
        """
        Execute a complete drag and drop gesture.

        Args:
            start_x: Starting X coordinate
            start_y: Starting Y coordinate
            end_x: Ending X coordinate
            end_y: Ending Y coordinate
            path_type: Path type ('straight', 'arc', 'curve')

        Returns:
            Tuple of (final_x, final_y, drop_zone) where drop_zone is
            the DropZone if successfully dropped in one, None otherwise
        """
        # Emit drag start
        self._emit_event("drag_start", start_x, start_y)

        # Generate path points
        points = self._generate_path(
            start_x, start_y, end_x, end_y, path_type
        )

        # Execute drag along path
        for x, y in points:
            self._emit_event("drag_move", x, y)
            time.sleep(self.config.duration / self.config.num_steps)

        # Hold briefly
        time.sleep(self.config.hold_duration)

        # Emit drag end
        self._emit_event("drag_end", end_x, end_y)

        # Detect drop zone
        drop_zone = self._detect_drop_zone(end_x, end_y)

        if drop_zone:
            self._emit_event("drop_success", end_x, end_y, {"zone": drop_zone.name})
        else:
            self._emit_event("drop_failed", end_x, end_y)

        return end_x, end_y, drop_zone

    def _generate_path(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        path_type: str,
    ) -> list[tuple[float, float]]:
        """Generate path points based on path type."""
        points: list[tuple[float, float]] = []
        num_steps = self.config.num_steps

        for i in range(num_steps + 1):
            t = i / num_steps

            # Apply easing
            if self.config.ease_profile == "ease_out":
                t = 1.0 - (1.0 - t) ** 2
            elif self.config.ease_profile == "ease_in":
                t = t ** 2
            elif self.config.ease_profile == "ease_in_out":
                t = 2 * t if t < 0.5 else 1.0 - (-2 * t + 2) ** 2 / 2

            if path_type == "straight":
                x = start_x + (end_x - start_x) * t
                y = start_y + (end_y - start_y) * t
            elif path_type == "arc":
                x, y = self._arc_path(start_x, start_y, end_x, end_y, t)
            elif path_type == "curve":
                x, y = self._curve_path(start_x, start_y, end_x, end_y, t)
            else:
                x = start_x + (end_x - start_x) * t
                y = start_y + (end_y - start_y) * t

            points.append((x, y))

        return points

    def _arc_path(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        t: float,
    ) -> tuple[float, float]:
        """Generate points along an arc path."""
        mid_x = (start_x + end_x) / 2
        mid_y = (start_y + end_y) / 2

        # Calculate arc height based on distance
        distance = math.sqrt((end_x - start_x)**2 + (end_y - start_y)**2)
        arc_height = distance * 0.3

        # Arc goes upward (negative Y direction)
        arc_mid_y = mid_y - arc_height

        # Quadratic bezier
        x = (1 - t)**2 * start_x + 2 * (1 - t) * t * mid_x + t**2 * end_x
        y = (1 - t)**2 * start_y + 2 * (1 - t) * t * arc_mid_y + t**2 * end_y
        return x, y

    def _curve_path(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        t: float,
    ) -> tuple[float, float]:
        """Generate points along a curved path with control point."""
        # Control point creates a natural curve
        dx = end_x - start_x
        dy = end_y - start_y

        control_x = start_x + dx * 0.5
        control_y = start_y - abs(dy) * 0.3 if dy != 0 else start_y - 50

        # Quadratic bezier
        x = (1 - t)**2 * start_x + 2 * (1 - t) * t * control_x + t**2 * end_x
        y = (1 - t)**2 * start_y + 2 * (1 - t) * t * control_y + t**2 * end_y
        return x, y

    def _detect_drop_zone(
        self,
        x: float,
        y: float,
    ) -> Optional[DropZone]:
        """Detect which drop zone (if any) the point is in."""
        for zone in self._drop_zones.values():
            if self._is_in_zone(x, y, zone):
                return zone
        return None

    def _is_in_zone(self, x: float, y: float, zone: DropZone) -> bool:
        """Check if point is within a drop zone."""
        if self.config.drop_detection == DropZonePolicy.CENTER:
            center_x = zone.x + zone.width / 2
            center_y = zone.y + zone.height / 2
            distance = math.sqrt((x - center_x)**2 + (y - center_y)**2)
            threshold = min(zone.width, zone.height) / 2
            return distance <= threshold
        elif self.config.drop_detection == DropZonePolicy.OVERLAP:
            return (
                zone.x <= x <= zone.x + zone.width and
                zone.y <= y <= zone.y + zone.height
            )
        return False

    def _emit_event(
        self,
        event_type: str,
        x: float,
        y: float,
        metadata: Optional[dict] = None,
    ) -> None:
        """Emit a drag drop event."""
        if self._event_callback:
            self._event_callback(DragDropEvent(
                event_type=event_type,
                x=x, y=y,
                timestamp=time.time(),
                metadata=metadata or {},
            ))


def calculate_drop_confidence(
    drop_x: float,
    drop_y: float,
    zone: DropZone,
) -> float:
    """
    Calculate confidence score for a drop into a zone.

    Args:
        drop_x: X coordinate where item was dropped
        drop_y: Y coordinate where item was dropped
        zone: Drop zone

    Returns:
        Confidence score between 0.0 and 1.0
    """
    center_x = zone.x + zone.width / 2
    center_y = zone.y + zone.height / 2

    # Distance from center
    distance = math.sqrt((drop_x - center_x)**2 + (drop_y - center_y)**2)
    max_distance = math.sqrt(zone.width**2 + zone.height**2) / 2

    # Confidence decreases with distance
    confidence = 1.0 - (distance / max_distance)
    return max(0.0, min(1.0, confidence))
