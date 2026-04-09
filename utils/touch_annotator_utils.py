"""
Touch Annotator Utilities for UI Automation.

This module provides utilities for annotating touch events
with semantic labels in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Callable
from enum import Enum


class TouchAction(Enum):
    """Semantic types of touch actions."""
    TAP = "tap"
    DOUBLE_TAP = "double_tap"
    LONG_PRESS = "long_press"
    DRAG = "drag"
    SWIPE = "swipe"
    PINCH = "pinch"
    ROTATE = "rotate"
    SCROLL = "scroll"
    UNKNOWN = "unknown"


@dataclass
class AnnotatedTouch:
    """A touch event with semantic annotation."""
    x: float
    y: float
    action: TouchAction
    confidence: float
    timestamp: float
    duration_ms: float = 0.0
    direction: str = "none"
    velocity: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AnnotationConfig:
    """Configuration for touch annotation."""
    min_swipe_distance: float = 50.0
    min_drag_distance: float = 20.0
    long_press_threshold_ms: float = 500.0
    velocity_weight: float = 0.4
    distance_weight: float = 0.4
    duration_weight: float = 0.2


class TouchAnnotator:
    """Annotates touch events with semantic labels."""

    def __init__(self, config: Optional[AnnotationConfig] = None) -> None:
        self._config = config or AnnotationConfig()
        self._touch_buffer: List[AnnotatedTouch] = []
        self._max_buffer_size: int = 100

    def set_config(self, config: AnnotationConfig) -> None:
        """Update the annotation configuration."""
        self._config = config

    def annotate(
        self,
        x: float,
        y: float,
        timestamp: Optional[float] = None,
        duration_ms: float = 0.0,
        distance: float = 0.0,
        velocity: float = 0.0,
        direction: str = "none",
    ) -> AnnotatedTouch:
        """Annotate a touch event with semantic type."""
        if timestamp is None:
            timestamp = time.time()

        action, confidence = self._classify_action(
            duration_ms=duration_ms,
            distance=distance,
            velocity=velocity,
            direction=direction,
        )

        annotated = AnnotatedTouch(
            x=x,
            y=y,
            action=action,
            confidence=confidence,
            timestamp=timestamp,
            duration_ms=duration_ms,
            direction=direction,
            velocity=velocity,
        )

        self._add_to_buffer(annotated)
        return annotated

    def _classify_action(
        self,
        duration_ms: float,
        distance: float,
        velocity: float,
        direction: str,
    ) -> tuple[TouchAction, float]:
        """Classify the touch action based on features."""
        if duration_ms >= self._config.long_press_threshold_ms:
            if distance < self._config.min_drag_distance:
                return (TouchAction.LONG_PRESS, 0.9)

        if distance >= self._config.min_swipe_distance and velocity > 0:
            if direction in ("up", "down"):
                return (TouchAction.SCROLL, min(velocity / 500.0, 1.0))
            return (TouchAction.SWIPE, min(velocity / 500.0, 1.0))

        if distance >= self._config.min_drag_distance:
            return (TouchAction.DRAG, min(distance / 100.0, 1.0))

        if duration_ms < 200:
            return (TouchAction.TAP, 0.85)

        return (TouchAction.UNKNOWN, 0.5)

    def _add_to_buffer(self, touch: AnnotatedTouch) -> None:
        """Add annotated touch to buffer."""
        self._touch_buffer.append(touch)
        if len(self._touch_buffer) > self._max_buffer_size:
            self._touch_buffer.pop(0)

    def get_buffer(self) -> List[AnnotatedTouch]:
        """Get the annotation buffer."""
        return list(self._touch_buffer)

    def get_last_action(self) -> Optional[TouchAction]:
        """Get the action type of the last annotated touch."""
        if not self._touch_buffer:
            return None
        return self._touch_buffer[-1].action

    def clear_buffer(self) -> None:
        """Clear the annotation buffer."""
        self._touch_buffer.clear()

    def get_action_summary(self) -> Dict[TouchAction, int]:
        """Get a count summary of all actions in the buffer."""
        summary: Dict[TouchAction, int] = {}
        for touch in self._touch_buffer:
            summary[touch.action] = summary.get(touch.action, 0) + 1
        return summary
