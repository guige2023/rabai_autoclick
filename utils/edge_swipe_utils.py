"""
Edge Swipe Utilities for UI Automation.

This module provides utilities for detecting and handling
edge swipe gestures in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple, Callable
from enum import Enum


class EdgeLocation(Enum):
    """Screen edge locations."""
    TOP = "top"
    BOTTOM = "bottom"
    LEFT = "left"
    RIGHT = "right"
    TOP_LEFT = "top_left"
    TOP_RIGHT = "top_right"
    BOTTOM_LEFT = "bottom_left"
    BOTTOM_RIGHT = "bottom_right"
    NONE = "none"


class EdgeSwipeDirection(Enum):
    """Directions for edge swipe gestures."""
    INTO_SCREEN = "into_screen"
    OUT_OF_SCREEN = "out_of_screen"
    PARALLEL_TO_EDGE = "parallel_to_edge"
    UNKNOWN = "unknown"


@dataclass
class EdgeSwipeEvent:
    """Represents an edge swipe gesture."""
    edge: EdgeLocation
    direction: EdgeSwipeDirection
    start_x: float
    start_y: float
    end_x: float
    end_y: float
    distance: float
    velocity: float
    timestamp: float
    recognized: bool = False


@dataclass
class EdgeSwipeConfig:
    """Configuration for edge swipe detection."""
    edge_threshold_px: float = 30.0
    min_swipe_distance_px: float = 80.0
    max_swipe_duration_ms: float = 500.0
    edge_pixels_px: float = 5.0
    enable_top_edge: bool = True
    enable_bottom_edge: bool = True
    enable_left_edge: bool = True
    enable_right_edge: bool = True


class EdgeSwipeDetector:
    """Detects edge swipe gestures from touch input."""

    def __init__(self, config: Optional[EdgeSwipeConfig] = None) -> None:
        self._config = config or EdgeSwipeConfig()
        self._is_tracking: bool = False
        self._start_x: float = 0.0
        self._start_y: float = 0.0
        self._start_time: float = 0.0
        self._current_x: float = 0.0
        self._current_y: float = 0.0
        self._screen_bounds: Tuple[float, float, float, float] = (0, 0, 1920, 1080)
        self._on_swipe_callbacks: List[Callable[[EdgeSwipeEvent], None]] = []

    def set_screen_bounds(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
    ) -> None:
        """Set the screen bounds for edge detection."""
        self._screen_bounds = (x, y, width, height)

    def on_swipe(self, callback: Callable[[EdgeSwipeEvent], None]) -> None:
        """Register a callback for detected edge swipes."""
        self._on_swipe_callbacks.append(callback)

    def touch_start(self, x: float, y: float) -> bool:
        """Handle touch start - returns True if on an edge."""
        self._start_x = x
        self._start_y = y
        self._current_x = x
        self._current_y = y
        self._start_time = time.time()
        self._is_tracking = True

        edge = self._get_nearest_edge(x, y)
        return edge != EdgeLocation.NONE

    def touch_move(self, x: float, y: float) -> None:
        """Handle touch move during edge swipe."""
        if not self._is_tracking:
            return
        self._current_x = x
        self._current_y = y

    def touch_end(self) -> Optional[EdgeSwipeEvent]:
        """Handle touch end and return detected edge swipe event."""
        if not self._is_tracking:
            return None

        self._is_tracking = False

        dx = self._current_x - self._start_x
        dy = self._current_y - self._start_y
        duration = (time.time() - self._start_time) * 1000.0

        if duration > self._config.max_swipe_duration_ms:
            return None

        edge = self._get_nearest_edge(self._start_x, self._start_y)
        direction = self._determine_direction(dx, dy, edge)

        if edge == EdgeLocation.NONE:
            return None

        if not self._is_edge_enabled(edge):
            return None

        distance = self._calculate_distance(dx, dy)
        if distance < self._config.min_swipe_distance_px:
            return None

        velocity = distance / duration if duration > 0 else 0.0

        event = EdgeSwipeEvent(
            edge=edge,
            direction=direction,
            start_x=self._start_x,
            start_y=self._start_y,
            end_x=self._current_x,
            end_y=self._current_y,
            distance=distance,
            velocity=velocity,
            timestamp=self._start_time,
            recognized=True,
        )

        for callback in self._on_swipe_callbacks:
            callback(event)

        return event

    def cancel(self) -> None:
        """Cancel the current edge swipe tracking."""
        self._is_tracking = False

    def _get_nearest_edge(self, x: float, y: float) -> EdgeLocation:
        """Determine which edge the given point is nearest to."""
        min_x, min_y, width, height = self._screen_bounds
        max_x = min_x + width
        max_y = min_y + height

        dist_left = x - min_x
        dist_right = max_x - x
        dist_top = y - min_y
        dist_bottom = max_y - y

        threshold = self._config.edge_threshold_px

        if dist_left <= threshold and dist_left <= min(dist_right, dist_top, dist_bottom):
            if dist_top <= threshold:
                return EdgeLocation.TOP_LEFT
            if dist_bottom <= threshold:
                return EdgeLocation.BOTTOM_LEFT
            return EdgeLocation.LEFT

        if dist_right <= threshold and dist_right <= min(dist_left, dist_top, dist_bottom):
            if dist_top <= threshold:
                return EdgeLocation.TOP_RIGHT
            if dist_bottom <= threshold:
                return EdgeLocation.BOTTOM_RIGHT
            return EdgeLocation.RIGHT

        if dist_top <= threshold and dist_top <= min(dist_left, dist_right, dist_bottom):
            return EdgeLocation.TOP

        if dist_bottom <= threshold and dist_bottom <= min(dist_left, dist_right, dist_top):
            return EdgeLocation.BOTTOM

        return EdgeLocation.NONE

    def _is_edge_enabled(self, edge: EdgeLocation) -> bool:
        """Check if the edge is enabled for detection."""
        cfg = self._config
        if edge in (EdgeLocation.LEFT, EdgeLocation.TOP_LEFT, EdgeLocation.BOTTOM_LEFT):
            return cfg.enable_left_edge
        if edge in (EdgeLocation.RIGHT, EdgeLocation.TOP_RIGHT, EdgeLocation.BOTTOM_RIGHT):
            return cfg.enable_right_edge
        if edge == EdgeLocation.TOP:
            return cfg.enable_top_edge
        if edge == EdgeLocation.BOTTOM:
            return cfg.enable_bottom_edge
        return False

    def _determine_direction(
        self,
        dx: float,
        dy: float,
        edge: EdgeLocation,
    ) -> EdgeSwipeDirection:
        """Determine the swipe direction based on edge and movement."""
        if edge in (EdgeLocation.LEFT, EdgeLocation.TOP_LEFT, EdgeLocation.BOTTOM_LEFT):
            if dx > 0:
                return EdgeSwipeDirection.INTO_SCREEN
            return EdgeSwipeDirection.PARALLEL_TO_EDGE

        if edge in (EdgeLocation.RIGHT, EdgeLocation.TOP_RIGHT, EdgeLocation.BOTTOM_RIGHT):
            if dx < 0:
                return EdgeSwipeDirection.INTO_SCREEN
            return EdgeSwipeDirection.PARALLEL_TO_EDGE

        if edge == EdgeLocation.TOP:
            if dy > 0:
                return EdgeSwipeDirection.INTO_SCREEN
            return EdgeSwipeDirection.PARALLEL_TO_EDGE

        if edge == EdgeLocation.BOTTOM:
            if dy < 0:
                return EdgeSwipeDirection.INTO_SCREEN
            return EdgeSwipeDirection.PARALLEL_TO_EDGE

        return EdgeSwipeDirection.UNKNOWN

    def _calculate_distance(self, dx: float, dy: float) -> float:
        """Calculate Euclidean distance."""
        return (dx * dx + dy * dy) ** 0.5
