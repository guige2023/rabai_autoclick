"""
Touch Coordinate Utilities for UI Automation.

This module provides utilities for coordinate transformations
in touch-based UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple


@dataclass
class CoordinateFrame:
    """Represents a coordinate frame for transformations."""
    origin_x: float
    origin_y: float
    scale_x: float = 1.0
    scale_y: float = 1.0
    rotation_deg: float = 0.0


@dataclass
class Point2D:
    """A 2D point."""
    x: float
    y: float


class TouchCoordinateTransformer:
    """Transforms touch coordinates between different frames."""

    def __init__(self) -> None:
        self._source_frame: CoordinateFrame = CoordinateFrame(0.0, 0.0)
        self._target_frame: CoordinateFrame = CoordinateFrame(0.0, 0.0)

    def set_source_frame(self, frame: CoordinateFrame) -> None:
        """Set the source coordinate frame."""
        self._source_frame = frame

    def set_target_frame(self, frame: CoordinateFrame) -> None:
        """Set the target coordinate frame."""
        self._target_frame = frame

    def transform_point(self, x: float, y: float) -> Tuple[float, float]:
        """Transform a point from source to target frame."""
        x, y = self._from_source(x, y)
        x, y = self._to_target(x, y)
        return (x, y)

    def transform_points(
        self,
        points: List[Tuple[float, float]],
    ) -> List[Tuple[float, float]]:
        """Transform multiple points."""
        return [self.transform_point(x, y) for x, y in points]

    def _from_source(self, x: float, y: float) -> Tuple[float, float]:
        """Convert from source frame to normalized space."""
        x = (x - self._source_frame.origin_x) / self._source_frame.scale_x
        y = (y - self._source_frame.origin_y) / self._source_frame.scale_y

        if self._source_frame.rotation_deg != 0.0:
            angle_rad = math.radians(self._source_frame.rotation_deg)
            cos_a = math.cos(angle_rad)
            sin_a = math.sin(angle_rad)
            x_rot = x * cos_a - y * sin_a
            y_rot = x * sin_a + y * cos_a
            x, y = x_rot, y_rot

        return (x, y)

    def _to_target(self, x: float, y: float) -> Tuple[float, float]:
        """Convert from normalized space to target frame."""
        if self._target_frame.rotation_deg != 0.0:
            angle_rad = math.radians(-self._target_frame.rotation_deg)
            cos_a = math.cos(angle_rad)
            sin_a = math.sin(angle_rad)
            x_rot = x * cos_a - y * sin_a
            y_rot = x * sin_a + y * cos_a
            x, y = x_rot, y_rot

        x = x * self._target_frame.scale_x + self._target_frame.origin_x
        y = y * self._target_frame.scale_y + self._target_frame.origin_y

        return (x, y)

    def inverse_transform(self, x: float, y: float) -> Tuple[float, float]:
        """Apply inverse transformation."""
        x, y = self._from_target(x, y)
        x, y = self._to_source(x, y)
        return (x, y)

    def _from_target(self, x: float, y: float) -> Tuple[float, float]:
        """Convert from target frame to normalized space."""
        x = (x - self._target_frame.origin_x) / self._target_frame.scale_x
        y = (y - self._target_frame.origin_y) / self._target_frame.scale_y

        if self._target_frame.rotation_deg != 0.0:
            angle_rad = math.radians(self._target_frame.rotation_deg)
            cos_a = math.cos(angle_rad)
            sin_a = math.sin(angle_rad)
            x_rot = x * cos_a - y * sin_a
            y_rot = x * sin_a + y * cos_a
            x, y = x_rot, y_rot

        return (x, y)

    def _to_source(self, x: float, y: float) -> Tuple[float, float]:
        """Convert from normalized space to source frame."""
        if self._source_frame.rotation_deg != 0.0:
            angle_rad = math.radians(-self._source_frame.rotation_deg)
            cos_a = math.cos(angle_rad)
            sin_a = math.sin(angle_rad)
            x_rot = x * cos_a - y * sin_a
            y_rot = x * sin_a + y * cos_a
            x, y = x_rot, y_rot

        x = x * self._source_frame.scale_x + self._source_frame.origin_x
        y = y * self._source_frame.scale_y + self._source_frame.origin_y

        return (x, y)

    def normalize_to_screen(
        self,
        x: float,
        y: float,
        screen_width: float,
        screen_height: float,
    ) -> Tuple[float, float]:
        """Normalize coordinates to screen space (0-1 range)."""
        return (x / screen_width, y / screen_height)

    def denormalize_from_screen(
        self,
        nx: float,
        ny: float,
        screen_width: float,
        screen_height: float,
    ) -> Tuple[float, float]:
        """Convert normalized coordinates back to screen space."""
        return (nx * screen_width, ny * screen_height)


def rotate_point(
    x: float,
    y: float,
    cx: float,
    cy: float,
    angle_deg: float,
) -> Tuple[float, float]:
    """Rotate a point around a center by the given angle."""
    angle_rad = math.radians(angle_deg)
    cos_a = math.cos(angle_rad)
    sin_a = math.sin(angle_rad)

    dx = x - cx
    dy = y - cy

    x_rot = cx + dx * cos_a - dy * sin_a
    y_rot = cy + dx * sin_a + dy * cos_a

    return (x_rot, y_rot)
