"""
Touch trajectory validator utilities.

This module provides utilities for validating touch trajectories,
checking for errors, and filtering invalid inputs.
"""

from __future__ import annotations

import math
from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass, field
from enum import Enum, auto


class ValidationError(Enum):
    """Types of trajectory validation errors."""
    NONE = auto()
    TOO_SHORT = auto()
    TOO_LONG = auto()
    EXCESSIVE_SPEED = auto()
    BACKWARD_MOTION = auto()
    DISCONNECTED = auto()
    INVALID_COORDINATES = auto()
    PHANTOM_POINTS = auto()


@dataclass
class ValidationResult:
    """Result of trajectory validation."""
    is_valid: bool
    errors: List[ValidationError]
    score: float
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __bool__(self) -> bool:
        return self.is_valid


@dataclass
class TrajectoryValidatorConfig:
    """Configuration for trajectory validation."""
    min_points: int = 3
    max_points: int = 10000
    min_distance_px: float = 5.0
    max_speed_pxs: float = 5000.0
    max_duration_ms: float = 30000.0
    allow_backward: bool = True
    allow_disconnected: bool = False
    max_gap_px: float = 100.0


# Type aliases
Point2D = Tuple[float, float]
Trajectory = List[Point2D]


def validate_trajectory(
    points: Trajectory,
    timestamps: List[float],
    config: Optional[TrajectoryValidatorConfig] = None,
) -> ValidationResult:
    """
    Validate a touch trajectory.

    Args:
        points: Trajectory points.
        timestamps: Timestamps for each point (ms).
        config: Validation configuration.

    Returns:
        ValidationResult with validation outcome.
    """
    if config is None:
        config = TrajectoryValidatorConfig()

    errors: List[ValidationError] = []
    metadata: Dict[str, Any] = {}

    # Check point count
    if len(points) < config.min_points:
        errors.append(ValidationError.TOO_SHORT)
    if len(points) > config.max_points:
        errors.append(ValidationError.TOO_LONG)

    # Check duration
    if timestamps:
        duration_ms = timestamps[-1] - timestamps[0]
        metadata["duration_ms"] = duration_ms
        if duration_ms > config.max_duration_ms:
            errors.append(ValidationError.TOO_LONG)

    # Check coordinates
    for i, (x, y) in enumerate(points):
        if math.isnan(x) or math.isnan(y) or math.isinf(x) or math.isinf(y):
            errors.append(ValidationError.INVALID_COORDINATES)
            metadata["invalid_index"] = i
            break

    # Check for disconnected segments
    if not config.allow_disconnected:
        for i in range(1, len(points)):
            dx = points[i][0] - points[i - 1][0]
            dy = points[i][1] - points[i - 1][1]
            dist = math.sqrt(dx * dx + dy * dy)
            if dist > config.max_gap_px:
                errors.append(ValidationError.DISCONNECTED)
                metadata["disconnect_index"] = i
                break

    # Check total distance
    total_dist = sum(
        math.sqrt((points[i][0] - points[i - 1][0]) ** 2 + (points[i][1] - points[i - 1][1]) ** 2)
        for i in range(1, len(points))
    )
    metadata["total_distance_px"] = total_dist
    if total_dist < config.min_distance_px:
        errors.append(ValidationError.TOO_SHORT)

    # Check speed
    if timestamps and len(points) >= 2:
        speeds: List[float] = []
        for i in range(1, len(points)):
            dx = points[i][0] - points[i - 1][0]
            dy = points[i][1] - points[i - 1][1]
            dt = (timestamps[i] - timestamps[i - 1]) / 1000.0
            if dt > 0:
                speed = math.sqrt(dx * dx + dy * dy) / dt
                speeds.append(speed)

        if speeds:
            max_speed = max(speeds)
            metadata["max_speed_pxs"] = max_speed
            if max_speed > config.max_speed_pxs:
                errors.append(ValidationError.EXCESSIVE_SPEED)

    # Check backward motion
    if not config.allow_backward:
        backward_count = 0
        for i in range(1, len(points)):
            # Only check primary direction (first segment as reference)
            dx = points[i][0] - points[i - 1][0]
            dy = points[i][1] - points[i - 1][1]
            if dx < -2 or dy < -2:
                backward_count += 1
        if backward_count > len(points) * 0.3:
            errors.append(ValidationError.BACKWARD_MOTION)

    # Compute validity score
    is_valid = len(errors) == 0
    score = 1.0 - (len(errors) / len(ValidationError)) if errors else 1.0

    return ValidationResult(is_valid=is_valid, errors=errors, score=score, metadata=metadata)


def filter_valid_trajectory(
    points: Trajectory,
    timestamps: List[float],
    config: Optional[TrajectoryValidatorConfig] = None,
) -> Tuple[Trajectory, List[float]]:
    """
    Filter a trajectory to only include valid points.

    Args:
        points: Trajectory points.
        timestamps: Timestamps for each point.
        config: Validation configuration.

    Returns:
        Tuple of (filtered_points, filtered_timestamps).
    """
    if config is None:
        config = TrajectoryValidatorConfig()
    if len(points) < 2:
        return points[:], timestamps[:]

    valid_indices = [0]
    for i in range(1, len(points)):
        # Check coordinate validity
        x, y = points[i]
        if math.isnan(x) or math.isnan(y) or math.isinf(x) or math.isinf(y):
            continue

        # Check gap from last valid point
        if valid_indices:
            last = valid_indices[-1]
            dx = points[i][0] - points[last][0]
            dy = points[i][1] - points[last][1]
            dist = math.sqrt(dx * dx + dy * dy)
            if dist > config.max_gap_px:
                continue

        valid_indices.append(i)

    return [points[i] for i in valid_indices], [timestamps[i] for i in valid_indices]
