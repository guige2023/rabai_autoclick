"""
Touch Velocity Profile Utilities

Build and analyze velocity profiles for touch gestures,
including acceleration, deceleration, and flick detection.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class VelocitySample:
    """A velocity measurement at a point in time."""
    vx: float  # velocity x (px/s)
    vy: float  # velocity y (px/s)
    speed: float  # magnitude (px/s)
    timestamp_ms: float


@dataclass
class VelocityProfile:
    """Velocity profile summary for a gesture."""
    avg_speed_px_s: float
    max_speed_px_s: float
    min_speed_px_s: float
    is_flick: bool
    dominant_direction: str  # 'horizontal', 'vertical', 'diagonal', 'stationary'
    total_distance_px: float


def compute_velocity_sample(
    x1: float, y1: float, t1_ms: float,
    x2: float, y2: float, t2_ms: float,
) -> VelocitySample:
    """Compute velocity between two touch points."""
    dt = max(1.0, t2_ms - t1_ms) / 1000.0
    vx = (x2 - x1) / dt
    vy = (y2 - y1) / dt
    speed = math.sqrt(vx * vx + vy * vy)
    return VelocitySample(
        vx=vx, vy=vy, speed=speed,
        timestamp_ms=t2_ms,
    )


def build_velocity_profile(
    points: List[tuple[float, float, float]],  # (x, y, timestamp_ms)
) -> VelocityProfile:
    """
    Build a velocity profile from a list of touch points.

    Args:
        points: List of (x, y, timestamp_ms) tuples in chronological order.

    Returns:
        VelocityProfile with summary statistics.
    """
    if len(points) < 2:
        return VelocityProfile(
            avg_speed_px_s=0.0,
            max_speed_px_s=0.0,
            min_speed_px_s=0.0,
            is_flick=False,
            dominant_direction="stationary",
            total_distance_px=0.0,
        )

    samples = [
        compute_velocity_sample(
            points[i][0], points[i][1], points[i][2],
            points[i + 1][0], points[i + 1][1], points[i + 1][2],
        )
        for i in range(len(points) - 1)
    ]

    speeds = [s.speed for s in samples]
    avg_speed = sum(speeds) / len(speeds)
    max_speed = max(speeds)
    min_speed = min(speeds)

    total_distance = sum(
        math.sqrt((points[i + 1][0] - points[i][0]) ** 2 + (points[i + 1][1] - points[i][1]) ** 2)
        for i in range(len(points) - 1)
    )

    is_flick = max_speed > 1000.0 and avg_speed > 500.0

    # Determine dominant direction
    avg_vx = sum(s.vx for s in samples) / len(samples)
    avg_vy = sum(s.vy for s in samples) / len(samples)
    angle = math.degrees(math.atan2(avg_vy, avg_vx))

    if avg_speed < 10:
        dominant = "stationary"
    elif abs(angle) < 30 or abs(angle) > 150:
        dominant = "horizontal"
    elif 60 < abs(angle) < 120:
        dominant = "vertical"
    else:
        dominant = "diagonal"

    return VelocityProfile(
        avg_speed_px_s=avg_speed,
        max_speed_px_s=max_speed,
        min_speed_px_s=min_speed,
        is_flick=is_flick,
        dominant_direction=dominant,
        total_distance_px=total_distance,
    )


def detect_flick(
    points: List[tuple[float, float, float]],
    speed_threshold: float = 1000.0,
) -> bool:
    """Detect if a gesture qualifies as a flick."""
    if len(points) < 2:
        return False
    samples = [
        compute_velocity_sample(
            points[i][0], points[i][1], points[i][2],
            points[i + 1][0], points[i + 1][1], points[i + 1][2],
        )
        for i in range(len(points) - 1)
    ]
    return any(s.speed > speed_threshold for s in samples)
