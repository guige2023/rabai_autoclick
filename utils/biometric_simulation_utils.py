"""Biometric simulation utilities for touch pressure and fingerprint emulation.

This module provides utilities for simulating biometric input patterns
commonly found in mobile automation scenarios.
"""

from __future__ import annotations

import random
import math
from typing import Sequence
from dataclasses import dataclass
from enum import Enum


class BiometricType(Enum):
    """Types of biometric simulation."""
    FINGERPRINT = "fingerprint"
    FACE = "face"
    IRIS = "iris"
    TOUCH_PRESSURE = "touch_pressure"
    TOUCH_SIZE = "touch_size"


@dataclass
class PressurePoint:
    """Touch pressure data point.

    Attributes:
        x: X coordinate.
        y: Y coordinate.
        pressure: Pressure value (0.0 to 1.0).
        size: Touch size in points.
        timestamp: Time offset in ms.
    """
    x: float
    y: float
    pressure: float
    size: float
    timestamp: float


@dataclass
class FingerprintPattern:
    """Simulated fingerprint pattern data.

    Attributes:
        ridge_data: 2D array of ridge values.
        core_position: Position of fingerprint core.
        minutiae_points: List of minutiae (ridge endings/bifurcations).
    """
    ridge_data: list[list[float]]
    core_position: tuple[float, float]
    minutiae_points: list[dict]


def generate_pressure_curve(
    duration_ms: float,
    peak_pressure: float = 0.8,
    initial_pressure: float = 0.1,
    final_pressure: float = 0.0,
    variance: float = 0.05
) -> list[PressurePoint]:
    """Generate a realistic touch pressure curve over time.

    Simulates the natural pressure variation during a touch interaction,
    including ramp-up, peak, and release phases.

    Args:
        duration_ms: Total duration in milliseconds.
        peak_pressure: Maximum pressure during the gesture.
        initial_pressure: Starting pressure.
        final_pressure: Ending pressure.
        variance: Random variance factor.

    Returns:
        List of PressurePoints representing the pressure curve.
    """
    points: list[PressurePoint] = []
    num_samples = int(duration_ms / 10)

    for i in range(num_samples + 1):
        t = i / num_samples

        if t < 0.1:
            phase_pressure = initial_pressure + (peak_pressure - initial_pressure) * (t / 0.1)
        elif t > 0.9:
            phase_pressure = peak_pressure - (peak_pressure - final_pressure) * ((t - 0.9) / 0.1)
        else:
            phase_pressure = peak_pressure + random.uniform(-variance, variance)

        phase_pressure = max(0.0, min(1.0, phase_pressure))

        points.append(PressurePoint(
            x=0.0,
            y=0.0,
            pressure=phase_pressure,
            size=10.0 + phase_pressure * 20.0,
            timestamp=t * duration_ms
        ))

    return points


def apply_pressure_to_path(
    path: Sequence[tuple[float, float]],
    base_pressure: float = 0.5,
    variance: float = 0.1,
    follow_velocity: bool = True
) -> list[PressurePoint]:
    """Apply pressure values to a touch path.

    Simulates how real fingers apply more pressure when moving slower.

    Args:
        path: Sequence of (x, y) coordinates.
        base_pressure: Base pressure value.
        variance: Random variance to apply.
        follow_velocity: If True, slower movement gets higher pressure.

    Returns:
        List of PressurePoints with pressure values.
    """
    if len(path) < 2:
        return [
            PressurePoint(
                x=p[0], y=p[1],
                pressure=base_pressure,
                size=10.0,
                timestamp=0.0
            ) for p in path
        ]

    points: list[PressurePoint] = []
    total_distance = 0.0

    for i, (x, y) in enumerate(path):
        if i > 0:
            prev_x, prev_y = path[i - 1]
            total_distance += math.sqrt((x - prev_x) ** 2 + (y - prev_y) ** 2)

    cumulative_distance = 0.0

    for i, (x, y) in enumerate(path):
        if i > 0:
            prev_x, prev_y = path[i - 1]
            segment_dist = math.sqrt((x - prev_x) ** 2 + (y - prev_y) ** 2)
            cumulative_distance += segment_dist

            if follow_velocity and total_distance > 0:
                velocity_factor = 1.0 - (cumulative_distance / total_distance) * 0.3
                pressure = base_pressure * velocity_factor
            else:
                pressure = base_pressure

            pressure += random.uniform(-variance, variance)
            pressure = max(0.1, min(1.0, pressure))
        else:
            pressure = base_pressure * 0.5

        points.append(PressurePoint(
            x=x,
            y=y,
            pressure=pressure,
            size=8.0 + pressure * 15.0,
            timestamp=cumulative_distance * 10.0
        ))

    return points


def generate_fingerprint_pattern(
    width: int = 256,
    height: int = 256,
    pattern_type: str = "whorl"
) -> FingerprintPattern:
    """Generate a simulated fingerprint ridge pattern.

    Args:
        width: Pattern width in points.
        height: Pattern height in points.
        pattern_type: Type of pattern ("whorl", "loop", "arch").

    Returns:
        FingerprintPattern with ridge data and features.
    """
    ridge_data: list[list[float]] = []

    center_x = width / 2
    center_y = height / 2

    for y in range(height):
        row: list[float] = []
        for x in range(width):
            dx = x - center_x
            dy = y - center_y
            dist = math.sqrt(dx * dx + dy * dy)

            if pattern_type == "whorl":
                angle = math.atan2(dy, dx)
                frequency = 0.1
                ridge_value = math.sin(angle * 3 + dist * frequency)
            elif pattern_type == "loop":
                ridge_value = math.sin(dy * 0.15 + dx * 0.05)
            else:
                ridge_value = math.sin(dy * 0.2)

            ridge_value = (ridge_value + 1.0) / 2.0
            noise = random.uniform(-0.1, 0.1)
            ridge_value = max(0.0, min(1.0, ridge_value + noise))

            row.append(ridge_value)

        ridge_data.append(row)

    return FingerprintPattern(
        ridge_data=ridge_data,
        core_position=(center_x, center_y),
        minutiae_points=generate_minutiae(ridge_data, center_x, center_y)
    )


def generate_minutiae(
    ridge_data: list[list[float]],
    core_x: float,
    core_y: float
) -> list[dict]:
    """Generate fingerprint minutiae points from ridge data.

    Args:
        ridge_data: 2D ridge intensity array.
        core_x: Core region X center.
        core_y: Core region Y center.

    Returns:
        List of minutiae point dicts with type and position.
    """
    minutiae: list[dict] = []
    height = len(ridge_data)
    width = len(ridge_data[0]) if height > 0 else 0

    num_minutiae = random.randint(15, 30)

    for _ in range(num_minutiae):
        mx = int(core_x + random.uniform(-width * 0.3, width * 0.3))
        my = int(core_y + random.uniform(-height * 0.3, height * 0.3))

        mx = max(1, min(width - 2, mx))
        my = max(1, min(height - 2, my))

        minutiae.append({
            "x": mx,
            "y": my,
            "type": random.choice(["ending", "bifurcation"]),
            "angle": random.uniform(0, 360)
        })

    return minutiae


def simulate_swipe_pressure(
    start_x: float,
    start_y: float,
    end_x: float,
    end_y: float,
    duration_ms: float,
    num_points: int = 50
) -> list[PressurePoint]:
    """Simulate pressure along a swipe gesture.

    Args:
        start_x: Start X coordinate.
        start_y: Start Y coordinate.
        end_x: End X coordinate.
        end_y: End Y coordinate.
        duration_ms: Gesture duration in milliseconds.
        num_points: Number of points to generate.

    Returns:
        List of PressurePoints along the swipe.
    """
    points: list[PressurePoint] = []

    for i in range(num_points):
        t = i / (num_points - 1)

        x = start_x + (end_x - start_x) * t
        y = start_y + (end_y - start_y) * t

        pressure = 0.3 + 0.5 * math.sin(t * math.pi)

        pressure += random.uniform(-0.05, 0.05)
        pressure = max(0.1, min(1.0, pressure))

        timestamp = t * duration_ms

        points.append(PressurePoint(
            x=x,
            y=y,
            pressure=pressure,
            size=10.0 + pressure * 12.0,
            timestamp=timestamp
        ))

    return points


def simulate_pinch_pressure(
    center_x: float,
    center_y: float,
    initial_distance: float,
    final_distance: float,
    duration_ms: float,
    num_points: int = 50
) -> tuple[list[PressurePoint], list[PressurePoint]]:
    """Simulate pressure for a pinch gesture with two fingers.

    Args:
        center_x: Center X of pinch gesture.
        center_y: Center Y of pinch gesture.
        initial_distance: Initial distance between fingers.
        final_distance: Final distance between fingers.
        duration_ms: Gesture duration in milliseconds.
        num_points: Number of points per finger.

    Returns:
        Tuple of (finger1_points, finger2_points).
    """
    finger1: list[PressurePoint] = []
    finger2: list[PressurePoint] = []

    for i in range(num_points):
        t = i / (num_points - 1)

        current_distance = initial_distance + (final_distance - initial_distance) * t

        half_dist = current_distance / 2

        fx1 = center_x - half_dist
        fy1 = center_y
        fx2 = center_x + half_dist
        fy2 = center_y

        pressure = 0.4 + 0.3 * math.sin(t * math.pi)
        pressure += random.uniform(-0.03, 0.03)
        pressure = max(0.1, min(1.0, pressure))

        timestamp = t * duration_ms

        finger1.append(PressurePoint(
            x=fx1, y=fy1,
            pressure=pressure,
            size=12.0 + pressure * 10.0,
            timestamp=timestamp
        ))

        finger2.append(PressurePoint(
            x=fx2, y=fy2,
            pressure=pressure,
            size=12.0 + pressure * 10.0,
            timestamp=timestamp
        ))

    return (finger1, finger2)


def normalize_pressure(value: float, min_val: float = 0.0, max_val: float = 1.0) -> float:
    """Normalize a pressure value to 0.0-1.0 range.

    Args:
        value: Raw pressure value.
        min_val: Minimum expected value.
        max_val: Maximum expected value.

    Returns:
        Normalized pressure between 0.0 and 1.0.
    """
    if max_val == min_val:
        return 0.5

    normalized = (value - min_val) / (max_val - min_val)
    return max(0.0, min(1.0, normalized))
