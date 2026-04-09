"""
Input anomaly classification utilities.

This module provides classifiers for detecting anomalous input patterns
such as jitter, drift, stuck keys, and phantom touches.
"""

from __future__ import annotations

import math
from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass, field
from enum import Enum, auto


class AnomalyType(Enum):
    """Types of input anomalies."""
    NONE = auto()
    JITTER = auto()
    DRIFT = auto()
    STUCK = auto()
    PHANTOM_TOUCH = auto()
    GHOST_CLICK = auto()
    VELOCITY_SPIKE = auto()
    TRAJECTORY_LOOP = auto()


@dataclass
class AnomalyResult:
    """Result of anomaly classification."""
    anomaly_type: AnomalyType = AnomalyType.NONE
    confidence: float = 0.0
    severity: float = 0.0
    details: Dict[str, Any] = field(default_factory=dict)

    def is_anomalous(self, threshold: float = 0.5) -> bool:
        """Return True if confidence exceeds threshold."""
        return self.confidence >= threshold


@dataclass
class JitterClassifierConfig:
    """Configuration for jitter classification."""
    min_displacement_threshold: float = 3.0
    max_duration_ms: int = 100
    min_frequency: int = 5


@dataclass
class DriftClassifierConfig:
    """Configuration for drift classification."""
    drift_speed_threshold: float = 2.0
    min_duration_ms: int = 500


@dataclass
class StuckClassifierConfig:
    """Configuration for stuck input classification."""
    stationary_threshold: float = 1.0
    stuck_duration_ms: int = 3000


def compute_velocity(points: List[Tuple[float, float]], dt_ms: float) -> List[float]:
    """
    Compute velocity magnitude at each point given time intervals.

    Args:
        points: List of (x, y) coordinate tuples.
        dt_ms: Time delta in milliseconds between points.

    Returns:
        List of velocity magnitudes.
    """
    if len(points) < 2 or dt_ms <= 0:
        return [0.0] * len(points)
    dt_s = dt_ms / 1000.0
    velocities: List[float] = []
    for i in range(1, len(points)):
        dx = points[i][0] - points[i - 1][0]
        dy = points[i][1] - points[i - 1][1]
        velocities.append(math.sqrt(dx * dx + dy * dy) / dt_s)
    return velocities


def classify_jitter(
    points: List[Tuple[float, float]],
    dt_ms: float,
    config: Optional[JitterClassifierConfig] = None,
) -> AnomalyResult:
    """
    Classify whether a sequence of input points exhibits jitter.

    Args:
        points: Sequence of input points.
        dt_ms: Time between points in milliseconds.
        config: Jitter classifier configuration.

    Returns:
        AnomalyResult with jitter classification.
    """
    if config is None:
        config = JitterClassifierConfig()
    if len(points) < config.min_frequency:
        return AnomalyResult()

    velocities = compute_velocity(points, dt_ms)
    if not velocities:
        return AnomalyResult()

    avg_vel = sum(velocities) / len(velocities)
    std_vel = math.sqrt(sum((v - avg_vel) ** 2 for v in velocities) / len(velocities))

    # Count high-frequency reversals
    reversals = 0
    for i in range(1, len(points)):
        dx = points[i][0] - points[i - 1][0]
        dy = points[i][1] - points[i - 1][1]
        if i >= 2:
            prev_dx = points[i - 1][0] - points[i - 2][0]
            prev_dy = points[i - 1][1] - points[i - 2][1]
            dot = dx * prev_dx + dy * prev_dy
            if dot < 0:
                reversals += 1

    reversal_rate = reversals / max(1, len(points) - 2)
    displacement = sum(math.sqrt((points[i][0] - points[i - 1][0]) ** 2 + (points[i][1] - points[i - 1][1]) ** 2) for i in range(1, len(points)))

    if reversal_rate > 0.3 and displacement < config.min_displacement_threshold * len(points):
        confidence = min(1.0, reversal_rate * 2)
        return AnomalyResult(
            anomaly_type=AnomalyType.JITTER,
            confidence=confidence,
            severity=std_vel / max(avg_vel, 1.0),
            details={"reversal_rate": reversal_rate, "std_vel": std_vel},
        )

    return AnomalyResult()


def classify_drift(
    points: List[Tuple[float, float]],
    dt_ms: float,
    config: Optional[DriftClassifierConfig] = None,
) -> AnomalyResult:
    """
    Classify whether input exhibits slow continuous drift.

    Args:
        points: Sequence of input points.
        dt_ms: Time between points in milliseconds.
        config: Drift classifier configuration.

    Returns:
        AnomalyResult with drift classification.
    """
    if config is None:
        config = DriftClassifierConfig()
    if len(points) < 3:
        return AnomalyResult()

    velocities = compute_velocity(points, dt_ms)
    if not velocities:
        return AnomalyResult()

    # Check for consistent slow movement direction
    directions = []
    for i in range(1, len(points)):
        dx = points[i][0] - points[i - 1][0]
        dy = points[i][1] - points[i - 1][1]
        if abs(dx) > 0.1 or abs(dy) > 0.1:
            directions.append(math.atan2(dy, dx))

    if len(directions) < 2:
        return AnomalyResult()

    avg_dir = sum(directions) / len(directions)
    dir_variance = sum((d - avg_dir) ** 2 for d in directions) / len(directions)
    avg_speed = sum(velocities) / len(velocities)

    # Low direction variance + slow speed = drift
    if dir_variance < 0.5 and avg_speed < config.drift_speed_threshold:
        confidence = 1.0 - math.sqrt(dir_variance) / math.pi
        return AnomalyResult(
            anomaly_type=AnomalyType.DRIFT,
            confidence=min(1.0, confidence),
            severity=avg_speed / max(config.drift_speed_threshold, 1.0),
            details={"avg_speed": avg_speed, "direction_variance": dir_variance},
        )

    return AnomalyResult()


def classify_stuck(
    points: List[Tuple[float, float]],
    dt_ms: float,
    config: Optional[StuckClassifierConfig] = None,
) -> AnomalyResult:
    """
    Classify whether input contains stuck/stationary segments.

    Args:
        points: Sequence of input points.
        dt_ms: Time between points in milliseconds.
        config: Stuck classifier configuration.

    Returns:
        AnomalyResult with stuck classification.
    """
    if config is None:
        config = StuckClassifierConfig()
    if len(points) < 2:
        return AnomalyResult()

    stuck_duration_ms = 0
    max_stuck = 0
    for i in range(1, len(points)):
        dx = points[i][0] - points[i - 1][0]
        dy = points[i][1] - points[i - 1][1]
        dist = math.sqrt(dx * dx + dy * dy)
        if dist < config.stationary_threshold:
            stuck_duration_ms += dt_ms
            max_stuck = max(max_stuck, stuck_duration_ms)
        else:
            stuck_duration_ms = 0

    if max_stuck >= config.stuck_duration_ms:
        confidence = min(1.0, max_stuck / (config.stuck_duration_ms * 2))
        return AnomalyResult(
            anomaly_type=AnomalyType.STUCK,
            confidence=confidence,
            severity=max_stuck / 10000.0,
            details={"max_stuck_ms": max_stuck},
        )

    return AnomalyResult()


def classify_anomaly(
    points: List[Tuple[float, float]],
    dt_ms: float,
    configs: Optional[Dict[AnomalyType, Any]] = None,
) -> AnomalyResult:
    """
    Classify the type of anomaly present in an input sequence.

    Args:
        points: Sequence of input points.
        dt_ms: Time between points in milliseconds.
        configs: Optional dictionary of per-type configurations.

    Returns:
        AnomalyResult with highest-confidence classification.
    """
    results = [
        classify_jitter(points, dt_ms, configs.get(AnomalyType.JITTER)),
        classify_drift(points, dt_ms, configs.get(AnomalyType.DRIFT)),
        classify_stuck(points, dt_ms, configs.get(AnomalyType.STUCK)),
    ]

    best = AnomalyResult()
    for result in results:
        if result.confidence > best.confidence:
            best = result

    return best
