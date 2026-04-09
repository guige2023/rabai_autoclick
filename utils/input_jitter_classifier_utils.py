"""
Input jitter classification utilities.

This module classifies input jitter patterns and determines their
characteristics including frequency, amplitude, and dominant direction.
"""

from __future__ import annotations

import math
from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass, field
from enum import Enum, auto


class JitterPattern(Enum):
    """Classification of jitter pattern type."""
    NONE = auto()
    RANDOM = auto()
    PERIODIC = auto()
    DIRECTIONAL = auto()
    BURST = auto()


@dataclass
class JitterCharacteristics:
    """Detailed characteristics of an input jitter pattern."""
    pattern: JitterPattern = JitterPattern.NONE
    amplitude_px: float = 0.0
    frequency_hz: float = 0.0
    dominant_axis: str = "none"
    regularity: float = 0.0
    confidence: float = 0.0
    burst_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class JitterClassifierParams:
    """Parameters for jitter classification."""
    min_amplitude_px: float = 2.0
    max_amplitude_px: float = 50.0
    min_frequency_hz: float = 5.0
    regularity_threshold: float = 0.6


def compute_displacements(points: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
    """
    Compute per-sample displacements from a trajectory.

    Args:
        points: List of (x, y) coordinate tuples.

    Returns:
        List of (dx, dy) displacement tuples.
    """
    if len(points) < 2:
        return []
    return [(points[i][0] - points[i - 1][0], points[i][1] - points[i - 1][1]) for i in range(1, len(points))]


def compute_jitter_amplitude(displacements: List[Tuple[float, float]]) -> float:
    """
    Compute the amplitude of jitter in pixels.

    Args:
        displacements: List of (dx, dy) displacements.

    Returns:
        Average displacement magnitude.
    """
    if not displacements:
        return 0.0
    magnitudes = [math.sqrt(dx * dx + dy * dy) for dx, dy in displacements]
    return sum(magnitudes) / len(magnitudes)


def detect_dominant_axis(displacements: List[Tuple[float, float]]) -> str:
    """
    Determine which axis (x, y, or diagonal) dominates the motion.

    Args:
        displacements: List of (dx, dy) displacements.

    Returns:
        "x", "y", or "diagonal".
    """
    if not displacements:
        return "none"
    sum_x = sum(abs(dx) for dx, dy in displacements)
    sum_y = sum(abs(dy) for dx, dy in displacements)
    if sum_x > sum_y * 1.5:
        return "x"
    elif sum_y > sum_x * 1.5:
        return "y"
    return "diagonal"


def estimate_frequency(displacements: List[Tuple[float, float]], dt_ms: float) -> float:
    """
    Estimate dominant frequency of jitter via zero-crossing rate.

    Args:
        displacements: List of (dx, dy) displacements.
        dt_ms: Time between samples in milliseconds.

    Returns:
        Estimated frequency in Hz.
    """
    if len(displacements) < 3 or dt_ms <= 0:
        return 0.0

    magnitudes = [math.sqrt(dx * dx + dy * dy) for dx, dy in displacements]
    avg = sum(magnitudes) / len(magnitudes)

    # Count zero crossings (when magnitude goes above/below average)
    crossings = 0
    above = magnitudes[0] > avg
    for mag in magnitudes[1:]:
        curr_above = mag > avg
        if curr_above != above:
            crossings += 1
            above = curr_above

    dt_s = dt_ms / 1000.0
    zero_crossings_per_second = crossings / (len(magnitudes) * dt_s)
    return zero_crossings_per_second / 2.0


def compute_regularity(displacements: List[Tuple[float, float]]) -> float:
    """
    Compute regularity score (0-1) based on displacement consistency.

    Args:
        displacements: List of (dx, dy) displacements.

    Returns:
        Regularity score (1.0 = perfectly regular).
    """
    if len(displacements) < 2:
        return 0.0

    magnitudes = [math.sqrt(dx * dx + dy * dy) for dx, dy in displacements]
    mean_mag = sum(magnitudes) / len(magnitudes)
    if mean_mag < 0.1:
        return 0.0

    # Coefficient of variation (lower = more regular)
    variance = sum((m - mean_mag) ** 2 for m in magnitudes) / len(magnitudes)
    cv = math.sqrt(variance) / mean_mag
    regularity = max(0.0, 1.0 - min(1.0, cv))
    return regularity


def detect_bursts(magnitudes: List[float], threshold: float) -> int:
    """
    Count burst events where magnitude exceeds threshold.

    Args:
        magnitudes: Signal magnitudes.
        threshold: Burst detection threshold.

    Returns:
        Number of burst events.
    """
    if not magnitudes:
        return 0
    bursts = 0
    in_burst = False
    for mag in magnitudes:
        if mag > threshold and not in_burst:
            bursts += 1
            in_burst = True
        elif mag <= threshold:
            in_burst = False
    return bursts


def classify_jitter_pattern(
    points: List[Tuple[float, float]],
    dt_ms: float,
    params: Optional[JitterClassifierParams] = None,
) -> JitterCharacteristics:
    """
    Classify jitter characteristics of an input trajectory.

    Args:
        points: Input trajectory points.
        dt_ms: Time between points in milliseconds.
        params: Classification parameters.

    Returns:
        JitterCharacteristics with classification results.
    """
    if params is None:
        params = JitterClassifierParams()

    if len(points) < 3:
        return JitterCharacteristics()

    displacements = compute_displacements(points)
    if not displacements:
        return JitterCharacteristics()

    amplitude = compute_jitter_amplitude(displacements)
    if amplitude < params.min_amplitude_px:
        return JitterCharacteristics(pattern=JitterPattern.NONE, amplitude_px=amplitude)

    if amplitude > params.max_amplitude_px:
        amplitude = params.max_amplitude_px

    frequency = estimate_frequency(displacements, dt_ms)
    axis = detect_dominant_axis(displacements)
    regularity = compute_regularity(displacements)

    magnitudes = [math.sqrt(dx * dx + dy * dy) for dx, dy in displacements]
    burst_threshold = amplitude * 1.5
    burst_count = detect_bursts(magnitudes, burst_threshold)

    # Determine pattern
    pattern = JitterPattern.NONE
    if regularity >= params.regularity_threshold and frequency >= params.min_frequency_hz:
        pattern = JitterPattern.PERIODIC
    elif regularity < 0.3:
        pattern = JitterPattern.RANDOM
    elif axis != "diagonal" and regularity >= 0.4:
        pattern = JitterPattern.DIRECTIONAL
    elif burst_count > 2:
        pattern = JitterPattern.BURST

    confidence = min(1.0, regularity * 0.5 + (1.0 if pattern != JitterPattern.NONE else 0.0) * 0.5)

    return JitterCharacteristics(
        pattern=pattern,
        amplitude_px=amplitude,
        frequency_hz=frequency,
        dominant_axis=axis,
        regularity=regularity,
        confidence=confidence,
        burst_count=burst_count,
        metadata={
            "point_count": len(points),
            "avg_magnitude": sum(magnitudes) / len(magnitudes),
            "total_displacement": sum(magnitudes),
        },
    )
