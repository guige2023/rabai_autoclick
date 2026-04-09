"""
Input velocity profile utilities.

This module provides utilities for analyzing and comparing input velocity
profiles, including acceleration patterns and movement signature analysis.
"""

from __future__ import annotations

import math
from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass, field


# Type aliases
Point2D = Tuple[float, float]
VelocityProfile = List[float]


@dataclass
class VelocityStats:
    """Statistical summary of a velocity profile."""
    mean: float = 0.0
    max: float = 0.0
    min: float = 0.0
    std_dev: float = 0.0
    start_velocity: float = 0.0
    end_velocity: float = 0.0
    peak_count: int = 0

    def to_dict(self) -> Dict[str, float]:
        return {
            "mean": self.mean,
            "max": self.max,
            "min": self.min,
            "std_dev": self.std_dev,
            "start_velocity": self.start_velocity,
            "end_velocity": self.end_velocity,
            "peak_count": self.peak_count,
        }


@dataclass
class VelocityProfileSignature:
    """Signature characterizing a velocity profile shape."""
    stats: VelocityStats = field(default_factory=VelocityStats)
    acceleration_phase_ratio: float = 0.0
    deceleration_phase_ratio: float = 0.0
    cruise_phase_ratio: float = 0.0
    smoothness: float = 0.0
    profile_type: str = "unknown"


def compute_velocities(
    points: List[Point2D],
    dt_ms: float,
) -> VelocityProfile:
    """
    Compute velocity magnitude at each point.

    Args:
        points: Trajectory points.
        dt_ms: Time between points in milliseconds.

    Returns:
        List of velocity magnitudes.
    """
    if len(points) < 2 or dt_ms <= 0:
        return []
    dt_s = dt_ms / 1000.0
    velocities: List[float] = []
    for i in range(1, len(points)):
        dx = points[i][0] - points[i - 1][0]
        dy = points[i][1] - points[i - 1][1]
        velocities.append(math.sqrt(dx * dx + dy * dy) / dt_s)
    return velocities


def compute_velocity_stats(velocities: VelocityProfile) -> VelocityStats:
    """
    Compute statistics for a velocity profile.

    Args:
        velocities: List of velocity values.

    Returns:
        VelocityStats summary.
    """
    if not velocities:
        return VelocityStats()

    mean = sum(velocities) / len(velocities)
    variance = sum((v - mean) ** 2 for v in velocities) / len(velocities)

    # Count peaks (local maxima)
    peaks = 0
    for i in range(1, len(velocities) - 1):
        if velocities[i] > velocities[i - 1] and velocities[i] > velocities[i + 1]:
            peaks += 1

    return VelocityStats(
        mean=mean,
        max=max(velocities),
        min=min(velocities),
        std_dev=math.sqrt(variance),
        start_velocity=velocities[0],
        end_velocity=velocities[-1],
        peak_count=peaks,
    )


def classify_velocity_profile(velocities: VelocityProfile) -> str:
    """
    Classify the shape/type of a velocity profile.

    Args:
        velocities: List of velocity values.

    Returns:
        Profile type string.
    """
    if len(velocities) < 3:
        return "insufficient_data"

    stats = compute_velocity_stats(velocities)
    if stats.std_dev / max(stats.mean, 1.0) < 0.1:
        return "constant"
    if stats.peak_count > len(velocities) // 3:
        return "fluctuating"

    # Check acceleration/deceleration pattern
    first_third = velocities[:len(velocities) // 3]
    last_third = velocities[-len(velocities) // 3:]
    mid_velocities = velocities[len(velocities) // 3:2 * len(velocities) // 3]

    first_avg = sum(first_third) / len(first_third)
    last_avg = sum(last_third) / len(last_third)
    mid_avg = sum(mid_velocities) / len(mid_velocities) if mid_velocities else 0

    if first_avg < mid_avg and last_avg < mid_avg:
        return "bell_shaped"
    if first_avg < last_avg:
        return "accelerating"
    if first_avg > last_avg:
        return "decelerating"
    return "mixed"


def compute_smoothness(velocities: VelocityProfile) -> float:
    """
    Compute velocity profile smoothness (0-1, higher = smoother).

    Args:
        velocities: List of velocity values.

    Returns:
        Smoothness score.
    """
    if len(velocities) < 3:
        return 1.0

    # Second derivative (jerk) magnitude
    jerks: List[float] = []
    for i in range(1, len(velocities) - 1):
        jerk = abs(velocities[i + 1] - 2 * velocities[i] + velocities[i - 1])
        jerks.append(jerk)

    if not jerks:
        return 1.0

    avg_jerk = sum(jerks) / len(jerks)
    # Normalize (assuming max expected jerk ~10000)
    smoothness = max(0.0, 1.0 - avg_jerk / 10000.0)
    return smoothness


def get_velocity_signature(velocities: VelocityProfile) -> VelocityProfileSignature:
    """
    Generate a full signature for a velocity profile.

    Args:
        velocities: List of velocity values.

    Returns:
        VelocityProfileSignature with full characterization.
    """
    if not velocities:
        return VelocityProfileSignature()

    stats = compute_velocity_stats(velocities)
    n = len(velocities)

    # Phase ratios
    accel_count = sum(1 for i in range(1, n) if velocities[i] > velocities[i - 1])
    decel_count = sum(1 for i in range(1, n) if velocities[i] < velocities[i - 1])

    accel_ratio = accel_count / max(n - 1, 1)
    decel_ratio = decel_count / max(n - 1, 1)
    cruise_ratio = 1.0 - accel_ratio - decel_ratio

    smoothness = compute_smoothness(velocities)
    profile_type = classify_velocity_profile(velocities)

    return VelocityProfileSignature(
        stats=stats,
        acceleration_phase_ratio=accel_ratio,
        deceleration_phase_ratio=decel_ratio,
        cruise_phase_ratio=max(0.0, cruise_ratio),
        smoothness=smoothness,
        profile_type=profile_type,
    )


def compare_velocity_profiles(
    profile1: VelocityProfile,
    profile2: VelocityProfile,
    normalize: bool = True,
) -> float:
    """
    Compare two velocity profiles for similarity.

    Args:
        profile1: First velocity profile.
        profile2: Second velocity profile.
        normalize: Whether to normalize profiles before comparison.

    Returns:
        Similarity score 0-1 (1 = identical).
    """
    if not profile1 or not profile2:
        return 0.0

    # Resample to same length
    min_len = min(len(profile1), len(profile2))
    p1 = resample_profile(profile1, min_len)
    p2 = resample_profile(profile2, min_len)

    if normalize:
        p1 = normalize_profile(p1)
        p2 = normalize_profile(p2)

    # Correlation coefficient
    mean1 = sum(p1) / len(p1)
    mean2 = sum(p2) / len(p2)

    cov = sum((p1[i] - mean1) * (p2[i] - mean2) for i in range(len(p1)))
    std1 = math.sqrt(sum((v - mean1) ** 2 for v in p1))
    std2 = math.sqrt(sum((v - mean2) ** 2 for v in p2))

    if std1 < 1e-10 or std2 < 1e-10:
        return 0.0

    correlation = cov / (std1 * std2)
    return max(0.0, min(1.0, (correlation + 1.0) / 2.0))


def resample_profile(profile: VelocityProfile, target_len: int) -> VelocityProfile:
    """Resample a velocity profile to target length."""
    if len(profile) == target_len:
        return profile[:]
    if len(profile) < 2:
        return profile[:]

    indices = [i * (len(profile) - 1) / (target_len - 1) for i in range(target_len)]
    result: VelocityProfile = []
    for idx in indices:
        lo = int(idx)
        hi = min(lo + 1, len(profile) - 1)
        t = idx - lo
        result.append(profile[lo] * (1 - t) + profile[hi] * t)
    return result


def normalize_profile(profile: VelocityProfile) -> VelocityProfile:
    """Normalize a velocity profile to 0-1 range."""
    if not profile:
        return []
    min_v = min(profile)
    max_v = max(profile)
    if abs(max_v - min_v) < 1e-10:
        return [0.5] * len(profile)
    return [(v - min_v) / (max_v - min_v) for v in profile]
