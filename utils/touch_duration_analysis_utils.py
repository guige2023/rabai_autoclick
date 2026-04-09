"""
Touch Duration Analysis Utilities

Analyze touch (tap) durations to distinguish intentional taps from
accidental touches, long presses, and palm contacts.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class TouchDurationProfile:
    """Statistical profile of touch durations."""
    short_tap_threshold_ms: float = 50.0
    long_press_threshold_ms: float = 500.0
    mean_ms: float = 0.0
    std_dev_ms: float = 0.0
    sample_count: int = 0


@dataclass
class TouchDurationClassification:
    """Classification result for a touch duration."""
    duration_ms: float
    category: str  # 'micro', 'short', 'normal', 'long_press', 'held'
    confidence: float  # 0.0 to 1.0


def classify_touch_duration(
    duration_ms: float,
    profile: Optional[TouchDurationProfile] = None,
) -> TouchDurationClassification:
    """
    Classify a touch duration into a meaningful category.

    Args:
        duration_ms: Duration of the touch in milliseconds.
        profile: Optional statistical profile for confidence scoring.

    Returns:
        TouchDurationClassification with category and confidence.
    """
    if duration_ms < 30:
        category = "micro"
        confidence = min(1.0, duration_ms / 30)
    elif duration_ms < 80:
        category = "short"
        confidence = min(1.0, (duration_ms - 30) / 50)
    elif duration_ms < 500:
        category = "normal"
        confidence = 0.9
    elif duration_ms < 2000:
        category = "long_press"
        confidence = min(0.9, (duration_ms - 500) / 1500)
    else:
        category = "held"
        confidence = 1.0

    if profile and profile.sample_count > 5:
        z_score = (duration_ms - profile.mean_ms) / max(profile.std_dev_ms, 1.0)
        if abs(z_score) > 2.5:
            confidence *= 0.5

    return TouchDurationClassification(
        duration_ms=duration_ms,
        category=category,
        confidence=max(0.0, confidence),
    )


def compute_touch_profile(durations_ms: List[float]) -> TouchDurationProfile:
    """Compute a statistical profile from a list of touch durations."""
    if not durations_ms:
        return TouchDurationProfile()

    n = len(durations_ms)
    mean = sum(durations_ms) / n
    variance = sum((d - mean) ** 2 for d in durations_ms) / n
    std_dev = math.sqrt(variance)

    sorted_durations = sorted(durations_ms)
    short_threshold = sorted_durations[max(0, int(n * 0.1))]
    long_threshold = sorted_durations[min(n - 1, int(n * 0.9))]

    return TouchDurationProfile(
        short_tap_threshold_ms=short_threshold,
        long_press_threshold_ms=long_threshold,
        mean_ms=mean,
        std_dev_ms=std_dev,
        sample_count=n,
    )
