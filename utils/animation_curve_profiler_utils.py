"""
Animation Curve Profiler Utilities

Profile and analyze animation curves for performance and quality,
detecting jank, stuttering, and suboptimal easing functions.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import List, Optional, Tuple


@dataclass
class AnimationFrame:
    """A single frame in an animation."""
    timestamp_ms: float
    progress: float  # 0.0 to 1.0
    expected_progress: float  # what progress should be at this time


@dataclass
class AnimationProfile:
    """Profile result for an animation."""
    duration_ms: float
    frame_count: int
    avg_frame_time_ms: float
    max_frame_time_ms: float
    min_frame_time_ms: float
    is_smooth: bool
    jank_count: int  # frames that took > 2x expected time
    deviation_score: float  # how far actual deviates from expected


def compute_expected_progress(
    elapsed_ms: float,
    duration_ms: float,
    curve: str = "ease_in_out",
) -> float:
    """Compute expected progress given elapsed time and easing curve."""
    t = max(0.0, min(1.0, elapsed_ms / duration_ms))

    if curve == "linear":
        return t
    elif curve == "ease_in":
        return t * t
    elif curve == "ease_out":
        return 1 - (1 - t) * (1 - t)
    elif curve == "ease_in_out":
        return 2 * t * t if t < 0.5 else 1 - ((-2 * t + 2) ** 2) / 2
    elif curve == "spring":
        # Approximate spring physics
        omega = 2 * math.pi * 1.5  # frequency
        decay = math.exp(-4 * t)
        return 1 - decay * math.cos(omega * t)
    else:
        return t


class AnimationCurveProfiler:
    """Profile animation curves to detect performance issues."""

    def __init__(
        self,
        jank_threshold_ms: float = 16.67,  # 60fps = 16.67ms per frame
        max_deviation: float = 0.15,  # 15% deviation threshold
    ):
        self.jank_threshold_ms = jank_threshold_ms
        self.max_deviation = max_deviation
        self._profiles: List[AnimationProfile] = []

    def profile_animation(
        self,
        frames: List[AnimationFrame],
        expected_duration_ms: float,
        easing_curve: str = "ease_in_out",
    ) -> AnimationProfile:
        """
        Profile an animation given its frames.

        Args:
            frames: List of AnimationFrames with timestamp and progress.
            expected_duration_ms: Expected animation duration.
            easing_curve: Expected easing curve type.

        Returns:
            AnimationProfile with analysis results.
        """
        if len(frames) < 2:
            return AnimationProfile(
                duration_ms=0.0,
                frame_count=0,
                avg_frame_time_ms=0.0,
                max_frame_time_ms=0.0,
                min_frame_time_ms=0.0,
                is_smooth=False,
                jank_count=0,
                deviation_score=0.0,
            )

        # Compute frame times
        frame_times = [
            frames[i].timestamp_ms - frames[i - 1].timestamp_ms
            for i in range(1, len(frames))
        ]

        # Compute deviation from expected progress
        total_deviation = 0.0
        for frame in frames:
            elapsed = frame.timestamp_ms - frames[0].timestamp_ms
            expected = compute_expected_progress(elapsed, expected_duration_ms, easing_curve)
            total_deviation += abs(frame.progress - expected)

        avg_deviation = total_deviation / len(frames)
        deviation_score = avg_deviation

        # Count janky frames
        jank_count = sum(1 for ft in frame_times if ft > self.jank_threshold_ms * 2)

        duration_ms = frames[-1].timestamp_ms - frames[0].timestamp_ms
        profile = AnimationProfile(
            duration_ms=duration_ms,
            frame_count=len(frames),
            avg_frame_time_ms=sum(frame_times) / len(frame_times) if frame_times else 0.0,
            max_frame_time_ms=max(frame_times) if frame_times else 0.0,
            min_frame_time_ms=min(frame_times) if frame_times else 0.0,
            is_smooth=jank_count == 0 and deviation_score < self.max_deviation,
            jank_count=jank_count,
            deviation_score=deviation_score,
        )

        self._profiles.append(profile)
        return profile
