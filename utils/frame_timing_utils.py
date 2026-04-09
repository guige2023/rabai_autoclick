"""
Frame Timing Utilities for Animation Testing.

This module provides utilities for measuring and validating
frame timing, frame rate, and animation smoothness.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Callable
from enum import Enum
import statistics


class TimingQuality(Enum):
    """Quality assessment for frame timing."""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    BAD = "bad"


@dataclass
class FrameTiming:
    """Timing data for a single frame."""
    frame_index: int
    timestamp: float
    duration: float
    dropped: bool = False


@dataclass
class FrameTimingStats:
    """Statistics for frame timing."""
    total_frames: int
    dropped_frames: int
    avg_frame_time: float
    min_frame_time: float
    max_frame_time: float
    std_dev: float
    target_fps: float
    actual_fps: float
    quality: TimingQuality
    percentiles: Dict[str, float] = field(default_factory=dict)


class FrameTimer:
    """
    Measure and analyze frame timing.
    """

    def __init__(self, target_fps: float = 60.0):
        """
        Initialize frame timer.

        Args:
            target_fps: Target frames per second
        """
        self.target_fps = target_fps
        self.target_frame_time = 1.0 / target_fps
        self._frames: List[FrameTiming] = []
        self._last_timestamp: Optional[float] = None
        self._frame_index: int = 0

    def begin_frame(self) -> float:
        """
        Begin timing a frame.

        Returns:
            Current timestamp
        """
        return time.perf_counter()

    def end_frame(self, timestamp: float, dropped_threshold: float = 2.0) -> FrameTiming:
        """
        End timing a frame.

        Args:
            timestamp: Frame start timestamp
            dropped_threshold: Multiplier for dropped frame detection

        Returns:
            FrameTiming data
        """
        end_time = time.perf_counter()
        duration = end_time - timestamp

        dropped = duration > (self.target_frame_time * dropped_threshold)

        timing = FrameTiming(
            frame_index=self._frame_index,
            timestamp=timestamp,
            duration=duration,
            dropped=dropped
        )

        self._frames.append(timing)
        self._frame_index += 1
        self._last_timestamp = end_time

        return timing

    def get_stats(self) -> FrameTimingStats:
        """
        Calculate frame timing statistics.

        Returns:
            FrameTimingStats
        """
        if not self._frames:
            return FrameTimingStats(
                total_frames=0,
                dropped_frames=0,
                avg_frame_time=0.0,
                min_frame_time=0.0,
                max_frame_time=0.0,
                std_dev=0.0,
                target_fps=self.target_fps,
                actual_fps=0.0,
                quality=TimingQuality.POOR
            )

        durations = [f.duration for f in self._frames]
        dropped_count = sum(1 for f in self._frames if f.dropped)

        avg_time = statistics.mean(durations)
        min_time = min(durations)
        max_time = max(durations)
        std = statistics.stdev(durations) if len(durations) > 1 else 0.0
        actual_fps = 1.0 / avg_time if avg_time > 0 else 0.0

        sorted_durations = sorted(durations)
        p50 = sorted_durations[len(sorted_durations) // 2]
        p90 = sorted_durations[int(len(sorted_durations) * 0.9)]
        p99 = sorted_durations[int(len(sorted_durations) * 0.99)]

        quality = self._assess_quality(avg_time, std, dropped_count)

        return FrameTimingStats(
            total_frames=len(self._frames),
            dropped_frames=dropped_count,
            avg_frame_time=avg_time,
            min_frame_time=min_time,
            max_frame_time=max_time,
            std_dev=std,
            target_fps=self.target_fps,
            actual_fps=actual_fps,
            quality=quality,
            percentiles={"p50": p50, "p90": p90, "p99": p99}
        )

    def _assess_quality(
        self,
        avg_time: float,
        std: float,
        dropped_count: int
    ) -> TimingQuality:
        """Assess timing quality."""
        drop_rate = dropped_count / len(self._frames) if self._frames else 0.0

        if std > self.target_frame_time * 0.5:
            return TimingQuality.POOR
        if drop_rate > 0.1:
            return TimingQuality.BAD
        if std > self.target_frame_time * 0.25:
            return TimingQuality.FAIR
        if drop_rate > 0.05:
            return TimingQuality.FAIR
        if abs(avg_time - self.target_frame_time) < self.target_frame_time * 0.1:
            return TimingQuality.EXCELLENT
        return TimingQuality.GOOD

    def reset(self) -> None:
        """Reset frame timing data."""
        self._frames.clear()
        self._last_timestamp = None
        self._frame_index = 0

    @property
    def frames(self) -> List[FrameTiming]:
        """Get recorded frames."""
        return self._frames.copy()


class FrameRateMonitor:
    """
    Monitor frame rate over time with callbacks.
    """

    def __init__(self, window_size: int = 60):
        """
        Initialize frame rate monitor.

        Args:
            window_size: Rolling window size for FPS calculation
        """
        self.window_size = window_size
        self._timestamps: List[float] = []
        self._callbacks: List[Callable[[float], None]] = []

    def record_frame(self, timestamp: Optional[float] = None) -> float:
        """
        Record a frame and calculate current FPS.

        Args:
            timestamp: Frame timestamp (default: now)

        Returns:
            Current FPS
        """
        timestamp = timestamp or time.perf_counter()
        self._timestamps.append(timestamp)

        if len(self._timestamps) > self.window_size:
            self._timestamps = self._timestamps[-self.window_size:]

        return self.calculate_fps()

    def calculate_fps(self) -> float:
        """
        Calculate current FPS from rolling window.

        Returns:
            FPS value
        """
        if len(self._timestamps) < 2:
            return 0.0

        elapsed = self._timestamps[-1] - self._timestamps[0]
        if elapsed <= 0:
            return 0.0

        return (len(self._timestamps) - 1) / elapsed

    def on_low_fps(self, threshold: float, callback: Callable[[float], None]) -> None:
        """Register callback for low FPS events."""
        self._callbacks.append(lambda fps: callback(fps) if fps < threshold else None)


def validate_animation_timing(
    frames: List[FrameTiming],
    target_fps: float,
    tolerance: float = 0.1
) -> Dict[str, Any]:
    """
    Validate animation timing against target FPS.

    Args:
        frames: List of frame timings
        target_fps: Target frames per second
        tolerance: Tolerance for FPS deviation

    Returns:
        Validation results dictionary
    """
    if not frames:
        return {"valid": False, "reason": "no_frames"}

    target_frame_time = 1.0 / target_fps
    tolerance_ms = target_frame_time * tolerance * 1000

    valid_frames = []
    invalid_frames = []

    for frame in frames:
        deviation = abs(frame.duration - target_frame_time)
        if deviation <= tolerance_ms / 1000:
            valid_frames.append(frame)
        else:
            invalid_frames.append(frame)

    valid_ratio = len(valid_frames) / len(frames)

    return {
        "valid": valid_ratio >= (1.0 - tolerance),
        "valid_ratio": valid_ratio,
        "valid_count": len(valid_frames),
        "invalid_count": len(invalid_frames),
        "target_fps": target_fps,
        "tolerance": tolerance
    }
