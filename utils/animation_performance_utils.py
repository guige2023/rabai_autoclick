"""Animation Performance Utilities.

Measures and monitors animation performance metrics.

Example:
    >>> from animation_performance_utils import AnimationPerformanceMonitor
    >>> monitor = AnimationPerformanceMonitor()
    >>> monitor.start_frame()
    >>> monitor.end_frame()
    >>> print(monitor.get_fps())
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional


@dataclass
class FrameMetrics:
    """Metrics for a single animation frame."""
    frame_number: int
    timestamp: float
    duration_ms: float
    fps: float


class AnimationPerformanceMonitor:
    """Monitors animation performance."""

    def __init__(self, window_size: int = 60):
        """Initialize monitor.

        Args:
            window_size: Number of frames to keep in rolling window.
        """
        self.window_size = window_size
        self._frames: Deque[FrameMetrics] = deque(maxlen=window_size)
        self._last_timestamp: Optional[float] = None
        self._frame_number = 0

    def start_frame(self) -> None:
        """Mark the start of a frame."""
        self._last_timestamp = time.perf_counter()

    def end_frame(self) -> Optional[FrameMetrics]:
        """Mark the end of a frame and record metrics.

        Returns:
            FrameMetrics for this frame, or None if no start timestamp.
        """
        if self._last_timestamp is None:
            return None

        now = time.perf_counter()
        duration_ms = (now - self._last_timestamp) * 1000.0
        self._frame_number += 1

        fps = 1000.0 / duration_ms if duration_ms > 0 else 0.0

        metrics = FrameMetrics(
            frame_number=self._frame_number,
            timestamp=now,
            duration_ms=duration_ms,
            fps=fps,
        )
        self._frames.append(metrics)
        self._last_timestamp = None
        return metrics

    def get_fps(self) -> float:
        """Get current estimated FPS.

        Returns:
            FPS estimate from recent frames.
        """
        if not self._frames:
            return 0.0
        return sum(f.fps for f in self._frames) / len(self._frames)

    def get_avg_frame_time(self) -> float:
        """Get average frame time in milliseconds.

        Returns:
            Average frame duration.
        """
        if not self._frames:
            return 0.0
        return sum(f.duration_ms for f in self._frames) / len(self._frames)

    def get_p95_frame_time(self) -> float:
        """Get 95th percentile frame time in milliseconds.

        Returns:
            P95 frame duration.
        """
        if not self._frames:
            return 0.0
        sorted_durations = sorted(f.duration_ms for f in self._frames)
        idx = int(len(sorted_durations) * 0.95)
        return sorted_durations[min(idx, len(sorted_durations) - 1)]

    def is_smooth(self, threshold: float = 16.67) -> bool:
        """Check if animation is smooth (avg frame time below threshold).

        Args:
            threshold: Max acceptable frame time in ms (default 60fps).

        Returns:
            True if animation is smooth.
        """
        return self.get_avg_frame_time() <= threshold
