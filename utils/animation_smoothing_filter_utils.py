"""
Animation Smoothing Filter Utilities

Smooth animation detections by filtering out transient spikes
and false positives in the animation detection pipeline.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass
class SmoothedAnimationState:
    """Animation state after smoothing."""
    is_animating: bool
    confidence: float  # 0.0 to 1.0
    frame_count: int


class AnimationSmoothingFilter:
    """
    Apply smoothing to raw animation detection signals.

    Uses a rolling window combined with a hysteresis threshold
    to reduce false positive animations triggered by noise.
    """

    def __init__(
        self,
        window_size: int = 5,
        on_threshold: float = 0.6,
        off_threshold: float = 0.3,
        min_on_count: int = 3,
        min_off_count: int = 3,
    ):
        self.window_size = window_size
        self.on_threshold = on_threshold
        self.off_threshold = off_threshold
        self.min_on_count = min_on_count
        self.min_off_count = min_off_count

        self._window: deque[float] = deque(maxlen=window_size)
        self._consecutive_on = 0
        self._consecutive_off = 0
        self._is_animating = False
        self._frame_count = 0

    def update(self, raw_confidence: float) -> SmoothedAnimationState:
        """
        Update the filter with a new raw confidence value.

        Args:
            raw_confidence: Raw animation confidence from the detector (0.0 to 1.0).

        Returns:
            SmoothedAnimationState with debounced animation state.
        """
        self._window.append(raw_confidence)
        self._frame_count += 1

        window_avg = sum(self._window) / len(self._window)

        if self._is_animating:
            if window_avg < self.off_threshold:
                self._consecutive_off += 1
                self._consecutive_on = 0
                if self._consecutive_off >= self.min_off_count:
                    self._is_animating = False
                    self._consecutive_off = 0
            else:
                self._consecutive_off = 0
                self._consecutive_on += 1
        else:
            if window_avg >= self.on_threshold:
                self._consecutive_on += 1
                self._consecutive_off = 0
                if self._consecutive_on >= self.min_on_count:
                    self._is_animating = True
                    self._consecutive_on = 0
            else:
                self._consecutive_off = 0
                self._consecutive_on = 0

        return SmoothedAnimationState(
            is_animating=self._is_animating,
            confidence=window_avg,
            frame_count=self._frame_count,
        )

    def reset(self) -> None:
        """Reset the filter to its initial state."""
        self._window.clear()
        self._consecutive_on = 0
        self._consecutive_off = 0
        self._is_animating = False
        self._frame_count = 0
