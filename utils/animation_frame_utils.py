"""Animation Frame Utilities.

Utilities for working with animation frames and frame sequences.

Example:
    >>> from animation_frame_utils import FrameSequence
    >>> seq = FrameSequence(fps=30)
    >>> seq.add_frame(image_data, duration=0.1)
    >>> seq.get_frame_at(0.5)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Optional, Tuple


@dataclass
class AnimationFrame:
    """A single animation frame."""
    index: int
    data: Any
    duration: float
    timestamp: float = 0.0


class FrameSequence:
    """A sequence of animation frames."""

    def __init__(self, fps: float = 30.0, loop: bool = False):
        """Initialize frame sequence.

        Args:
            fps: Frames per second.
            loop: Whether sequence loops.
        """
        self.fps = fps
        self.loop = loop
        self.frames: List[AnimationFrame] = []
        self._total_duration = 0.0

    def add_frame(self, data: Any, duration: float = -1.0) -> None:
        """Add a frame to the sequence.

        Args:
            data: Frame image data.
            duration: Frame duration in seconds (-1 for default).
        """
        if duration < 0:
            duration = 1.0 / self.fps

        frame = AnimationFrame(
            index=len(self.frames),
            data=data,
            duration=duration,
            timestamp=self._total_duration,
        )
        self.frames.append(frame)
        self._total_duration += duration

    def get_frame_at(self, time: float) -> Optional[AnimationFrame]:
        """Get frame at given time.

        Args:
            time: Time in seconds.

        Returns:
            AnimationFrame at that time or None.
        """
        if not self.frames:
            return None

        if self.loop:
            time = time % self._total_duration
        elif time >= self._total_duration:
            return self.frames[-1]

        for frame in self.frames:
            if time < frame.timestamp + frame.duration:
                return frame
        return self.frames[-1]

    def get_frame_index(self, index: int) -> Optional[AnimationFrame]:
        """Get frame by index.

        Args:
            index: Frame index.

        Returns:
            AnimationFrame or None.
        """
        if self.loop:
            index = index % len(self.frames)
        if 0 <= index < len(self.frames):
            return self.frames[index]
        return None

    @property
    def total_duration(self) -> float:
        """Get total duration of sequence.

        Returns:
            Duration in seconds.
        """
        return self._total_duration

    @property
    def frame_count(self) -> int:
        """Get number of frames.

        Returns:
            Frame count.
        """
        return len(self.frames)
