"""Animation Comparison Utilities.

Compares UI animations and transitions for visual regression testing.
Supports timeline alignment, frame diffing, and animation curve analysis.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Optional


class TransitionType(Enum):
    """Types of UI transitions."""

    NONE = auto()
    FADE = auto()
    SLIDE = auto()
    SCALE = auto()
    ROTATE = auto()
    RESIZE = auto()
    COLOR_CHANGE = auto()
    CUSTOM = auto()


@dataclass
class AnimationKeyframe:
    """A single keyframe in an animation.

    Attributes:
        timestamp: Time in milliseconds from animation start.
        progress: Normalized progress (0.0 to 1.0).
        x: X position at this keyframe.
        y: Y position at this keyframe.
        width: Width at this keyframe.
        height: Height at this keyframe.
        opacity: Opacity at this keyframe.
        rotation: Rotation in degrees at this keyframe.
        scale: Scale factor at this keyframe.
    """

    timestamp: int
    progress: float
    x: float = 0.0
    y: float = 0.0
    width: float = 0.0
    height: float = 0.0
    opacity: float = 1.0
    rotation: float = 0.0
    scale: float = 1.0

    def interpolate_to(
        self,
        other: "AnimationKeyframe",
        t: float,
    ) -> "AnimationKeyframe":
        """Interpolate to another keyframe.

        Args:
            other: Target keyframe.
            t: Interpolation factor (0.0 to 1.0).

        Returns:
            Interpolated keyframe.
        """
        return AnimationKeyframe(
            timestamp=int(self.timestamp + (other.timestamp - self.timestamp) * t),
            progress=self.progress + (other.progress - self.progress) * t,
            x=self.x + (other.x - self.x) * t,
            y=self.y + (other.y - self.y) * t,
            width=self.width + (other.width - self.width) * t,
            height=self.height + (other.height - self.height) * t,
            opacity=self.opacity + (other.opacity - self.opacity) * t,
            rotation=self.rotation + (other.rotation - self.rotation) * t,
            scale=self.scale + (other.scale - self.scale) * t,
        )


@dataclass
class AnimationMetrics:
    """Metrics for an animation.

    Attributes:
        duration_ms: Total animation duration.
        start_x: Starting X position.
        start_y: Starting Y position.
        end_x: Ending X position.
        end_y: Ending Y position.
        distance: Total movement distance.
        average_velocity: Average movement velocity.
        peak_velocity: Maximum movement velocity.
        total_rotation: Total rotation amount.
        easing_name: Name of easing function used.
    """

    duration_ms: int
    start_x: float = 0.0
    start_y: float = 0.0
    end_x: float = 0.0
    end_y: float = 0.0
    distance: float = 0.0
    average_velocity: float = 0.0
    peak_velocity: float = 0.0
    total_rotation: float = 0.0
    easing_name: str = "linear"


@dataclass
class AnimationComparisonResult:
    """Result of comparing two animations.

    Attributes:
        are_similar: Whether animations are considered similar.
        duration_diff_ms: Difference in duration.
        position_diff: Maximum position difference at any point.
        timing_diff_ms: Maximum timing difference.
        visual_diff: Visual difference score (0.0 to 1.0).
        keyframe_matches: Number of matching keyframes.
    """

    are_similar: bool
    duration_diff_ms: int = 0
    position_diff: float = 0.0
    timing_diff_ms: int = 0
    visual_diff: float = 0.0
    keyframe_matches: int = 0


class EasingFunction:
    """Easing functions for animations.

    Provides standard easing curves for animation comparison.
    """

    @staticmethod
    def linear(t: float) -> float:
        """Linear easing (no acceleration)."""
        return t

    @staticmethod
    def ease_in_quad(t: float) -> float:
        """Quadratic ease-in."""
        return t * t

    @staticmethod
    def ease_out_quad(t: float) -> float:
        """Quadratic ease-out."""
        return t * (2 - t)

    @staticmethod
    def ease_in_out_quad(t: float) -> float:
        """Quadratic ease-in-out."""
        return 2 * t * t if t < 0.5 else -1 + (4 - 2 * t) * t

    @staticmethod
    def ease_in_cubic(t: float) -> float:
        """Cubic ease-in."""
        return t * t * t

    @staticmethod
    def ease_out_cubic(t: float) -> float:
        """Cubic ease-out."""
        return (t - 1) ** 3 + 1

    @staticmethod
    def ease_in_out_cubic(t: float) -> float:
        """Cubic ease-in-out."""
        return 4 * t * t * t if t < 0.5 else 1 - 4 * (t - 1) ** 3

    @staticmethod
    def ease_out_elastic(t: float) -> float:
        """Elastic ease-out with bounce."""
        if t == 0 or t == 1:
            return t
        p = 0.3
        return (2 ** (-10 * t)) * math.sin((t - p / 4) * (2 * math.pi) / p) + 1

    @staticmethod
    def spring(t: float, damping: float = 0.5) -> float:
        """Spring-like easing."""
        return 1 - math.cos(t * math.pi * 2) * math.exp(-t / damping)

    @classmethod
    def get_function(cls, name: str) -> Callable[[float], float]:
        """Get easing function by name.

        Args:
            name: Name of easing function.

        Returns:
            Easing function.
        """
        functions = {
            "linear": cls.linear,
            "ease_in_quad": cls.ease_in_quad,
            "ease_out_quad": cls.ease_out_quad,
            "ease_in_out_quad": cls.ease_in_out_quad,
            "ease_in_cubic": cls.ease_in_cubic,
            "ease_out_cubic": cls.ease_out_cubic,
            "ease_in_out_cubic": cls.ease_in_out_cubic,
            "ease_out_elastic": cls.ease_out_elastic,
            "spring": cls.spring,
        }
        return functions.get(name.lower(), cls.linear)


class AnimationExtractor:
    """Extracts animation data from UI state sequences.

    Example:
        extractor = AnimationExtractor()
        frames = extractor.extract_frames(before_state, after_state, fps=30)
    """

    def __init__(self, fps: int = 30):
        """Initialize extractor.

        Args:
            fps: Frames per second for extracted animations.
        """
        self.fps = fps

    def extract_keyframes(
        self,
        frames: list[dict],
        duration_ms: int,
    ) -> list[AnimationKeyframe]:
        """Extract keyframes from a sequence of frames.

        Args:
            frames: List of frame states with position/size data.
            duration_ms: Total duration in milliseconds.

        Returns:
            List of AnimationKeyframes.
        """
        if not frames:
            return []

        keyframes = []
        num_frames = len(frames)

        for i, frame in enumerate(frames):
            progress = i / max(1, num_frames - 1)
            timestamp = int(progress * duration_ms)

            keyframe = AnimationKeyframe(
                timestamp=timestamp,
                progress=progress,
                x=frame.get("x", 0.0),
                y=frame.get("y", 0.0),
                width=frame.get("width", 0.0),
                height=frame.get("height", 0.0),
                opacity=frame.get("opacity", 1.0),
                rotation=frame.get("rotation", 0.0),
                scale=frame.get("scale", 1.0),
            )
            keyframes.append(keyframe)

        return keyframes

    def compute_metrics(
        self,
        keyframes: list[AnimationKeyframe],
    ) -> AnimationMetrics:
        """Compute animation metrics from keyframes.

        Args:
            keyframes: List of animation keyframes.

        Returns:
            AnimationMetrics for the animation.
        """
        if not keyframes:
            return AnimationMetrics(duration_ms=0)

        first = keyframes[0]
        last = keyframes[-1]
        duration_ms = last.timestamp

        # Calculate movement distance
        distance = 0.0
        prev_x, prev_y = first.x, first.y
        for kf in keyframes[1:]:
            dx = kf.x - prev_x
            dy = kf.y - prev_y
            distance += math.sqrt(dx * dx + dy * dy)
            prev_x, prev_y = kf.x, kf.y

        # Calculate velocity
        duration_s = duration_ms / 1000.0
        avg_velocity = distance / duration_s if duration_s > 0 else 0.0

        # Find peak velocity
        peak_velocity = 0.0
        for i in range(1, len(keyframes)):
            kf1 = keyframes[i - 1]
            kf2 = keyframes[i]
            dt = (kf2.timestamp - kf1.timestamp) / 1000.0
            if dt > 0:
                dx = kf2.x - kf1.x
                dy = kf2.y - kf1.y
                dist = math.sqrt(dx * dx + dy * dy)
                vel = dist / dt
                peak_velocity = max(peak_velocity, vel)

        # Calculate total rotation
        total_rotation = abs(last.rotation - first.rotation)

        return AnimationMetrics(
            duration_ms=duration_ms,
            start_x=first.x,
            start_y=first.y,
            end_x=last.x,
            end_y=last.y,
            distance=distance,
            average_velocity=avg_velocity,
            peak_velocity=peak_velocity,
            total_rotation=total_rotation,
        )


class AnimationComparator:
    """Compares two animations for similarity.

    Example:
        comparator = AnimationComparator()
        result = comparator.compare(anim1, anim2, threshold=0.1)
    """

    def __init__(self):
        """Initialize the comparator."""
        self.extractor = AnimationExtractor()

    def compare(
        self,
        frames1: list[dict],
        frames2: list[dict],
        duration1_ms: int,
        duration2_ms: int,
        threshold: float = 0.1,
    ) -> AnimationComparisonResult:
        """Compare two animations.

        Args:
            frames1: First animation frames.
            frames2: Second animation frames.
            duration1_ms: First animation duration.
            duration2_ms: Second animation duration.
            threshold: Similarity threshold (0.0 to 1.0).

        Returns:
            AnimationComparisonResult with differences.
        """
        keyframes1 = self.extractor.extract_keyframes(frames1, duration1_ms)
        keyframes2 = self.extractor.extract_keyframes(frames2, duration2_ms)

        # Compare duration
        duration_diff = abs(duration1_ms - duration2_ms)

        # Compare positions at normalized time points
        max_pos_diff = 0.0
        max_timing_diff = 0
        matches = 0

        for t in [0.0, 0.25, 0.5, 0.75, 1.0]:
            kf1 = self._get_keyframe_at_progress(keyframes1, t)
            kf2 = self._get_keyframe_at_progress(keyframes2, t)

            if kf1 and kf2:
                pos_diff = math.sqrt(
                    (kf1.x - kf2.x) ** 2 + (kf1.y - kf2.y) ** 2
                )
                max_pos_diff = max(max_pos_diff, pos_diff)

                timing_diff = abs(kf1.timestamp - kf2.timestamp)
                max_timing_diff = max(max_timing_diff, timing_diff)

                if pos_diff < 5.0:  # Within 5 pixels
                    matches += 1

        # Calculate visual difference score
        metrics1 = self.extractor.compute_metrics(keyframes1)
        metrics2 = self.extractor.compute_metrics(keyframes2)

        duration_score = 1.0 - min(1.0, duration_diff / max(duration1_ms, duration2_ms, 1))
        distance_score = 1.0 - min(1.0, abs(metrics1.distance - metrics2.distance) / max(metrics1.distance, metrics2.distance, 1))
        visual_diff = 1.0 - (duration_score + distance_score) / 2

        are_similar = (
            max_pos_diff < 10.0
            and duration_diff < max(duration1_ms, duration2_ms, 1) * 0.1
            and visual_diff < threshold
        )

        return AnimationComparisonResult(
            are_similar=are_similar,
            duration_diff_ms=duration_diff,
            position_diff=max_pos_diff,
            timing_diff_ms=max_timing_diff,
            visual_diff=visual_diff,
            keyframe_matches=matches,
        )

    def _get_keyframe_at_progress(
        self,
        keyframes: list[AnimationKeyframe],
        progress: float,
    ) -> Optional[AnimationKeyframe]:
        """Get keyframe at a specific progress point.

        Args:
            keyframes: List of keyframes.
            progress: Progress value (0.0 to 1.0).

        Returns:
            Interpolated keyframe at the progress point.
        """
        if not keyframes:
            return None
        if len(keyframes) == 1:
            return keyframes[0]

        target_time = int(progress * keyframes[-1].timestamp)
        prev_kf = keyframes[0]
        next_kf = keyframes[-1]

        for kf in keyframes:
            if kf.timestamp <= target_time:
                prev_kf = kf
            if kf.timestamp >= target_time:
                next_kf = kf
                break

        if prev_kf.timestamp == next_kf.timestamp:
            return prev_kf

        t = (target_time - prev_kf.timestamp) / (next_kf.timestamp - prev_kf.timestamp)
        return prev_kf.interpolate_to(next_kf, t)
