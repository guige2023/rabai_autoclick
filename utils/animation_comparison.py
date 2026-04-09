"""
Animation Comparison Module.

Provides utilities for comparing animations, detecting animation differences,
and scoring animation similarity for testing and validation purposes.
"""

from __future__ import annotations

import math
import logging
from dataclasses import dataclass, field
from typing import Any


logger = logging.getLogger(__name__)


@dataclass
class KeyFrame:
    """Represents a single keyframe in an animation."""
    time: float
    x: float
    y: float
    scale: float = 1.0
    rotation: float = 0.0
    opacity: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class AnimationSequence:
    """A sequence of keyframes representing an animation."""
    name: str
    duration: float
    keyframes: list[KeyFrame] = field(default_factory=list)

    def get_frame_at(self, time: float) -> KeyFrame | None:
        """
        Get interpolated frame at a specific time.

        Args:
            time: Time in seconds.

        Returns:
            Interpolated KeyFrame at the given time.
        """
        if not self.keyframes:
            return None

        if time <= self.keyframes[0].time:
            return self.keyframes[0]

        if time >= self.keyframes[-1].time:
            return self.keyframes[-1]

        for i in range(len(self.keyframes) - 1):
            kf1 = self.keyframes[i]
            kf2 = self.keyframes[i + 1]

            if kf1.time <= time <= kf2.time:
                t = (time - kf1.time) / (kf2.time - kf1.time)
                return self._interpolate(kf1, kf2, t)

        return None

    def _interpolate(
        self,
        kf1: KeyFrame,
        kf2: KeyFrame,
        t: float
    ) -> KeyFrame:
        """Interpolate between two keyframes."""
        return KeyFrame(
            time=kf1.time + t * (kf2.time - kf1.time),
            x=kf1.x + t * (kf2.x - kf1.x),
            y=kf1.y + t * (kf2.y - kf1.y),
            scale=kf1.scale + t * (kf2.scale - kf1.scale),
            rotation=kf1.rotation + t * (kf2.rotation - kf1.rotation),
            opacity=kf1.opacity + t * (kf2.opacity - kf1.opacity)
        )


@dataclass
class ComparisonResult:
    """Result of comparing two animations."""
    is_similar: bool
    similarity_score: float
    max_position_delta: float
    max_scale_delta: float
    max_rotation_delta: float
    max_opacity_delta: float
    duration_delta: float
    frame_differences: list[float] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


class AnimationComparator:
    """
    Compares two animations and produces similarity metrics.

    Example:
        >>> comparator = AnimationComparator()
        >>> result = comparator.compare(anim1, anim2)
        >>> if result.similarity_score > 0.9:
        ...     print("Animations are similar")
    """

    def __init__(
        self,
        position_threshold: float = 5.0,
        scale_threshold: float = 0.05,
        rotation_threshold: float = 5.0,
        opacity_threshold: float = 0.05
    ) -> None:
        """
        Initialize the comparator.

        Args:
            position_threshold: Max position difference for match (pixels).
            scale_threshold: Max scale difference for match.
            rotation_threshold: Max rotation difference for match (degrees).
            opacity_threshold: Max opacity difference for match.
        """
        self.position_threshold = position_threshold
        self.scale_threshold = scale_threshold
        self.rotation_threshold = rotation_threshold
        self.opacity_threshold = opacity_threshold

    def compare(
        self,
        expected: AnimationSequence,
        actual: AnimationSequence,
        sample_count: int = 100
    ) -> ComparisonResult:
        """
        Compare two animations.

        Args:
            expected: Expected animation sequence.
            actual: Actual animation sequence.
            sample_count: Number of sample points to compare.

        Returns:
            ComparisonResult with similarity metrics.
        """
        min_duration = min(expected.duration, actual.duration)
        max_duration = max(expected.duration, actual.duration)

        frame_differences: list[float] = []
        max_pos_delta = 0.0
        max_scale_delta = 0.0
        max_rot_delta = 0.0
        max_opacity_delta = 0.0

        for i in range(sample_count):
            time = (i / sample_count) * min_duration

            expected_frame = expected.get_frame_at(time)
            actual_frame = actual.get_frame_at(time)

            if expected_frame and actual_frame:
                pos_delta = math.sqrt(
                    (expected_frame.x - actual_frame.x) ** 2 +
                    (expected_frame.y - actual_frame.y) ** 2
                )
                scale_delta = abs(expected_frame.scale - actual_frame.scale)
                rot_delta = abs(expected_frame.rotation - actual_frame.rotation)
                opacity_delta = abs(expected_frame.opacity - actual_frame.opacity)

                frame_diff = self._compute_frame_diff(
                    pos_delta, scale_delta, rot_delta, opacity_delta
                )
                frame_differences.append(frame_diff)

                max_pos_delta = max(max_pos_delta, pos_delta)
                max_scale_delta = max(max_scale_delta, scale_delta)
                max_rot_delta = max(max_rot_delta, rot_delta)
                max_opacity_delta = max(max_opacity_delta, opacity_delta)

        avg_diff = sum(frame_differences) / len(frame_differences) if frame_differences else 0.0
        similarity = max(0.0, 1.0 - avg_diff)

        is_similar = (
            max_pos_delta <= self.position_threshold and
            max_scale_delta <= self.scale_threshold and
            max_rot_delta <= self.rotation_threshold and
            max_opacity_delta <= self.opacity_threshold
        )

        return ComparisonResult(
            is_similar=is_similar,
            similarity_score=similarity,
            max_position_delta=max_pos_delta,
            max_scale_delta=max_scale_delta,
            max_rotation_delta=max_rot_delta,
            max_opacity_delta=max_opacity_delta,
            duration_delta=max_duration - min_duration,
            frame_differences=frame_differences,
            metadata={
                "expected_name": expected.name,
                "actual_name": actual.name,
                "sample_count": sample_count
            }
        )

    def _compute_frame_diff(
        self,
        pos_delta: float,
        scale_delta: float,
        rot_delta: float,
        opacity_delta: float
    ) -> float:
        """Compute weighted difference for a single frame."""
        pos_weight = 1.0 / (self.position_threshold + 1e-6)
        scale_weight = 1.0 / (self.scale_threshold + 1e-6)
        rot_weight = 1.0 / (self.rotation_threshold + 1e-6)
        opacity_weight = 1.0 / (self.opacity_threshold + 1e-6)

        total_weight = pos_weight + scale_weight + rot_weight + opacity_weight

        return (
            pos_delta * pos_weight +
            scale_delta * scale_weight +
            rot_delta * rot_weight +
            opacity_delta * opacity_weight
        ) / total_weight


class AnimationDiffCalculator:
    """
    Calculates detailed differences between animations.
    """

    def __init__(self) -> None:
        """Initialize the diff calculator."""
        self._differences: list[dict[str, Any]] = []

    def calculate_diff(
        self,
        expected: AnimationSequence,
        actual: AnimationSequence
    ) -> list[dict[str, Any]]:
        """
        Calculate detailed frame-by-frame differences.

        Args:
            expected: Expected animation.
            actual: Actual animation.

        Returns:
            List of difference records for each frame.
        """
        self._differences.clear()

        times = set()
        for kf in expected.keyframes:
            times.add(kf.time)
        for kf in actual.keyframes:
            times.add(kf.time)

        for time in sorted(times):
            exp_frame = expected.get_frame_at(time)
            act_frame = actual.get_frame_at(time)

            if exp_frame and act_frame:
                diff = {
                    "time": time,
                    "position": {
                        "expected": (exp_frame.x, exp_frame.y),
                        "actual": (act_frame.x, act_frame.y),
                        "delta": math.sqrt(
                            (exp_frame.x - act_frame.x) ** 2 +
                            (exp_frame.y - act_frame.y) ** 2
                        )
                    },
                    "scale": {
                        "expected": exp_frame.scale,
                        "actual": act_frame.scale,
                        "delta": abs(exp_frame.scale - act_frame.scale)
                    },
                    "rotation": {
                        "expected": exp_frame.rotation,
                        "actual": act_frame.rotation,
                        "delta": abs(exp_frame.rotation - act_frame.rotation)
                    },
                    "opacity": {
                        "expected": exp_frame.opacity,
                        "actual": act_frame.opacity,
                        "delta": abs(exp_frame.opacity - act_frame.opacity)
                    }
                }
                self._differences.append(diff)

        return self._differences

    def get_summary(self) -> dict[str, Any]:
        """
        Get a summary of calculated differences.

        Returns:
            Summary statistics.
        """
        if not self._differences:
            return {}

        pos_deltas = [d["position"]["delta"] for d in self._differences]
        scale_deltas = [d["scale"]["delta"] for d in self._differences]
        rot_deltas = [d["rotation"]["delta"] for d in self._differences]
        opacity_deltas = [d["opacity"]["delta"] for d in self._differences]

        return {
            "total_frames": len(self._differences),
            "position": {
                "max": max(pos_deltas),
                "avg": sum(pos_deltas) / len(pos_deltas)
            },
            "scale": {
                "max": max(scale_deltas),
                "avg": sum(scale_deltas) / len(scale_deltas)
            },
            "rotation": {
                "max": max(rot_deltas),
                "avg": sum(rot_deltas) / len(rot_deltas)
            },
            "opacity": {
                "max": max(opacity_deltas),
                "avg": sum(opacity_deltas) / len(opacity_deltas)
            }
        }


@dataclass
class AnimationMetrics:
    """Metrics for an animation."""
    name: str
    duration: float
    frame_count: int
    avg_velocity: float
    avg_acceleration: float
    complexity_score: float


class AnimationAnalyzer:
    """
    Analyzes animation characteristics and produces metrics.
    """

    def __init__(self) -> None:
        """Initialize the analyzer."""
        pass

    def analyze(self, animation: AnimationSequence) -> AnimationMetrics:
        """
        Analyze an animation and produce metrics.

        Args:
            animation: Animation to analyze.

        Returns:
            AnimationMetrics with computed statistics.
        """
        velocities = self._calculate_velocities(animation)
        accelerations = self._calculate_accelerations(velocities)

        avg_velocity = sum(velocities) / len(velocities) if velocities else 0.0
        avg_accel = sum(accelerations) / len(accelerations) if accelerations else 0.0
        complexity = self._calculate_complexity(animation)

        return AnimationMetrics(
            name=animation.name,
            duration=animation.duration,
            frame_count=len(animation.keyframes),
            avg_velocity=avg_velocity,
            avg_acceleration=avg_accel,
            complexity_score=complexity
        )

    def _calculate_velocities(
        self,
        animation: AnimationSequence
    ) -> list[float]:
        """Calculate velocities between keyframes."""
        velocities = []

        for i in range(len(animation.keyframes) - 1):
            kf1 = animation.keyframes[i]
            kf2 = animation.keyframes[i + 1]

            dt = kf2.time - kf1.time
            if dt > 0:
                dx = kf2.x - kf1.x
                dy = kf2.y - kf1.y
                distance = math.sqrt(dx * dx + dy * dy)
                velocity = distance / dt
                velocities.append(velocity)

        return velocities

    def _calculate_accelerations(
        self,
        velocities: list[float]
    ) -> list[float]:
        """Calculate accelerations from velocities."""
        accelerations = []

        for i in range(len(velocities) - 1):
            accel = abs(velocities[i + 1] - velocities[i])
            accelerations.append(accel)

        return accelerations

    def _calculate_complexity(self, animation: AnimationSequence) -> float:
        """Calculate a complexity score for the animation."""
        if len(animation.keyframes) < 2:
            return 0.0

        direction_changes = 0
        prev_dx = 0.0
        prev_dy = 0.0

        for i in range(len(animation.keyframes) - 1):
            kf1 = animation.keyframes[i]
            kf2 = animation.keyframes[i + 1]

            dx = kf2.x - kf1.x
            dy = kf2.y - kf1.y

            if (prev_dx * dx < 0) or (prev_dy * dy < 0):
                direction_changes += 1

            prev_dx = dx
            prev_dy = dy

        scale_changes = sum(
            1 for i in range(len(animation.keyframes) - 1)
            if abs(animation.keyframes[i + 1].scale - animation.keyframes[i].scale) > 0.01
        )

        rotation_changes = sum(
            1 for i in range(len(animation.keyframes) - 1)
            if abs(animation.keyframes[i + 1].rotation - animation.keyframes[i].rotation) > 1.0
        )

        complexity = (
            direction_changes * 1.0 +
            scale_changes * 0.5 +
            rotation_changes * 0.3 +
            len(animation.keyframes) * 0.1
        )

        return min(complexity, 100.0)
