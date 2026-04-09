"""
Pointer trajectory segmentation utilities.

This module segments pointer trajectories into meaningful units such as
gestures, pauses, clicks, and drags.
"""

from __future__ import annotations

import math
from typing import List, Tuple, Callable, Optional, Dict, Any
from dataclasses import dataclass, field
from enum import Enum, auto


# Type aliases
Point2D = Tuple[float, float]
Trajectory = List[Point2D]
SegmentPredicate = Callable[[Trajectory], bool]


class SegmentType(Enum):
    """Semantic type of a trajectory segment."""
    UNKNOWN = auto()
    TAP = auto()
    LONG_PRESS = auto()
    SWIPE = auto()
    DRAG = auto()
    PINCH = auto()
    PAUSE = auto()
    HOVER = auto()


@dataclass
class TrajectorySegment:
    """A contiguous segment of a trajectory."""
    segment_type: SegmentType = SegmentType.UNKNOWN
    points: Trajectory = field(default_factory=list)
    start_index: int = 0
    end_index: int = 0
    confidence: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration_ms(self) -> float:
        """Duration of the segment in milliseconds (metadata must contain dt_ms)."""
        return self.metadata.get("duration_ms", 0.0)

    @property
    def length(self) -> float:
        """Total path length of the segment."""
        if len(self.points) < 2:
            return 0.0
        return sum(
            math.sqrt((p[0] - self.points[i - 1][0]) ** 2 + (p[1] - self.points[i - 1][1]) ** 2)
            for i, p in enumerate(self.points[1:], 1)
        )

    @property
    def displacement(self) -> float:
        """Straight-line distance from start to end."""
        if len(self.points) < 2:
            return 0.0
        dx = self.points[-1][0] - self.points[0][0]
        dy = self.points[-1][1] - self.points[0][1]
        return math.sqrt(dx * dx + dy * dy)


@dataclass
class SegmentationConfig:
    """Configuration for trajectory segmentation."""
    pause_threshold_ms: float = 150.0
    tap_max_duration_ms: float = 200.0
    tap_max_length: float = 10.0
    long_press_min_duration_ms: float = 500.0
    drag_min_length: float = 30.0
    swipe_min_length: float = 50.0
    velocity_change_threshold: float = 0.5


def segment_by_pause(trajectory: Trajectory, dt_ms: float, threshold_ms: float = 150.0) -> List[TrajectorySegment]:
    """
    Split trajectory at pauses where dwell time exceeds threshold.

    Args:
        trajectory: Input trajectory.
        dt_ms: Time delta between points.
        threshold_ms: Pause duration threshold.

    Returns:
        List of trajectory segments split at pauses.
    """
    if len(trajectory) < 2:
        return [TrajectorySegment(points=trajectory, start_index=0, end_index=len(trajectory) - 1)]

    segments: List[TrajectorySegment] = []
    current: Trajectory = [trajectory[0]]
    pause_start = -1

    for i in range(1, len(trajectory)):
        dx = trajectory[i][0] - trajectory[i - 1][0]
        dy = trajectory[i][1] - trajectory[i - 1][1]
        dist = math.sqrt(dx * dx + dy * dy)

        if dist < 1.0:
            if pause_start < 0:
                pause_start = i - 1
        else:
            if pause_start >= 0:
                pause_duration = (i - pause_start) * dt_ms
                if pause_duration >= threshold_ms:
                    # End current segment
                    segments.append(TrajectorySegment(
                        points=current[:],
                        start_index=i - len(current),
                        end_index=i - 1,
                        segment_type=SegmentType.PAUSE if len(current) > 1 else SegmentType.UNKNOWN,
                    ))
                    current = [trajectory[i]]
                    pause_start = -1
                else:
                    pause_start = -1
            current.append(trajectory[i])

    if current:
        segments.append(TrajectorySegment(
            points=current[:],
            start_index=len(trajectory) - len(current),
            end_index=len(trajectory) - 1,
        ))

    return segments


def classify_segment(seg: TrajectorySegment, config: Optional[SegmentationConfig] = None) -> TrajectorySegment:
    """
    Classify the semantic type of a trajectory segment.

    Args:
        seg: The segment to classify.
        config: Segmentation configuration.

    Returns:
        The same segment with updated segment_type and confidence.
    """
    if config is None:
        config = SegmentationConfig()

    seg.segment_type = SegmentType.UNKNOWN
    seg.confidence = 0.0

    length = seg.length
    displacement = seg.displacement
    duration = seg.duration_ms
    point_count = len(seg.points)

    if point_count < 2:
        return seg

    # Tap: short duration, minimal movement
    if duration <= config.tap_max_duration_ms and displacement <= config.tap_max_length:
        seg.segment_type = SegmentType.TAP
        seg.confidence = min(1.0, 1.0 - displacement / config.tap_max_length)
        return seg

    # Long press: stationary with duration
    if duration >= config.long_press_min_duration_ms and length <= config.tap_max_length:
        seg.segment_type = SegmentType.LONG_PRESS
        seg.confidence = min(1.0, duration / (config.long_press_min_duration_ms * 2))
        return seg

    # Drag: moderate length, mostly straight
    if length >= config.drag_min_length:
        straightness = displacement / max(length, 1.0)
        if straightness > 0.8:
            seg.segment_type = SegmentType.DRAG
            seg.confidence = straightness
            return seg

    # Swipe: long movement with direction consistency
    if length >= config.swipe_min_length:
        seg.segment_type = SegmentType.SWIPE
        seg.confidence = min(1.0, displacement / max(length, 1.0))
        return seg

    # Hover: non-stationary but low velocity
    avg_vel = length / max(duration / 1000.0, 0.001)
    if avg_vel < 100.0:
        seg.segment_type = SegmentType.HOVER
        seg.confidence = 0.5

    return seg


def segment_trajectory(
    trajectory: Trajectory,
    dt_ms: float,
    config: Optional[SegmentationConfig] = None,
) -> List[TrajectorySegment]:
    """
    Segment a full trajectory into semantic units.

    Args:
        trajectory: Input pointer trajectory.
        dt_ms: Time between points in milliseconds.
        config: Segmentation configuration.

    Returns:
        List of classified trajectory segments.
    """
    if config is None:
        config = SegmentationConfig()
    if len(trajectory) < 2:
        return [TrajectorySegment(points=trajectory, start_index=0, end_index=max(0, len(trajectory) - 1))]

    # Split by pauses
    raw_segments = segment_by_pause(trajectory, dt_ms, config.pause_threshold_ms)

    # Classify each segment
    classified: List[TrajectorySegment] = []
    for seg in raw_segments:
        if len(seg.points) > 0:
            seg.metadata["duration_ms"] = len(seg.points) * dt_ms
        classify_segment(seg, config)
        classified.append(seg)

    return classified


def merge_consecutive_segments(
    segments: List[TrajectorySegment],
    segment_types: List[SegmentType],
) -> List[TrajectorySegment]:
    """
    Merge consecutive segments of the same type.

    Args:
        segments: List of trajectory segments.
        segment_types: Types that should be merged.

    Returns:
        List with consecutive same-type segments merged.
    """
    if len(segments) < 2:
        return segments[:]

    merged: List[TrajectorySegment] = []
    current: Optional[TrajectorySegment] = None

    for seg in segments:
        if current is None:
            current = seg
            continue

        if current.segment_type in segment_types and seg.segment_type == current.segment_type:
            current.points.extend(seg.points)
            current.end_index = seg.end_index
            current.metadata.update(seg.metadata)
        else:
            merged.append(current)
            current = seg

    if current is not None:
        merged.append(current)

    return merged
