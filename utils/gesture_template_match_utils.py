"""
Gesture template matching utilities.

This module provides utilities for matching input gestures against
predefined templates using various similarity metrics.
"""

from __future__ import annotations

import math
from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass, field


# Type aliases
Point2D = Tuple[float, float]
Trajectory = List[Point2D]
GestureTemplate = Dict[str, Any]


@dataclass
class TemplateMatch:
    """Result of matching a gesture against a template."""
    template_id: str
    similarity: float
    matched_points: int
    total_points: int
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def match_percentage(self) -> float:
        """Percentage of points matched."""
        if self.total_points == 0:
            return 0.0
        return self.matched_points / self.total_points


@dataclass
class TemplateMatchingConfig:
    """Configuration for template matching."""
    distance_threshold: float = 15.0
    min_match_ratio: float = 0.7
    normalize_trajectories: bool = True
    use_rotation_invariance: bool = False


def normalize_trajectory(trajectory: Trajectory, target_size: int = 64) -> Trajectory:
    """
    Normalize a trajectory to a standard size and position.

    Args:
        trajectory: Input trajectory points.
        target_size: Target number of points.

    Returns:
        Normalized trajectory.
    """
    if len(trajectory) == 0:
        return []
    if len(trajectory) == target_size:
        return trajectory[:]

    # Translate to origin
    min_x = min(p[0] for p in trajectory)
    min_y = min(p[1] for p in trajectory)
    translated = [(p[0] - min_x, p[1] - min_y) for p in trajectory]

    # Scale to unit square
    max_x = max(p[0] for p in translated)
    max_y = max(p[1] for p in translated)
    scale = max(max_x, max_y, 1.0)
    scaled = [(p[0] / scale, p[1] / scale) for p in translated]

    # Resample to target size
    path_length = sum(
        math.sqrt((scaled[i][0] - scaled[i - 1][0]) ** 2 + (scaled[i][1] - scaled[i - 1][1]) ** 2)
        for i in range(1, len(scaled))
    )

    if path_length < 1e-10:
        return [(0.0, 0.0)] * target_size

    result: List[Point2D] = []
    step = path_length / (target_size - 1)
    accumulated = 0.0
    result.append(scaled[0])

    for i in range(1, len(scaled)):
        seg_len = math.sqrt((scaled[i][0] - scaled[i - 1][0]) ** 2 + (scaled[i][1] - scaled[i - 1][1]) ** 2)
        while accumulated + seg_len >= step and len(result) < target_size:
            t = (step - accumulated) / seg_len
            x = scaled[i - 1][0] + t * (scaled[i][0] - scaled[i - 1][0])
            y = scaled[i - 1][1] + t * (scaled[i][1] - scaled[i - 1][1])
            result.append((x, y))
            accumulated = 0.0
        accumulated += seg_len

    while len(result) < target_size:
        result.append(result[-1] if result else (0.0, 0.0))

    return result[:target_size]


def point_to_point_distance(p1: Point2D, p2: Point2D) -> float:
    """Compute Euclidean distance between two points."""
    dx = p1[0] - p2[0]
    dy = p1[1] - p2[1]
    return math.sqrt(dx * dx + dy * dy)


def match_trajectory_to_template(
    trajectory: Trajectory,
    template: Trajectory,
    config: Optional[TemplateMatchingConfig] = None,
) -> float:
    """
    Compute similarity between a trajectory and a template.

    Args:
        trajectory: Input trajectory.
        template: Reference template trajectory.
        config: Matching configuration.

    Returns:
        Similarity score between 0 and 1.
    """
    if config is None:
        config = TemplateMatchingConfig()
    if len(trajectory) < 2 or len(template) < 2:
        return 0.0

    # Normalize if configured
    if config.normalize_trajectories:
        traj = normalize_trajectory(trajectory)
        tpl = normalize_trajectory(template)
    else:
        traj = trajectory
        tpl = template

    # Resample both to same length
    n = max(len(traj), len(tpl))
    traj = resample_to_length(traj, n)
    tpl = resample_to_length(tpl, n)

    # Compute matching
    total_dist = 0.0
    for tp, tt in zip(traj, tpl):
        total_dist += point_to_point_distance(tp, tt)

    avg_dist = total_dist / n
    # Convert distance to similarity (0 distance = 1.0 similarity)
    similarity = max(0.0, 1.0 - avg_dist / config.distance_threshold)

    return similarity


def resample_to_length(trajectory: Trajectory, target_len: int) -> Trajectory:
    """Resample trajectory to target length."""
    if len(trajectory) == target_len:
        return trajectory[:]
    if len(trajectory) < 2:
        return [(trajectory[0] if trajectory else (0.0, 0.0))] * target_len

    # Compute arc lengths
    lengths = [0.0]
    for i in range(1, len(trajectory)):
        d = point_to_point_distance(trajectory[i], trajectory[i - 1])
        lengths.append(lengths[-1] + d)

    total = lengths[-1]
    if total < 1e-10:
        return [trajectory[0]] * target_len

    step = total / (target_len - 1)
    result: List[Point2D] = []
    accumulated = 0.0
    j = 1

    for i in range(target_len):
        target_dist = i * step
        while j < len(lengths) and lengths[j] < target_dist:
            j += 1
        if j >= len(trajectory):
            result.append(trajectory[-1])
            continue
        t = (target_dist - lengths[j - 1]) / max(lengths[j] - lengths[j - 1], 1e-10)
        x = trajectory[j - 1][0] + t * (trajectory[j][0] - trajectory[j - 1][0])
        y = trajectory[j - 1][1] + t * (trajectory[j][1] - trajectory[j - 1][1])
        result.append((x, y))

    return result


def find_best_template_match(
    trajectory: Trajectory,
    templates: Dict[str, Trajectory],
    config: Optional[TemplateMatchingConfig] = None,
) -> TemplateMatch:
    """
    Find the best matching template for a trajectory.

    Args:
        trajectory: Input trajectory.
        templates: Dictionary of template_id -> template points.
        config: Matching configuration.

    Returns:
        Best matching TemplateMatch.
    """
    if config is None:
        config = TemplateMatchingConfig()
    if not templates:
        return TemplateMatch(template_id="", similarity=0.0, matched_points=0, total_points=len(trajectory))

    best: Optional[TemplateMatch] = None
    for tid, tpl in templates.items():
        similarity = match_trajectory_to_template(trajectory, tpl, config)
        if best is None or similarity > best.similarity:
            matched = int(similarity * len(trajectory))
            best = TemplateMatch(
                template_id=tid,
                similarity=similarity,
                matched_points=matched,
                total_points=len(trajectory),
            )

    return best or TemplateMatch(template_id="", similarity=0.0, matched_points=0, total_points=len(trajectory))
