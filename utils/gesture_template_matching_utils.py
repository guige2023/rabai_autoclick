"""
Gesture Template Matching Utilities for UI Automation.

This module provides utilities for matching touch gestures
against predefined templates in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple, Callable
from enum import Enum


class MatchResult(Enum):
    """Result of gesture template matching."""
    MATCH = "match"
    PARTIAL = "partial"
    NO_MATCH = "no_match"


@dataclass
class GestureTemplate:
    """A predefined gesture template for matching."""
    name: str
    points: List[Tuple[float, float]]
    normalized: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.normalized:
            self.points = self._normalize_points()

    def _normalize_points(self) -> List[Tuple[float, float]]:
        """Normalize template points to unit square."""
        if not self.points:
            return []

        xs = [p[0] for p in self.points]
        ys = [p[1] for p in self.points]

        min_x, max_x = min(xs), max(xs)
        min_y, max_y = min(ys), max(ys)

        dx = max_x - min_x
        dy = max_y - min_y

        if dx < 1e-6 and dy < 1e-6:
            return [(0.5, 0.5) for _ in self.points]

        return [
            ((p[0] - min_x) / dx, (p[1] - min_y) / dy)
            for p in self.points
        ]


@dataclass
class GestureMatch:
    """Result of matching a gesture against a template."""
    template_name: str
    result: MatchResult
    score: float
    matched_indices: List[int]
    total_points: int
    timestamp: float


@dataclass
class MatchingConfig:
    """Configuration for gesture matching."""
    match_threshold: float = 0.8
    resample_count: int = 64
    rotation_invariance: bool = True
    scale_invariance: bool = True
    max_distance: float = 50.0


class GestureTemplateMatcher:
    """Matches touch gestures against predefined templates."""

    def __init__(self, config: Optional[MatchingConfig] = None) -> None:
        self._config = config or MatchingConfig()
        self._templates: Dict[str, GestureTemplate] = {}
        self._current_gesture: List[Tuple[float, float]] = []

    def register_template(self, template: GestureTemplate) -> None:
        """Register a gesture template for matching."""
        self._templates[template.name] = template

    def get_template(self, name: str) -> Optional[GestureTemplate]:
        """Get a registered template by name."""
        return self._templates.get(name)

    def get_template_names(self) -> List[str]:
        """Get all registered template names."""
        return list(self._templates.keys())

    def begin_gesture(self) -> None:
        """Begin recording a new gesture."""
        self._current_gesture.clear()

    def add_point(self, x: float, y: float) -> None:
        """Add a point to the current gesture."""
        self._current_gesture.append((x, y))

    def end_gesture(self) -> List[GestureMatch]:
        """End recording and match against all templates."""
        if not self._current_gesture:
            return []

        gesture_points = self._resample(self._current_gesture)
        matches: List[GestureMatch] = []

        for name, template in self._templates.items():
            score, indices = self._match_gesture(gesture_points, template)
            result = self._determine_result(score)

            matches.append(
                GestureMatch(
                    template_name=name,
                    result=result,
                    score=score,
                    matched_indices=indices,
                    total_points=len(self._current_gesture),
                    timestamp=time.time(),
                )
            )

        matches.sort(key=lambda m: m.score, reverse=True)
        return matches

    def match_single(
        self,
        gesture: List[Tuple[float, float]],
        template_name: str,
    ) -> Optional[GestureMatch]:
        """Match a gesture directly against a specific template."""
        template = self._templates.get(template_name)
        if template is None:
            return None

        gesture_points = self._resample(gesture)
        score, indices = self._match_gesture(gesture_points, template)
        result = self._determine_result(score)

        return GestureMatch(
            template_name=template_name,
            result=result,
            score=score,
            matched_indices=indices,
            total_points=len(gesture),
            timestamp=time.time(),
        )

    def _resample(
        self,
        points: List[Tuple[float, float]],
    ) -> List[Tuple[float, float]]:
        """Resample gesture points to a fixed count."""
        if len(points) < 2:
            return points

        total_length = self._path_length(points)
        step = total_length / (self._config.resample_count - 1)

        resampled = [points[0]]
        accumulated = 0.0
        i = 1

        while i < len(points) and len(resampled) < self._config.resample_count:
            d = math.sqrt(
                (points[i][0] - points[i - 1][0]) ** 2 +
                (points[i][1] - points[i - 1][1]) ** 2
            )

            if accumulated + d >= step:
                t = (step - accumulated) / d if d > 0 else 0
                new_x = points[i - 1][0] + t * (points[i][0] - points[i - 1][0])
                new_y = points[i - 1][1] + t * (points[i][1] - points[i - 1][1])
                resampled.append((new_x, new_y))
                points = points[:i] + [(new_x, new_y)] + points[i:]
                accumulated = 0.0
            else:
                accumulated += d
                i += 1

        while len(resampled) < self._config.resample_count:
            resampled.append(points[-1])

        return resampled[:self._config.resample_count]

    def _path_length(self, points: List[Tuple[float, float]]) -> float:
        """Calculate total path length of gesture."""
        total = 0.0
        for i in range(1, len(points)):
            total += math.sqrt(
                (points[i][0] - points[i - 1][0]) ** 2 +
                (points[i][1] - points[i - 1][1]) ** 2
            )
        return total

    def _match_gesture(
        self,
        gesture: List[Tuple[float, float]],
        template: GestureTemplate,
    ) -> Tuple[float, List[int]]:
        """Match gesture against a template and return score."""
        if len(gesture) != len(template.points):
            gesture = self._resample(gesture)

        total_distance = 0.0
        matched = 0

        for i, (gp, tp) in enumerate(zip(gesture, template.points)):
            distance = math.sqrt((gp[0] - tp[0]) ** 2 + (gp[1] - tp[1]) ** 2)
            total_distance += distance

            if distance < self._config.max_distance:
                matched += 1

        avg_distance = total_distance / len(gesture)
        normalized_score = max(0.0, 1.0 - avg_distance / 2.0)

        match_ratio = matched / len(gesture)
        final_score = (normalized_score * 0.6 + match_ratio * 0.4)

        return final_score, list(range(matched))

    def _determine_result(self, score: float) -> MatchResult:
        """Determine match result from score."""
        if score >= self._config.match_threshold:
            return MatchResult.MATCH
        if score >= self._config.match_threshold * 0.5:
            return MatchResult.PARTIAL
        return MatchResult.NO_MATCH


def create_gesture_template(
    name: str,
    points: List[Tuple[float, float]],
    **kwargs: Any,
) -> GestureTemplate:
    """Create a gesture template with the specified points."""
    return GestureTemplate(
        name=name,
        points=points,
        normalized=kwargs.get("normalized", False),
        metadata=kwargs.get("metadata", {}),
    )
