"""
Element similarity utilities for matching similar UI elements.

Provides similarity scoring between elements based on visual features,
accessibility attributes, and structural properties.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class ElementFeatures:
    """Extracted features from a UI element."""
    element_id: str
    label: str = ""
    role: str = ""
    subrole: str = ""
    width: float = 0.0
    height: float = 0.0
    x: float = 0.0
    y: float = 0.0
    color_avg: tuple[float, float, float] = (0.0, 0.0, 0.0)
    text_content: str = ""
    is_enabled: bool = True
    is_visible: bool = True


@dataclass
class SimilarityScore:
    """Similarity score between two elements."""
    element_a: str
    element_b: str
    overall_score: float
    feature_scores: dict[str, float]
    is_match: bool = False
    match_threshold: float = 0.8


class ElementSimilarityEngine:
    """Engine for computing element similarity."""

    def __init__(self, weights: Optional[dict[str, float]] = None):
        self.weights = weights or {
            "role": 0.2,
            "label": 0.15,
            "size": 0.15,
            "position": 0.1,
            "color": 0.2,
            "text": 0.2,
        }

    def compute_similarity(
        self,
        features_a: ElementFeatures,
        features_b: ElementFeatures,
    ) -> SimilarityScore:
        """Compute overall similarity between two elements."""
        scores: dict[str, float] = {}

        scores["role"] = self._role_similarity(features_a, features_b)
        scores["label"] = self._string_similarity(features_a.label, features_b.label)
        scores["size"] = self._size_similarity(features_a, features_b)
        scores["position"] = self._position_similarity(features_a, features_b)
        scores["color"] = self._color_similarity(features_a, features_b)
        scores["text"] = self._string_similarity(features_a.text_content, features_b.text_content)

        total = sum(self.weights.get(k, 0) * v for k, v in scores.items())
        is_match = total >= 0.8

        return SimilarityScore(
            element_a=features_a.element_id,
            element_b=features_b.element_id,
            overall_score=round(total, 3),
            feature_scores={k: round(v, 3) for k, v in scores.items()},
            is_match=is_match,
        )

    def find_similar(
        self,
        target: ElementFeatures,
        candidates: list[ElementFeatures],
        top_k: int = 5,
        threshold: float = 0.6,
    ) -> list[SimilarityScore]:
        """Find the most similar elements from a candidate list."""
        results = []
        for cand in candidates:
            if cand.element_id == target.element_id:
                continue
            score = self.compute_similarity(target, cand)
            if score.overall_score >= threshold:
                results.append(score)

        results.sort(key=lambda s: s.overall_score, reverse=True)
        return results[:top_k]

    def _role_similarity(self, a: ElementFeatures, b: ElementFeatures) -> float:
        if a.role == b.role:
            if a.subrole == b.subrole:
                return 1.0
            return 0.8
        return 0.0

    def _string_similarity(self, s1: str, s2: str) -> float:
        if not s1 or not s2:
            return 0.0
        if s1.lower() == s2.lower():
            return 1.0
        # Levenshtein-based similarity
        len1, len2 = len(s1), len(s2)
        if len1 == 0 or len2 == 0:
            return 0.0
        max_len = max(len1, len2)
        dist = self._levenshtein_distance(s1.lower(), s2.lower())
        return 1.0 - dist / max_len

    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        if len(s1) < len(s2):
            return self._levenshtein_distance(s2, s1)
        if len(s2) == 0:
            return len(s1)

        prev = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            curr = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = prev[j + 1] + 1
                deletions = curr[j] + 1
                substitutions = prev[j] + (c1 != c2)
                curr.append(min(insertions, deletions, substitutions))
            prev = curr
        return prev[-1]

    def _size_similarity(self, a: ElementFeatures, b: ElementFeatures) -> float:
        if a.width == 0 or a.height == 0 or b.width == 0 or b.height == 0:
            return 0.0
        w_ratio = min(a.width, b.width) / max(a.width, b.width)
        h_ratio = min(a.height, b.height) / max(a.height, b.height)
        area_a = a.width * a.height
        area_b = b.width * b.height
        area_ratio = min(area_a, area_b) / max(area_a, area_b)
        return (w_ratio + h_ratio + area_ratio) / 3

    def _position_similarity(self, a: ElementFeatures, b: ElementFeatures) -> float:
        dist = math.hypot(a.x - b.x, a.y - b.y)
        # Normalize by typical screen size
        max_dist = 1920.0
        similarity = max(0.0, 1.0 - dist / max_dist)
        return similarity

    def _color_similarity(self, a: ElementFeatures, b: ElementFeatures) -> float:
        r1, g1, b1 = a.color_avg
        r2, g2, b2 = b.color_avg
        dist = math.hypot(r1 - r2, g1 - g2, b1 - b2)
        max_dist = math.sqrt(3 * 255 ** 2)
        return max(0.0, 1.0 - dist / max_dist)


__all__ = ["ElementSimilarityEngine", "ElementFeatures", "SimilarityScore"]
