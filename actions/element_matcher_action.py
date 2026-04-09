"""
Element Matcher Action Module.

Matches elements using visual similarity, structural similarity,
and attribute-based matching for robust element identification.
"""

import difflib
from typing import Any, Callable, Optional


class ElementMatcher:
    """Matches elements using various similarity strategies."""

    def __init__(self):
        """Initialize element matcher."""
        pass

    def match_by_attributes(
        self,
        target: dict,
        candidates: list[dict],
        required_attrs: Optional[list[str]] = None,
        optional_attrs: Optional[list[str]] = None,
    ) -> Optional[dict]:
        """
        Match element by attribute comparison.

        Args:
            target: Target element with attributes.
            candidates: List of candidate elements.
            required_attrs: Attributes that must match exactly.
            optional_attrs: Attributes used for scoring.

        Returns:
            Best matching element or None.
        """
        required_attrs = required_attrs or ["tag", "role"]
        optional_attrs = optional_attrs or ["class", "text", "name"]

        best_match = None
        best_score = 0.0

        for candidate in candidates:
            if not self._matches_required(target, candidate, required_attrs):
                continue

            score = self._compute_similarity(target, candidate, optional_attrs)

            if score > best_score:
                best_score = score
                best_match = candidate

        return best_match

    def match_by_visual_similarity(
        self,
        target_bounds: tuple[int, int, int, int],
        candidates: list[dict],
        threshold: float = 0.8,
    ) -> Optional[dict]:
        """
        Match element by visual bounds similarity.

        Args:
            target_bounds: Target bounding box.
            candidates: Candidate elements.
            threshold: Similarity threshold.

        Returns:
            Best matching element or None.
        """
        target_area = self._bounds_area(target_bounds)

        best_match = None
        best_score = 0.0

        for candidate in candidates:
            bounds = candidate.get("bounds", (0, 0, 0, 0))
            score = self._bounds_similarity(target_bounds, bounds)

            if score >= threshold and score > best_score:
                best_score = score
                best_match = candidate

        return best_match

    def match_by_text_similarity(
        self,
        target_text: str,
        candidates: list[dict],
        threshold: float = 0.7,
    ) -> Optional[dict]:
        """
        Match element by text content similarity.

        Args:
            target_text: Target text content.
            candidates: Candidate elements.
            threshold: Similarity threshold.

        Returns:
            Best matching element or None.
        """
        best_match = None
        best_score = 0.0

        for candidate in candidates:
            candidate_text = candidate.get("text", "")
            score = self._text_similarity(target_text, candidate_text)

            if score >= threshold and score > best_score:
                best_score = score
                best_match = candidate

        return best_match

    def fuzzy_match(
        self,
        target: dict,
        candidates: list[dict],
        weights: Optional[dict[str, float]] = None,
    ) -> tuple[Optional[dict], float]:
        """
        Fuzzy match using multiple attributes with weights.

        Args:
            target: Target element.
            candidates: Candidate elements.
            weights: Attribute weights for scoring.

        Returns:
            Tuple of (best_match, score).
        """
        weights = weights or {
            "tag": 0.3,
            "role": 0.2,
            "text": 0.3,
            "class": 0.1,
            "id": 0.1,
        }

        best_match = None
        best_score = 0.0

        for candidate in candidates:
            score = self._weighted_similarity(target, candidate, weights)

            if score > best_score:
                best_score = score
                best_match = candidate

        return best_match, best_score

    def _matches_required(
        self,
        target: dict,
        candidate: dict,
        required_attrs: list[str],
    ) -> bool:
        """Check if candidate matches required attributes."""
        for attr in required_attrs:
            if target.get(attr) != candidate.get(attr):
                return False
        return True

    def _compute_similarity(
        self,
        target: dict,
        candidate: dict,
        attrs: list[str],
    ) -> float:
        """Compute attribute-based similarity score."""
        matches = 0
        for attr in attrs:
            t_val = str(target.get(attr, ""))
            c_val = str(candidate.get(attr, ""))
            if t_val == c_val:
                matches += 1
        return matches / len(attrs) if attrs else 0.0

    @staticmethod
    def _bounds_area(bounds: tuple[int, int, int, int]) -> float:
        """Calculate area of bounding box."""
        return float((bounds[2] - bounds[0]) * (bounds[3] - bounds[1]))

    @staticmethod
    def _bounds_similarity(
        b1: tuple[int, int, int, int],
        b2: tuple[int, int, int, int],
    ) -> float:
        """Calculate similarity between two bounding boxes."""
        x_overlap = max(0, min(b1[2], b2[2]) - max(b1[0], b2[0]))
        y_overlap = max(0, min(b1[3], b2[3]) - max(b1[1], b2[1]))

        if x_overlap <= 0 or y_overlap <= 0:
            return 0.0

        overlap = x_overlap * y_overlap
        area1 = ElementMatcher._bounds_area(b1)
        area2 = ElementMatcher._bounds_area(b2)

        iou = overlap / (area1 + area2 - overlap)
        return iou

    @staticmethod
    def _text_similarity(s1: str, s2: str) -> float:
        """Calculate text similarity using SequenceMatcher."""
        if not s1 or not s2:
            return 0.0
        return difflib.SequenceMatcher(None, s1, s2).ratio()

    def _weighted_similarity(
        self,
        target: dict,
        candidate: dict,
        weights: dict[str, float],
    ) -> float:
        """Compute weighted similarity across attributes."""
        total_weight = sum(weights.values())
        score = 0.0

        for attr, weight in weights.items():
            t_val = str(target.get(attr, "")).lower()
            c_val = str(candidate.get(attr, "")).lower()

            if t_val == c_val:
                score += weight
            elif t_val and c_val:
                sim = self._text_similarity(t_val, c_val)
                score += weight * sim

        return score / total_weight if total_weight > 0 else 0.0
