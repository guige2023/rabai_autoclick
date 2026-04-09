"""Gesture confidence calculator for evaluating gesture recognition certainty."""
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass


@dataclass
class GestureConfidence:
    """Confidence scores for a gesture recognition result."""
    overall: float
    shape_match: float
    timing_match: float
    trajectory_match: float
    contextual: float
    is_confident: bool
    warnings: List[str]


class GestureConfidenceCalculator:
    """Calculates confidence scores for gesture recognition results.
    
    Evaluates multiple aspects of a recognized gesture to determine
    how confident the recognition system is.
    
    Example:
        calc = GestureConfidenceCalculator()
        confidence = calc.calculate(
            gesture_type="swipe",
            shape_score=0.85,
            trajectory_score=0.92,
        )
        if confidence.is_confident:
            print("High confidence")
    """

    def __init__(
        self,
        confidence_threshold: float = 0.7,
        min_shape_score: float = 0.6,
        min_trajectory_score: float = 0.5,
    ) -> None:
        self._threshold = confidence_threshold
        self._min_shape = min_shape_score
        self._min_trajectory = min_trajectory_score

    def calculate(
        self,
        gesture_type: str,
        shape_score: float,
        trajectory_score: float,
        timing_score: Optional[float] = None,
        expected_duration: Optional[Tuple[float, float]] = None,
        actual_duration: Optional[float] = None,
        contextual_factors: Optional[Dict[str, float]] = None,
    ) -> GestureConfidence:
        """Calculate overall confidence for a gesture."""
        warnings = []
        
        if shape_score < self._min_shape:
            warnings.append(f"Low shape match: {shape_score:.2f}")
        if trajectory_score < self._min_trajectory:
            warnings.append(f"Low trajectory match: {trajectory_score:.2f}")
        
        timing_match = 1.0
        if timing_score is not None:
            timing_match = timing_score
        elif expected_duration and actual_duration:
            min_d, max_d = expected_duration
            midpoint = (min_d + max_d) / 2
            range_d = (max_d - min_d) / 2
            if range_d > 0:
                timing_match = max(0, 1 - abs(actual_duration - midpoint) / range_d)
        
        contextual_score = 0.5
        if contextual_factors:
            contextual_score = sum(contextual_factors.values()) / len(contextual_factors)
        
        overall = (
            shape_score * 0.35 +
            trajectory_score * 0.35 +
            timing_match * 0.15 +
            contextual_score * 0.15
        )
        
        return GestureConfidence(
            overall=round(overall, 3),
            shape_match=round(shape_score, 3),
            timing_match=round(timing_match, 3),
            trajectory_match=round(trajectory_score, 3),
            contextual=round(contextual_score, 3),
            is_confident=overall >= self._threshold,
            warnings=warnings,
        )

    def combine_confidences(
        self,
        confidences: List[GestureConfidence],
        method: str = "max",
    ) -> GestureConfidence:
        """Combine multiple confidence scores."""
        if not confidences:
            return GestureConfidence(overall=0, shape_match=0, timing_match=0,
                                     trajectory_match=0, contextual=0, is_confident=False, warnings=[])
        
        if method == "max":
            overall = max(c.overall for c in confidences)
            shape = max(c.shape_match for c in confidences)
            timing = max(c.timing_match for c in confidences)
            trajectory = max(c.trajectory_match for c in confidences)
            contextual = max(c.contextual for c in confidences)
        elif method == "min":
            overall = min(c.overall for c in confidences)
            shape = min(c.shape_match for c in confidences)
            timing = min(c.timing_match for c in confidences)
            trajectory = min(c.trajectory_match for c in confidences)
            contextual = min(c.contextual for c in confidences)
        else:
            n = len(confidences)
            overall = sum(c.overall for c in confidences) / n
            shape = sum(c.shape_match for c in confidences) / n
            timing = sum(c.timing_match for c in confidences) / n
            trajectory = sum(c.trajectory_match for c in confidences) / n
            contextual = sum(c.contextual for c in confidences) / n
        
        all_warnings = []
        for c in confidences:
            all_warnings.extend(c.warnings)
        
        return GestureConfidence(
            overall=round(overall, 3),
            shape_match=round(shape, 3),
            timing_match=round(timing, 3),
            trajectory_match=round(trajectory, 3),
            contextual=round(contextual, 3),
            is_confident=overall >= self._threshold,
            warnings=list(set(all_warnings)),
        )
