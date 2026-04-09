"""
Element reliability scoring utilities.

This module provides utilities for scoring the reliability of UI elements
based on their stability, consistency, and historical accessibility data.
"""

from __future__ import annotations

import time
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass, field
from dataclasses import dataclass as dc


# Type aliases
ElementID = str
Score = float


@dataclass
class ElementHistoryEntry:
    """A historical observation of an element."""
    timestamp: float
    visible: bool
    enabled: bool
    position: Tuple[int, int]
    size: Tuple[int, int]
    label: str = ""
    value: str = ""


@dataclass
class ReliabilityScore:
    """Reliability score for a UI element."""
    element_id: ElementID
    stability_score: Score
    consistency_score: Score
    availability_score: Score
    overall_score: Score
    observation_count: int = 0
    last_observed: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_reliable(self, threshold: float = 0.7) -> bool:
        """Return True if element meets reliability threshold."""
        return self.overall_score >= threshold


@dataclass
class ReliabilityThresholds:
    """Thresholds for reliability scoring."""
    stability_weight: float = 0.4
    consistency_weight: float = 0.3
    availability_weight: float = 0.3
    min_observations: int = 5
    stability_window_seconds: float = 60.0


def compute_position_stability(
    history: List[ElementHistoryEntry],
    window_seconds: float = 60.0,
) -> Score:
    """
    Compute position stability score from element history.

    Args:
        history: Historical observations.
        window_seconds: Time window to consider.

    Returns:
        Stability score between 0 and 1.
    """
    if len(history) < 2:
        return 0.5

    now = time.time()
    recent = [e for e in history if now - e.timestamp <= window_seconds]
    if len(recent) < 2:
        return 0.5

    positions = [(e.position[0], e.position[1]) for e in recent]
    sizes = [(e.size[0], e.size[1]) for e in recent]

    # Compute variance in position
    avg_x = sum(p[0] for p in positions) / len(positions)
    avg_y = sum(p[1] for p in positions) / len(positions)
    var_x = sum((p[0] - avg_x) ** 2 for p in positions) / len(positions)
    var_y = sum((p[1] - avg_y) ** 2 for p in positions) / len(positions)

    total_var = var_x + var_y
    # Convert to stability (low variance = high stability)
    stability = max(0.0, 1.0 - min(1.0, total_var / 10000.0))
    return stability


def compute_consistency_score(
    history: List[ElementHistoryEntry],
) -> Score:
    """
    Compute label/value consistency score.

    Args:
        history: Historical observations.

    Returns:
        Consistency score between 0 and 1.
    """
    if len(history) < 2:
        return 1.0

    labels = [e.label for e in history]
    values = [e.value for e in history]

    # Label consistency
    most_common_label = max(set(labels), key=labels.count) if labels else ""
    label_consistency = labels.count(most_common_label) / len(labels) if labels else 0.0

    # Value consistency
    most_common_value = max(set(values), key=values.count) if values else ""
    value_consistency = values.count(most_common_value) / len(values) if values else 0.0

    return (label_consistency + value_consistency) / 2.0


def compute_availability_score(
    history: List[ElementHistoryEntry],
    window_seconds: float = 60.0,
) -> Score:
    """
    Compute element availability (visible+enabled) score.

    Args:
        history: Historical observations.
        window_seconds: Time window to consider.

    Returns:
        Availability score between 0 and 1.
    """
    if not history:
        return 0.0

    now = time.time()
    recent = [e for e in history if now - e.timestamp <= window_seconds]
    if not recent:
        return 0.5  # No recent data

    available_count = sum(1 for e in recent if e.visible and e.enabled)
    return available_count / len(recent)


def compute_reliability_score(
    element_id: ElementID,
    history: List[ElementHistoryEntry],
    thresholds: Optional[ReliabilityThresholds] = None,
) -> ReliabilityScore:
    """
    Compute overall reliability score for an element.

    Args:
        element_id: Unique element identifier.
        history: Historical observations.
        thresholds: Scoring thresholds.

    Returns:
        ReliabilityScore with all component scores.
    """
    if thresholds is None:
        thresholds = ReliabilityThresholds()

    stability = compute_position_stability(history, thresholds.stability_window_seconds)
    consistency = compute_consistency_score(history)
    availability = compute_availability_score(history, thresholds.stability_window_seconds)

    overall = (
        thresholds.stability_weight * stability
        + thresholds.consistency_weight * consistency
        + thresholds.availability_weight * availability
    )

    return ReliabilityScore(
        element_id=element_id,
        stability_score=stability,
        consistency_score=consistency,
        availability_score=availability,
        overall_score=overall,
        observation_count=len(history),
        last_observed=history[-1].timestamp if history else 0.0,
        metadata={
            "stability_weight": thresholds.stability_weight,
            "consistency_weight": thresholds.consistency_weight,
            "availability_weight": thresholds.availability_weight,
        },
    )


def rank_elements_by_reliability(
    scores: List[ReliabilityScore],
) -> List[ReliabilityScore]:
    """
    Rank elements by their overall reliability score.

    Args:
        scores: List of reliability scores.

    Returns:
        Sorted list (highest reliability first).
    """
    return sorted(scores, key=lambda s: s.overall_score, reverse=True)


def filter_reliable_elements(
    scores: List[ReliabilityScore],
    threshold: float = 0.7,
) -> List[ReliabilityScore]:
    """Filter elements meeting reliability threshold."""
    return [s for s in scores if s.is_reliable(threshold)]
