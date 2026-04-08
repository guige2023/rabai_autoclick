"""
Affinity Tracker Utilities

Provides utilities for tracking element affinity
in UI automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class AffinityScore:
    """Affinity score between elements."""
    source_id: str
    target_id: str
    score: float


class AffinityTracker:
    """
    Tracks affinity/relationship between UI elements.
    
    Maintains scores representing how often elements
    appear together or in sequence.
    """

    def __init__(self) -> None:
        self._affinities: dict[tuple[str, str], float] = {}

    def record_affinity(
        self,
        source_id: str,
        target_id: str,
        increment: float = 1.0,
    ) -> None:
        """Record affinity between two elements."""
        key = (source_id, target_id)
        self._affinities[key] = self._affinities.get(key, 0.0) + increment

    def get_affinity(
        self,
        source_id: str,
        target_id: str,
    ) -> float:
        """Get affinity score between elements."""
        key = (source_id, target_id)
        return self._affinities.get(key, 0.0)

    def get_strongest_affinity(
        self,
        source_id: str,
    ) -> tuple[str, float] | None:
        """Get strongest affinity target for an element."""
        best_target = None
        best_score = 0.0
        for (src, tgt), score in self._affinities.items():
            if src == source_id and score > best_score:
                best_target = tgt
                best_score = score
        if best_target:
            return (best_target, best_score)
        return None

    def clear(self) -> None:
        """Clear all affinity data."""
        self._affinities.clear()
