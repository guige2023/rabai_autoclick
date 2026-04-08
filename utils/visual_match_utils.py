"""Visual match utilities for RabAI AutoClick.

Provides:
- Template matching helpers
- Region finding
- Match scoring
"""

from __future__ import annotations

from typing import (
    Any,
    List,
    NamedTuple,
    Optional,
    Tuple,
)


class MatchResult(NamedTuple):
    """Result of a visual match."""
    x: int
    y: int
    width: int
    height: int
    score: float


def find_template(
    source: List[List[int]],
    template: List[List[int]],
    threshold: float = 0.8,
) -> List[MatchResult]:
    """Find template matches in source image.

    Args:
        source: Source image pixels.
        template: Template to find.
        threshold: Match threshold (0-1).

    Returns:
        List of match results.
    """
    if not source or not template:
        return []

    matches: List[MatchResult] = []
    t_height = len(template)
    t_width = len(template[0]) if t_height > 0 else 0

    for y in range(len(source) - t_height + 1):
        for x in range(len(source[0]) - t_width + 1):
            score = _template_score(source, template, x, y)
            if score >= threshold:
                matches.append(MatchResult(
                    x=x, y=y, width=t_width, height=t_height, score=score
                ))

    return matches


def _template_score(
    source: List[List[int]],
    template: List[List[int]],
    x: int,
    y: int,
) -> float:
    """Compute match score at position."""
    t_height = len(template)
    t_width = len(template[0]) if t_height > 0 else 0

    matches = 0
    total = t_height * t_width

    for ty in range(t_height):
        for tx in range(t_width):
            if source[y + ty][x + tx] == template[ty][tx]:
                matches += 1

    return matches / total if total > 0 else 0.0


__all__ = [
    "MatchResult",
    "find_template",
]
