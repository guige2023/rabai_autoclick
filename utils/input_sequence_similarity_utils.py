"""
Input Sequence Similarity Utilities

Compute similarity between sequences of input events (clicks, keypresses,
gestures) for deduplication, replay validation, and pattern matching.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Sequence, Tuple, Optional


@dataclass
class SequenceSimilarityResult:
    """Similarity score between two input sequences."""
    score: float  # 0.0 (different) to 1.0 (identical)
    alignment_score: float
    length_penalty: float


def levenshtein_distance(seq_a: Sequence, seq_b: Sequence) -> int:
    """Compute the Levenshtein edit distance between two sequences."""
    n, m = len(seq_a), len(seq_b)
    if n == 0:
        return m
    if m == 0:
        return n

    dp = [[0] * (m + 1) for _ in range(n + 1)]
    for i in range(n + 1):
        dp[i][0] = i
    for j in range(m + 1):
        dp[0][j] = j

    for i in range(1, n + 1):
        for j in range(1, m + 1):
            cost = 0 if seq_a[i - 1] == seq_b[j - 1] else 1
            dp[i][j] = min(
                dp[i - 1][j] + 1,      # deletion
                dp[i][j - 1] + 1,      # insertion
                dp[i - 1][j - 1] + cost,  # substitution
            )
    return dp[n][m]


def compute_sequence_similarity(
    seq_a: Sequence,
    seq_b: Sequence,
) -> SequenceSimilarityResult:
    """
    Compute similarity between two input sequences.

    Uses normalized Levenshtein distance with a length penalty
    to account for sequences of different lengths.
    """
    if not seq_a and not seq_b:
        return SequenceSimilarityResult(score=1.0, alignment_score=1.0, length_penalty=1.0)

    if not seq_a or not seq_b:
        return SequenceSimilarityResult(score=0.0, alignment_score=0.0, length_penalty=0.0)

    max_len = max(len(seq_a), len(seq_b))
    edit_dist = levenshtein_distance(seq_a, seq_b)

    alignment_score = max(0.0, 1.0 - edit_dist / max_len)
    length_penalty = min(len(seq_a), len(seq_b)) / max(len(seq_a), len(seq_b))
    score = alignment_score * length_penalty

    return SequenceSimilarityResult(
        score=score,
        alignment_score=alignment_score,
        length_penalty=length_penalty,
    )


def deduplicate_sequence(
    events: Sequence,
    similarity_threshold: float = 0.8,
) -> List:
    """
    Remove duplicate events from a sequence based on similarity.

    Events that are too similar (score > threshold) to an earlier
    event are removed from the sequence.
    """
    if len(events) <= 1:
        return list(events)

    result = [events[0]]
    for event in events[1:]:
        is_duplicate = False
        for existing in result:
            sim = compute_sequence_similarity(existing, event)
            if sim.score >= similarity_threshold:
                is_duplicate = True
                break
        if not is_duplicate:
            result.append(event)
    return result
