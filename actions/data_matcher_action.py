"""Data matching and fuzzy lookup utilities.

This module provides fuzzy matching capabilities:
- String similarity (Levenshtein, Jaro-Winkler)
- Record linkage and deduplication
- Approximate matching
- Threshold-based matching

Example:
    >>> from actions.data_matcher_action import fuzzy_match, deduplicate
    >>> matches = fuzzy_match(names, threshold=0.8)
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional, Callable
from collections import defaultdict

logger = logging.getLogger(__name__)


def levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate Levenshtein distance between two strings.

    Args:
        s1: First string.
        s2: Second string.

    Returns:
        Edit distance.
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)
    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    return previous_row[-1]


def jaro_similarity(s1: str, s2: str) -> float:
    """Calculate Jaro similarity between two strings.

    Args:
        s1: First string.
        s2: Second string.

    Returns:
        Similarity score between 0 and 1.
    """
    if s1 == s2:
        return 1.0
    len1, len2 = len(s1), len(s2)
    if len1 == 0 or len2 == 0:
        return 0.0
    match_distance = max(len1, len2) // 2 - 1
    match_distance = max(0, match_distance)
    s1_matches = [False] * len1
    s2_matches = [False] * len2
    matches = 0
    transpositions = 0
    for i in range(len1):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, len2)
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break
    if matches == 0:
        return 0.0
    k = 0
    for i in range(len1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1
    return (matches / len1 + matches / len2 +
            (matches - transpositions / 2) / matches) / 3


def jaro_winkler_similarity(s1: str, s2: str, p: float = 0.1) -> float:
    """Calculate Jaro-Winkler similarity.

    Args:
        s1: First string.
        s2: Second string.
        p: Scaling factor (default 0.1).

    Returns:
        Similarity score between 0 and 1.
    """
    jaro = jaro_similarity(s1, s2)
    prefix_len = 0
    for i in range(min(len(s1), len(s2), 4)):
        if s1[i] == s2[i]:
            prefix_len += 1
        else:
            break
    return jaro + prefix_len * p * (1 - jaro)


def cosine_similarity(s1: str, s2: str) -> float:
    """Calculate cosine similarity between two strings.

    Args:
        s1: First string.
        s2: Second string.

    Returns:
        Similarity score between 0 and 1.
    """
    def tokenize(text: str) -> set[str]:
        return set(re.findall(r"\w+", text.lower()))
    tokens1 = tokenize(s1)
    tokens2 = tokenize(s2)
    if not tokens1 or not tokens2:
        return 0.0
    intersection = tokens1 & tokens2
    numerator = sum(1 for _ in intersection)
    denominator = (len(tokens1) * len(tokens2)) ** 0.5
    return numerator / denominator if denominator else 0.0


def fuzzy_match(
    source: list[str],
    target: list[str],
    threshold: float = 0.8,
    algorithm: str = "jaro_winkler",
) -> list[tuple[int, int, float]]:
    """Find fuzzy matches between source and target strings.

    Args:
        source: Source strings.
        target: Target strings.
        threshold: Minimum similarity score.
        algorithm: Similarity algorithm (levenshtein, jaro_winkler, cosine).

    Returns:
        List of (source_idx, target_idx, score) tuples.
    """
    algorithms = {
        "levenshtein": lambda s, t: 1 - levenshtein_distance(s, t) / max(len(s), len(t), 1),
        "jaro_winkler": jaro_winkler_similarity,
        "cosine": cosine_similarity,
    }
    if algorithm not in algorithms:
        algorithm = "jaro_winkler"
    sim_func = algorithms[algorithm]
    matches = []
    for i, s in enumerate(source):
        for j, t in enumerate(target):
            score = sim_func(s, t)
            if score >= threshold:
                matches.append((i, j, score))
    return matches


def deduplicate(
    items: list[dict[str, Any]],
    fields: list[str],
    threshold: float = 0.85,
    algorithm: str = "jaro_winkler",
) -> list[list[int]]:
    """Find duplicate groups in a collection.

    Args:
        items: Items to check for duplicates.
        fields: Fields to compare.
        threshold: Minimum similarity.
        algorithm: Similarity algorithm.

    Returns:
        List of duplicate groups (each group is list of item indices).
    """
    def get_comparable(item: dict[str, Any]) -> str:
        return " ".join(str(item.get(f, "")) for f in fields)
    comparables = [get_comparable(item) for item in items]
    matches = fuzzy_match(comparables, comparables, threshold=threshold, algorithm=algorithm)
    groups: list[list[int]] = []
    uf = UnionFind(len(items))
    for i, j, score in matches:
        if i != j:
            uf.union(i, j)
    for root in uf.roots:
        group = uf.get_group(root)
        if len(group) > 1:
            groups.append(sorted(group))
    return groups


class UnionFind:
    """Union-Find data structure for grouping."""

    def __init__(self, n: int) -> None:
        self.parent = list(range(n))
        self.rank = [0] * n
        self.roots = set(range(n))

    def find(self, x: int) -> int:
        """Find root with path compression."""
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x: int, y: int) -> None:
        """Union two sets."""
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if self.rank[rx] < self.rank[ry]:
            rx, ry = ry, rx
        self.parent[ry] = rx
        if self.rank[rx] == self.rank[ry]:
            self.rank[rx] += 1
        self.roots.discard(ry)

    def get_group(self, x: int) -> list[int]:
        """Get all elements in the same set as x."""
        root = self.find(x)
        return [i for i in range(len(self.parent)) if self.find(i) == root]


def match_score(
    record1: dict[str, Any],
    record2: dict[str, Any],
    field_weights: Optional[dict[str, float]] = None,
    threshold: float = 0.8,
) -> float:
    """Calculate weighted match score between two records.

    Args:
        record1: First record.
        record2: Second record.
        field_weights: Optional weights per field.
        threshold: Minimum score for a match.

    Returns:
        Match score between 0 and 1.
    """
    if not field_weights:
        all_fields = set(record1.keys()) & set(record2.keys())
        field_weights = {f: 1.0 / len(all_fields) for f in all_fields} if all_fields else {}
    total_weight = 0.0
    weighted_sum = 0.0
    for field, weight in field_weights.items():
        v1, v2 = record1.get(field), record2.get(field)
        if v1 is None or v2 is None:
            continue
        if v1 == v2:
            similarity = 1.0
        elif isinstance(v1, str) and isinstance(v2, str):
            similarity = jaro_winkler_similarity(v1, v2)
        else:
            similarity = 1.0 if v1 == v2 else 0.0
        weighted_sum += similarity * weight
        total_weight += weight
    return weighted_sum / total_weight if total_weight else 0.0
