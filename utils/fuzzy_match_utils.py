"""Fuzzy string matching utilities.

Provides fuzzy matching algorithms for approximate string
comparison and search functionality.
"""

import re
from typing import List, Optional, Tuple


def fuzzy_score(s: str, pattern: str) -> float:
    """Calculate fuzzy match score between string and pattern.

    Args:
        s: String to match against.
        pattern: Pattern to match.

    Returns:
        Score from 0.0 (no match) to 1.0 (perfect match).
    """
    if not pattern:
        return 1.0 if not s else 0.0
    if not s:
        return 0.0

    s_lower = s.lower()
    pattern_lower = pattern.lower()

    if pattern_lower == s_lower:
        return 1.0
    if pattern_lower in s_lower:
        return 0.9

    score = 0.0
    pattern_idx = 0
    prev_match_idx = -1
    consecutive = 0

    for i, ch in enumerate(s_lower):
        if pattern_idx < len(pattern_lower) and ch == pattern_lower[pattern_idx]:
            score += 1.0
            if prev_match_idx == i - 1:
                consecutive += 1
                score += consecutive * 0.1
            else:
                consecutive = 0
            prev_match_idx = i
            pattern_idx += 1

    if pattern_idx != len(pattern_lower):
        return 0.0

    return min(score / len(pattern), 1.0)


def fuzzy_match(s: str, pattern: str) -> bool:
    """Check if string fuzzy-matches pattern.

    Args:
        s: String to match.
        pattern: Pattern to match.

    Returns:
        True if fuzzy score >= 0.5.
    """
    return fuzzy_score(s, pattern) >= 0.5


def fuzzy_filter(strings: List[str], pattern: str) -> List[Tuple[str, float]]:
    """Filter and score list of strings by fuzzy match.

    Args:
        strings: List of strings to filter.
        pattern: Pattern to match.

    Returns:
        List of (string, score) tuples sorted by score descending.
    """
    scored = [(s, fuzzy_score(s, pattern)) for s in strings]
    return sorted([(s, score) for s, score in scored if score > 0], key=lambda x: -x[1])


def levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate Levenshtein edit distance between strings.

    Args:
        s1: First string.
        s2: Second string.

    Returns:
        Minimum edit distance.
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    prev_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr_row = [i + 1]
        for j, c2 in enumerate(s2):
            cost = 0 if c1 == c2 else 1
            curr_row.append(
                min(
                    prev_row[j + 1] + 1,
                    curr_row[j] + 1,
                    prev_row[j] + cost,
                )
            )
        prev_row = curr_row

    return prev_row[-1]


def similarity(s1: str, s2: str) -> float:
    """Calculate string similarity using Levenshtein distance.

    Args:
        s1: First string.
        s2: Second string.

    Returns:
        Similarity from 0.0 to 1.0.
    """
    if not s1 and not s2:
        return 1.0
    max_len = max(len(s1), len(s2))
    if max_len == 0:
        return 1.0
    dist = levenshtein_distance(s1, s2)
    return 1.0 - dist / max_len


def contains_fuzzy(s: str, pattern: str) -> bool:
    """Check if string contains pattern (case-insensitive).

    Args:
        s: String to search.
        pattern: Pattern to find.

    Returns:
        True if pattern found.
    """
    return pattern.lower() in s.lower()


def match_highlight(s: str, pattern: str) -> str:
    """Highlight matched characters in string.

    Args:
        s: String to process.
        pattern: Pattern to highlight.

    Returns:
        String with matched chars wrapped in **.
    """
    if not pattern:
        return s
    pattern_lower = pattern.lower()
    s_lower = s.lower()
    result = []
    pidx = 0
    for i, ch in enumerate(s_lower):
        if pidx < len(pattern_lower) and ch == pattern_lower[pidx]:
            result.append(f"**{s[i]}**")
            pidx += 1
        else:
            result.append(s[i])
    return "".join(result)
