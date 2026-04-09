"""Fuzzy matching and string similarity utilities.

Supports Levenshtein distance, Jaro-Winkler, Soundex, and metaphone.
"""

from __future__ import annotations

import difflib
import logging
import re
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class MatchResult:
    """Result of a fuzzy match operation."""

    source: str
    target: str
    score: float
    matched: bool
    details: dict[str, Any] | None = None


def levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate Levenshtein edit distance between two strings.

    Args:
        s1: First string.
        s2: Second string.

    Returns:
        Minimum number of edits (insert, delete, substitute) to transform s1 to s2.
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


def levenshtein_similarity(s1: str, s2: str) -> float:
    """Calculate similarity ratio based on Levenshtein distance.

    Args:
        s1: First string.
        s2: Second string.

    Returns:
        Similarity score from 0.0 to 1.0.
    """
    if not s1 and not s2:
        return 1.0
    if not s1 or not s2:
        return 0.0

    max_len = max(len(s1), len(s2))
    distance = levenshtein_distance(s1, s2)
    return 1.0 - distance / max_len


def jaro_similarity(s1: str, s2: str) -> float:
    """Calculate Jaro similarity between two strings.

    Args:
        s1: First string.
        s2: Second string.

    Returns:
        Similarity score from 0.0 to 1.0.
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

    return (matches / len1 + matches / len2 + (matches - transpositions / 2) / matches) / 3


def jaro_winkler_similarity(s1: str, s2: str, p: float = 0.1) -> float:
    """Calculate Jaro-Winkler similarity (Jaro with prefix weighting).

    Args:
        s1: First string.
        s2: Second string.
        p: Prefix scaling factor (default 0.1).

    Returns:
        Similarity score from 0.0 to 1.0.
    """
    jaro = jaro_similarity(s1, s2)

    prefix_len = 0
    for i in range(min(len(s1), len(s2), 4)):
        if s1[i] == s2[i]:
            prefix_len += 1
        else:
            break

    return jaro + prefix_len * p * (1 - jaro)


def metaphone(s: str) -> str:
    """Calculate Metaphone phonetic code for a string.

    Args:
        s: Input string.

    Returns:
        Metaphone phonetic code.
    """
    s = s.upper()
    result = []

    i = 0
    while i < len(s):
        c = s[i]

        if c in "AEIOU":
            if i == 0 or result[-1] != "X" if result else True:
                result.append("X" if i == 0 else "A")
            i += 1
            continue

        if c == "B" and (i == len(s) - 1 or s[i - 1] == "M"):
            result.append("")
            i += 1
            continue

        if c == "C":
            if i + 1 < len(s) and s[i + 1] in "EIY":
                result.append("S")
            else:
                result.append("K")
            i += 1
            continue

        if c == "D":
            if i + 1 < len(s) and s[i + 1] in "EIY":
                result.append("J")
            else:
                result.append("T")
            i += 1
            continue

        if c == "F":
            result.append("F")
            i += 1
            continue

        if c == "G":
            if i + 1 < len(s) and s[i + 1] == "H":
                if i + 2 < len(s) and s[i + 2] not in "AEIOU":
                    result.append("F")
                i += 2
                continue
            if i + 1 < len(s) and s[i + 1] in "EIY":
                result.append("J")
            else:
                result.append("K")
            i += 1
            continue

        if c == "H":
            if i == 0 or s[i - 1] not in "AEIOU":
                if i + 1 < len(s) and s[i + 1] not in "AEIOU":
                    result.append("H")
            i += 1
            continue

        if c == "K":
            if not result or result[-1] != "K":
                result.append("K")
            i += 1
            continue

        if c == "L":
            result.append("L")
            i += 1
            continue

        if c == "M":
            result.append("M")
            i += 1
            continue

        if c == "N":
            result.append("N")
            i += 1
            continue

        if c == "P":
            if i + 1 < len(s) and s[i + 1] == "H":
                result.append("F")
                i += 2
                continue
            result.append("P")
            i += 1
            continue

        if c == "Q":
            result.append("K")
            i += 1
            continue

        if c == "R":
            result.append("R")
            i += 1
            continue

        if c == "S":
            if i + 1 < len(s) and s[i + 1 : i + 3] == "CH":
                result.append("X")
                i += 3
                continue
            if i + 1 < len(s) and s[i + 1] == "H":
                result.append("X")
                i += 2
                continue
            result.append("S")
            i += 1
            continue

        if c == "T":
            if i + 1 < len(s) and s[i + 1 : i + 3] == "CH":
                result.append("X")
                i += 3
                continue
            if i + 1 < len(s) and s[i + 1] == "H":
                result.append("X")
                i += 2
                continue
            result.append("T")
            i += 1
            continue

        if c == "V":
            result.append("F")
            i += 1
            continue

        if c == "W":
            if i + 1 < len(s) and s[i + 1] == "H":
                if i + 2 < len(s) and s[i + 2] not in "AEIOU":
                    result.append("H")
                i += 2
                continue
            result.append("")
            i += 1
            continue

        if c == "X":
            result.append("KS")
            i += 1
            continue

        if c == "Y":
            result.append("Y")
            i += 1
            continue

        if c == "Z":
            result.append("S")
            i += 1
            continue

        if c == " ":
            i += 1
            continue

        result.append(c)
        i += 1

    return "".join(result)


def soundex(s: str) -> str:
    """Calculate Soundex code for a string.

    Args:
        s: Input string (typically a word).

    Returns:
        4-character Soundex code.
    """
    s = s.upper()
    if not s:
        return "0000"

    codes = {
        "B": "1",
        "F": "1",
        "P": "1",
        "V": "1",
        "C": "2",
        "G": "2",
        "J": "2",
        "K": "2",
        "Q": "2",
        "S": "2",
        "X": "2",
        "Z": "2",
        "D": "3",
        "T": "3",
        "L": "4",
        "M": "5",
        "N": "5",
        "R": "6",
    }

    result = [s[0]]
    prev_code = codes.get(s[0], "")

    for char in s[1:]:
        code = codes.get(char, "")
        if code and code != prev_code:
            result.append(code)
        if len(result) == 4:
            break
        if char in "AEIOUY" and prev_code:
            prev_code = ""
        elif code:
            prev_code = code

    while len(result) < 4:
        result.append("0")

    return "".join(result[:4])


def match_score(s1: str, s2: str, algorithm: str = "levenshtein") -> float:
    """Calculate fuzzy match score between two strings.

    Args:
        s1: First string.
        s2: Second string.
        algorithm: One of 'levenshtein', 'jaro', 'jaro_winkler', 'difflib', 'metaphone', 'soundex'.

    Returns:
        Similarity score from 0.0 to 1.0.
    """
    s1 = s1.strip().lower()
    s2 = s2.strip().lower()

    if s1 == s2:
        return 1.0

    if algorithm == "levenshtein":
        return levenshtein_similarity(s1, s2)
    elif algorithm == "jaro":
        return jaro_similarity(s1, s2)
    elif algorithm == "jaro_winkler":
        return jaro_winkler_similarity(s1, s2)
    elif algorithm == "difflib":
        return difflib.SequenceMatcher(None, s1, s2).ratio()
    elif algorithm == "metaphone":
        return 1.0 if metaphone(s1) == metaphone(s2) else 0.0
    elif algorithm == "soundex":
        return 1.0 if soundex(s1) == soundex(s2) else 0.0
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")


def find_best_match(
    query: str,
    candidates: list[str],
    threshold: float = 0.0,
    algorithm: str = "levenshtein",
) -> MatchResult | None:
    """Find best matching string from candidates.

    Args:
        query: String to match.
        candidates: List of candidate strings.
        threshold: Minimum score threshold (0.0-1.0).
        algorithm: Matching algorithm to use.

    Returns:
        MatchResult with best match or None if none meet threshold.
    """
    best_score = threshold
    best_match = None

    for candidate in candidates:
        score = match_score(query, candidate, algorithm)
        if score > best_score:
            best_score = score
            best_match = candidate

    if best_match is None:
        return None

    return MatchResult(source=query, target=best_match, score=best_score, matched=best_score >= threshold)


def fuzzy_match(
    query: str,
    candidates: list[str],
    threshold: float = 0.6,
    algorithm: str = "levenshtein",
    top_k: int | None = None,
) -> list[MatchResult]:
    """Find all matches above threshold, optionally top K.

    Args:
        query: String to match.
        candidates: List of candidate strings.
        threshold: Minimum score threshold.
        algorithm: Matching algorithm.
        top_k: Return only top K results.

    Returns:
        List of MatchResult sorted by score descending.
    """
    results = []
    for candidate in candidates:
        score = match_score(query, candidate, algorithm)
        if score >= threshold:
            results.append(MatchResult(source=query, target=candidate, score=score, matched=True))

    results.sort(key=lambda r: r.score, reverse=True)

    if top_k is not None:
        results = results[:top_k]

    return results


class FuzzyMatcher:
    """Stateful fuzzy matcher for batch operations."""

    def __init__(self, algorithm: str = "levenshtein", threshold: float = 0.6) -> None:
        self.algorithm = algorithm
        self.threshold = threshold

    def match(self, query: str, candidates: list[str]) -> list[MatchResult]:
        """Match query against candidates."""
        return fuzzy_match(query, candidates, self.threshold, self.algorithm)

    def match_one(self, query: str, candidates: list[str]) -> MatchResult | None:
        """Match query, returning only best result."""
        results = self.match(query, candidates)
        return results[0] if results else None

    def index(self, items: list[str], key: str | None = None) -> dict[str, list[tuple[str, float]]]:
        """Build inverted index for faster matching.

        Args:
            items: List of items to index.
            key: Optional extraction function.

        Returns:
            Dict mapping characters/prefixes to (item, score) pairs.
        """
        index: dict[str, list[tuple[str, float]]] = {}

        for item in items:
            lookup_key = key(item) if key else item
            lookup_key = lookup_key.lower()

            for char in set(lookup_key):
                if char not in index:
                    index[char] = []
                index[char].append((item, 0.0))

            for prefix_len in range(2, min(len(lookup_key), 5)):
                prefix = lookup_key[:prefix_len]
                if prefix not in index:
                    index[prefix] = []
                index[prefix].append((item, 0.0))

        return index
