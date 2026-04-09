"""Data Fuzzy Match Action.

Fuzzy string matching using Levenshtein, Jaro-Winkler, and
phonetic algorithms. Supports record linkage and deduplication.
"""
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass
import math


@dataclass
class MatchResult:
    source: str
    target: str
    score: float
    algorithm: str
    matched: bool

    def as_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "target": self.target,
            "score": round(self.score, 4),
            "algorithm": self.algorithm,
            "matched": self.matched,
        }


def _levenshtein_distance(s1: str, s2: str) -> int:
    if len(s1) < len(s2):
        return _levenshtein_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)
    prev = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        curr = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = prev[j + 1] + 1
            deletions = curr[j] + 1
            substitutions = prev[j] + (c1 != c2)
            curr.append(min(insertions, deletions, substitutions))
        prev = curr
    return prev[-1]


def _levenshtein_score(s1: str, s2: str) -> float:
    if not s1 and not s2:
        return 1.0
    max_len = max(len(s1), len(s2))
    if max_len == 0:
        return 1.0
    return 1.0 - _levenshtein_distance(s1, s2) / max_len


def _jaro_similarity(s1: str, s2: str) -> float:
    if s1 == s2:
        return 1.0
    len1, len2 = len(s1), len(s2)
    if len1 == 0 or len2 == 0:
        return 0.0
    match_distance = max(len1, len2) // 2 - 1
    if match_distance < 0:
        match_distance = 0
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
    jaro = (matches / len1 + matches / len2 + (matches - transpositions / 2) / matches) / 3
    return jaro


def _jaro_winkler(s1: str, s2: str, p: float = 0.1) -> float:
    jaro = _jaro_similarity(s1, s2)
    prefix = 0
    for i in range(min(4, len(s1), len(s2))):
        if s1[i] == s2[i]:
            prefix += 1
        else:
            break
    return jaro + prefix * p * (1 - jaro)


def _phonetic_code(s: str, method: str = "metaphone") -> str:
    s = s.upper()
    if method == "soundex":
        # Simplified soundex
        mapping = {"BFPV": "1", "CGJKQSXZ": "2", "DT": "3", "L": "4", "MN": "5", "R": "6"}
        if not s:
            return "0000"
        first = s[0]
        result = first
        for char in s[1:]:
            for key, val in mapping.items():
                if char in key:
                    if val != result[-1]:
                        result += val
                    break
        result = result + "0000"
        return result[:4]
    elif method == "metaphone":
        # Simplified metaphone
        vowels = "AEIOU"
        result = []
        i = 0
        while i < len(s):
            c = s[i]
            if c in vowels:
                if i == 0:
                    result.append(c)
            elif c == "B" and i == len(s) - 1:
                pass
            elif c == "C":
                result.append("X")
            elif c == "G":
                result.append("K")
            elif c == "H" and (i == 0 or s[i-1] not in vowels):
                result.append("H")
            elif c == "K" and i > 0 and s[i-1] == "C":
                pass
            elif c == "P":
                result.append("F") if s[i+1] == "H" else result.append("P")
            elif c == "S":
                result.append("X")
            elif c == "T":
                result.append("X") if s[i+1:i+3] == "CH" else result.append("T")
            elif c == "W":
                pass
            elif c == "X":
                result.append("KS")
            elif c == "Y":
                result.append("Y")
            i += 1
        return "".join(result[:4]) if result else s[:4]
    return s[:4]


class DataFuzzyMatchAction:
    """Fuzzy string matching and record linkage."""

    def __init__(self, default_threshold: float = 0.85) -> None:
        self.default_threshold = default_threshold

    def levenshtein(self, s1: str, s2: str) -> MatchResult:
        score = _levenshtein_score(s1, s2)
        return MatchResult(
            source=s1,
            target=s2,
            score=score,
            algorithm="levenshtein",
            matched=score >= self.default_threshold,
        )

    def jaro_winkler(self, s1: str, s2: str) -> MatchResult:
        score = _jaro_winkler(s1, s2)
        return MatchResult(
            source=s1,
            target=s2,
            score=score,
            algorithm="jaro_winkler",
            matched=score >= self.default_threshold,
        )

    def match_best(
        self,
        source: str,
        candidates: List[str],
        algorithm: str = "levenshtein",
        threshold: Optional[float] = None,
    ) -> Optional[MatchResult]:
        if not candidates:
            return None
        thresh = threshold or self.default_threshold
        results = []
        if algorithm == "levenshtein":
            for cand in candidates:
                results.append(self.levenshtein(source, cand))
        else:
            for cand in candidates:
                results.append(self.jaro_winkler(source, cand))
        best = max(results, key=lambda r: r.score)
        if best.score < thresh:
            return None
        return best

    def match_all(
        self,
        source: str,
        candidates: List[str],
        algorithm: str = "levenshtein",
        threshold: Optional[float] = None,
    ) -> List[MatchResult]:
        thresh = threshold or self.default_threshold
        results = []
        if algorithm == "levenshtein":
            for cand in candidates:
                r = self.levenshtein(source, cand)
                if r.score >= thresh:
                    results.append(r)
        else:
            for cand in candidates:
                r = self.jaro_winkler(source, cand)
                if r.score >= thresh:
                    results.append(r)
        return sorted(results, key=lambda r: r.score, reverse=True)

    def deduplicate(
        self,
        records: List[Tuple[str, Any]],
        key_fn: Callable[[Any], str],
        threshold: float = 0.85,
        algorithm: str = "levenshtein",
    ) -> List[List[int]]:
        """Groups duplicate records. Returns list of index groups."""
        n = len(records)
        union_find = list(range(n))
        def find(x):
            if union_find[x] != x:
                union_find[x] = find(union_find[x])
            return union_find[x]
        def union(x, y):
            rx, ry = find(x), find(y)
            if rx != ry:
                union_find[rx] = ry
        for i in range(n):
            for j in range(i + 1, n):
                s1 = key_fn(records[i][1])
                s2 = key_fn(records[j][1])
                match = self.match_best(s1, [s2], algorithm=algorithm, threshold=threshold)
                if match:
                    union(i, j)
        groups: Dict[int, List[int]] = {}
        for i in range(n):
            r = find(i)
            if r not in groups:
                groups[r] = []
            groups[r].append(i)
        return list(groups.values())
