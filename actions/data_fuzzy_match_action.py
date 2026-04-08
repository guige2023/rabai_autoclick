"""Data fuzzy matching action module for RabAI AutoClick.

Provides fuzzy string matching: Levenshtein distance, Jaro-Winkler,
phonetic matching (Soundex, Metaphone), and record linkage.
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List, Optional, Tuple
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


def levenshtein_distance(s1: str, s2: str) -> int:
    """Compute Levenshtein edit distance between two strings."""
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)

    prev_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = prev_row[j + 1] + 1
            deletions = curr_row[j] + 1
            substitutions = prev_row[j] + (c1 != c2)
            curr_row.append(min(insertions, deletions, substitutions))
        prev_row = curr_row

    return prev_row[len(s2)]


def jaro_similarity(s1: str, s2: str) -> float:
    """Compute Jaro similarity between two strings (0 to 1)."""
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

    for i, c1 in enumerate(s1):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, len2)
        for j in range(start, end):
            if s2_matches[j] or c1 != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i, c1 in enumerate(s1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if c1 != s2[k]:
            transpositions += 1
        k += 1

    return (matches / len1 + matches / len2 +
            (matches - transpositions / 2) / matches) / 3.0


def jaro_winkler_similarity(s1: str, s2: str, p: float = 0.1) -> float:
    """Jaro-Winkler similarity with prefix weighting (0 to 1)."""
    jaro = jaro_similarity(s1, s2)
    prefix_len = 0
    for i in range(min(4, len(s1), len(s2))):
        if s1[i] == s2[i]:
            prefix_len += 1
        else:
            break
    return jaro + prefix_len * p * (1 - jaro)


def soundex(s: str) -> str:
    """Compute Soundex code for a string."""
    s = s.upper()
    if not s:
        return "0000"

    mappings = {
        'B': '1', 'F': '1', 'P': '1', 'V': '1',
        'C': '2', 'G': '2', 'J': '2', 'K': '2', 'Q': '2', 'S': '2', 'X': '2', 'Z': '2',
        'D': '3', 'T': '3',
        'L': '4',
        'M': '5', 'N': '5',
        'R': '6',
        'A': '0', 'E': '0', 'I': '0', 'O': '0', 'U': '0', 'H': '0', 'W': '0', 'Y': '0'
    }

    first_char = s[0]
    coded = first_char
    prev_code = mappings.get(first_char, '0')

    for ch in s[1:]:
        code = mappings.get(ch, '0')
        if code != '0' and code != prev_code:
            coded += code
        if len(coded) == 4:
            break
        prev_code = code

    return (coded + "0000")[:4]


def metaphone(s: str) -> str:
    """Compute Metaphone phonetic code for a string."""
    s = s.upper()
    if not s:
        return ""

    vowels = "AEIOU"
    result = []
    i = 0
    original_len = len(s)

    while len("".join(result)) < original_len and i < len(s):
        ch = s[i]

        if ch in vowels:
            if i == 0 or (i > 0 and s[i-1] not in vowels):
                result.append(ch)
            i += 1
            continue

        if ch == 'B':
            if i == original_len - 1 and s[i-1] == 'M':
                pass
            else:
                result.append('F')
            i += 1
            continue

        if ch == 'C':
            if i + 1 < original_len and s[i+1] in 'EIY':
                result.append('S')
            else:
                result.append('K')
            i += 1
            continue

        if ch == 'D':
            if i + 1 < original_len and s[i+1] == 'G' and s[i+2] in 'EIY':
                result.extend(['J'])
                i += 3
            else:
                result.append('T')
            i += 1
            continue

        if ch == 'G':
            if i + 1 < original_len and s[i+1] == 'H':
                if i + 2 < original_len and s[i+2] not in vowels:
                    i += 2
                    continue
                result.append('F')
                i += 2
                continue
            elif i + 1 < original_len and s[i+1] in 'EIY':
                result.append('J')
            else:
                result.append('K')
            i += 1
            continue

        if ch == 'H':
            if i == 0 or (i > 0 and s[i-1] not in vowels) and (i + 1 < original_len and s[i+1] in vowels):
                result.append('H')
            i += 1
            continue

        if ch == 'K':
            if i > 0 and s[i-1] == 'C':
                pass
            else:
                result.append('K')
            i += 1
            continue

        if ch == 'P':
            if i + 1 < original_len and s[i+1] == 'H':
                result.append('F')
                i += 2
                continue
            result.append('P')
            i += 1
            continue

        if ch == 'Q':
            result.append('K')
            i += 1
            continue

        if ch == 'S':
            if i + 1 < original_len and s[i+1:i+3] == 'CH':
                result.append('X')
                i += 3
                continue
            result.append('S')
            i += 1
            continue

        if ch == 'T':
            if i + 1 < original_len and s[i+1:i+3] == 'CH':
                result.append('X')
                i += 3
                continue
            result.append('T')
            i += 1
            continue

        if ch == 'V':
            result.append('F')
            i += 1
            continue

        if ch == 'W':
            if i + 1 < original_len and s[i+1] in vowels:
                result.append('W')
            i += 1
            continue

        if ch == 'X':
            result.extend(['K', 'S'])
            i += 1
            continue

        if ch == 'Y':
            if i + 1 < original_len and s[i+1] in vowels:
                result.append('Y')
            i += 1
            continue

        if ch == 'Z':
            result.append('S')
            i += 1
            continue

        i += 1

    return "".join(result)[:6]


class FuzzyMatchAction(BaseAction):
    """Fuzzy string matching with multiple algorithms.
    
    Supports Levenshtein, Jaro-Winkler, Soundex, and Metaphone.
    Returns similarity scores and best matches.
    
    Args:
        threshold: Minimum similarity score (0-1) for a match
    """

    def __init__(self, threshold: float = 0.8):
        super().__init__()
        self.threshold = threshold

    def execute(
        self,
        action: str,
        text_a: Optional[str] = None,
        text_b: Optional[str] = None,
        strings: Optional[List[str]] = None,
        reference: Optional[str] = None,
        algorithm: str = "levenshtein",
        threshold: float = 0.8
    ) -> ActionResult:
        try:
            thresh = threshold or self.threshold

            if action == "similarity":
                if not text_a or not text_b:
                    return ActionResult(success=False, error="text_a and text_b required")

                if algorithm == "levenshtein":
                    dist = levenshtein_distance(text_a, text_b)
                    max_len = max(len(text_a), len(text_b))
                    score = 1.0 - (dist / max_len) if max_len > 0 else 1.0
                    return ActionResult(success=True, data={
                        "algorithm": "levenshtein",
                        "distance": dist,
                        "similarity": round(score, 4),
                        "threshold": thresh,
                        "matches": dist <= int((1 - thresh) * max_len) if max_len > 0 else True
                    })

                elif algorithm == "jaro_winkler":
                    score = jaro_winkler_similarity(text_a, text_b)
                    return ActionResult(success=True, data={
                        "algorithm": "jaro_winkler",
                        "similarity": round(score, 4),
                        "threshold": thresh,
                        "matches": score >= thresh
                    })

                elif algorithm == "soundex":
                    code_a = soundex(text_a)
                    code_b = soundex(text_b)
                    return ActionResult(success=True, data={
                        "algorithm": "soundex",
                        "code_a": code_a,
                        "code_b": code_b,
                        "matches": code_a == code_b
                    })

                elif algorithm == "metaphone":
                    code_a = metaphone(text_a)
                    code_b = metaphone(text_b)
                    return ActionResult(success=True, data={
                        "algorithm": "metaphone",
                        "code_a": code_a,
                        "code_b": code_b,
                        "matches": code_a == code_b
                    })

                else:
                    return ActionResult(success=False, error=f"Unknown algorithm: {algorithm}")

            elif action == "find_matches":
                if not strings or reference is None:
                    return ActionResult(success=False, error="strings and reference required")

                results = []
                for s in strings:
                    score = jaro_winkler_similarity(str(reference), s)
                    if score >= thresh:
                        results.append({"string": s, "similarity": round(score, 4)})

                results.sort(key=lambda x: -x["similarity"])
                return ActionResult(success=True, data={
                    "reference": reference,
                    "matches": results,
                    "count": len(results)
                })

            elif action == "deduplicate":
                if not strings:
                    return ActionResult(success=False, error="strings required")

                groups: List[List[str]] = []
                used = set()

                for i, s in enumerate(strings):
                    if i in used:
                        continue
                    group = [s]
                    for j in range(i + 1, len(strings)):
                        if j not in used:
                            score = jaro_winkler_similarity(s, strings[j])
                            if score >= thresh:
                                group.append(strings[j])
                                used.add(j)
                    groups.append(group)
                    used.add(i)

                return ActionResult(success=True, data={
                    "n_groups": len(groups),
                    "groups": groups,
                    "representatives": [g[0] for g in groups]
                })

            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class RecordLinkageAction(BaseAction):
    """Record linkage for joining two datasets on fuzzy keys.
    
    Uses multiple fields and similarity functions to find
    matching records between datasets.
    """

    def execute(
        self,
        left_data: List[Dict[str, Any]],
        right_data: List[Dict[str, Any]],
        left_key: str,
        right_key: str,
        threshold: float = 0.85,
        algorithm: str = "jaro_winkler"
    ) -> ActionResult:
        try:
            matches: List[Dict[str, Any]] = []
            for left in left_data:
                best_match = None
                best_score = 0.0
                for right in right_data:
                    score = jaro_winkler_similarity(
                        str(left.get(left_key, "")),
                        str(right.get(right_key, ""))
                    )
                    if score > best_score and score >= threshold:
                        best_score = score
                        best_match = right

                if best_match:
                    matches.append({
                        "left": left,
                        "right": best_match,
                        "score": round(best_score, 4)
                    })

            return ActionResult(success=True, data={
                "n_matches": len(matches),
                "matches": matches[:100],  # cap
                "left_coverage": round(len(matches) / len(left_data), 4) if left_data else 0,
                "right_coverage": round(len(matches) / len(right_data), 4) if right_data else 0
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))
