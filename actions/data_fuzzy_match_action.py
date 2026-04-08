"""
Data Fuzzy Match Action Module.

Performs fuzzy string matching and record linkage using
Levenshtein distance, Jaro-Winkler, and phonetic algorithms.

Author: RabAi Team
"""

from __future__ import annotations

import re
import sys
import os
import time
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class MatchAlgorithm(Enum):
    """Fuzzy matching algorithms."""
    LEVENSHTEIN = "levenshtein"
    JARO_WINKLER = "jaro_winkler"
    LEVENSHTEIN_RATIO = "levenshtein_ratio"
    PARTIAL_RATIO = "partial_ratio"
    TOKEN_SORT_RATIO = "token_sort_ratio"
    TOKEN_SET_RATIO = "token_set_ratio"
    METAPHONE = "metaphone"
    SOUNDEX = "soundex"
    SEQUENCE_MATCHER = "sequence_matcher"


@dataclass
class MatchResult:
    """Result of a fuzzy match operation."""
    source: Any
    target: Any
    score: float
    algorithm: MatchAlgorithm
    matched: bool
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MatchGroup:
    """A group of matched records."""
    group_id: int
    matches: List[Dict[str, Any]]
    canonical: Any
    match_type: str


class DataFuzzyMatchAction(BaseAction):
    """Data fuzzy match action.
    
    Performs fuzzy string matching and record deduplication
    with configurable algorithms and threshold settings.
    """
    action_type = "data_fuzzy_match"
    display_name = "数据模糊匹配"
    description = "模糊字符串匹配与记录关联"
    
    def __init__(self):
        super().__init__()
        self._phonetic_cache: Dict[str, str] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Perform fuzzy matching.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - operation: match_one/match_all/group/dedupe
                - source: Source value or list
                - candidates: List of candidate values
                - algorithm: Matching algorithm
                - threshold: Match threshold (0-1)
                - case_sensitive: Case sensitive matching
                - field: Field name for record matching
                
        Returns:
            ActionResult with match results.
        """
        start_time = time.time()
        
        operation = params.get("operation", "match_one")
        source = params.get("source")
        candidates = params.get("candidates", [])
        algorithm_str = params.get("algorithm", "levenshtein")
        threshold = params.get("threshold", 0.8)
        case_sensitive = params.get("case_sensitive", False)
        field_name = params.get("field")
        
        try:
            algorithm = MatchAlgorithm(algorithm_str)
        except ValueError:
            return ActionResult(
                success=False,
                message=f"Unknown algorithm: {algorithm_str}",
                duration=time.time() - start_time
            )
        
        try:
            if operation == "match_one":
                result = self._match_one(source, candidates, algorithm, threshold, case_sensitive)
            elif operation == "match_all":
                result = self._match_all(source, candidates, algorithm, threshold, case_sensitive)
            elif operation == "group":
                result = self._group(candidates, algorithm, threshold, case_sensitive, field_name)
            elif operation == "dedupe":
                result = self._dedupe(candidates, algorithm, threshold, case_sensitive, field_name)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
            
            return ActionResult(
                success=True,
                message=f"Fuzzy match complete: {operation}",
                data=result,
                duration=time.time() - start_time
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Matching failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _normalize(self, s: str, case_sensitive: bool) -> str:
        """Normalize a string for comparison."""
        if not case_sensitive:
            s = s.lower()
        return " ".join(s.split())
    
    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """Calculate Levenshtein distance between two strings."""
        if len(s1) < len(s2):
            return self._levenshtein_distance(s2, s1)
        
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
    
    def _levenshtein_ratio(self, s1: str, s2: str) -> float:
        """Calculate Levenshtein ratio (0-1, higher is more similar)."""
        if not s1 and not s2:
            return 1.0
        distance = self._levenshtein_distance(s1, s2)
        max_len = max(len(s1), len(s2))
        if max_len == 0:
            return 1.0
        return 1.0 - (distance / max_len)
    
    def _jaro_winkler_similarity(self, s1: str, s2: str) -> float:
        """Calculate Jaro-Winkler similarity."""
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
        
        jaro = (matches / len1 + matches / len2 + (matches - transpositions / 2) / matches) / 3
        
        prefix_len = 0
        for i in range(min(4, len1, len2)):
            if s1[i] == s2[i]:
                prefix_len += 1
            else:
                break
        
        return jaro + prefix_len * 0.1 * (1 - jaro)
    
    def _partial_ratio(self, s1: str, s2: str) -> float:
        """Calculate partial ratio (best match of shorter in longer)."""
        shorter, longer = (s1, s2) if len(s1) <= len(s2) else (s2, s1)
        
        if len(longer) == 0:
            return 1.0 if len(shorter) == 0 else 0.0
        
        window = len(shorter)
        best = 0.0
        
        for i in range(len(longer) - window + 1):
            substr = longer[i:i + window]
            ratio = self._levenshtein_ratio(shorter, substr)
            best = max(best, ratio)
        
        return best
    
    def _token_sort_ratio(self, s1: str, s2: str) -> float:
        """Calculate token sort ratio (sorted token matching)."""
        tokens1 = sorted(s1.lower().split())
        tokens2 = sorted(s2.lower().split())
        sorted1 = " ".join(tokens1)
        sorted2 = " ".join(tokens2)
        return self._levenshtein_ratio(sorted1, sorted2)
    
    def _token_set_ratio(self, s1: str, s2: str) -> float:
        """Calculate token set ratio (intersection/union of tokens)."""
        tokens1 = set(s1.lower().split())
        tokens2 = set(s2.lower().split())
        
        intersection = tokens1 & tokens2
        diff1 = tokens1 - tokens2
        diff2 = tokens2 - tokens1
        
        sorted_sect = " ".join(sorted(intersection))
        sorted_diff1 = " ".join(sorted(diff1))
        sorted_diff2 = " ".join(sorted(diff2))
        
        pairs = [
            (sorted_sect, sorted_diff1),
            (sorted_sect, sorted_diff2),
            (sorted_diff1, sorted_diff2)
        ]
        
        best = 0.0
        for p1, p2 in pairs:
            if p1 or p2:
                ratio = self._levenshtein_ratio(p1, p2)
                best = max(best, ratio)
        
        return best
    
    def _metaphone(self, s: str) -> str:
        """Calculate Metaphone phonetic code."""
        if s in self._phonetic_cache:
            return self._phonetic_cache[s]
        
        s = s.lower()
        result = []
        
        if s.startswith("kn") or s.startswith("gn") or s.startswith("pn") or s.startswith("ae"):
            s = s[1:]
        
        vowels = set("aeiou")
        i = 0
        while i < len(s):
            char = s[i]
            next_char = s[i + 1] if i + 1 < len(s) else ""
            
            if char in vowels:
                result.append(char)
            elif char == "b":
                if i == len(s) - 1 or s[i - 1] == "m":
                    pass
                else:
                    result.append("b")
            elif char == "c":
                if next_char == "h":
                    result.append("x")
                    i += 1
                elif next_char in "iey":
                    result.append("s")
                else:
                    result.append("k")
            elif char == "d":
                if next_char == "g" and s[i + 2] if i + 2 < len(s) else "" in "iey":
                    result.append("j")
                    i += 1
                else:
                    result.append("t")
            elif char == "g":
                if next_char == "h" and s[i + 2] if i + 2 < len(s) else "" not in "aeiou":
                    i += 1
                elif next_char in "iey":
                    result.append("j")
                else:
                    result.append("k")
            elif char == "k":
                if i > 0 and s[i - 1] == "c":
                    pass
                else:
                    result.append("k")
            elif char == "p":
                if next_char == "h":
                    result.append("f")
                    i += 1
                else:
                    result.append("p")
            elif char == "q":
                result.append("k")
            elif char == "s":
                if next_char == "h":
                    result.append("x")
                    i += 1
                else:
                    result.append("s")
            elif char == "t":
                if next_char == "h":
                    result.append("0")
                    i += 1
                elif next_char == "i" and s[i + 2] if i + 2 < len(s) else "" in "aoe":
                    result.append("x")
                else:
                    result.append("t")
            elif char == "v":
                result.append("f")
            elif char == "w":
                if next_char == "h":
                    result.append("w")
                    i += 1
            elif char == "x":
                result.append("ks")
            elif char == "y":
                result.append("y")
            elif char == "z":
                result.append("s")
            
            i += 1
        
        code = "".join(result)[:4].ljust(4, "0")
        self._phonetic_cache[s] = code
        return code
    
    def _soundex(self, s: str) -> str:
        """Calculate Soundex code."""
        if s in self._phonetic_cache:
            return self._phonetic_cache[s]
        
        s = s.upper()
        first = s[0]
        mapping = {
            "BFPV": "1", "CGJKQSXZ": "2", "DT": "3",
            "L": "4", "MN": "5", "R": "6"
        }
        
        coded = first
        prev_code = ""
        
        for char in s[1:]:
            for key, code in mapping.items():
                if char in key:
                    if code != prev_code:
                        coded += code
                        prev_code = code
                    break
        
        result = (coded + "000")[:4]
        self._phonetic_cache[s] = result
        return result
    
    def _sequence_matcher_ratio(self, s1: str, s2: str) -> float:
        """Calculate SequenceMatcher ratio."""
        from difflib import SequenceMatcher
        return SequenceMatcher(None, s1, s2).ratio()
    
    def _calculate_score(self, s1: str, s2: str, algorithm: MatchAlgorithm) -> float:
        """Calculate similarity score using specified algorithm."""
        s1_norm = self._normalize(s1, False)
        s2_norm = self._normalize(s2, False)
        
        if algorithm == MatchAlgorithm.LEVENSHTEIN:
            distance = self._levenshtein_distance(s1_norm, s2_norm)
            max_len = max(len(s1_norm), len(s2_norm))
            return 1.0 - (distance / max_len) if max_len > 0 else 1.0
        elif algorithm == MatchAlgorithm.JARO_WINKLER:
            return self._jaro_winkler_similarity(s1_norm, s2_norm)
        elif algorithm == MatchAlgorithm.LEVENSHTEIN_RATIO:
            return self._levenshtein_ratio(s1_norm, s2_norm)
        elif algorithm == MatchAlgorithm.PARTIAL_RATIO:
            return self._partial_ratio(s1_norm, s2_norm)
        elif algorithm == MatchAlgorithm.TOKEN_SORT_RATIO:
            return self._token_sort_ratio(s1_norm, s2_norm)
        elif algorithm == MatchAlgorithm.TOKEN_SET_RATIO:
            return self._token_set_ratio(s1_norm, s2_norm)
        elif algorithm == MatchAlgorithm.METAPHONE:
            return 1.0 if self._metaphone(s1_norm) == self._metaphone(s2_norm) else 0.0
        elif algorithm == MatchAlgorithm.SOUNDEX:
            return 1.0 if self._soundex(s1_norm) == self._soundex(s2_norm) else 0.0
        elif algorithm == MatchAlgorithm.SEQUENCE_MATCHER:
            return self._sequence_matcher_ratio(s1_norm, s2_norm)
        else:
            return self._levenshtein_ratio(s1_norm, s2_norm)
    
    def _match_one(
        self, source: Any, candidates: List[Any], algorithm: MatchAlgorithm,
        threshold: float, case_sensitive: bool
    ) -> Dict[str, Any]:
        """Find best match for source in candidates."""
        if not candidates:
            return {"matched": False, "score": 0.0, "result": None}
        
        source_str = str(source)
        best_score = 0.0
        best_match = None
        
        for candidate in candidates:
            score = self._calculate_score(source_str, str(candidate), algorithm)
            if score > best_score:
                best_score = score
                best_match = candidate
        
        return {
            "matched": best_score >= threshold,
            "score": best_score,
            "result": best_match,
            "algorithm": algorithm.value,
            "threshold": threshold
        }
    
    def _match_all(
        self, source: Any, candidates: List[Any], algorithm: MatchAlgorithm,
        threshold: float, case_sensitive: bool
    ) -> Dict[str, Any]:
        """Find all matches above threshold."""
        if not candidates:
            return {"matches": [], "count": 0}
        
        source_str = str(source)
        matches = []
        
        for candidate in candidates:
            score = self._calculate_score(source_str, str(candidate), algorithm)
            if score >= threshold:
                matches.append({
                    "candidate": candidate,
                    "score": score
                })
        
        matches.sort(key=lambda x: -x["score"])
        
        return {
            "matches": matches,
            "count": len(matches),
            "algorithm": algorithm.value,
            "threshold": threshold
        }
    
    def _group(
        self, items: List[Any], algorithm: MatchAlgorithm,
        threshold: float, case_sensitive: bool, field_name: Optional[str]
    ) -> Dict[str, Any]:
        """Group similar items together."""
        if not items:
            return {"groups": [], "total_groups": 0}
        
        groups: List[List[int]] = []
        used = set()
        
        for i, item1 in enumerate(items):
            if i in used:
                continue
            
            group = [i]
            used.add(i)
            
            for j, item2 in enumerate(items[i + 1:], i + 1):
                if j in used:
                    continue
                
                val1 = str(item1.get(field_name, item1) if isinstance(item1, dict) else item1)
                val2 = str(item2.get(field_name, item2) if isinstance(item2, dict) else item2)
                
                score = self._calculate_score(val1, val2, algorithm)
                if score >= threshold:
                    group.append(j)
                    used.add(j)
            
            groups.append(group)
        
        result_groups = []
        for gid, group_indices in enumerate(groups):
            group_items = [
                items[i] for i in group_indices
            ]
            result_groups.append({
                "group_id": gid,
                "size": len(group_items),
                "items": group_items
            })
        
        return {
            "groups": result_groups,
            "total_groups": len(result_groups),
            "algorithm": algorithm.value,
            "threshold": threshold
        }
    
    def _dedupe(
        self, items: List[Any], algorithm: MatchAlgorithm,
        threshold: float, case_sensitive: bool, field_name: Optional[str]
    ) -> Dict[str, Any]:
        """Deduplicate items using fuzzy matching."""
        groups_result = self._group(items, algorithm, threshold, case_sensitive, field_name)
        
        deduped = []
        removed = []
        
        for group in groups_result["groups"]:
            canonical = group["items"][0]
            deduped.append(canonical)
            if len(group["items"]) > 1:
                for removed_item in group["items"][1:]:
                    removed.append(removed_item)
        
        return {
            "deduped": deduped,
            "deduped_count": len(deduped),
            "removed": removed,
            "removed_count": len(removed),
            "original_count": len(items),
            "algorithm": algorithm.value,
            "threshold": threshold
        }
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate fuzzy match parameters."""
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return []
