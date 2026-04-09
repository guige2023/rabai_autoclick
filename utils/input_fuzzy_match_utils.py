"""
Input element fuzzy matching utilities.

Fuzzy matching for UI element identification.
"""

from __future__ import annotations

import re
from typing import Optional, Any
from dataclasses import dataclass


@dataclass
class MatchScore:
    """Score for a fuzzy match."""
    element_id: str
    score: float
    matched_fields: dict[str, float]


class ElementFuzzyMatcher:
    """Fuzzy matcher for UI element identification."""
    
    def __init__(self, threshold: float = 0.7):
        self.threshold = threshold
        self._weights = {
            "text": 0.3,
            "accessibility_label": 0.25,
            "accessibility_hint": 0.15,
            "class_name": 0.2,
            "resource_id": 0.1
        }
    
    def set_weight(self, field: str, weight: float) -> None:
        """Set weight for a specific field."""
        self._weights[field] = weight
    
    def match(
        self,
        target: str,
        candidates: list[dict[str, Any]]
    ) -> Optional[MatchScore]:
        """Find best fuzzy match for target among candidates."""
        best_score: Optional[MatchScore] = None
        best_value = 0.0
        
        for candidate in candidates:
            score = self._calculate_score(target, candidate)
            if score and score.score > best_value:
                best_value = score.score
                best_score = score
        
        if best_score and best_score.score >= self.threshold:
            return best_score
        
        return None
    
    def match_all(
        self,
        target: str,
        candidates: list[dict[str, Any]]
    ) -> list[MatchScore]:
        """Get all matches above threshold."""
        results = []
        
        for candidate in candidates:
            score = self._calculate_score(target, candidate)
            if score and score.score >= self.threshold:
                results.append(score)
        
        return sorted(results, key=lambda s: s.score, reverse=True)
    
    def _calculate_score(
        self,
        target: str,
        candidate: dict[str, Any]
    ) -> Optional[MatchScore]:
        """Calculate fuzzy match score."""
        matched_fields = {}
        total_score = 0.0
        total_weight = 0.0
        
        for field, weight in self._weights.items():
            if field in candidate and candidate[field]:
                field_score = self._fuzzy_compare(target, str(candidate[field]))
                matched_fields[field] = field_score
                total_score += field_score * weight
                total_weight += weight
        
        if total_weight > 0:
            total_score /= total_weight
        
        element_id = candidate.get("element_id") or candidate.get("id") or str(hash(str(candidate)))
        
        return MatchScore(
            element_id=element_id,
            score=total_score,
            matched_fields=matched_fields
        )
    
    def _fuzzy_compare(self, s1: str, s2: str) -> float:
        """Compare two strings with fuzzy matching."""
        s1_lower = s1.lower().strip()
        s2_lower = s2.lower().strip()
        
        if s1_lower == s2_lower:
            return 1.0
        
        if s1_lower in s2_lower:
            return 0.8 + 0.2 * (len(s1_lower) / len(s2_lower))
        
        if s2_lower in s1_lower:
            return 0.7 + 0.3 * (len(s2_lower) / len(s1_lower))
        
        levenshtein_score = 1.0 - (self._levenshtein_distance(s1_lower, s2_lower) / max(len(s1_lower), len(s2_lower)))
        
        token_score = self._token_similarity(s1_lower, s2_lower)
        
        return max(levenshtein_score, token_score) * 0.6
    
    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """Calculate Levenshtein distance."""
        if len(s1) < len(s2):
            return self._levenshtein_distance(s2, s1)
        
        if len(s2) == 0:
            return len(s1)
        
        previous_row = list(range(len(s2) + 1))
        
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        return previous_row[-1]
    
    def _token_similarity(self, s1: str, s2: str) -> float:
        """Calculate token-based similarity."""
        tokens1 = set(re.split(r'[\s_\-]+', s1))
        tokens2 = set(re.split(r'[\s_\-]+', s2))
        
        if not tokens1 or not tokens2:
            return 0.0
        
        intersection = tokens1 & tokens2
        union = tokens1 | tokens2
        
        return len(intersection) / len(union) if union else 0.0


class WildcardMatcher:
    """Matcher with wildcard support."""
    
    @staticmethod
    def match(pattern: str, text: str) -> bool:
        """Match text against pattern with wildcards."""
        regex = re.escape(pattern)
        regex = regex.replace(r'\*', '.*')
        regex = regex.replace(r'\?', '.')
        regex = f'^{regex}$'
        
        return bool(re.match(regex, text, re.IGNORECASE))
    
    @staticmethod
    def match_with_score(pattern: str, text: str) -> float:
        """Match with confidence score."""
        if WildcardMatcher.match(pattern, text):
            if pattern.lower() == text.lower():
                return 1.0
            return 0.8
        return 0.0
