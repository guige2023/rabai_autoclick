"""
Element similarity and matching utilities for UI automation.

Provides functions for comparing UI elements, computing similarity
scores, and finding best matches across element collections.
"""

from __future__ import annotations

import re
from typing import List, Dict, Optional, Tuple, Any, Callable
from dataclasses import dataclass
from difflib import SequenceMatcher


TextMatch = Tuple[str, str, float]  # (pattern, text, score)


@dataclass
class SimilarityResult:
    """Result of element similarity comparison."""
    score: float  # 0.0 to 1.0
    matched_fields: Dict[str, float]
    unmatched_fields: List[str]
    details: str


class ElementSimilarity:
    """Compute similarity between UI elements."""
    
    def __init__(
        self,
        text_weight: float = 0.3,
        bounds_weight: float = 0.2,
        role_weight: float = 0.2,
        attr_weight: float = 0.3,
        threshold: float = 0.7,
    ) -> None:
        """Initialize element similarity calculator.
        
        Args:
            text_weight: Weight for text content similarity
            bounds_weight: Weight for position/size similarity
            role_weight: Weight for role/type similarity
            attr_weight: Weight for attribute similarity
            threshold: Minimum score to consider a match
        """
        self.text_weight = text_weight
        self.bounds_weight = bounds_weight
        self.role_weight = role_weight
        self.attr_weight = attr_weight
        self.threshold = threshold
    
    def compare(self, elem1: dict, elem2: dict) -> SimilarityResult:
        """Compare two UI elements.
        
        Args:
            elem1: First element dict
            elem2: Second element dict
        
        Returns:
            SimilarityResult with score and details
        """
        matched: Dict[str, float] = {}
        unmatched: List[str] = []
        
        scores = []
        
        text_score = self._compare_text(elem1.get('text', ''), elem2.get('text', ''))
        if text_score > 0:
            matched['text'] = text_score
            scores.append((text_score, self.text_weight))
        else:
            unmatched.append('text')
        
        bounds_score = self._compare_bounds(
            elem1.get('bounds'),
            elem2.get('bounds'),
        )
        if bounds_score is not None:
            matched['bounds'] = bounds_score
            scores.append((bounds_score, self.bounds_weight))
        else:
            unmatched.append('bounds')
        
        role_score = self._compare_role(elem1.get('role'), elem2.get('role'))
        if role_score is not None:
            matched['role'] = role_score
            scores.append((role_score, self.role_weight))
        else:
            unmatched.append('role')
        
        attr_score = self._compare_attrs(elem1, elem2)
        if attr_score > 0:
            matched['attributes'] = attr_score
            scores.append((attr_score, self.attr_weight))
        
        total_weight = sum(w for _, w in scores)
        if total_weight > 0:
            final_score = sum(s * w for s, w in scores) / total_weight
        else:
            final_score = 0.0
        
        return SimilarityResult(
            score=final_score,
            matched_fields=matched,
            unmatched_fields=unmatched,
            details=f"Score: {final_score:.2f}, Matched: {list(matched.keys())}",
        )
    
    def _compare_text(self, text1: str, text2: str) -> float:
        """Compare text content."""
        if not text1 or not text2:
            return 0.0
        
        if text1 == text2:
            return 1.0
        
        return SequenceMatcher(None, text1, text2).ratio()
    
    def _compare_bounds(
        self,
        bounds1: Optional[Tuple[int, int, int, int]],
        bounds2: Optional[Tuple[int, int, int, int]],
    ) -> Optional[float]:
        """Compare element bounds."""
        if bounds1 is None or bounds2 is None:
            return None
        
        x1, y1, w1, h1 = bounds1
        x2, y2, w2, h2 = bounds2
        
        area1 = w1 * h1
        area2 = w2 * h2
        
        if area1 == 0 or area2 == 0:
            return 0.0
        
        overlap_x = max(0, min(x1 + w1, x2 + w2) - max(x1, x2))
        overlap_y = max(0, min(y1 + h1, y2 + h2) - max(y1, y2))
        overlap = overlap_x * overlap_y
        
        iou = overlap / (area1 + area2 - overlap)
        
        center_dist = ((x1 + w1/2 - x2 - w2/2) ** 2 + 
                       (y1 + h1/2 - y2 - h2/2) ** 2) ** 0.5
        max_dist = (w1**2 + h1**2 + w2**2 + h2**2) ** 0.5
        
        size_sim = min(area1, area2) / max(area1, area2)
        pos_sim = max(0, 1 - center_dist / max_dist)
        
        return 0.4 * iou + 0.3 * size_sim + 0.3 * pos_sim
    
    def _compare_role(
        self,
        role1: Optional[str],
        role2: Optional[str],
    ) -> Optional[float]:
        """Compare element roles."""
        if role1 is None or role2 is None:
            return None
        
        if role1 == role2:
            return 1.0
        
        role_hierarchy: Dict[str, List[str]] = {
            'button': ['button', 'clickable', 'control'],
            'input': ['input', 'textfield', 'textarea', 'textbox'],
            'container': ['container', 'group', 'panel', 'region'],
        }
        
        for base, variants in role_hierarchy.items():
            if role1 in variants and role2 in variants:
                return 0.8
        
        return 0.0
    
    def _compare_attrs(self, elem1: dict, elem2: dict) -> float:
        """Compare element attributes."""
        compare_keys = {
            'enabled', 'visible', 'focused', 'selected',
            'checked', 'expanded', 'pressed',
        }
        
        matches = 0
        total = 0
        
        for key in compare_keys:
            if key in elem1 or key in elem2:
                total += 1
                if elem1.get(key) == elem2.get(key):
                    matches += 1
        
        if total == 0:
            return 0.0
        
        return matches / total


def find_best_match(
    target: dict,
    candidates: List[dict],
    similarity: Optional[ElementSimilarity] = None,
) -> Tuple[Optional[dict], float]:
    """Find best matching element from candidates.
    
    Args:
        target: Target element to match
        candidates: List of candidate elements
        similarity: ElementSimilarity instance (creates new if None)
    
    Returns:
        (best_match, score) or (None, 0.0) if no match above threshold
    """
    if not candidates:
        return None, 0.0
    
    if similarity is None:
        similarity = ElementSimilarity()
    
    best: Optional[dict] = None
    best_score = 0.0
    
    for candidate in candidates:
        result = similarity.compare(target, candidate)
        if result.score > best_score:
            best_score = result.score
            best = candidate
    
    if best_score < similarity.threshold:
        return None, 0.0
    
    return best, best_score


def rank_matches(
    target: dict,
    candidates: List[dict],
    similarity: Optional[ElementSimilarity] = None,
) -> List[Tuple[dict, float]]:
    """Rank all candidates by similarity to target.
    
    Args:
        target: Target element
        candidates: Candidate elements
        similarity: ElementSimilarity instance
    
    Returns:
        List of (element, score) sorted by score descending
    """
    if similarity is None:
        similarity = ElementSimilarity()
    
    results = []
    for candidate in candidates:
        result = similarity.compare(target, candidate)
        results.append((candidate, result.score))
    
    return sorted(results, key=lambda x: x[1], reverse=True)


def fuzzy_text_match(
    pattern: str,
    text: str,
    case_sensitive: bool = False,
) -> float:
    """Fuzzy match pattern against text.
    
    Args:
        pattern: Pattern to match
        text: Text to search
        case_sensitive: Whether to respect case
    
    Returns:
        Similarity score 0.0 to 1.0
    """
    if not pattern or not text:
        return 0.0
    
    if case_sensitive:
        pattern_lower = pattern
        text_lower = text
    else:
        pattern_lower = pattern.lower()
        text_lower = text.lower()
    
    if pattern_lower == text_lower:
        return 1.0
    
    if pattern_lower in text_lower:
        return 0.8 + 0.2 * (len(pattern_lower) / len(text_lower))
    
    return SequenceMatcher(None, pattern_lower, text_lower).ratio()


def wildcard_match(pattern: str, text: str, case_sensitive: bool = False) -> bool:
    """Match text against wildcard pattern (* and ?).
    
    Args:
        pattern: Wildcard pattern
        text: Text to match
        case_sensitive: Whether to respect case
    
    Returns:
        True if text matches pattern
    """
    if not case_sensitive:
        pattern = pattern.lower()
        text = text.lower()
    
    regex_pattern = re.escape(pattern)
    regex_pattern = regex_pattern.replace(r'\*', '.*').replace(r'\?', '.')
    regex_pattern = f'^{regex_pattern}$'
    
    return bool(re.match(regex_pattern, text))


def find_similar_elements(
    elements: List[dict],
    reference: dict,
    min_score: float = 0.5,
    similarity: Optional[ElementSimilarity] = None,
) -> List[Tuple[dict, float]]:
    """Find all elements similar to reference.
    
    Args:
        elements: List of elements to search
        reference: Reference element
        min_score: Minimum similarity score
        similarity: ElementSimilarity instance
    
    Returns:
        List of (element, score) for matches above threshold
    """
    if similarity is None:
        similarity = ElementSimilarity()
    
    results = []
    for elem in elements:
        result = similarity.compare(reference, elem)
        if result.score >= min_score:
            results.append((elem, result.score))
    
    return sorted(results, key=lambda x: x[1], reverse=True)


@dataclass
class MatchGroup:
    """Group of matched elements with shared characteristics."""
    key: str
    elements: List[dict]
    average_score: float


def group_by_similarity(
    elements: List[dict],
    key_func: Callable[[dict], str],
    threshold: float = 0.8,
) -> List[MatchGroup]:
    """Group elements by similarity using key function.
    
    Args:
        elements: Elements to group
        key_func: Function to extract grouping key from element
        threshold: Similarity threshold for same group
    
    Returns:
        List of MatchGroup
    """
    groups: Dict[str, List[dict]] = {}
    
    for elem in elements:
        key = key_func(elem)
        if key not in groups:
            groups[key] = []
        groups[key].append(elem)
    
    return [
        MatchGroup(
            key=key,
            elements=elems,
            average_score=1.0,
        )
        for key, elems in groups.items()
    ]


def dedupe_elements(
    elements: List[dict],
    similarity: Optional[ElementSimilarity] = None,
    threshold: float = 0.9,
) -> List[dict]:
    """Remove duplicate elements based on similarity.
    
    Args:
        elements: Elements to dedupe
        similarity: ElementSimilarity instance
        threshold: Similarity threshold for duplicates
    
    Returns:
        Deduplicated list preserving original order
    """
    if similarity is None:
        similarity = ElementSimilarity()
    
    result: List[dict] = []
    
    for elem in elements:
        is_duplicate = False
        
        for existing in result:
            result_sim = similarity.compare(elem, existing)
            if result_sim.score >= threshold:
                is_duplicate = True
                break
        
        if not is_duplicate:
            result.append(elem)
    
    return result
