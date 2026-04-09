"""Element matching action for UI automation.

Matches UI elements using various strategies:
- By ID, class, accessibility
- By visual similarity
- By position
- Composite matching
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable


class MatchStrategy(Enum):
    """Element matching strategies."""
    ID = auto()
    ACCESSIBILITY_ID = auto()
    TEXT = auto()
    CONTENT_DESC = auto()
    CLASS_NAME = auto()
    XPATH = auto()
    CSS_SELECTOR = auto()
    VISUAL = auto()
    POSITION = auto()
    FUZZY = auto()


@dataclass
class MatchCriteria:
    """Criteria for element matching."""
    strategy: MatchStrategy
    value: str
    timeout: float = 5.0
    index: int = 0  # For multiple matches
    parent: MatchCriteria | None = None
    siblings: list[MatchCriteria] = field(default_factory=list)


@dataclass
class ElementMatch:
    """Element match result."""
    element_id: str
    match_score: float  # 0.0 to 1.0
    criteria: MatchCriteria
    element_data: dict = field(default_factory=dict)


class ElementMatcher:
    """Matches UI elements using various strategies.

    Features:
    - Single and composite matching
    - Fuzzy matching
    - Visual similarity matching
    - Match scoring and ranking
    """

    def __init__(self):
        self._query_func: Callable | None = None
        self._visual_func: Callable | None = None
        self._cache: dict[str, list[ElementMatch]] = {}
        self._cache_ttl: float = 30.0
        self._cache_times: dict[str, float] = {}

    def set_query_func(self, func: Callable) -> None:
        """Set element query function.

        Args:
            func: Function(criteria) -> list[element_dict]
        """
        self._query_func = func

    def set_visual_func(self, func: Callable) -> None:
        """Set visual matching function.

        Args:
            func: Function(template_image, threshold) -> list[match]
        """
        self._visual_func = func

    def find_element(self, criteria: MatchCriteria) -> ElementMatch | None:
        """Find single element matching criteria.

        Args:
            criteria: Match criteria

        Returns:
            First matching element or None
        """
        matches = self.find_elements(criteria)
        return matches[0] if matches else None

    def find_elements(self, criteria: MatchCriteria) -> list[ElementMatch]:
        """Find all elements matching criteria.

        Args:
            criteria: Match criteria

        Returns:
            List of matching elements
        """
        cache_key = self._criteria_to_key(criteria)

        # Check cache
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        start = time.time()
        results: list[ElementMatch] = []

        if criteria.strategy == MatchStrategy.VISUAL:
            results = self._find_by_visual(criteria)
        elif criteria.strategy == MatchStrategy.POSITION:
            results = self._find_by_position(criteria)
        elif criteria.strategy == MatchStrategy.FUZZY:
            results = self._find_by_fuzzy(criteria)
        else:
            results = self._find_by_query(criteria)

        # Filter by timeout
        results = [r for r in results if time.time() - start < criteria.timeout]

        # Cache results
        self._cache[cache_key] = results
        self._cache_times[cache_key] = time.time()

        return results

    def _find_by_query(self, criteria: MatchCriteria) -> list[ElementMatch]:
        """Find by query function."""
        if not self._query_func:
            return []

        elements = self._query_func(criteria)
        return [
            ElementMatch(
                element_id=e.get("id", e.get("element_id", "")),
                match_score=self._calculate_score(e, criteria),
                criteria=criteria,
                element_data=e,
            )
            for e in elements
        ]

    def _find_by_visual(self, criteria: MatchCriteria) -> list[ElementMatch]:
        """Find by visual similarity."""
        if not self._visual_func:
            return []

        matches = self._visual_func(criteria.value, 0.8)
        return [
            ElementMatch(
                element_id=m.get("element_id", ""),
                match_score=m.get("score", 0.0),
                criteria=criteria,
                element_data=m,
            )
            for m in matches
        ]

    def _find_by_position(self, criteria: MatchCriteria) -> list[ElementMatch]:
        """Find by position."""
        if not self._query_func:
            return []

        # Parse position from criteria.value (e.g., "100,200")
        try:
            x, y = map(float, criteria.value.split(","))
        except ValueError:
            return []

        elements = self._query_func(criteria)
        results = []

        for e in elements:
            bounds = e.get("bounds", {})
            ex = bounds.get("x", 0)
            ey = bounds.get("y", 0)
            ew = bounds.get("width", 0)
            eh = bounds.get("height", 0)

            # Check if position is within element
            if ex <= x <= ex + ew and ey <= y <= ey + eh:
                results.append(ElementMatch(
                    element_id=e.get("id", ""),
                    match_score=1.0,
                    criteria=criteria,
                    element_data=e,
                ))

        return results

    def _find_by_fuzzy(self, criteria: MatchCriteria) -> list[ElementMatch]:
        """Find by fuzzy text matching."""
        if not self._query_func:
            return []

        elements = self._query_func(criteria)
        results = []

        for e in elements:
            text = e.get("text", "") or e.get("content_desc", "") or ""
            score = self._fuzzy_score(text, criteria.value)
            if score > 0.5:
                results.append(ElementMatch(
                    element_id=e.get("id", ""),
                    match_score=score,
                    criteria=criteria,
                    element_data=e,
                ))

        results.sort(key=lambda r: r.match_score, reverse=True)
        return results

    def _calculate_score(self, element: dict, criteria: MatchCriteria) -> float:
        """Calculate match score for element."""
        value = criteria.value.lower()

        if criteria.strategy == MatchStrategy.ID:
            return 1.0 if element.get("id", "").lower() == value else 0.0
        elif criteria.strategy == MatchStrategy.TEXT:
            return 1.0 if element.get("text", "").lower() == value else 0.0
        elif criteria.strategy == MatchStrategy.CONTENT_DESC:
            return 1.0 if element.get("content_desc", "").lower() == value else 0.0
        elif criteria.strategy == MatchStrategy.CLASS_NAME:
            return 1.0 if element.get("class_name", "").lower() == value else 0.0

        return 0.0

    def _fuzzy_score(self, text: str, pattern: str) -> float:
        """Calculate fuzzy match score."""
        if not text or not pattern:
            return 0.0

        text = text.lower()
        pattern = pattern.lower()

        if pattern in text:
            return 1.0

        # Simple character overlap score
        text_chars = set(text)
        pattern_chars = set(pattern)
        overlap = len(text_chars & pattern_chars)
        return overlap / len(pattern_chars) if pattern_chars else 0.0

    def _criteria_to_key(self, criteria: MatchCriteria) -> str:
        """Convert criteria to cache key."""
        return f"{criteria.strategy.name}:{criteria.value}:{criteria.index}"

    def _is_cache_valid(self, key: str) -> bool:
        """Check if cache entry is valid."""
        if key not in self._cache:
            return False
        if key not in self._cache_times:
            return False
        return time.time() - self._cache_times[key] < self._cache_ttl

    def clear_cache(self) -> None:
        """Clear match cache."""
        self._cache.clear()
        self._cache_times.clear()

    def find_composite(
        self,
        primary: MatchCriteria,
        fallback: list[MatchCriteria] | None = None,
    ) -> ElementMatch | None:
        """Find element with fallback criteria.

        Args:
            primary: Primary match criteria
            fallback: List of fallback criteria

        Returns:
            First match from primary or fallbacks
        """
        # Try primary
        match = self.find_element(primary)
        if match:
            return match

        # Try fallbacks
        if fallback:
            for fb in fallback:
                match = self.find_element(fb)
                if match:
                    return match

        return None


def create_element_matcher() -> ElementMatcher:
    """Create element matcher."""
    return ElementMatcher()
