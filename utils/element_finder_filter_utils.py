"""Element Finder Filter Utilities.

Advanced filtering and ranking for element search results.
Supports score-based matching, weighted attributes, and candidate ranking.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class ElementCandidate:
    """A candidate element from a search.

    Attributes:
        element_id: Unique element identifier.
        element_data: Element properties dictionary.
        match_score: Computed match score (0.0 to 1.0).
        matched_attributes: Which attributes matched the search criteria.
        matched_values: Values that matched.
    """

    element_id: str
    element_data: dict
    match_score: float = 0.0
    matched_attributes: list[str] = field(default_factory=list)
    matched_values: dict[str, str] = field(default_factory=dict)


@dataclass
class FilterRule:
    """A single filter rule for element matching.

    Attributes:
        attribute_name: Element attribute to check.
        expected_value: Expected value (for exact match).
        expected_pattern: Expected regex pattern (for pattern match).
        weight: Score weight for this attribute.
        required: Whether matching this attribute is required.
    """

    attribute_name: str
    expected_value: Optional[str] = None
    expected_pattern: Optional[str] = None
    weight: float = 1.0
    required: bool = False


class ElementFilter:
    """Filters element candidates based on rules.

    Example:
        filter = ElementFilter()
        filter.add_rule(FilterRule(attribute_name="role", expected_value="button"))
        filter.add_rule(FilterRule(attribute_name="enabled", expected_value="true", weight=0.5))
        results = filter.apply(candidates)
    """

    def __init__(self):
        """Initialize the element filter."""
        self._rules: list[FilterRule] = []

    def add_rule(self, rule: FilterRule) -> "ElementFilter":
        """Add a filter rule.

        Args:
            rule: FilterRule to add.

        Returns:
            Self for chaining.
        """
        self._rules.append(rule)
        return self

    def add_required_attribute(
        self,
        attribute: str,
        value: str,
    ) -> "ElementFilter":
        """Add a required attribute filter.

        Args:
            attribute: Attribute name.
            value: Expected value.

        Returns:
            Self for chaining.
        """
        self._rules.append(
            FilterRule(
                attribute_name=attribute,
                expected_value=value,
                required=True,
            )
        )
        return self

    def add_optional_attribute(
        self,
        attribute: str,
        value: str,
        weight: float = 1.0,
    ) -> "ElementFilter":
        """Add an optional attribute filter.

        Args:
            attribute: Attribute name.
            value: Expected value.
            weight: Score weight.

        Returns:
            Self for chaining.
        """
        self._rules.append(
            FilterRule(
                attribute_name=attribute,
                expected_value=value,
                weight=weight,
                required=False,
            )
        )
        return self

    def apply(
        self,
        candidates: list[ElementCandidate],
    ) -> list[ElementCandidate]:
        """Apply filters to candidates.

        Args:
            candidates: List of ElementCandidates to filter.

        Returns:
            Filtered and scored candidates.
        """
        results = []

        for candidate in candidates:
            passes, score, matched = self._evaluate(candidate)

            if passes:
                candidate.match_score = score
                candidate.matched_attributes = matched
                results.append(candidate)

        # Sort by score descending
        results.sort(key=lambda c: c.match_score, reverse=True)
        return results

    def _evaluate(
        self,
        candidate: ElementCandidate,
    ) -> tuple[bool, float, list[str]]:
        """Evaluate a candidate against all rules.

        Args:
            candidate: ElementCandidate to evaluate.

        Returns:
            Tuple of (passes_filter, total_score, matched_attributes).
        """
        total_score = 0.0
        matched_attributes = []
        required_failed = False

        for rule in self._rules:
            value = candidate.element_data.get(rule.attribute_name, "")

            if rule.expected_value is not None:
                if str(value).lower() == str(rule.expected_value).lower():
                    total_score += rule.weight
                    matched_attributes.append(rule.attribute_name)
                elif rule.required:
                    required_failed = True

            elif rule.expected_pattern is not None:
                import re
                if re.match(rule.expected_pattern, str(value)):
                    total_score += rule.weight
                    matched_attributes.append(rule.attribute_name)
                elif rule.required:
                    required_failed = True

        if required_failed:
            return False, 0.0, []

        return True, total_score, matched_attributes


class ElementRanker:
    """Ranks element candidates by multiple criteria.

    Example:
        ranker = ElementRanker()
        ranker.add_criterion("visibility", weight=2.0, higher_is_better=True)
        ranker.add_criterion("enabled", weight=1.5)
        ranked = ranker.rank(candidates)
    """

    def __init__(self):
        """Initialize the element ranker."""
        self._criteria: list[tuple[str, float, bool]] = []

    def add_criterion(
        self,
        attribute: str,
        weight: float = 1.0,
        higher_is_better: bool = True,
    ) -> "ElementRanker":
        """Add a ranking criterion.

        Args:
            attribute: Element attribute to rank by.
            weight: Weight for this criterion.
            higher_is_better: Whether higher values are better.

        Returns:
            Self for chaining.
        """
        self._criteria.append((attribute, weight, higher_is_better))
        return self

    def rank(
        self,
        candidates: list[ElementCandidate],
    ) -> list[ElementCandidate]:
        """Rank candidates by all criteria.

        Args:
            candidates: List of ElementCandidates to rank.

        Returns:
            Sorted list of candidates.
        """
        if not candidates:
            return []

        if not self._criteria:
            return candidates

        # Calculate normalized scores for each criterion
        for attribute, weight, higher_is_better in self._criteria:
            values = []
            for c in candidates:
                val = self._get_numeric_value(c.element_data.get(attribute, ""))
                values.append((c, val))

            if values:
                min_val = min(v for _, v in values)
                max_val = max(v for _, v in values)
                range_val = max_val - min_val if max_val != min_val else 1

                for c, val in values:
                    if higher_is_better:
                        normalized = (val - min_val) / range_val
                    else:
                        normalized = (max_val - val) / range_val
                    c.match_score += normalized * weight

        return sorted(candidates, key=lambda c: c.match_score, reverse=True)

    def _get_numeric_value(self, value: str) -> float:
        """Convert attribute value to numeric for comparison.

        Args:
            value: Attribute value.

        Returns:
            Numeric value.
        """
        if isinstance(value, (int, float)):
            return float(value)

        val_lower = str(value).lower()
        if val_lower in ("true", "yes", "enabled", "visible"):
            return 1.0
        elif val_lower in ("false", "no", "disabled", "hidden"):
            return 0.0

        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0


class ElementFinder:
    """High-level element finding with filtering and ranking.

    Combines filtering and ranking for powerful element search.

    Example:
        finder = ElementFinder(all_elements)
        result = finder.find(
            role="button",
            enabled=True,
            name_contains="Submit",
        )
    """

    def __init__(self, elements: list[dict]):
        """Initialize the finder.

        Args:
            elements: List of element data dictionaries.
        """
        self._elements = elements
        self._filter = ElementFilter()
        self._ranker = ElementRanker()

    def where(
        self,
        attribute: str,
        value: str,
        required: bool = True,
        weight: float = 1.0,
    ) -> "ElementFinder":
        """Add a where clause.

        Args:
            attribute: Element attribute.
            value: Expected value.
            required: Whether this is required.
            weight: Score weight.

        Returns:
            Self for chaining.
        """
        if required:
            self._filter.add_required_attribute(attribute, value)
        else:
            self._filter.add_optional_attribute(attribute, value, weight)
        return self

    def where_contains(
        self,
        attribute: str,
        substring: str,
        weight: float = 1.0,
    ) -> "ElementFinder":
        """Add a contains filter.

        Args:
            attribute: Element attribute.
            substring: Substring to match.
            weight: Score weight.

        Returns:
            Self for chaining.
        """
        import re
        pattern = f".*{re.escape(substring)}.*"
        self._filter.add_rule(
            FilterRule(
                attribute_name=attribute,
                expected_pattern=pattern,
                weight=weight,
            )
        )
        return self

    def order_by(
        self,
        attribute: str,
        weight: float = 1.0,
        higher_is_better: bool = True,
    ) -> "ElementFinder":
        """Add an ordering criterion.

        Args:
            attribute: Element attribute.
            weight: Weight for ranking.
            higher_is_better: Whether higher values rank higher.

        Returns:
            Self for chaining.
        """
        self._ranker.add_criterion(attribute, weight, higher_is_better)
        return self

    def find_first(self) -> Optional[ElementCandidate]:
        """Find the first matching element.

        Returns:
            ElementCandidate or None.
        """
        results = self.find()
        return results[0] if results else None

    def find(self) -> list[ElementCandidate]:
        """Execute the find operation.

        Returns:
            List of matching ElementCandidates, ranked.
        """
        # Convert elements to candidates
        candidates = [
            ElementCandidate(
                element_id=e.get("id", str(i)),
                element_data=e,
            )
            for i, e in enumerate(self._elements)
        ]

        # Apply filters
        filtered = self._filter.apply(candidates)

        # Apply ranking
        ranked = self._ranker.rank(filtered)

        return ranked
