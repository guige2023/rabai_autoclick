"""Accessibility Query and Search Utilities.

Query and search UI elements using accessibility attributes.
Supports complex queries with multiple conditions and result ranking.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class QueryOperator(Enum):
    """Operators for query conditions."""

    EQUALS = auto()
    NOT_EQUALS = auto()
    CONTAINS = auto()
    STARTS_WITH = auto()
    ENDS_WITH = auto()
    MATCHES = auto()
    GREATER_THAN = auto()
    LESS_THAN = auto()
    EXISTS = auto()
    NOT_EXISTS = auto()


@dataclass
class QueryCondition:
    """A single condition in a query.

    Attributes:
        attribute: Element attribute to check.
        operator: QueryOperator to use.
        value: Value to compare against.
        case_sensitive: Whether comparison is case-sensitive.
    """

    attribute: str
    operator: QueryOperator
    value: Any = None
    case_sensitive: bool = False


@dataclass
class AccessibilityQuery:
    """A query for finding accessibility elements.

    Attributes:
        conditions: List of conditions (AND logic).
        role: Optional role filter.
        name: Optional name filter.
        max_results: Maximum results to return.
    """

    conditions: list[QueryCondition] = field(default_factory=list)
    role: Optional[str] = None
    name: Optional[str] = None
    max_results: int = 100

    def add_condition(
        self,
        attribute: str,
        operator: QueryOperator,
        value: Any = None,
    ) -> "AccessibilityQuery":
        """Add a condition to the query.

        Args:
            attribute: Attribute to check.
            operator: Comparison operator.
            value: Value to compare.

        Returns:
            Self for chaining.
        """
        self.conditions.append(
            QueryCondition(attribute=attribute, operator=operator, value=value)
        )
        return self

    def where_role(self, role: str) -> "AccessibilityQuery":
        """Add role filter.

        Args:
            role: Role to match.

        Returns:
            Self for chaining.
        """
        self.role = role
        return self

    def where_name(self, name: str) -> "AccessibilityQuery":
        """Add name filter.

        Args:
            name: Name to match.

        Returns:
            Self for chaining.
        """
        self.name = name
        return self


@dataclass
class QueryResult:
    """Result of a query execution.

    Attributes:
        element: Element data dictionary.
        matched_conditions: Conditions that matched.
        score: Match score (0.0 to 1.0).
    """

    element: dict
    matched_conditions: list[str] = field(default_factory=list)
    score: float = 0.0


class AccessibilityQueryEngine:
    """Executes accessibility queries against element trees.

    Example:
        engine = AccessibilityQueryEngine(elements)
        results = engine.execute(
            AccessibilityQuery()
            .where_role("button")
            .add_condition("enabled", QueryOperator.EQUALS, True)
        )
    """

    def __init__(self, elements: list[dict]):
        """Initialize the query engine.

        Args:
            elements: List of element data dictionaries.
        """
        self._elements = elements

    def execute(self, query: AccessibilityQuery) -> list[QueryResult]:
        """Execute a query.

        Args:
            query: AccessibilityQuery to execute.

        Returns:
            List of QueryResults, ranked by score.
        """
        results = []

        for element in self._elements:
            result = self._evaluate_element(element, query)
            if result:
                results.append(result)

        # Sort by score descending
        results.sort(key=lambda r: r.score, reverse=True)

        # Apply limit
        return results[: query.max_results]

    def _evaluate_element(
        self,
        element: dict,
        query: AccessibilityQuery,
    ) -> Optional[QueryResult]:
        """Evaluate a single element against query.

        Args:
            element: Element data dictionary.
            query: Query to evaluate.

        Returns:
            QueryResult or None if element doesn't match.
        """
        matched_conditions = []
        total_score = 0.0

        # Check role condition
        if query.role:
            role = element.get("role", "")
            if role.lower() == query.role.lower():
                matched_conditions.append("role")
                total_score += 1.0
            else:
                return None

        # Check name condition
        if query.name:
            name = element.get("name", "")
            if query.name.lower() in name.lower():
                matched_conditions.append("name")
                total_score += 0.8

        # Check all conditions
        for condition in query.conditions:
            if self._evaluate_condition(element, condition):
                matched_conditions.append(condition.attribute)
                total_score += 0.5

        if matched_conditions:
            return QueryResult(
                element=element,
                matched_conditions=matched_conditions,
                score=total_score,
            )

        return None

    def _evaluate_condition(
        self,
        element: dict,
        condition: QueryCondition,
    ) -> bool:
        """Evaluate a single condition.

        Args:
            element: Element data dictionary.
            condition: Condition to evaluate.

        Returns:
            True if condition matches.
        """
        value = element.get(condition.attribute)

        if condition.operator == QueryOperator.EQUALS:
            cmp_value = value if condition.case_sensitive else str(value).lower() if value else ""
            cmp_expected = condition.value if condition.case_sensitive else str(condition.value).lower() if condition.value else ""
            return cmp_value == cmp_expected

        elif condition.operator == QueryOperator.NOT_EQUALS:
            cmp_value = value if condition.case_sensitive else str(value).lower() if value else ""
            cmp_expected = condition.value if condition.case_sensitive else str(condition.value).lower() if condition.value else ""
            return cmp_value != cmp_expected

        elif condition.operator == QueryOperator.CONTAINS:
            str_value = str(value) if value else ""
            str_expected = str(condition.value) if condition.value else ""
            if not condition.case_sensitive:
                str_value = str_value.lower()
                str_expected = str_expected.lower()
            return str_expected in str_value

        elif condition.operator == QueryOperator.STARTS_WITH:
            str_value = str(value) if value else ""
            str_expected = str(condition.value) if condition.value else ""
            if not condition.case_sensitive:
                str_value = str_value.lower()
                str_expected = str_expected.lower()
            return str_value.startswith(str_expected)

        elif condition.operator == QueryOperator.ENDS_WITH:
            str_value = str(value) if value else ""
            str_expected = str(condition.value) if condition.value else ""
            if not condition.case_sensitive:
                str_value = str_value.lower()
                str_expected = str_expected.lower()
            return str_value.endswith(str_expected)

        elif condition.operator == QueryOperator.MATCHES:
            pattern = str(condition.value) if condition.value else ""
            str_value = str(value) if value else ""
            flags = 0 if condition.case_sensitive else re.IGNORECASE
            return bool(re.search(pattern, str_value, flags))

        elif condition.operator == QueryOperator.GREATER_THAN:
            try:
                return float(value) > float(condition.value)
            except (TypeError, ValueError):
                return False

        elif condition.operator == QueryOperator.LESS_THAN:
            try:
                return float(value) < float(condition.value)
            except (TypeError, ValueError):
                return False

        elif condition.operator == QueryOperator.EXISTS:
            return value is not None

        elif condition.operator == QueryOperator.NOT_EXISTS:
            return value is None

        return False


class AccessibilitySearchBuilder:
    """Builder for accessibility search queries.

    Example:
        builder = AccessibilitySearchBuilder()
        results = builder.find_buttons().where_enabled(True).execute()
    """

    def __init__(self, elements: list[dict]):
        """Initialize the builder.

        Args:
            elements: List of element data dictionaries.
        """
        self._elements = elements
        self._engine = AccessibilityQueryEngine(elements)
        self._query = AccessibilityQuery()

    @classmethod
    def for_elements(cls, elements: list[dict]) -> "AccessibilitySearchBuilder":
        """Create builder for elements.

        Args:
            elements: List of element data dictionaries.

        Returns:
            AccessibilitySearchBuilder instance.
        """
        return cls(elements)

    def find_all(self) -> "AccessibilitySearchBuilder":
        """Find all elements.

        Returns:
            Self for chaining.
        """
        return self

    def find_buttons(self) -> "AccessibilitySearchBuilder":
        """Find button elements.

        Returns:
            Self for chaining.
        """
        self._query.where_role("button")
        return self

    def find_links(self) -> "AccessibilitySearchBuilder":
        """Find link elements.

        Returns:
            Self for chaining.
        """
        self._query.where_role("link")
        return self

    def find_inputs(self) -> "AccessibilitySearchBuilder":
        """Find input elements.

        Returns:
            Self for chaining.
        """
        self._query.where_role("textbox")
        return self

    def find_checkboxes(self) -> "AccessibilitySearchBuilder":
        """Find checkbox elements.

        Returns:
            Self for chaining.
        """
        self._query.where_role("checkbox")
        return self

    def find_by_label(self, label: str) -> "AccessibilitySearchBuilder":
        """Find elements with a specific label.

        Args:
            label: Label to search for.

        Returns:
            Self for chaining.
        """
        self._query.add_condition("label", QueryOperator.CONTAINS, label)
        return self

    def where_enabled(self, enabled: bool = True) -> "AccessibilitySearchBuilder":
        """Filter by enabled state.

        Args:
            enabled: Enabled state to match.

        Returns:
            Self for chaining.
        """
        self._query.add_condition("enabled", QueryOperator.EQUALS, enabled)
        return self

    def where_visible(self, visible: bool = True) -> "AccessibilitySearchBuilder":
        """Filter by visibility.

        Args:
            visible: Visible state to match.

        Returns:
            Self for chaining.
        """
        self._query.add_condition("visible", QueryOperator.EQUALS, visible)
        return self

    def where_focused(self, focused: bool = True) -> "AccessibilitySearchBuilder":
        """Filter by focus state.

        Args:
            focused: Focus state to match.

        Returns:
            Self for chaining.
        """
        self._query.add_condition("focused", QueryOperator.EQUALS, focused)
        return self

    def where_name(self, name: str) -> "AccessibilitySearchBuilder":
        """Filter by exact name.

        Args:
            name: Name to match.

        Returns:
            Self for chaining.
        """
        self._query.where_name(name)
        return self

    def limit(self, max_results: int) -> "AccessibilitySearchBuilder":
        """Set maximum results.

        Args:
            max_results: Maximum number of results.

        Returns:
            Self for chaining.
        """
        self._query.max_results = max_results
        return self

    def execute(self) -> list[QueryResult]:
        """Execute the search.

        Returns:
            List of QueryResults.
        """
        return self._engine.execute(self._query)

    def first(self) -> Optional[QueryResult]:
        """Execute and return first result.

        Returns:
            First QueryResult or None.
        """
        results = self.limit(1).execute()
        return results[0] if results else None
