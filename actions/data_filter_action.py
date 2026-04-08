"""Data Filter Action Module.

Provides data filtering, searching, and querying
capabilities for structured and unstructured data.
"""

from typing import Any, Dict, List, Optional, Callable, Union, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
import re
import json
from datetime import datetime


T = TypeVar("T")


class FilterOperator(Enum):
    """Filter comparison operators."""
    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GE = "ge"
    LT = "lt"
    LE = "le"
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    REGEX = "regex"
    EXISTS = "exists"
    BETWEEN = "between"


@dataclass
class FilterCondition:
    """A single filter condition."""
    field: str
    operator: FilterOperator
    value: Any = None
    value2: Any = None
    negate: bool = False

    def evaluate(self, item: Dict[str, Any]) -> bool:
        """Evaluate condition against an item."""
        field_value = self._get_field_value(item, self.field)

        result = self._apply_operator(field_value)

        if self.negate:
            result = not result

        return result

    def _get_field_value(self, item: Dict[str, Any], field: str) -> Any:
        """Get field value using dot notation."""
        parts = field.split(".")
        value = item

        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            elif isinstance(value, list):
                try:
                    index = int(part)
                    value = value[index] if 0 <= index < len(value) else None
                except ValueError:
                    return None
            else:
                return None

        return value

    def _apply_operator(self, field_value: Any) -> bool:
        """Apply the filter operator."""
        if self.operator == FilterOperator.EQ:
            return field_value == self.value
        elif self.operator == FilterOperator.NE:
            return field_value != self.value
        elif self.operator == FilterOperator.GT:
            return field_value is not None and field_value > self.value
        elif self.operator == FilterOperator.GE:
            return field_value is not None and field_value >= self.value
        elif self.operator == FilterOperator.LT:
            return field_value is not None and field_value < self.value
        elif self.operator == FilterOperator.LE:
            return field_value is not None and field_value <= self.value
        elif self.operator == FilterOperator.IN:
            return field_value in self.value if isinstance(self.value, list) else False
        elif self.operator == FilterOperator.NOT_IN:
            return field_value not in self.value if isinstance(self.value, list) else True
        elif self.operator == FilterOperator.CONTAINS:
            return str(self.value) in str(field_value) if field_value else False
        elif self.operator == FilterOperator.NOT_CONTAINS:
            return str(self.value) not in str(field_value) if field_value else True
        elif self.operator == FilterOperator.STARTS_WITH:
            return str(field_value).startswith(str(self.value)) if field_value else False
        elif self.operator == FilterOperator.ENDS_WITH:
            return str(field_value).endswith(str(self.value)) if field_value else False
        elif self.operator == FilterOperator.REGEX:
            try:
                return bool(re.search(str(self.value), str(field_value)))
            except re.error:
                return False
        elif self.operator == FilterOperator.EXISTS:
            return field_value is not None
        elif self.operator == FilterOperator.BETWEEN:
            return (
                field_value is not None
                and self.value <= field_value <= self.value2
            )

        return True


@dataclass
class FilterGroup:
    """Groups multiple filter conditions."""
    conditions: List[FilterCondition]
    logical_op: str = "and"

    def evaluate(self, item: Dict[str, Any]) -> bool:
        """Evaluate all conditions in group."""
        if self.logical_op == "and":
            return all(c.evaluate(item) for c in self.conditions)
        else:
            return any(c.evaluate(item) for c in self.conditions)


class DataFilter:
    """Filters data based on conditions."""

    def __init__(self):
        self._filters: List[FilterGroup] = []

    def add_filter(self, filter_group: FilterGroup):
        """Add a filter group."""
        self._filters.append(filter_group)

    def add_condition(
        self,
        field: str,
        operator: FilterOperator,
        value: Any = None,
        logical_op: str = "and",
    ):
        """Add a single condition."""
        condition = FilterCondition(
            field=field,
            operator=operator,
            value=value,
        )
        self._filters.append(FilterGroup([condition], logical_op))

    def filter_list(
        self,
        items: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Filter a list of items."""
        if not self._filters:
            return items

        result = items
        for filter_group in self._filters:
            result = [item for item in result if filter_group.evaluate(item)]

        return result

    def filter_single(
        self,
        item: Dict[str, Any],
    ) -> bool:
        """Check if single item matches filters."""
        if not self._filters:
            return True

        for filter_group in self._filters:
            if not filter_group.evaluate(item):
                return False

        return True

    def clear_filters(self):
        """Remove all filters."""
        self._filters.clear()


class SearchEngine:
    """Full-text search capabilities."""

    def __init__(self):
        self._index: Dict[str, List[int]] = {}
        self._documents: List[Dict[str, Any]] = []

    def index(self, documents: List[Dict[str, Any]], fields: List[str]):
        """Build search index."""
        self._documents = documents
        self._index.clear()

        for idx, doc in enumerate(documents):
            for field in fields:
                value = doc.get(field, "")
                if isinstance(value, str):
                    words = self._tokenize(value)
                    for word in words:
                        if word not in self._index:
                            self._index[word] = []
                        self._index[word].append(idx)

    def _tokenize(self, text: str) -> List[str]:
        """Tokenize text into words."""
        text = text.lower()
        words = re.findall(r'\w+', text)
        return words

    def search(
        self,
        query: str,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Search for documents matching query."""
        query_words = self._tokenize(query)

        if not query_words:
            return []

        doc_scores: Dict[int, float] = {}
        for word in query_words:
            if word in self._index:
                for doc_idx in self._index[word]:
                    doc_scores[doc_idx] = doc_scores.get(doc_idx, 0) + 1

        ranked = sorted(
            doc_scores.items(),
            key=lambda x: x[1],
            reverse=True,
        )[:limit]

        return [self._documents[idx] for idx, score in ranked]


class QueryBuilder:
    """Builds complex queries programmatically."""

    def __init__(self):
        self._conditions: List[FilterCondition] = []
        self._logical_op: str = "and"
        self._sort_field: Optional[str] = None
        self._sort_desc: bool = False
        self._limit_count: Optional[int] = None
        self._offset_count: Optional[int] = None

    def where(
        self,
        field: str,
        operator: Union[FilterOperator, str],
        value: Any = None,
    ) -> "QueryBuilder":
        """Add WHERE condition."""
        if isinstance(operator, str):
            operator = FilterOperator(operator)

        self._conditions.append(FilterCondition(
            field=field,
            operator=operator,
            value=value,
        ))
        return self

    def where_in(self, field: str, values: List[Any]) -> "QueryBuilder":
        """Add WHERE field IN values."""
        self._conditions.append(FilterCondition(
            field=field,
            operator=FilterOperator.IN,
            value=values,
        ))
        return self

    def where_contains(self, field: str, value: str) -> "QueryBuilder":
        """Add WHERE field contains value."""
        self._conditions.append(FilterCondition(
            field=field,
            operator=FilterOperator.CONTAINS,
            value=value,
        ))
        return self

    def where_between(
        self,
        field: str,
        min_value: Any,
        max_value: Any,
    ) -> "QueryBuilder":
        """Add WHERE field BETWEEN values."""
        self._conditions.append(FilterCondition(
            field=field,
            operator=FilterOperator.BETWEEN,
            value=min_value,
            value2=max_value,
        ))
        return self

    def where_exists(self, field: str) -> "QueryBuilder":
        """Add WHERE field EXISTS."""
        self._conditions.append(FilterCondition(
            field=field,
            operator=FilterOperator.EXISTS,
        ))
        return self

    def and_where(
        self,
        field: str,
        operator: Union[FilterOperator, str],
        value: Any = None,
    ) -> "QueryBuilder":
        """Add AND condition."""
        return self.where(field, operator, value)

    def or_where(
        self,
        field: str,
        operator: Union[FilterOperator, str],
        value: Any = None,
    ) -> "QueryBuilder":
        """Add OR condition."""
        if self._conditions:
            self._conditions[-1].negate = False
        return self.where(field, operator, value)

    def order_by(self, field: str, descending: bool = False) -> "QueryBuilder":
        """Add ORDER BY."""
        self._sort_field = field
        self._sort_desc = descending
        return self

    def limit(self, count: int) -> "QueryBuilder":
        """Add LIMIT."""
        self._limit_count = count
        return self

    def offset(self, count: int) -> "QueryBuilder":
        """Add OFFSET."""
        self._offset_count = count
        return self

    def build(self) -> List[FilterCondition]:
        """Build filter conditions."""
        return self._conditions

    def apply(
        self,
        items: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Apply query to items."""
        filter_group = FilterGroup(self._conditions, self._logical_op)
        result = [item for item in items if filter_group.evaluate(item)]

        if self._sort_field:
            result = sorted(
                result,
                key=lambda x: x.get(self._sort_field, ""),
                reverse=self._sort_desc,
            )

        if self._offset_count:
            result = result[self._offset_count:]

        if self._limit_count:
            result = result[:self._limit_count]

        return result


class DataFilterAction:
    """High-level data filter action."""

    def __init__(
        self,
        data_filter: Optional[DataFilter] = None,
        search_engine: Optional[SearchEngine] = None,
    ):
        self.data_filter = data_filter or DataFilter()
        self.search_engine = search_engine or SearchEngine()

    def filter(
        self,
        items: List[Dict[str, Any]],
        conditions: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Filter items with conditions."""
        self.data_filter.clear_filters()

        for cond in conditions:
            operator = FilterOperator(cond["operator"])
            self.data_filter.add_condition(
                field=cond["field"],
                operator=operator,
                value=cond.get("value"),
            )

        return self.data_filter.filter_list(items)

    def search(
        self,
        items: List[Dict[str, Any]],
        query: str,
        fields: List[str],
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Full-text search in items."""
        self.search_engine.index(items, fields)
        return self.search_engine.search(query, limit)

    def query(self) -> QueryBuilder:
        """Create a new query builder."""
        return QueryBuilder()


# Module exports
__all__ = [
    "DataFilterAction",
    "DataFilter",
    "SearchEngine",
    "QueryBuilder",
    "FilterCondition",
    "FilterGroup",
    "FilterOperator",
]
