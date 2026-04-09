"""Data Filter Action Module.

Provides powerful data filtering with support for multiple filter types,
operators, compound conditions, and data pipeline integration.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class FilterOperator(Enum):
    """Supported filter operators."""
    EQUALS = "eq"
    NOT_EQUALS = "ne"
    GREATER_THAN = "gt"
    GREATER_EQUALS = "ge"
    LESS_THAN = "lt"
    LESS_EQUALS = "le"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    REGEX = "regex"
    IN = "in"
    NOT_IN = "not_in"
    BETWEEN = "between"
    EXISTS = "exists"
    NOT_EXISTS = "not_exists"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    TYPE_OF = "type_of"


class FilterLogic(Enum):
    """Logic combinators for filters."""
    AND = "and"
    OR = "or"


@dataclass
class FilterCondition:
    """A single filter condition."""
    field: str
    operator: FilterOperator
    value: Any = None
    case_sensitive: bool = False

    def matches(self, item: Dict[str, Any]) -> bool:
        """Check if item matches this condition."""
        field_value = self._get_nested_field(item, self.field)

        if self.operator == FilterOperator.EQUALS:
            return self._compare_eq(field_value, self.value)
        elif self.operator == FilterOperator.NOT_EQUALS:
            return not self._compare_eq(field_value, self.value)
        elif self.operator == FilterOperator.GREATER_THAN:
            return self._compare_gt(field_value, self.value)
        elif self.operator == FilterOperator.GREATER_EQUALS:
            return self._compare_ge(field_value, self.value)
        elif self.operator == FilterOperator.LESS_THAN:
            return self._compare_lt(field_value, self.value)
        elif self.operator == FilterOperator.LESS_EQUALS:
            return self._compare_le(field_value, self.value)
        elif self.operator == FilterOperator.CONTAINS:
            return self._contains(field_value, self.value)
        elif self.operator == FilterOperator.NOT_CONTAINS:
            return not self._contains(field_value, self.value)
        elif self.operator == FilterOperator.STARTS_WITH:
            return self._starts_with(field_value, self.value)
        elif self.operator == FilterOperator.ENDS_WITH:
            return self._ends_with(field_value, self.value)
        elif self.operator == FilterOperator.REGEX:
            return self._regex_match(field_value, self.value)
        elif self.operator == FilterOperator.IN:
            return field_value in (self.value if isinstance(self.value, (list, tuple, set)) else [self.value])
        elif self.operator == FilterOperator.NOT_IN:
            return field_value not in (self.value if isinstance(self.value, (list, tuple, set)) else [self.value])
        elif self.operator == FilterOperator.BETWEEN:
            return self._between(field_value, self.value)
        elif self.operator == FilterOperator.EXISTS:
            return field_value is not None
        elif self.operator == FilterOperator.NOT_EXISTS:
            return field_value is None
        elif self.operator == FilterOperator.IS_NULL:
            return field_value is None or field_value == ""
        elif self.operator == FilterOperator.IS_NOT_NULL:
            return field_value is not None and field_value != ""
        elif self.operator == FilterOperator.TYPE_OF:
            return isinstance(field_value, self.value)

        return False

    def _get_nested_field(self, item: Dict[str, Any], field_path: str) -> Any:
        """Get nested field value using dot notation."""
        parts = field_path.split(".")
        value = item
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            elif isinstance(value, (list, tuple)):
                try:
                    idx = int(part)
                    value = value[idx]
                except (ValueError, IndexError):
                    return None
            else:
                return None
            if value is None:
                return None
        return value

    def _to_lower(self, val: str) -> str:
        """Convert to lowercase if case insensitive."""
        return val.lower() if isinstance(val, str) and not self.case_sensitive else val

    def _compare_eq(self, a: Any, b: Any) -> bool:
        """Equality comparison."""
        if isinstance(a, str) and not self.case_sensitive:
            return self._to_lower(str(a)) == self._to_lower(str(b))
        return a == b

    def _compare_gt(self, a: Any, b: Any) -> bool:
        """Greater than comparison."""
        try:
            return float(a) > float(b)
        except (TypeError, ValueError):
            return str(a) > str(b)

    def _compare_ge(self, a: Any, b: Any) -> bool:
        """Greater than or equal comparison."""
        try:
            return float(a) >= float(b)
        except (TypeError, ValueError):
            return str(a) >= str(b)

    def _compare_lt(self, a: Any, b: Any) -> bool:
        """Less than comparison."""
        try:
            return float(a) < float(b)
        except (TypeError, ValueError):
            return str(a) < str(b)

    def _compare_le(self, a: Any, b: Any) -> bool:
        """Less than or equal comparison."""
        try:
            return float(a) <= float(b)
        except (TypeError, ValueError):
            return str(a) <= str(b)

    def _contains(self, field_val: Any, search_val: Any) -> bool:
        """Contains check."""
        if field_val is None:
            return False
        field_str = str(field_val)
        search_str = str(search_val)
        if not self.case_sensitive:
            field_str = field_str.lower()
            search_str = search_str.lower()
        return search_str in field_str

    def _starts_with(self, field_val: Any, prefix: Any) -> bool:
        """Starts with check."""
        if field_val is None:
            return False
        field_str = str(field_val)
        prefix_str = str(prefix)
        if not self.case_sensitive:
            field_str = field_str.lower()
            prefix_str = prefix_str.lower()
        return field_str.startswith(prefix_str)

    def _ends_with(self, field_val: Any, suffix: Any) -> bool:
        """Ends with check."""
        if field_val is None:
            return False
        field_str = str(field_val)
        suffix_str = str(suffix)
        if not self.case_sensitive:
            field_str = field_str.lower()
            suffix_str = suffix_str.lower()
        return field_str.endswith(suffix_str)

    def _regex_match(self, field_val: Any, pattern: Any) -> bool:
        """Regex match check."""
        if field_val is None:
            return False
        try:
            flags = 0 if self.case_sensitive else re.IGNORECASE
            return bool(re.search(str(pattern), str(field_val), flags))
        except re.error:
            return False

    def _between(self, field_val: Any, range_val: Any) -> bool:
        """Between check (inclusive)."""
        if not isinstance(range_val, (list, tuple)) or len(range_val) != 2:
            return False
        try:
            return float(range_val[0]) <= float(field_val) <= float(range_val[1])
        except (TypeError, ValueError):
            return False


@dataclass
class FilterGroup:
    """A group of filters with AND/OR logic."""
    logic: FilterLogic = FilterLogic.AND
    conditions: List[Union[FilterCondition, FilterGroup]] = field(default_factory=list)
    negate: bool = False

    def matches(self, item: Dict[str, Any]) -> bool:
        """Check if item matches this filter group."""
        if not self.conditions:
            return True

        if self.logic == FilterLogic.AND:
            result = all(c.matches(item) for c in self.conditions)
        else:
            result = any(c.matches(item) for c in self.conditions)

        return not result if self.negate else result


@dataclass
class FilterStats:
    """Statistics for filter operations."""
    total_items: int = 0
    matched_items: int = 0
    filtered_items: int = 0
    filter_time_ms: float = 0.0


class DataFilterAction(BaseAction):
    """Data Filter Action for complex data filtering.

    Supports single conditions, compound groups, nested fields,
    and various operators for flexible data filtering.

    Examples:
        >>> action = DataFilterAction()
        >>> result = action.execute(ctx, {
        ...     "data": [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
        ...     "filters": {
        ...         "field": "age",
        ...         "operator": "gt",
        ...         "value": 20
        ...     }
        ... })
    """

    action_type = "data_filter"
    display_name = "数据过滤"
    description = "支持多条件AND/OR组合、多种运算符的灵活数据过滤"

    def __init__(self):
        super().__init__()
        self._stats = FilterStats()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute data filtering.

        Args:
            context: Execution context.
            params: Dict with keys:
                - data: List of dicts to filter
                - filters: FilterCondition or FilterGroup definition
                - filter_type: 'include' (keep matches) or 'exclude' (remove matches)
                - limit: Max results to return (optional)
                - offset: Offset for pagination (optional)
                - sort_by: Field to sort results (optional)
                - sort_desc: Sort descending (default: False)

        Returns:
            ActionResult with filtered data and stats.
        """
        import time
        start_time = time.time()

        data = params.get("data", [])
        filter_type = params.get("filter_type", "include")  # include or exclude
        limit = params.get("limit")
        offset = params.get("offset", 0)
        sort_by = params.get("sort_by")
        sort_desc = params.get("sort_desc", False)

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message="'data' parameter must be a list"
            )

        # Build filter
        filter_group = self._build_filter(params.get("filters", {}))

        # Apply filter
        filtered = []
        for item in data:
            if not isinstance(item, dict):
                continue
            matches = filter_group.matches(item)
            # Include mode keeps matches, exclude mode removes them
            if filter_type == "include":
                if matches:
                    filtered.append(item)
            else:
                if not matches:
                    filtered.append(item)

        # Sort if requested
        if sort_by:
            filtered = self._sort_results(filtered, sort_by, sort_desc)

        # Pagination
        total = len(filtered)
        if offset > 0:
            filtered = filtered[offset:]
        if limit:
            filtered = filtered[:limit]

        duration_ms = (time.time() - start_time) * 1000
        self._stats = FilterStats(
            total_items=len(data),
            matched_items=len(filtered),
            filtered_items=len(data) - len(filtered),
            filter_time_ms=duration_ms,
        )

        return ActionResult(
            success=True,
            message=f"Filtered {len(data)} items to {len(filtered)} results",
            data={
                "filtered_data": filtered,
                "total_input": len(data),
                "total_output": len(filtered),
                "filtered_count": len(data) - len(filtered),
                "filter_time_ms": duration_ms,
            }
        )

    def _build_filter(self, filter_def: Dict[str, Any]) -> FilterGroup:
        """Build a FilterGroup from definition."""
        if not filter_def:
            return FilterGroup()

        # Single condition
        if "field" in filter_def:
            cond = FilterCondition(
                field=filter_def["field"],
                operator=FilterOperator(filter_def.get("operator", "eq")),
                value=filter_def.get("value"),
                case_sensitive=filter_def.get("case_sensitive", False),
            )
            return FilterGroup(conditions=[cond])

        # Compound filter
        logic = FilterLogic(filter_def.get("logic", "and"))
        conditions = []
        for cond_def in filter_def.get("conditions", []):
            if isinstance(cond_def, dict):
                conditions.append(self._build_filter(cond_def))
            elif isinstance(cond_def, FilterCondition):
                conditions.append(cond_def)

        group = FilterGroup(logic=logic, conditions=conditions)
        if filter_def.get("negate"):
            group.negate = True
        return group

    def _sort_results(
        self, data: List[Dict[str, Any]], sort_by: str, descending: bool
    ) -> List[Dict[str, Any]]:
        """Sort results by field."""
        def get_sort_value(item: Dict[str, Any]) -> Any:
            parts = sort_by.split(".")
            value = item
            for part in parts:
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    return None
            return value

        return sorted(
            data,
            key=get_sort_value,
            reverse=descending,
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get filter statistics."""
        return {
            "total_items": self._stats.total_items,
            "matched_items": self._stats.matched_items,
            "filtered_items": self._stats.filtered_items,
            "filter_time_ms": self._stats.filter_time_ms,
            "match_rate": (
                self._stats.matched_items / self._stats.total_items
                if self._stats.total_items > 0 else 0
            ),
        }

    def get_required_params(self) -> List[str]:
        return ["data", "filters"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "filter_type": "include",
            "limit": None,
            "offset": 0,
            "sort_by": None,
            "sort_desc": False,
        }
