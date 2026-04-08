"""Data filter action module for RabAI AutoClick.

Provides data filtering:
- DataFilter: General data filter
- FilterExpression: Filter expression parser
- FilterChain: Chain multiple filters
- FilterBuilder: Build complex filters
"""

import re
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FilterOperator(Enum):
    """Filter operators."""
    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    REGEX = "regex"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


@dataclass
class FilterRule:
    """Filter rule."""
    field: str
    operator: FilterOperator
    value: Any


@dataclass
class FilterResult:
    """Filter result."""
    total: int
    filtered: int
    removed: int
    data: List[Any]


class FilterExpression:
    """Filter expression parser and evaluator."""

    def __init__(self):
        self._operators = {
            "==": FilterOperator.EQ,
            "!=": FilterOperator.NE,
            ">": FilterOperator.GT,
            ">=": FilterOperator.GTE,
            "<": FilterOperator.LT,
            "<=": FilterOperator.LTE,
            "in": FilterOperator.IN,
            "not in": FilterOperator.NOT_IN,
            "contains": FilterOperator.CONTAINS,
            "startswith": FilterOperator.STARTS_WITH,
            "endswith": FilterOperator.ENDS_WITH,
            "regex": FilterOperator.REGEX,
        }

    def parse(self, expression: str) -> Optional[FilterRule]:
        """Parse filter expression."""
        for op_str, op_enum in self._operators.items():
            if op_str in expression:
                parts = expression.split(op_str, 1)
                if len(parts) == 2:
                    field = parts[0].strip()
                    value = parts[1].strip().strip("'\"")
                    return FilterRule(field=field, operator=op_enum, value=value)
        return None

    def evaluate(self, item: Dict, rule: FilterRule) -> bool:
        """Evaluate filter rule on item."""
        value = item.get(rule.field)

        if rule.operator == FilterOperator.EQ:
            return value == rule.value
        elif rule.operator == FilterOperator.NE:
            return value != rule.value
        elif rule.operator == FilterOperator.GT:
            return value is not None and value > rule.value
        elif rule.operator == FilterOperator.GTE:
            return value is not None and value >= rule.value
        elif rule.operator == FilterOperator.LT:
            return value is not None and value < rule.value
        elif rule.operator == FilterOperator.LTE:
            return value is not None and value <= rule.value
        elif rule.operator == FilterOperator.IN:
            return value in (rule.value if isinstance(rule.value, list) else [rule.value])
        elif rule.operator == FilterOperator.NOT_IN:
            return value not in (rule.value if isinstance(rule.value, list) else [rule.value])
        elif rule.operator == FilterOperator.CONTAINS:
            return value is not None and str(rule.value) in str(value)
        elif rule.operator == FilterOperator.NOT_CONTAINS:
            return value is not None and str(rule.value) not in str(value)
        elif rule.operator == FilterOperator.STARTS_WITH:
            return value is not None and str(value).startswith(str(rule.value))
        elif rule.operator == FilterOperator.ENDS_WITH:
            return value is not None and str(value).endswith(str(rule.value))
        elif rule.operator == FilterOperator.REGEX:
            try:
                return value is not None and bool(re.match(str(rule.value), str(value)))
            except Exception:
                return False
        elif rule.operator == FilterOperator.IS_NULL:
            return value is None
        elif rule.operator == FilterOperator.IS_NOT_NULL:
            return value is not None

        return True


class DataFilter:
    """General data filter."""

    def __init__(self):
        self.expression_parser = FilterExpression()
        self._custom_filters: Dict[str, Callable] = {}

    def add_filter(self, name: str, filter_fn: Callable[[Dict], bool]) -> "DataFilter":
        """Add custom filter."""
        self._custom_filters[name] = filter_fn
        return self

    def filter(
        self,
        data: List[Dict],
        rules: Union[str, List[FilterRule], Callable],
        mode: str = "all",
    ) -> FilterResult:
        """Filter data."""
        if not data:
            return FilterResult(total=0, filtered=0, removed=0, data=[])

        filtered = []
        removed = 0

        for item in data:
            if self._matches(item, rules, mode):
                filtered.append(item)
            else:
                removed += 1

        return FilterResult(
            total=len(data),
            filtered=len(filtered),
            removed=removed,
            data=filtered,
        )

    def _matches(self, item: Dict, rules: Union[str, List[FilterRule], Callable], mode: str) -> bool:
        """Check if item matches rules."""
        if callable(rules):
            try:
                return rules(item)
            except Exception:
                return False

        if isinstance(rules, str):
            parsed = self.expression_parser.parse(rules)
            if parsed:
                return self.expression_parser.evaluate(item, parsed)
            return True

        if isinstance(rules, list):
            if mode == "all":
                for rule in rules:
                    if isinstance(rule, str):
                        parsed = self.expression_parser.parse(rule)
                        if parsed and not self.expression_parser.evaluate(item, parsed):
                            return False
                    elif isinstance(rule, FilterRule):
                        if not self.expression_parser.evaluate(item, rule):
                            return False
                return True
            elif mode == "any":
                for rule in rules:
                    if isinstance(rule, str):
                        parsed = self.expression_parser.parse(rule)
                        if parsed and self.expression_parser.evaluate(item, parsed):
                            return True
                    elif isinstance(rule, FilterRule):
                        if self.expression_parser.evaluate(item, rule):
                            return True
                return False

        return True

    def exclude(
        self,
        data: List[Dict],
        rules: Union[str, List[FilterRule], Callable],
    ) -> FilterResult:
        """Exclude items matching rules."""
        if not data:
            return FilterResult(total=0, filtered=0, removed=0, data=[])

        filtered = []
        removed = 0

        for item in data:
            if self._matches(item, rules, "all"):
                removed += 1
            else:
                filtered.append(item)

        return FilterResult(
            total=len(data),
            filtered=len(filtered),
            removed=removed,
            data=filtered,
        )


class FilterChain:
    """Chain multiple filters."""

    def __init__(self):
        self._filters: List[Tuple[Callable, str]] = []

    def add_filter(self, filter_fn: Callable, mode: str = "all") -> "FilterChain":
        """Add filter to chain."""
        self._filters.append((filter_fn, mode))
        return self

    def apply(self, data: List[Dict]) -> List[Dict]:
        """Apply all filters."""
        result = data

        for filter_fn, mode in self._filters:
            if mode == "all":
                result = [item for item in result if filter_fn(item)]
            elif mode == "any":
                result = result

        return result


class FilterBuilder:
    """Build complex filters."""

    def __init__(self):
        self._rules: List[FilterRule] = []
        self._mode = "all"

    def where(self, field: str, operator: FilterOperator, value: Any) -> "FilterBuilder":
        """Add filter rule."""
        self._rules.append(FilterRule(field=field, operator=operator, value=value))
        return self

    def where_eq(self, field: str, value: Any) -> "FilterBuilder":
        """Add equals filter."""
        return self.where(field, FilterOperator.EQ, value)

    def where_ne(self, field: str, value: Any) -> "FilterBuilder":
        """Add not equals filter."""
        return self.where(field, FilterOperator.NE, value)

    def where_gt(self, field: str, value: Any) -> "FilterBuilder":
        """Add greater than filter."""
        return self.where(field, FilterOperator.GT, value)

    def where_gte(self, field: str, value: Any) -> "FilterBuilder":
        """Add greater than or equals filter."""
        return self.where(field, FilterOperator.GTE, value)

    def where_lt(self, field: str, value: Any) -> "FilterBuilder":
        """Add less than filter."""
        return self.where(field, FilterOperator.LT, value)

    def where_lte(self, field: str, value: Any) -> "FilterBuilder":
        """Add less than or equals filter."""
        return self.where(field, FilterOperator.LTE, value)

    def where_contains(self, field: str, value: Any) -> "FilterBuilder":
        """Add contains filter."""
        return self.where(field, FilterOperator.CONTAINS, value)

    def where_in(self, field: str, values: List) -> "FilterBuilder":
        """Add in filter."""
        return self.where(field, FilterOperator.IN, values)

    def where_not_null(self, field: str) -> "FilterBuilder":
        """Add is not null filter."""
        return self.where(field, FilterOperator.IS_NOT_NULL, None)

    def mode_any(self) -> "FilterBuilder":
        """Set mode to any."""
        self._mode = "any"
        return self

    def mode_all(self) -> "FilterBuilder":
        """Set mode to all."""
        self._mode = "all"
        return self

    def build(self, data: List[Dict]) -> FilterResult:
        """Build and apply filter."""
        if not self._rules:
            return FilterResult(total=len(data), filtered=len(data), removed=0, data=data)

        filter_expr = FilterExpression()
        data_filter = DataFilter()

        return data_filter.filter(data, self._rules, self._mode)


class DataFilterAction(BaseAction):
    """Data filter action."""
    action_type = "data_filter"
    display_name = "数据过滤器"
    description = "数据过滤和筛选"

    def __init__(self):
        super().__init__()
        self._filter = DataFilter()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "filter")

            if operation == "filter":
                return self._filter_data(params)
            elif operation == "exclude":
                return self._exclude_data(params)
            elif operation == "parse":
                return self._parse_expression(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Filter error: {str(e)}")

    def _filter_data(self, params: Dict) -> ActionResult:
        """Filter data."""
        data = params.get("data", [])
        rules_data = params.get("rules", [])
        mode = params.get("mode", "all")

        rules = []
        for r in rules_data:
            try:
                operator = FilterOperator[r.get("operator", "EQ").upper()]
                rules.append(FilterRule(
                    field=r.get("field", ""),
                    operator=operator,
                    value=r.get("value"),
                ))
            except KeyError:
                pass

        result = self._filter.filter(data, rules, mode)

        return ActionResult(
            success=True,
            message=f"Filtered {result.removed}/{result.total} items",
            data={
                "total": result.total,
                "filtered": result.filtered,
                "removed": result.removed,
            },
        )

    def _exclude_data(self, params: Dict) -> ActionResult:
        """Exclude data."""
        data = params.get("data", [])
        rules_data = params.get("rules", [])

        rules = []
        for r in rules_data:
            try:
                operator = FilterOperator[r.get("operator", "EQ").upper()]
                rules.append(FilterRule(
                    field=r.get("field", ""),
                    operator=operator,
                    value=r.get("value"),
                ))
            except KeyError:
                pass

        result = self._filter.exclude(data, rules)

        return ActionResult(
            success=True,
            message=f"Excluded {result.removed}/{result.total} items",
            data={
                "total": result.total,
                "filtered": result.filtered,
                "removed": result.removed,
            },
        )

    def _parse_expression(self, params: Dict) -> ActionResult:
        """Parse filter expression."""
        expression = params.get("expression", "")

        parser = FilterExpression()
        rule = parser.parse(expression)

        if rule:
            return ActionResult(
                success=True,
                message="Expression parsed",
                data={
                    "field": rule.field,
                    "operator": rule.operator.value,
                    "value": rule.value,
                },
            )
        else:
            return ActionResult(success=False, message="Failed to parse expression")
