"""
Data Filtering Action Module.

Provides data filtering capabilities with predicates,
expressions, and transformation pipelines.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging
import operator
import re

logger = logging.getLogger(__name__)


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
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    REGEX = "regex"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


@dataclass
class FilterCondition:
    """Single filter condition."""
    field: str
    operator: FilterOperator
    value: Any = None


@dataclass
class FilterRule:
    """Filter rule with conditions."""
    rule_id: str
    name: str
    conditions: List[FilterCondition]
    combine_mode: str = "and"
    negate: bool = False


class ExpressionEvaluator:
    """Evaluates filter expressions."""

    OPS = {
        FilterOperator.EQ: operator.eq,
        FilterOperator.NE: operator.ne,
        FilterOperator.GT: operator.gt,
        FilterOperator.GTE: operator.ge,
        FilterOperator.LT: operator.lt,
        FilterOperator.LTE: operator.le,
        FilterOperator.CONTAINS: lambda a, b: b in a if a is not None else False,
        FilterOperator.STARTS_WITH: lambda a, b: str(a).startswith(b) if a is not None else False,
        FilterOperator.ENDS_WITH: lambda a, b: str(a).endswith(b) if a is not None else False,
    }

    def evaluate(self, record: Dict[str, Any], condition: FilterCondition) -> bool:
        """Evaluate a single condition."""
        value = record.get(condition.field)

        if condition.operator == FilterOperator.IS_NULL:
            return value is None

        if condition.operator == FilterOperator.IS_NOT_NULL:
            return value is not None

        if condition.operator == FilterOperator.IN:
            return value in condition.value

        if condition.operator == FilterOperator.NOT_IN:
            return value not in condition.value

        if condition.operator == FilterOperator.REGEX:
            try:
                return bool(re.match(condition.value, str(value)))
            except:
                return False

        op_func = self.OPS.get(condition.operator)
        if op_func:
            return op_func(value, condition.value)

        return False


class DataFilter:
    """Filters data based on rules."""

    def __init__(self):
        self.rules: List[FilterRule] = []
        self.evaluator = ExpressionEvaluator()

    def add_rule(self, rule: FilterRule):
        """Add a filter rule."""
        self.rules.append(rule)

    def remove_rule(self, rule_id: str) -> bool:
        """Remove a filter rule."""
        for i, rule in enumerate(self.rules):
            if rule.rule_id == rule_id:
                self.rules.pop(i)
                return True
        return False

    def _evaluate_rule(self, record: Dict[str, Any], rule: FilterRule) -> bool:
        """Evaluate a filter rule."""
        if rule.combine_mode == "and":
            result = all(
                self.evaluator.evaluate(record, cond)
                for cond in rule.conditions
            )
        else:
            result = any(
                self.evaluator.evaluate(record, cond)
                for cond in rule.conditions
            )

        return not result if rule.negate else result

    def filter(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter records based on all rules."""
        if not self.rules:
            return records

        filtered = []
        for record in records:
            if all(self._evaluate_rule(record, rule) for rule in self.rules):
                filtered.append(record)

        return filtered

    def filter_one(self, records: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Filter and return first match."""
        filtered = self.filter(records)
        return filtered[0] if filtered else None


class PredicateBuilder:
    """Builds filter predicates."""

    def __init__(self):
        self.conditions: List[FilterCondition] = []
        self._combine_mode = "and"

    def eq(self, field: str, value: Any) -> "PredicateBuilder":
        """Add equals condition."""
        self.conditions.append(FilterCondition(field, FilterOperator.EQ, value))
        return self

    def ne(self, field: str, value: Any) -> "PredicateBuilder":
        """Add not equals condition."""
        self.conditions.append(FilterCondition(field, FilterOperator.NE, value))
        return self

    def gt(self, field: str, value: Any) -> "PredicateBuilder":
        """Add greater than condition."""
        self.conditions.append(FilterCondition(field, FilterOperator.GT, value))
        return self

    def gte(self, field: str, value: Any) -> "PredicateBuilder":
        """Add greater than or equals condition."""
        self.conditions.append(FilterCondition(field, FilterOperator.GTE, value))
        return self

    def lt(self, field: str, value: Any) -> "PredicateBuilder":
        """Add less than condition."""
        self.conditions.append(FilterCondition(field, FilterOperator.LT, value))
        return self

    def lte(self, field: str, value: Any) -> "PredicateBuilder":
        """Add less than or equals condition."""
        self.conditions.append(FilterCondition(field, FilterOperator.LTE, value))
        return self

    def contains(self, field: str, value: Any) -> "PredicateBuilder":
        """Add contains condition."""
        self.conditions.append(FilterCondition(field, FilterOperator.CONTAINS, value))
        return self

    def in_list(self, field: str, values: List[Any]) -> "PredicateBuilder":
        """Add in list condition."""
        self.conditions.append(FilterCondition(field, FilterOperator.IN, values))
        return self

    def is_null(self, field: str) -> "PredicateBuilder":
        """Add is null condition."""
        self.conditions.append(FilterCondition(field, FilterOperator.IS_NULL))
        return self

    def is_not_null(self, field: str) -> "PredicateBuilder":
        """Add is not null condition."""
        self.conditions.append(FilterCondition(field, FilterOperator.IS_NOT_NULL))
        return self

    def regex(self, field: str, pattern: str) -> "PredicateBuilder":
        """Add regex condition."""
        self.conditions.append(FilterCondition(field, FilterOperator.REGEX, pattern))
        return self

    def build(self) -> List[FilterCondition]:
        """Build conditions list."""
        return self.conditions.copy()


class FilteringPipeline:
    """Pipeline of filtering operations."""

    def __init__(self):
        self.filters: List[DataFilter] = []

    def add_filter(self, filter_func: Callable[[List[Dict]], List[Dict]]):
        """Add a filter function."""
        new_filter = DataFilter()

        def wrapped_filter(records):
            return filter_func(records)

        return self

    def process(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process records through pipeline."""
        result = records
        for data_filter in self.filters:
            result = data_filter.filter(result)
        return result


def main():
    """Demonstrate data filtering."""
    data_filter = DataFilter()

    data_filter.add_rule(FilterRule(
        rule_id="r1",
        name="Active users",
        conditions=[
            FilterCondition("status", FilterOperator.EQ, "active"),
            FilterCondition("age", FilterOperator.GTE, 18)
        ],
        combine_mode="and"
    ))

    records = [
        {"id": 1, "name": "Alice", "status": "active", "age": 25},
        {"id": 2, "name": "Bob", "status": "inactive", "age": 30},
        {"id": 3, "name": "Charlie", "status": "active", "age": 15},
        {"id": 4, "name": "Diana", "status": "active", "age": 35},
    ]

    filtered = data_filter.filter(records)
    print(f"Filtered: {len(filtered)} records")
    for r in filtered:
        print(f"  - {r['name']}")

    builder = PredicateBuilder()
    builder.eq("status", "active").gte("age", 20)
    print(f"\nConditions: {len(builder.conditions)}")


if __name__ == "__main__":
    main()
