# Copyright (c) 2024. coded by claude
"""Data Filter Action Module.

Filters API response data with support for complex predicates,
field selection, and result transformation.
"""
from typing import Optional, Dict, Any, List, Callable, Set
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class FilterOperator(Enum):
    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GE = "ge"
    LT = "lt"
    LE = "le"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    IN = "in"
    NOT_IN = "not_in"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


@dataclass
class FilterCondition:
    field: str
    operator: FilterOperator
    value: Any


@dataclass
class FilterConfig:
    conditions: List[FilterCondition] = field(default_factory=list)
    combine_with: str = "AND"
    select_fields: Optional[Set[str]] = None
    exclude_fields: Optional[Set[str]] = None


class DataFilter:
    def __init__(self, config: Optional[FilterConfig] = None):
        self.config = config or FilterConfig()

    def filter(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not self.config.conditions:
            return self._apply_field_selection(items)
        filtered = [item for item in items if self._matches_conditions(item)]
        return self._apply_field_selection(filtered)

    def _matches_conditions(self, item: Dict[str, Any]) -> bool:
        if self.config.combine_with == "AND":
            return all(self._evaluate_condition(item, cond) for cond in self.config.conditions)
        return any(self._evaluate_condition(item, cond) for cond in self.config.conditions)

    def _evaluate_condition(self, item: Dict[str, Any], condition: FilterCondition) -> bool:
        value = item.get(condition.field)
        op = condition.operator
        if op == FilterOperator.EQ:
            return value == condition.value
        elif op == FilterOperator.NE:
            return value != condition.value
        elif op == FilterOperator.GT:
            return value is not None and value > condition.value
        elif op == FilterOperator.GE:
            return value is not None and value >= condition.value
        elif op == FilterOperator.LT:
            return value is not None and value < condition.value
        elif op == FilterOperator.LE:
            return value is not None and value <= condition.value
        elif op == FilterOperator.CONTAINS:
            return value is not None and str(condition.value) in str(value)
        elif op == FilterOperator.STARTS_WITH:
            return value is not None and str(value).startswith(str(condition.value))
        elif op == FilterOperator.ENDS_WITH:
            return value is not None and str(value).endswith(str(condition.value))
        elif op == FilterOperator.IN:
            return value in condition.value if isinstance(condition.value, list) else value == condition.value
        elif op == FilterOperator.NOT_IN:
            return value not in condition.value if isinstance(condition.value, list) else value != condition.value
        elif op == FilterOperator.IS_NULL:
            return value is None
        elif op == FilterOperator.IS_NOT_NULL:
            return value is not None
        return True

    def _apply_field_selection(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not self.config.select_fields and not self.config.exclude_fields:
            return items
        result = []
        for item in items:
            if self.config.select_fields:
                result.append({k: v for k, v in item.items() if k in self.config.select_fields})
            elif self.config.exclude_fields:
                result.append({k: v for k, v in item.items() if k not in self.config.exclude_fields})
            else:
                result.append(item)
        return result

    def add_condition(self, condition: FilterCondition) -> None:
        self.config.conditions.append(condition)

    def clear_conditions(self) -> None:
        self.config.conditions.clear()

    def filter_by_function(self, items: List[Dict[str, Any]], predicate: Callable[[Dict[str, Any]], bool]) -> List[Dict[str, Any]]:
        return [item for item in items if predicate(item)]
