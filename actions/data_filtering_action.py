"""
Data Filtering Action Module

Provides advanced data filtering capabilities.
Supports predicate-based filtering, range filters, and compound conditions.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import operator
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, TypeVar, Union
from datetime import datetime

T = TypeVar('T')


class FilterOperator(Enum):
    """Filter operators."""
    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GE = "ge"
    LT = "lt"
    LE = "le"
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    REGEX = "regex"
    BETWEEN = "between"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


class FilterLogic(Enum):
    """Logic for combining filters."""
    AND = "and"
    OR = "or"


@dataclass
class FilterCondition:
    """A single filter condition."""
    field: str
    operator: FilterOperator
    value: Any = None
    value2: Any = None  # For BETWEEN operator


@dataclass
class FilterGroup:
    """A group of filter conditions."""
    logic: FilterLogic = FilterLogic.AND
    conditions: list[FilterCondition] = field(default_factory=list)
    groups: list[FilterGroup] = field(default_factory=list)


@dataclass
class FilterResult:
    """Result of a filtering operation."""
    original_count: int
    filtered_count: int
    removed_count: int
    filter_logic: FilterLogic
    duration_ms: float = 0.0


class DataFilterAction:
    """
    Advanced data filtering.
    
    Example:
        filter_action = DataFilterAction()
        
        result = filter_action.filter(
            data=records,
            conditions=[
                FilterCondition("age", FilterOperator.GE, 18),
                FilterCondition("status", FilterOperator.EQ, "active")
            ]
        )
    """
    
    def __init__(self):
        self._filter_cache: dict[str, Callable] = {}
        self._history: deque[FilterResult] = deque(maxlen=100)
        self._stats = {
            "total_filters": 0,
            "total_items_processed": 0,
            "total_items_removed": 0
        }
    
    def _get_operator_fn(self, op: FilterOperator) -> Callable:
        """Get the operator function for a filter operator."""
        ops = {
            FilterOperator.EQ: operator.eq,
            FilterOperator.NE: operator.ne,
            FilterOperator.GT: operator.gt,
            FilterOperator.GE: operator.ge,
            FilterOperator.LT: operator.lt,
            FilterOperator.LE: operator.le,
            FilterOperator.CONTAINS: lambda a, b: a in b if a and b else False,
            FilterOperator.STARTS_WITH: lambda a, b: str(a).startswith(str(b)) if a else False,
            FilterOperator.ENDS_WITH: lambda a, b: str(a).endswith(str(b)) if a else False,
        }
        return ops.get(op, operator.eq)
    
    def _get_field_value(self, item: Any, field_path: str) -> Any:
        """Get a field value from an item using dot notation."""
        parts = field_path.split(".")
        value = item
        
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            elif hasattr(value, part):
                value = getattr(value, part)
            else:
                return None
        
        return value
    
    def _evaluate_condition(self, item: Any, condition: FilterCondition) -> bool:
        """Evaluate a single filter condition against an item."""
        field_value = self._get_field_value(item, condition.field)
        
        if condition.operator == FilterOperator.IS_NULL:
            return field_value is None
        elif condition.operator == FilterOperator.IS_NOT_NULL:
            return field_value is not None
        elif condition.operator == FilterOperator.IN:
            return field_value in condition.value if condition.value else False
        elif condition.operator == FilterOperator.NOT_IN:
            return field_value not in condition.value if condition.value else True
        elif condition.operator == FilterOperator.BETWEEN:
            return condition.value <= field_value <= condition.value2 if field_value else False
        elif condition.operator == FilterOperator.REGEX:
            import re
            return bool(re.match(condition.value, str(field_value))) if field_value else False
        else:
            op_fn = self._get_operator_fn(condition.operator)
            return op_fn(field_value, condition.value)
    
    def _evaluate_group(self, item: Any, group: FilterGroup) -> bool:
        """Evaluate a filter group against an item."""
        if not group.conditions and not group.groups:
            return True
        
        results = []
        
        for condition in group.conditions:
            results.append(self._evaluate_condition(item, condition))
        
        for sub_group in group.groups:
            results.append(self._evaluate_group(item, sub_group))
        
        if group.logic == FilterLogic.AND:
            return all(results) if results else True
        else:
            return any(results) if results else False
    
    def filter(
        self,
        data: list[Any],
        conditions: list[FilterCondition],
        logic: FilterLogic = FilterLogic.AND
    ) -> list[Any]:
        """
        Filter data using conditions.
        
        Args:
            data: List of items to filter
            conditions: List of filter conditions
            logic: Logic for combining conditions (AND/OR)
            
        Returns:
            Filtered list of items
        """
        start_time = datetime.now()
        self._stats["total_filters"] += 1
        self._stats["total_items_processed"] += len(data)
        
        group = FilterGroup(logic=logic, conditions=conditions)
        
        filtered = [item for item in data if self._evaluate_group(item, group)]
        
        removed_count = len(data) - len(filtered)
        self._stats["total_items_removed"] += removed_count
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        result = FilterResult(
            original_count=len(data),
            filtered_count=len(filtered),
            removed_count=removed_count,
            filter_logic=logic,
            duration_ms=duration_ms
        )
        
        self._history.append(result)
        return filtered
    
    def filter_with_group(self, data: list[Any], group: FilterGroup) -> list[Any]:
        """Filter data using a filter group."""
        start_time = datetime.now()
        self._stats["total_filters"] += 1
        self._stats["total_items_processed"] += len(data)
        
        filtered = [item for item in data if self._evaluate_group(item, group)]
        
        removed_count = len(data) - len(filtered)
        self._stats["total_items_removed"] += removed_count
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        result = FilterResult(
            original_count=len(data),
            filtered_count=len(filtered),
            removed_count=removed_count,
            filter_logic=group.logic,
            duration_ms=duration_ms
        )
        
        self._history.append(result)
        return filtered
    
    def filter_dict(
        self,
        data: list[dict],
        field: str,
        operator: FilterOperator,
        value: Any = None,
        value2: Any = None
    ) -> list[dict]:
        """Filter a list of dictionaries."""
        condition = FilterCondition(field, operator, value, value2)
        return self.filter(data, [condition])
    
    def exclude(
        self,
        data: list[Any],
        conditions: list[FilterCondition],
        logic: FilterLogic = FilterLogic.AND
    ) -> list[Any]:
        """Exclude items matching conditions (inverse of filter)."""
        filtered = self.filter(data, conditions, logic)
        return [item for item in data if item not in filtered]
    
    def filter_range(
        self,
        data: list[Any],
        field: str,
        min_value: Optional[Any] = None,
        max_value: Optional[Any] = None
    ) -> list[Any]:
        """Filter items within a range."""
        conditions = []
        
        if min_value is not None:
            conditions.append(FilterCondition(field, FilterOperator.GE, min_value))
        
        if max_value is not None:
            conditions.append(FilterCondition(field, FilterOperator.LE, max_value))
        
        if conditions:
            return self.filter(data, conditions)
        return data
    
    def filter_compound(
        self,
        data: list[Any],
        filter_groups: list[FilterGroup],
        group_logic: FilterLogic = FilterLogic.AND
    ) -> list[Any]:
        """Apply multiple filter groups."""
        result = data
        for group in filter_groups:
            result = self.filter_with_group(result, group)
        return result
    
    def get_stats(self) -> dict[str, Any]:
        """Get filtering statistics."""
        return {
            **self._stats,
            "filter_rate": (
                self._stats["total_items_removed"] / self._stats["total_items_processed"]
                if self._stats["total_items_processed"] > 0 else 0
            )
        }
    
    def get_history(self, limit: int = 100) -> list[FilterResult]:
        """Get filtering history."""
        return list(self._history)[-limit:]
