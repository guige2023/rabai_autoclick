"""
Data Filter Action - Filters and selects data based on criteria.

This module provides data filtering capabilities including
conditional filtering, complex predicates, and multi-stage filtering.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar
from enum import Enum


T = TypeVar("T")


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
    BETWEEN = "between"


@dataclass
class FilterCondition:
    """A single filter condition."""
    field: str
    operator: FilterOperator
    value: Any = None
    combine_with: str = "and"


@dataclass
class FilterSpec:
    """Specification for filtering data."""
    conditions: list[FilterCondition]
    combine_mode: str = "and"


@dataclass
class FilterResult:
    """Result of filtering operation."""
    total_records: int
    matching_records: int
    filtered_records: list[dict[str, Any]]
    excluded_count: int


class DataFilter:
    """
    Filters data based on conditions and predicates.
    
    Example:
        filter = DataFilter()
        result = filter.filter(
            records,
            FilterSpec([
                FilterCondition("age", FilterOperator.GTE, 18),
                FilterCondition("status", FilterOperator.EQ, "active"),
            ])
        )
    """
    
    def __init__(self) -> None:
        self._operator_funcs = {
            FilterOperator.EQ: lambda a, b: a == b,
            FilterOperator.NE: lambda a, b: a != b,
            FilterOperator.GT: lambda a, b: a > b,
            FilterOperator.GTE: lambda a, b: a >= b,
            FilterOperator.LT: lambda a, b: a < b,
            FilterOperator.LTE: lambda a, b: a <= b,
            FilterOperator.CONTAINS: lambda a, b: b in a if a else False,
            FilterOperator.STARTS_WITH: lambda a, b: str(a).startswith(b) if a else False,
            FilterOperator.ENDS_WITH: lambda a, b: str(a).endswith(b) if a else False,
        }
    
    def filter(
        self,
        data: list[dict[str, Any]],
        spec: FilterSpec | list[FilterCondition] | Callable[[dict[str, Any]], bool],
    ) -> FilterResult:
        """Filter data based on spec."""
        if callable(spec):
            return self._filter_with_predicate(data, spec)
        
        if isinstance(spec, list):
            spec = FilterSpec(conditions=spec)
        
        return self._filter_with_spec(data, spec)
    
    def _filter_with_predicate(
        self,
        data: list[dict[str, Any]],
        predicate: Callable[[dict[str, Any]], bool],
    ) -> FilterResult:
        """Filter using a predicate function."""
        filtered = []
        for record in data:
            try:
                if predicate(record):
                    filtered.append(record)
            except Exception:
                pass
        
        return FilterResult(
            total_records=len(data),
            matching_records=len(filtered),
            filtered_records=filtered,
            excluded_count=len(data) - len(filtered),
        )
    
    def _filter_with_spec(
        self,
        data: list[dict[str, Any]],
        spec: FilterSpec,
    ) -> FilterResult:
        """Filter using a FilterSpec."""
        filtered = []
        
        for record in data:
            if self._evaluate_conditions(record, spec.conditions, spec.combine_mode):
                filtered.append(record)
        
        return FilterResult(
            total_records=len(data),
            matching_records=len(filtered),
            filtered_records=filtered,
            excluded_count=len(data) - len(filtered),
        )
    
    def _evaluate_conditions(
        self,
        record: dict[str, Any],
        conditions: list[FilterCondition],
        combine_mode: str,
    ) -> bool:
        """Evaluate conditions against a record."""
        results = []
        
        for condition in conditions:
            result = self._evaluate_condition(record, condition)
            results.append(result)
        
        if combine_mode == "and":
            return all(results) if results else True
        else:
            return any(results) if results else True
    
    def _evaluate_condition(
        self,
        record: dict[str, Any],
        condition: FilterCondition,
    ) -> bool:
        """Evaluate a single condition."""
        value = self._get_nested(record, condition.field)
        
        if condition.operator == FilterOperator.IS_NULL:
            return value is None
        
        if condition.operator == FilterOperator.IS_NOT_NULL:
            return value is not None
        
        if condition.operator == FilterOperator.IN:
            return value in (condition.value or [])
        
        if condition.operator == FilterOperator.NOT_IN:
            return value not in (condition.value or [])
        
        if condition.operator == FilterOperator.BETWEEN:
            if isinstance(condition.value, (list, tuple)) and len(condition.value) == 2:
                return condition.value[0] <= value <= condition.value[1]
            return False
        
        if condition.operator == FilterOperator.REGEX:
            import re
            try:
                return bool(re.search(condition.value, str(value)))
            except Exception:
                return False
        
        op_func = self._operator_funcs.get(condition.operator)
        if op_func and condition.value is not None:
            return op_func(value, condition.value)
        
        return True
    
    def _get_nested(self, data: dict[str, Any], path: str) -> Any:
        """Get nested value using dot notation."""
        keys = path.split(".")
        current = data
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return None
        return current


class MultiStageFilter:
    """Applies multiple filtering stages in sequence."""
    
    def __init__(self) -> None:
        self._stages: list[tuple[str, FilterSpec | Callable]] = []
    
    def add_stage(
        self,
        name: str,
        spec: FilterSpec | Callable,
    ) -> MultiStageFilter:
        """Add a filtering stage."""
        self._stages.append((name, spec))
        return self
    
    def filter(
        self,
        data: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], dict[str, FilterResult]]:
        """Apply all filtering stages."""
        current = data
        results = {}
        
        for name, spec in self._stages:
            result = DataFilter().filter(current, spec)
            results[name] = result
            current = result.filtered_records
        
        return current, results


class DataFilterAction:
    """
    Data filter action for automation workflows.
    
    Example:
        action = DataFilterAction()
        
        action.add_stage("adults", [
            FilterCondition("age", FilterOperator.GTE, 18)
        ])
        action.add_stage("active", [
            FilterCondition("status", FilterOperator.EQ, "active")
        ])
        
        result = await action.filter_records(records)
    """
    
    def __init__(self) -> None:
        self._filter = DataFilter()
        self._multi_stage = MultiStageFilter()
    
    def add_stage(
        self,
        name: str,
        conditions: list[FilterCondition],
        combine_mode: str = "and",
    ) -> None:
        """Add a named filter stage."""
        spec = FilterSpec(conditions=conditions, combine_mode=combine_mode)
        self._multi_stage.add_stage(name, spec)
    
    async def filter_records(
        self,
        records: list[dict[str, Any]],
    ) -> FilterResult:
        """Filter records through all stages."""
        if not self._multi_stage._stages:
            return self._filter.filter(records, FilterSpec(conditions=[]))
        
        filtered, _ = self._multi_stage.filter(records)
        return FilterResult(
            total_records=len(records),
            matching_records=len(filtered),
            filtered_records=filtered,
            excluded_count=len(records) - len(filtered),
        )
    
    def filter_single(
        self,
        record: dict[str, Any],
        conditions: list[FilterCondition],
    ) -> bool:
        """Check if a single record matches conditions."""
        spec = FilterSpec(conditions=conditions)
        result = self._filter.filter([record], spec)
        return len(result.filtered_records) > 0


# Export public API
__all__ = [
    "FilterOperator",
    "FilterCondition",
    "FilterSpec",
    "FilterResult",
    "DataFilter",
    "MultiStageFilter",
    "DataFilterAction",
]
