"""
Data Filter Action Module

Advanced data filtering with boolean expressions,
field comparisons, and nested object queries.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

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
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    REGEX = "regex"
    EXISTS = "exists"
    NOT_EXISTS = "not_exists"
    AND = "and"
    OR = "or"
    NOT = "not"


@dataclass
class FilterCondition:
    """A single filter condition."""
    
    field: str
    operator: FilterOperator
    value: Any = None
    conditions: List["FilterCondition"] = field(default_factory=list)


@dataclass
class FilterConfig:
    """Configuration for filtering."""
    
    conditions: List[FilterCondition] = field(default_factory=list)
    logic: str = "and"
    case_sensitive: bool = False


class ExpressionEvaluator:
    """Evaluates filter expressions."""
    
    def __init__(self, case_sensitive: bool = False):
        self.case_sensitive = case_sensitive
    
    def evaluate(self, condition: FilterCondition, data: Dict) -> bool:
        """Evaluate a filter condition against data."""
        if condition.operator == FilterOperator.AND:
            return all(
                self.evaluate(c, data) for c in condition.conditions
            )
        
        if condition.operator == FilterOperator.OR:
            return any(
                self.evaluate(c, data) for c in condition.conditions
            )
        
        if condition.operator == FilterOperator.NOT:
            return not all(
                self.evaluate(c, data) for c in condition.conditions
            )
        
        field_value = self._get_nested_value(data, condition.field)
        
        return self._compare(field_value, condition.operator, condition.value)
    
    def _get_nested_value(self, data: Dict, path: str) -> Any:
        """Get value from nested path."""
        keys = path.split(".")
        value = data
        
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        
        return value
    
    def _compare(self, field_value: Any, operator: FilterOperator, target: Any) -> bool:
        """Compare field value with target using operator."""
        if not self.case_sensitive and isinstance(field_value, str):
            field_value = field_value.lower()
            if isinstance(target, str):
                target = target.lower()
        
        if operator == FilterOperator.EQ:
            return field_value == target
        
        if operator == FilterOperator.NE:
            return field_value != target
        
        if operator == FilterOperator.GT:
            return field_value > target
        
        if operator == FilterOperator.GTE:
            return field_value >= target
        
        if operator == FilterOperator.LT:
            return field_value < target
        
        if operator == FilterOperator.LTE:
            return field_value <= target
        
        if operator == FilterOperator.IN:
            return field_value in target if target else False
        
        if operator == FilterOperator.NOT_IN:
            return field_value not in target if target else True
        
        if operator == FilterOperator.CONTAINS:
            return str(target) in str(field_value) if field_value else False
        
        if operator == FilterOperator.NOT_CONTAINS:
            return str(target) not in str(field_value) if field_value else True
        
        if operator == FilterOperator.STARTS_WITH:
            return str(field_value).startswith(str(target)) if field_value else False
        
        if operator == FilterOperator.ENDS_WITH:
            return str(field_value).endswith(str(target)) if field_value else False
        
        if operator == FilterOperator.REGEX:
            try:
                return bool(re.search(str(target), str(field_value)))
            except Exception:
                return False
        
        if operator == FilterOperator.EXISTS:
            return field_value is not None
        
        if operator == FilterOperator.NOT_EXISTS:
            return field_value is None
        
        return False


class DataFilterAction:
    """
    Main data filter action handler.
    
    Provides advanced filtering with boolean expressions,
    field comparisons, and nested object queries.
    """
    
    def __init__(self, config: Optional[FilterConfig] = None):
        self.config = config or FilterConfig()
        self._evaluator = ExpressionEvaluator(not self.config.case_sensitive)
    
    def add_condition(
        self,
        field: str,
        operator: FilterOperator,
        value: Any = None
    ) -> None:
        """Add a filter condition."""
        condition = FilterCondition(
            field=field,
            operator=operator,
            value=value
        )
        self.config.conditions.append(condition)
    
    def where(
        self,
        field: str,
        operator: Union[FilterOperator, str],
        value: Any = None
    ) -> "DataFilterAction":
        """Add a where condition (fluent interface)."""
        if isinstance(operator, str):
            operator = FilterOperator(operator)
        
        self.add_condition(field, operator, value)
        return self
    
    def and_where(
        self,
        field: str,
        operator: Union[FilterOperator, str],
        value: Any = None
    ) -> "DataFilterAction":
        """Add an AND condition (fluent interface)."""
        return self.where(field, operator, value)
    
    def or_where(
        self,
        field: str,
        operator: Union[FilterOperator, str],
        value: Any = None
    ) -> "DataFilterAction":
        """Add an OR condition (fluent interface)."""
        if isinstance(operator, str):
            operator = FilterOperator(operator)
        
        condition = FilterCondition(
            field=field,
            operator=FilterOperator.OR,
            value=value
        )
        
        if self.config.conditions and self.config.conditions[-1].operator != FilterOperator.OR:
            last_condition = self.config.conditions.pop()
            or_condition = FilterCondition(
                field="",
                operator=FilterOperator.OR,
                conditions=[last_condition, condition]
            )
            self.config.conditions.append(or_condition)
        else:
            self.config.conditions.append(condition)
        
        return self
    
    def filter(self, records: List[Dict]) -> List[Dict]:
        """Filter records based on conditions."""
        if not self.config.conditions:
            return records
        
        results = []
        
        for record in records:
            if self._evaluate_record(record):
                results.append(record)
        
        return results
    
    def _evaluate_record(self, record: Dict) -> bool:
        """Evaluate all conditions against a record."""
        if self.config.logic == "and":
            return all(
                self._evaluator.evaluate(c, record)
                for c in self.config.conditions
            )
        else:
            return any(
                self._evaluator.evaluate(c, record)
                for c in self.config.conditions
            )
    
    def exclude(self, records: List[Dict]) -> List[Dict]:
        """Exclude records matching filter."""
        filtered = self.filter(records)
        filtered_ids = set(id(r) for r in filtered)
        return [r for r in records if id(r) not in filtered_ids]
    
    def partition(self, records: List[Dict]) -> tuple[List[Dict], List[Dict]]:
        """Partition records into matching and non-matching."""
        filtered = self.filter(records)
        filtered_ids = set(id(r) for r in filtered)
        non_matching = [r for r in records if id(r) not in filtered_ids]
        return filtered, non_matching
    
    def transform(self, records: List[Dict]) -> List[Dict]:
        """Filter and return only selected fields."""
        filtered = self.filter(records)
        
        if not self.config.conditions:
            return records
        
        return filtered
    
    @staticmethod
    def parse_query(query: str) -> FilterConfig:
        """Parse a query string into FilterConfig."""
        import json
        
        try:
            parsed = json.loads(query)
            conditions = []
            
            for key, value in parsed.items():
                if "__" in key:
                    field, op_str = key.rsplit("__", 1)
                    operator = FilterOperator(op_str)
                else:
                    field = key
                    operator = FilterOperator.EQ
                
                conditions.append(FilterCondition(
                    field=field,
                    operator=operator,
                    value=value
                ))
            
            return FilterConfig(conditions=conditions)
        
        except Exception:
            return FilterConfig()
    
    def clear(self) -> None:
        """Clear all conditions."""
        self.config.conditions.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get filter statistics."""
        return {
            "conditions_count": len(self.config.conditions),
            "logic": self.config.logic,
            "case_sensitive": self.config.case_sensitive
        }
