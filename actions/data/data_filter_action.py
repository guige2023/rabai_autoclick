"""Data Filter Action Module.

Provides flexible data filtering capabilities for various data types
including lists, dictionaries, and nested structures.

Example:
    >>> from actions.data.data_filter_action import DataFilterAction
    >>> action = DataFilterAction()
    >>> result = action.filter(data, conditions=[gt("age", 18), eq("active", True)])
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import threading


class FilterOperator(Enum):
    """Filter operator types."""
    EQ = "eq"           # Equal
    NE = "ne"           # Not equal
    GT = "gt"           # Greater than
    GE = "ge"           # Greater than or equal
    LT = "lt"           # Less than
    LE = "le"           # Less than or equal
    IN = "in"           # In list
    NOT_IN = "not_in"   # Not in list
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    REGEX = "regex"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    AND = "and"
    OR = "or"
    NOT = "not"


@dataclass
class FilterCondition:
    """A single filter condition.
    
    Attributes:
        field: Field path to filter on
        operator: Filter operator
        value: Value to compare against
        case_sensitive: Whether string comparison is case-sensitive
    """
    field: str
    operator: FilterOperator
    value: Any = None
    case_sensitive: bool = True


@dataclass
class FilterResult:
    """Result of a filter operation.
    
    Attributes:
        success: Whether the filter operation succeeded
        data: Filtered data
        matched_count: Number of items that matched
        total_count: Total number of items before filtering
        errors: List of errors encountered
    """
    success: bool
    data: Any = None
    matched_count: int = 0
    total_count: int = 0
    errors: List[str] = field(default_factory=list)


# Factory functions for creating filter conditions
def eq(field: str, value: Any) -> FilterCondition:
    """Create an equal filter condition."""
    return FilterCondition(field=field, operator=FilterOperator.EQ, value=value)

def ne(field: str, value: Any) -> FilterCondition:
    """Create a not-equal filter condition."""
    return FilterCondition(field=field, operator=FilterOperator.NE, value=value)

def gt(field: str, value: Union[int, float]) -> FilterCondition:
    """Create a greater-than filter condition."""
    return FilterCondition(field=field, operator=FilterOperator.GT, value=value)

def ge(field: str, value: Union[int, float]) -> FilterCondition:
    """Create a greater-than-or-equal filter condition."""
    return FilterCondition(field=field, operator=FilterOperator.GE, value=value)

def lt(field: str, value: Union[int, float]) -> FilterCondition:
    """Create a less-than filter condition."""
    return FilterCondition(field=field, operator=FilterOperator.LT, value=value)

def le(field: str, value: Union[int, float]) -> FilterCondition:
    """Create a less-than-or-equal filter condition."""
    return FilterCondition(field=field, operator=FilterOperator.LE, value=value)

def is_null(field: str) -> FilterCondition:
    """Create an is-null filter condition."""
    return FilterCondition(field=field, operator=FilterOperator.IS_NULL)

def is_not_null(field: str) -> FilterCondition:
    """Create an is-not-null filter condition."""
    return FilterCondition(field=field, operator=FilterOperator.IS_NOT_NULL)

def contains(field: str, value: str, case_sensitive: bool = True) -> FilterCondition:
    """Create a contains filter condition."""
    return FilterCondition(
        field=field, operator=FilterOperator.CONTAINS,
        value=value, case_sensitive=case_sensitive
    )

def in_list(field: str, values: List[Any]) -> FilterCondition:
    """Create an in-list filter condition."""
    return FilterCondition(field=field, operator=FilterOperator.IN, value=values)


class DataFilterAction:
    """Handles data filtering operations.
    
    Provides a flexible filtering system with support for
    complex conditions, nested fields, and various data types.
    
    Example:
        >>> action = DataFilterAction()
        >>> result = action.filter(data, conditions=[gt("age", 18)])
    """
    
    def __init__(self):
        """Initialize the data filter action."""
        self._custom_filters: Dict[str, Callable[[Any, Any], bool]] = {}
        self._lock = threading.RLock()
    
    def register_custom_filter(
        self,
        name: str,
        filter_fn: Callable[[Any, Any], bool]
    ) -> "DataFilterAction":
        """Register a custom filter function.
        
        Args:
            name: Filter name
            filter_fn: Filter function (value, compare_value) -> bool
        
        Returns:
            Self for method chaining
        """
        with self._lock:
            self._custom_filters[name] = filter_fn
            return self
    
    def filter(
        self,
        data: Any,
        conditions: Optional[List[FilterCondition]] = None,
        filter_fn: Optional[Callable[[Any], bool]] = None,
        condition_mode: str = "all"
    ) -> FilterResult:
        """Filter data based on conditions.
        
        Args:
            data: Data to filter (list, dict, or other)
            conditions: List of filter conditions
            filter_fn: Optional custom filter function
            condition_mode: "all" (AND) or "any" (OR) for combining conditions
        
        Returns:
            FilterResult with filtered data
        """
        errors: List[str] = []
        total_count = 0
        
        try:
            if isinstance(data, list):
                total_count = len(data)
                
                if filter_fn:
                    filtered = [item for item in data if filter_fn(item)]
                elif conditions:
                    filtered = self._filter_list(data, conditions, condition_mode)
                else:
                    filtered = data
                
                matched_count = len(filtered)
                
                return FilterResult(
                    success=True,
                    data=filtered,
                    matched_count=matched_count,
                    total_count=total_count
                )
            
            elif isinstance(data, dict):
                total_count = len(data)
                filtered = self._filter_dict(data, conditions or [], filter_fn, condition_mode)
                matched_count = len(filtered) if isinstance(filtered, dict) else 0
                
                return FilterResult(
                    success=True,
                    data=filtered,
                    matched_count=matched_count,
                    total_count=total_count
                )
            
            else:
                return FilterResult(
                    success=False,
                    data=data,
                    errors=["Unsupported data type for filtering"]
                )
                
        except Exception as e:
            return FilterResult(
                success=False,
                data=None,
                errors=[f"Filter error: {str(e)}"]
            )
    
    def _filter_list(
        self,
        data: List[Any],
        conditions: List[FilterCondition],
        condition_mode: str
    ) -> List[Any]:
        """Filter a list based on conditions.
        
        Args:
            data: List to filter
            conditions: Filter conditions
            condition_mode: "all" or "any"
        
        Returns:
            Filtered list
        """
        def item_matches(item: Any) -> bool:
            if condition_mode == "all":
                return all(self._check_condition(item, cond) for cond in conditions)
            else:
                return any(self._check_condition(item, cond) for cond in conditions)
        
        return [item for item in data if item_matches(item)]
    
    def _filter_dict(
        self,
        data: Dict[str, Any],
        conditions: List[FilterCondition],
        filter_fn: Optional[Callable[[Any], bool]],
        condition_mode: str
    ) -> Dict[str, Any]:
        """Filter a dictionary based on conditions.
        
        Args:
            data: Dict to filter
            conditions: Filter conditions
            filter_fn: Custom filter function
            condition_mode: "all" or "any"
        
        Returns:
            Filtered dict
        """
        if filter_fn:
            return {k: v for k, v in data.items() if filter_fn(v)}
        
        def item_matches(item: Any) -> bool:
            if condition_mode == "all":
                return all(self._check_condition(item, cond) for cond in conditions)
            else:
                return any(self._check_condition(item, cond) for cond in conditions)
        
        return {k: v for k, v in data.items() if item_matches(v)}
    
    def _check_condition(self, item: Any, condition: FilterCondition) -> bool:
        """Check if an item matches a condition.
        
        Args:
            item: Item to check
            condition: Condition to check against
        
        Returns:
            True if item matches the condition
        """
        value = self._get_field_value(item, condition.field)
        
        if condition.operator == FilterOperator.IS_NULL:
            return value is None
        
        if condition.operator == FilterOperator.IS_NOT_NULL:
            return value is not None
        
        if value is None:
            return False
        
        if condition.operator == FilterOperator.EQ:
            return self._compare_values(value, condition.value, condition.case_sensitive) == 0
        
        elif condition.operator == FilterOperator.NE:
            return self._compare_values(value, condition.value, condition.case_sensitive) != 0
        
        elif condition.operator == FilterOperator.GT:
            return self._compare_values(value, condition.value, condition.case_sensitive) > 0
        
        elif condition.operator == FilterOperator.GE:
            return self._compare_values(value, condition.value, condition.case_sensitive) >= 0
        
        elif condition.operator == FilterOperator.LT:
            return self._compare_values(value, condition.value, condition.case_sensitive) < 0
        
        elif condition.operator == FilterOperator.LE:
            return self._compare_values(value, condition.value, condition.case_sensitive) <= 0
        
        elif condition.operator == FilterOperator.IN:
            return value in condition.value
        
        elif condition.operator == FilterOperator.NOT_IN:
            return value not in condition.value
        
        elif condition.operator == FilterOperator.CONTAINS:
            if isinstance(value, str) and isinstance(condition.value, str):
                if condition.case_sensitive:
                    return condition.value in value
                else:
                    return condition.value.lower() in value.lower()
            return False
        
        elif condition.operator == FilterOperator.STARTS_WITH:
            if isinstance(value, str) and isinstance(condition.value, str):
                if condition.case_sensitive:
                    return value.startswith(condition.value)
                else:
                    return value.lower().startswith(condition.value.lower())
            return False
        
        elif condition.operator == FilterOperator.ENDS_WITH:
            if isinstance(value, str) and isinstance(condition.value, str):
                if condition.case_sensitive:
                    return value.endswith(condition.value)
                else:
                    return value.lower().endswith(condition.value.lower())
            return False
        
        return False
    
    def _get_field_value(self, item: Any, field_path: str) -> Any:
        """Get a field value from an item using dot notation.
        
        Args:
            item: Item to get value from
            field_path: Dot-separated field path
        
        Returns:
            Field value or None
        """
        if field_path == "" or field_path == ".":
            return item
        
        keys = field_path.split(".")
        current = item
        
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            elif hasattr(current, key):
                current = getattr(current, key)
            else:
                return None
        
        return current
    
    def _compare_values(
        self,
        value: Any,
        compare_to: Any,
        case_sensitive: bool
    ) -> int:
        """Compare two values.
        
        Args:
            value: First value
            compare_to: Second value
            case_sensitive: Whether to use case-sensitive comparison
        
        Returns:
            -1 if value < compare_to, 0 if equal, 1 if value > compare_to
        """
        if isinstance(value, str) and isinstance(compare_to, str):
            if not case_sensitive:
                value = value.lower()
                compare_to = compare_to.lower()
        
        if value == compare_to:
            return 0
        elif value < compare_to:
            return -1
        else:
            return 1
    
    def filter_by_query(
        self,
        data: List[Dict[str, Any]],
        query: str
    ) -> FilterResult:
        """Filter data using a simple query string.
        
        Args:
            data: List of dictionaries to filter
            query: Simple query string (e.g., "age > 18 AND active == True")
        
        Returns:
            FilterResult with filtered data
        """
        # Simple parser for query strings
        conditions: List[FilterCondition] = []
        
        # Basic AND/OR parsing
        parts = query.replace(" AND ", " && ").replace(" OR ", " || ").split("&&")
        
        for part in parts:
            part = part.strip()
            if "==" in part:
                field, val = part.split("==")
                conditions.append(eq(field.strip(), self._parse_value(val.strip())))
            elif "!=" in part:
                field, val = part.split("!=")
                conditions.append(ne(field.strip(), self._parse_value(val.strip())))
            elif ">" in part:
                field, val = part.split(">")
                conditions.append(gt(field.strip(), float(val.strip())))
            elif "<" in part:
                field, val = part.split("<")
                conditions.append(lt(field.strip(), float(val.strip())))
        
        return self.filter(data, conditions=conditions)
    
    def _parse_value(self, value_str: str) -> Any:
        """Parse a string value to appropriate type.
        
        Args:
            value_str: String value to parse
        
        Returns:
            Parsed value
        """
        value_str = value_str.strip()
        
        if value_str == "True":
            return True
        elif value_str == "False":
            return False
        elif value_str == "None":
            return None
        
        # Try to parse as number
        try:
            if "." in value_str:
                return float(value_str)
            return int(value_str)
        except ValueError:
            pass
        
        # Remove quotes if present
        if (value_str.startswith('"') and value_str.endswith('"')) or \
           (value_str.startswith("'") and value_str.endswith("'")):
            return value_str[1:-1]
        
        return value_str
