"""Data filter action for filtering and processing data.

This module provides data filtering with support for
multiple conditions, operators, and data type conversion.

Example:
    >>> action = DataFilterAction()
    >>> result = action.execute(data=[{"a": 1}, {"a": 2}], filter={"a": ">1"})
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Union


@dataclass
class FilterCondition:
    """Represents a filter condition."""
    field: str
    operator: str
    value: Any


@dataclass
class FilterRule:
    """Represents a filter rule."""
    conditions: list[FilterCondition]
    logic: str = "AND"  # AND or OR


class DataFilterAction:
    """Data filtering and processing action.

    Provides data filtering with multiple conditions,
    operators, and type-aware comparisons.

    Example:
        >>> action = DataFilterAction()
        >>> result = action.execute(
        ...     data=[{"name": "Alice", "age": 30}],
        ...     filter_rules=[
        ...         {"field": "age", "operator": ">=", "value": 25}
        ...     ]
        ... )
    """

    def __init__(self) -> None:
        """Initialize data filter."""
        self._last_filtered: Optional[list[dict]] = None

    def execute(
        self,
        data: Any,
        filter_rules: Optional[list[dict]] = None,
        filter_str: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "asc",
        limit: Optional[int] = None,
        distinct: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute filter operation.

        Args:
            data: Data to filter (list of dicts or list of values).
            filter_rules: List of filter rule dicts.
            filter_str: String filter expression (e.g., "age > 25").
            sort_by: Field to sort by.
            sort_order: Sort order ('asc' or 'desc').
            limit: Maximum results to return.
            distinct: Whether to return distinct values.
            **kwargs: Additional parameters.

        Returns:
            Filter result dictionary.

        Raises:
            ValueError: If data is invalid.
        """
        if not isinstance(data, list):
            raise ValueError("data must be a list")

        result: dict[str, Any] = {"success": True, "input_count": len(data)}

        # Parse filter rules
        rules = self._parse_filter_rules(filter_rules or [])

        # Parse string filter
        if filter_str:
            rules.extend(self._parse_filter_string(filter_str))

        # Apply filters
        filtered = data
        if rules:
            filtered = self._apply_rules(data, rules)

        # Sort
        if sort_by:
            filtered = self._sort_data(filtered, sort_by, sort_order)

        # Distinct
        if distinct:
            filtered = self._distinct(filtered)

        # Limit
        if limit:
            filtered = filtered[:limit]

        self._last_filtered = filtered

        result["output_count"] = len(filtered)
        result["filtered"] = filtered
        result["removed"] = len(data) - len(filtered)

        return result

    def _parse_filter_rules(self, filter_rules: list[dict]) -> list[FilterRule]:
        """Parse filter rules from dicts.

        Args:
            filter_rules: List of rule dictionaries.

        Returns:
            List of FilterRule objects.
        """
        rules = []
        for rule_dict in filter_rules:
            conditions = []
            for cond_dict in rule_dict.get("conditions", []):
                conditions.append(FilterCondition(
                    field=cond_dict["field"],
                    operator=cond_dict["operator"],
                    value=cond_dict["value"],
                ))
            rule = FilterRule(
                conditions=conditions,
                logic=rule_dict.get("logic", "AND"),
            )
            rules.append(rule)
        return rules

    def _parse_filter_string(self, filter_str: str) -> list[FilterRule]:
        """Parse filter string expression.

        Args:
            filter_str: Filter expression like "age > 25 AND name = 'Alice'".

        Returns:
            List of FilterRule objects.
        """
        rules = []
        # Split by AND/OR
        parts = re.split(r"\s+(AND|OR)\s+", filter_str, flags=re.IGNORECASE)

        conditions = []
        for part in parts:
            if part.upper() in ("AND", "OR"):
                continue

            # Parse comparison
            match = re.match(r"(\w+)\s*(>=|<=|!=|>|<|=)\s*(.+)", part.strip())
            if match:
                field_name, operator, value = match.groups()
                value = self._parse_value(value.strip())
                conditions.append(FilterCondition(
                    field=field_name,
                    operator=operator,
                    value=value,
                ))

        if conditions:
            rules.append(FilterRule(conditions=conditions))

        return rules

    def _parse_value(self, value_str: str) -> Any:
        """Parse value string to appropriate type.

        Args:
            value_str: Value string.

        Returns:
            Parsed value.
        """
        # String
        if value_str.startswith(("'", '"')) and value_str.endswith(("'", '"')):
            return value_str[1:-1]

        # Number
        try:
            if "." in value_str:
                return float(value_str)
            return int(value_str)
        except ValueError:
            pass

        # Boolean
        if value_str.lower() in ("true", "false"):
            return value_str.lower() == "true"

        # None/null
        if value_str.lower() in ("null", "none"):
            return None

        return value_str

    def _apply_rules(self, data: list, rules: list[FilterRule]) -> list:
        """Apply filter rules to data.

        Args:
            data: Data to filter.
            rules: Filter rules.

        Returns:
            Filtered data.
        """
        result = data

        for rule in rules:
            filtered = []
            for item in result:
                if self._check_rule(item, rule):
                    filtered.append(item)
            result = filtered

        return result

    def _check_rule(self, item: dict, rule: FilterRule) -> bool:
        """Check if item matches filter rule.

        Args:
            item: Data item.
            rule: Filter rule.

        Returns:
            True if item matches.
        """
        if rule.logic == "AND":
            return all(self._check_condition(item, c) for c in rule.conditions)
        else:  # OR
            return any(self._check_condition(item, c) for c in rule.conditions)

    def _check_condition(self, item: dict, condition: FilterCondition) -> bool:
        """Check if item matches condition.

        Args:
            item: Data item.
            condition: Filter condition.

        Returns:
            True if matches.
        """
        value = item.get(condition.field)

        op = condition.operator
        cond_value = condition.value

        if op == "=":
            return value == cond_value
        elif op == "!=":
            return value != cond_value
        elif op == ">":
            return value is not None and value > cond_value
        elif op == ">=":
            return value is not None and value >= cond_value
        elif op == "<":
            return value is not None and value < cond_value
        elif op == "<=":
            return value is not None and value <= cond_value
        elif op == "in":
            return value in cond_value if isinstance(cond_value, list) else value == cond_value
        elif op == "contains":
            return cond_value in str(value) if value is not None else False
        elif op == "startswith":
            return str(value).startswith(str(cond_value)) if value is not None else False
        elif op == "endswith":
            return str(value).endswith(str(cond_value)) if value is not None else False
        elif op == "regex":
            try:
                return bool(re.search(cond_value, str(value)))
            except re.error:
                return False
        elif op == "is_null":
            return value is None
        elif op == "is_not_null":
            return value is not None

        return False

    def _sort_data(self, data: list, key: str, order: str) -> list:
        """Sort data by key.

        Args:
            data: Data to sort.
            key: Sort key.
            order: Sort order.

        Returns:
            Sorted data.
        """
        reverse = order.lower() == "desc"
        return sorted(data, key=lambda x: x.get(key, ""), reverse=reverse)

    def _distinct(self, data: list) -> list:
        """Get distinct items.

        Args:
            data: Data to deduplicate.

        Returns:
            Deduplicated data.
        """
        seen = set()
        result = []
        for item in data:
            key = tuple(sorted(item.items())) if isinstance(item, dict) else item
            if key not in seen:
                seen.add(key)
                result.append(item)
        return result

    def apply_function(
        self,
        func: Callable[[dict], Any],
    ) -> list[Any]:
        """Apply function to last filtered data.

        Args:
            func: Function to apply.

        Returns:
            List of results.
        """
        if not self._last_filtered:
            return []
        return [func(item) for item in self._last_filtered]
