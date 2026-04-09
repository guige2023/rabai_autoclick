"""
Data Filter Action Module.

Provides predicate-based filtering with complex conditions,
field selection, and projection capabilities.
"""

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, TypeVar, Optional
import operator
import re

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
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    REGEX = "regex"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    BETWEEN = "between"


class FilterCondition:
    """Single filter condition."""

    OPS = {
        FilterOperator.EQ: operator.eq,
        FilterOperator.NE: operator.ne,
        FilterOperator.GT: operator.gt,
        FilterOperator.GTE: operator.ge,
        FilterOperator.LT: operator.lt,
        FilterOperator.LTE: operator.le,
        FilterOperator.CONTAINS: lambda a, b: b in a,
        FilterOperator.NOT_CONTAINS: lambda a, b: b not in a,
        FilterOperator.STARTS_WITH: lambda a, b: str(a).startswith(b),
        FilterOperator.ENDS_WITH: lambda a, b: str(a).endswith(b),
    }

    def __init__(
        self,
        field: str,
        op: FilterOperator,
        value: Any = None
    ):
        self.field = field
        self.op = op
        self.value = value

    def evaluate(self, record: dict) -> bool:
        """Evaluate condition against record."""
        field_value = self._get_field_value(record, self.field)

        if self.op == FilterOperator.IS_NULL:
            return field_value is None
        if self.op == FilterOperator.IS_NOT_NULL:
            return field_value is not None

        if self.op == FilterOperator.IN:
            return field_value in self.value
        if self.op == FilterOperator.NOT_IN:
            return field_value not in self.value

        if self.op == FilterOperator.BETWEEN:
            return self.value[0] <= field_value <= self.value[1]

        if self.op == FilterOperator.REGEX:
            try:
                return bool(re.search(self.value, str(field_value)))
            except re.error:
                return False

        op_func = self.OPS.get(self.op)
        if op_func is None:
            return False

        try:
            return op_func(field_value, self.value)
        except (TypeError, ValueError):
            return False

    def _get_field_value(self, record: dict, field_path: str) -> Any:
        """Get field value using dot notation."""
        parts = field_path.split(".")
        value = record
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        return value


class FilterLogic(Enum):
    """Logic combinators."""
    AND = "and"
    OR = "or"
    NOT = "not"


@dataclass
class CompositeFilter:
    """Composite filter with logic."""
    logic: FilterLogic
    conditions: list = field(default_factory=list)

    def evaluate(self, record: dict) -> bool:
        """Evaluate composite filter."""
        if self.logic == FilterLogic.AND:
            return all(c.evaluate(record) for c in self.conditions)
        elif self.logic == FilterLogic.OR:
            return any(c.evaluate(record) for c in self.conditions)
        elif self.logic == FilterLogic.NOT:
            return not all(c.evaluate(record) for c in self.conditions)
        return False


@dataclass
class FilterConfig:
    """Filter configuration."""
    conditions: list[FilterCondition] = field(default_factory=list)
    logic: FilterLogic = FilterLogic.AND
    composite_filters: list[CompositeFilter] = field(default_factory=list)


@dataclass
class ProjectionConfig:
    """Field projection configuration."""
    include: Optional[list[str]] = None
    exclude: Optional[list[str]] = None
    rename: Optional[dict[str, str]] = None
    compute: Optional[dict[str, Callable]] = None


class DataFilter:
    """Data filter with complex conditions."""

    def __init__(self, config: Optional[FilterConfig] = None):
        self.config = config or FilterConfig()
        self._projection: Optional[ProjectionConfig] = None

    def add_condition(
        self,
        field: str,
        op: FilterOperator,
        value: Any = None
    ) -> "DataFilter":
        """Add filter condition."""
        self.config.conditions.append(
            FilterCondition(field, op, value)
        )
        return self

    def add_composite_filter(
        self,
        logic: FilterLogic,
        conditions: list[FilterCondition]
    ) -> "DataFilter":
        """Add composite filter."""
        self.config.composite_filters.append(
            CompositeFilter(logic, conditions)
        )
        return self

    def set_projection(self, projection: ProjectionConfig) -> "DataFilter":
        """Set projection configuration."""
        self._projection = projection
        return self

    def _matches_filters(self, record: dict) -> bool:
        """Check if record matches all filters."""
        for condition in self.config.conditions:
            if not condition.evaluate(record):
                return False

        for composite in self.config.composite_filters:
            if not composite.evaluate(record):
                return False

        return True

    def _apply_projection(self, record: dict) -> dict:
        """Apply field projection."""
        if self._projection is None:
            return record

        result = {}

        if self._projection.include:
            for field_path in self._projection.include:
                value = self._get_nested_value(record, field_path)
                result[field_path] = value
        elif self._projection.exclude:
            result = record.copy()
            for field_path in self._projection.exclude:
                self._delete_nested_value(result, field_path)
        else:
            result = record.copy()

        if self._projection.rename:
            for old_name, new_name in self._projection.rename.items():
                if old_name in result:
                    result[new_name] = result.pop(old_name)

        if self._projection.compute:
            for field_name, func in self._projection.compute.items():
                result[field_name] = func(record)

        return result

    def _get_nested_value(self, data: dict, path: str) -> Any:
        """Get nested value."""
        parts = path.split(".")
        value = data
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        return value

    def _delete_nested_value(self, data: dict, path: str) -> None:
        """Delete nested value."""
        parts = path.split(".")
        current = data
        for part in parts[:-1]:
            if part not in current:
                return
            current = current[part]
        if parts[-1] in current:
            del current[parts[-1]]

    def filter(self, data: list[dict]) -> list[dict]:
        """Filter data."""
        results = []
        for record in data:
            if self._matches_filters(record):
                projected = self._apply_projection(record)
                results.append(projected)
        return results

    async def filter_async(self, data: list[dict], parallel: bool = True) -> list[dict]:
        """Filter data asynchronously."""
        if parallel and len(data) > 1000:
            chunk_size = 500
            chunks = [
                data[i:i + chunk_size]
                for i in range(0, len(data), chunk_size)
            ]

            async def filter_chunk(chunk: list[dict]) -> list[dict]:
                return self.filter(chunk)

            tasks = [filter_chunk(chunk) for chunk in chunks]
            chunk_results = await asyncio.gather(*tasks)

            results = []
            for chunk_result in chunk_results:
                results.extend(chunk_result)
            return results
        else:
            return self.filter(data)


class DataFilterAction:
    """
    Data filtering with complex conditions.

    Example:
        filter = DataFilterAction()
        filter.add_condition("age", FilterOperator.GTE, 18)
        filter.add_condition("status", FilterOperator.EQ, "active")
        filter.add_condition("name", FilterOperator.CONTAINS, "John")

        results = filter.filter(users)
    """

    def __init__(self, config: Optional[FilterConfig] = None):
        self._filter = DataFilter(config)

    def add_condition(
        self,
        field: str,
        op: FilterOperator,
        value: Any = None
    ) -> "DataFilterAction":
        """Add condition."""
        self._filter.add_condition(field, op, value)
        return self

    def add_composite_filter(
        self,
        logic: FilterLogic,
        conditions: list[FilterCondition]
    ) -> "DataFilterAction":
        """Add composite filter."""
        self._filter.add_composite_filter(logic, conditions)
        return self

    def set_projection(
        self,
        include: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
        rename: Optional[dict[str, str]] = None,
        compute: Optional[dict[str, Callable]] = None
    ) -> "DataFilterAction":
        """Set field projection."""
        self._filter.set_projection(
            ProjectionConfig(
                include=include,
                exclude=exclude,
                rename=rename,
                compute=compute
            )
        )
        return self

    def filter(self, data: list[dict]) -> list[dict]:
        """Filter data."""
        return self._filter.filter(data)

    async def filter_async(
        self,
        data: list[dict],
        parallel: bool = True
    ) -> list[dict]:
        """Filter async."""
        return await self._filter.filter_async(data, parallel)
